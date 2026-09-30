-module(eysql_transaction_tests).

-include_lib("eunit/include/eunit.hrl").

-import(eysql_test_util, [config/1, three/0, wait_until/2]).

tx_test_() ->
    {foreach,
     fun() -> eysql_fake_driver:start() end,
     fun(_) -> eysql_fake_driver:stop() end,
     [{"commits", fun commits/0},
      {"rolls back on request", fun rolls_back/0},
      {"retries a serialization failure at COMMIT", fun retries_commit_conflict/0},
      {"gives up after the attempts", fun retries_exhausted/0},
      {"max_backoff caps the wait between attempts", fun backoff_capped/0},
      {"max_backoff 0 retries at once; a negative one is refused", fun backoff_zero/0},
      {"retries a retryable error from the function", fun retries_fun_error/0},
      {"does not retry other errors", fun no_retry_other/0},
      {"a lost connection during COMMIT is reported, not retried", fun commit_unknown/0},
      {"a lost connection at BEGIN is retried on another connection", fun begin_lost/0},
      {"COMMIT that rolled back is reported", fun aborted/0},
      {"an exception discards the connection and propagates", fun exception/0},
      {"without a driver option, the pool's driver runs the transaction", fun pool_driver/0},
      {"a retry with no connection returns the error that made it retry", fun retry_without_connection/0},
      {"a first checkout that fails returns the checkout's error", fun first_checkout_fails/0},
      {"a connection still in a transaction after COMMIT or ROLLBACK is discarded",
       fun transaction_left_open/0},
      {"with_connection asks the pool's driver for the status", fun with_connection_driver/0},
      {"with_connection returns the result when the pool stops meanwhile", fun pool_stops_in_with_connection/0},
      {"a transaction whose pool stops meanwhile returns its result", fun pool_stops_in_transaction/0},
      {"a retry after the pool stopped returns the error that made it retry", fun pool_stops_before_retry/0}
     ]}.

%% eysql_sock_driver's connections are epgsql processes, whose transaction
%% status eysql_conn can read.
with_connection_test_() ->
    [{"with_connection returns the result and an idle connection", fun returns_idle/0},
     {"a connection left in a transaction is discarded", fun discards_open/0},
     {"a connection left in a failed transaction is discarded", fun discards_failed/0},
     {"a connection whose status cannot be read is discarded", fun discards_unknown/0},
     {"an exception in with_connection discards the connection", fun with_connection_raises/0},
     {"a query whose pool stops under it returns connection_lost", fun cluster_exit_in_query/0}
    ].

-define(OPTS, opts()).

opts() -> #{driver => eysql_fake_driver, attempts => 3}.

pool() -> pool(2).

pool(Size) ->
    eysql_fake_driver:set_servers(three()),
    {ok, Pool} = eysql_pool:start_link(config(#{pool_size => Size})),
    wait_until(fun() -> maps:get(idle, eysql_pool:stats(Pool)) =:= Size end, full),
    Pool.

stop(Pool) ->
    unlink(Pool),
    eysql_pool:stop(Pool).

e(Code) -> {error, eysql_fake_driver:pg_error(Code)}.

counter() -> counters:new(1, []).
bump(C) -> counters:add(C, 1, 1).
count(C) -> counters:get(C, 1).

commits() ->
    Pool = pool(),
    ?assertEqual({ok, 42}, eysql:transaction(Pool, fun(_) -> {ok, 42} end, ?OPTS)),
    stop(Pool).

rolls_back() ->
    Pool = pool(),
    ?assertEqual({error, {rollback, nope}}, eysql:transaction(Pool, fun(_) -> {rollback, nope} end, ?OPTS)),
    stop(Pool).

retries_commit_conflict() ->
    Pool = pool(),
    eysql_fake_driver:script("COMMIT", [e(<<"40001">>), {ok, [], []}]),
    C = counter(),
    ?assertEqual({ok, done}, eysql:transaction(Pool, fun(_) -> bump(C), {ok, done} end, ?OPTS)),
    ?assertEqual(2, count(C)),
    stop(Pool).

retries_exhausted() ->
    Pool = pool(),
    eysql_fake_driver:script("COMMIT", [e(<<"40001">>)]),
    C = counter(),
    {error, Error} = eysql:transaction(Pool, fun(_) -> bump(C), {ok, x} end, maps:put(attempts, 2, ?OPTS)),
    ?assertEqual(<<"40001">>, eysql_error:code(Error)),
    ?assertEqual(2, count(C)),
    stop(Pool).

%% Uncapped, the wait before attempt 30 alone would be days.
backoff_capped() ->
    Pool = pool(),
    eysql_fake_driver:script("COMMIT", [e(<<"40001">>)]),
    C = counter(),
    Start = erlang:monotonic_time(millisecond),
    {error, Error} = eysql:transaction(Pool, fun(_) -> bump(C), {ok, x} end,
                                      maps:merge(?OPTS, #{attempts => 30, max_backoff => 5})),
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    ?assertEqual(<<"40001">>, eysql_error:code(Error)),
    ?assertEqual(30, count(C)),
    ?assert(Elapsed < 3000),
    stop(Pool).

backoff_zero() ->
    Pool = pool(),
    eysql_fake_driver:script("COMMIT", [e(<<"40001">>)]),
    C = counter(),
    {error, Error} = eysql:transaction(Pool, fun(_) -> bump(C), {ok, x} end,
                                      maps:merge(?OPTS, #{attempts => 5, max_backoff => 0})),
    ?assertEqual(<<"40001">>, eysql_error:code(Error)),
    ?assertEqual(5, count(C)),
    ?assertError({invalid_option, max_backoff, -1},
                 eysql:transaction(Pool, fun(_) -> bump(C), {ok, x} end, maps:put(max_backoff, -1, ?OPTS))),
    ?assertEqual(5, count(C)),
    stop(Pool).

retries_fun_error() ->
    Pool = pool(),
    C = counter(),
    Fun = fun(_) ->
                  bump(C),
                  case count(C) of
                      1 -> e(<<"40P01">>);
                      _ -> {ok, second}
                  end
          end,
    ?assertEqual({ok, second}, eysql:transaction(Pool, Fun, ?OPTS)),
    stop(Pool).

no_retry_other() ->
    Pool = pool(),
    C = counter(),
    {error, Error} = eysql:transaction(Pool, fun(_) -> bump(C), e(<<"23505">>) end, ?OPTS),
    ?assertEqual(<<"23505">>, eysql_error:code(Error)),
    ?assertEqual(1, count(C)),
    stop(Pool).

commit_unknown() ->
    Pool = pool(),
    eysql_fake_driver:script("COMMIT", [{error, {connection_lost, closed}}]),
    C = counter(),
    ?assertEqual({error, commit_outcome_unknown},
                 eysql:transaction(Pool, fun(_) -> bump(C), {ok, x} end, ?OPTS)),
    ?assertEqual(1, count(C)),
    stop(Pool).

begin_lost() ->
    Pool = pool(),
    eysql_fake_driver:script("BEGIN", [{error, {connection_lost, closed}}, {ok, [], []}]),
    ?assertEqual({ok, fine}, eysql:transaction(Pool, fun(_) -> {ok, fine} end, ?OPTS)),
    stop(Pool).

aborted() ->
    Pool = pool(),
    eysql_fake_driver:set_committed(false),
    ?assertEqual({error, transaction_aborted}, eysql:transaction(Pool, fun(_) -> {ok, x} end, ?OPTS)),
    stop(Pool).

exception() ->
    Pool = pool(),
    ?assertError(boom, eysql:transaction(Pool, fun(_) -> error(boom) end, ?OPTS)),
    wait_until(fun() -> maps:get(leased, eysql_pool:stats(Pool)) =:= 0 end, released),
    wait_until(fun() -> maps:get(idle, eysql_pool:stats(Pool)) =:= 2 end, refilled),
    stop(Pool).

%% No `driver' in the options. Were eysql_conn used, BEGIN on a fake
%% connection would never answer; the fake driver's scripted COMMIT and its
%% committed/1 show it is the one in use.
pool_driver() ->
    Pool = pool(),
    ?assertEqual(eysql_fake_driver, eysql_pool:driver(Pool)),
    eysql_fake_driver:script("COMMIT", [e(<<"40001">>), {ok, [], []}]),
    C = counter(),
    ?assertEqual({ok, done}, eysql:transaction(Pool, fun(_) -> bump(C), {ok, done} end, #{})),
    ?assertEqual(2, count(C)),
    eysql_fake_driver:set_committed(false),
    ?assertEqual({error, transaction_aborted}, eysql:transaction(Pool, fun(_) -> {ok, x} end)),
    stop(Pool).

%% The pool's only connection goes, when the first attempt returns it, to a
%% process that queued for it during that attempt. The retry's checkout
%% times out; the result is the conflict, which says the transaction ran.
retry_without_connection() ->
    Pool = pool(1),
    eysql_fake_driver:script("COMMIT", [e(<<"40001">>)]),
    Self = self(),
    C = counter(),
    Fun = fun(_) ->
                  bump(C),
                  spawn(fun() -> hold(Pool, Self, 5000) end),
                  wait_until(fun() -> maps:get(waiting, eysql_pool:stats(Pool)) =:= 1 end, queued),
                  {ok, x}
          end,
    {error, Error} = eysql:transaction(Pool, Fun, maps:merge(?OPTS, #{checkout_timeout => 100,
                                                                     max_backoff => 0})),
    ?assertEqual(<<"40001">>, eysql_error:code(Error)),
    ?assertEqual(1, count(C)),
    release_holder(),
    stop(Pool).

first_checkout_fails() ->
    Pool = pool(1),
    Self = self(),
    spawn(fun() -> hold(Pool, Self, 1000) end),
    Holder = receive {holding, H} -> H after 2000 -> error(no_checkout) end,
    C = counter(),
    ?assertEqual({error, checkout_timeout},
                 eysql:transaction(Pool, fun(_) -> bump(C), {ok, x} end, maps:put(checkout_timeout, 100, ?OPTS))),
    ?assertEqual(0, count(C)),
    Holder ! release,
    stop(Pool).

%% The driver says the connection is still in a transaction after COMMIT
%% or ROLLBACK, as when a ROLLBACK could not be sent: the next holder must not
%% get it.
transaction_left_open() ->
    Pool = pool(),
    Self = self(),
    Fun = fun(Result) -> fun(C) -> Self ! {conn, C}, Result end end,
    ?assertEqual({ok, x}, eysql:transaction(Pool, Fun({ok, x}), ?OPTS)),
    Kept = receive {conn, C0} -> C0 after 0 -> error(no_conn) end,
    ?assertEqual(0, maps:get(leased, eysql_pool:stats(Pool))),
    ?assert(is_process_alive(Kept)),
    eysql_fake_driver:set_transaction_status(in_transaction),
    ?assertEqual({ok, x}, eysql:transaction(Pool, Fun({ok, x}), ?OPTS)),
    Committed = receive {conn, C1} -> C1 after 0 -> error(no_conn) end,
    ?assertEqual({error, {rollback, r}}, eysql:transaction(Pool, Fun({rollback, r}), ?OPTS)),
    RolledBack = receive {conn, C2} -> C2 after 0 -> error(no_conn) end,
    wait_until(fun() -> not is_process_alive(Committed) andalso not is_process_alive(RolledBack) end,
               discarded),
    stop(Pool).

%% A driver other than eysql_conn answers for its own connections, with no
%% wait: the fake driver's connections are not epgsql processes.
with_connection_driver() ->
    Pool = pool(),
    {Micros, Conn} = timer:tc(fun() -> eysql:with_connection(Pool, fun(C) -> C end) end),
    ?assert(Micros < 500000),
    ?assertEqual(0, maps:get(leased, eysql_pool:stats(Pool))),
    ?assert(is_process_alive(Conn)),
    eysql_fake_driver:set_transaction_status(unknown),
    Unknown = eysql:with_connection(Pool, fun(C) -> C end),
    wait_until(fun() -> not is_process_alive(Unknown) end, discarded),
    stop(Pool).

%% The pool erases the driver's persistent_term entry as it stops, so
%% looking the driver up after Fun would ask the stopped pool and exit. Once
%% for a pool known by its pid and once for one known by its name.
pool_stops_in_with_connection() ->
    Pool = pool(),
    ?assertEqual(done, eysql:with_connection(Pool, fun(_) -> stop(Pool), done end)),
    {ok, Named} = eysql_pool:start_link({local, eysql_tx_pool}, config(#{pool_size => 1})),
    wait_until(fun() -> maps:get(idle, eysql_pool:stats(Named)) =:= 1 end, full),
    ?assertEqual(done, eysql:with_connection(eysql_tx_pool, fun(_) -> stop(Named), done end)),
    ?assertEqual(undefined, whereis(eysql_tx_pool)).

%% The pool's driver is found before the first checkout, and nothing after
%% Fun asks the pool anything.
pool_stops_in_transaction() ->
    Pool = pool(),
    ?assertEqual({error, {rollback, r}},
                 eysql:transaction(Pool, fun(_) -> stop(Pool), {rollback, r} end, #{})).

%% The pool stops, its connections close, and the statement in flight fails
%% with connection_lost, which is retried. The retry's checkout finds no
%% pool: the result is the error that made it retry.
pool_stops_before_retry() ->
    Pool = pool(),
    C = counter(),
    Lost = {error, {connection_lost, closed}},
    ?assertEqual(Lost, eysql:transaction(Pool, fun(_) -> bump(C), stop(Pool), Lost end,
                                         #{max_backoff => 0})),
    ?assertEqual(1, count(C)).

%% Check a connection out and keep it until told to give it back.
hold(Pool, Parent, Timeout) ->
    {ok, Conn} = eysql_pool:checkout(Pool, Timeout),
    Parent ! {holding, self()},
    receive release -> eysql_pool:checkin(Pool, Conn) end.

release_holder() ->
    receive {holding, Holder} -> Holder ! release
    after 2000 -> error(no_holder)
    end.

%%%=============================================================================
%%% with_connection
%%%=============================================================================

sock_pool() ->
    {ok, Pool} = eysql_pool:start_link(config(#{driver => eysql_sock_driver, pool_size => 2,
                                                load_balance => false})),
    wait_until(fun() -> maps:get(idle, eysql_pool:stats(Pool)) =:= 2 end, full),
    Pool.

%% Run with_connection, leaving the connection as `Leave' makes it. Returns
%% the connection once the pool has taken it back.
leave(Pool, Leave) ->
    Conn = eysql:with_connection(Pool, fun(C) -> ok = Leave(C), C end),
    %% A call after the checkin's cast: the pool has handled the checkin.
    ?assertEqual(0, maps:get(leased, eysql_pool:stats(Pool))),
    Conn.

returns_idle() ->
    Pool = sock_pool(),
    ?assertEqual(result, eysql:with_connection(Pool, fun(C) -> true = is_pid(C), result end)),
    Conn = leave(Pool, fun(_) -> ok end),
    ?assert(is_process_alive(Conn)),
    stop(Pool).

discards_open() ->
    Pool = sock_pool(),
    Conn = leave(Pool, fun(C) -> eysql_sock_driver:set_txstatus(C, $T) end),
    ?assertNot(is_process_alive(Conn)),
    wait_until(fun() -> maps:get(idle, eysql_pool:stats(Pool)) =:= 2 end, replaced),
    stop(Pool).

discards_failed() ->
    Pool = sock_pool(),
    Conn = leave(Pool, fun(C) -> eysql_sock_driver:set_txstatus(C, $E) end),
    ?assertNot(is_process_alive(Conn)),
    stop(Pool).

discards_unknown() ->
    Pool = sock_pool(),
    Sync = leave(Pool, fun eysql_sock_driver:set_sync_required/1),
    ?assertNot(is_process_alive(Sync)),
    Copy = leave(Pool, fun eysql_sock_driver:set_copy_mode/1),
    ?assertNot(is_process_alive(Copy)),
    stop(Pool).

with_connection_raises() ->
    Pool = sock_pool(),
    Self = self(),
    ?assertError(boom, eysql:with_connection(Pool, fun(C) -> Self ! {conn, C}, error(boom) end)),
    Conn = receive {conn, C} -> C after 0 -> error(no_conn) end,
    ?assertEqual(0, maps:get(leased, eysql_pool:stats(Pool))),
    ?assertNot(is_process_alive(Conn)),
    wait_until(fun() -> maps:get(idle, eysql_pool:stats(Pool)) =:= 2 end, replaced),
    stop(Pool).

%% The pool's cluster process exits, so the pool stops, closing its
%% connections, while a query is under way. The query fails as the
%% connection closes, and the caller gets that error rather than an exit
%% from asking the stopped pool for its driver.
cluster_exit_in_query() ->
    Pool = sock_pool(),
    unlink(Pool),
    Cluster = eysql_pool:cluster(Pool),
    Monitor = erlang:monitor(process, Pool),
    Query = fun(C) ->
                    exit(Cluster, kill),
                    receive {'DOWN', Monitor, process, Pool, _} -> ok end,
                    eysql_conn:equery(C, "SELECT 1", [])
            end,
    %% The pool's crash report is expected; keep it out of the output. The
    %% pool logs it before it exits, so before the 'DOWN' Query waits for.
    #{level := Level} = logger:get_primary_config(),
    ok = logger:set_primary_config(level, none),
    try
        ?assertMatch({error, {connection_lost, _}}, eysql:with_connection(Pool, Query))
    after
        logger:set_primary_config(level, Level)
    end.
