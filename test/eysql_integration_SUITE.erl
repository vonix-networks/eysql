%% Against real servers: PostgreSQL and a three-zone YugabyteDB cluster.
%% Skipped unless EYSQL_IT is set; integration/run.sh starts the servers and
%% runs it.
-module(eysql_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1, init_per_group/2,
         end_per_group/2]).

-export([pg_query/1,
         pg_transaction/1,
         pg_static/1,
         pg_read_write/1,
         yb_discovery/1,
         yb_spread/1,
         yb_topology/1,
         yb_serialization_retries/1,
         yb_failover/1,
         idle_connection_kept/1,
         open_transaction_discarded/1,
         failed_transaction_discarded/1,
         transaction_connection_kept/1,
         squery_begin_discarded/1,
         pool_stops_under_query/1,
         socket_timeout_cuts_query/1,
         after_connect_prepares/1,
         after_connect_failure_keeps_servers/1
        ]).

%% The after_connect hook of after_connect_prepares/1.
-export([prepare_session/2]).

-define(ZONES, [<<"us-east1-b">>, <<"us-east1-c">>, <<"us-east1-d">>]).

all() ->
    [{group, postgres}, {group, yugabyte}].

groups() ->
    [{postgres, [sequence], [pg_query, pg_transaction, pg_static, pg_read_write | both()]},
     {yugabyte, [sequence], [yb_discovery, yb_spread, yb_topology | both()]
                            ++ [yb_serialization_retries, yb_failover]}].

%% Cases that run on both engines, which init_per_group/2 names.
both() ->
    [idle_connection_kept, open_transaction_discarded, failed_transaction_discarded,
     transaction_connection_kept, squery_begin_discarded, pool_stops_under_query,
     socket_timeout_cuts_query, after_connect_prepares, after_connect_failure_keeps_servers].

init_per_group(Engine, Config) ->
    [{engine, Engine} | Config].

end_per_group(_Engine, _Config) ->
    ok.

init_per_suite(Config) ->
    case os:getenv("EYSQL_IT") of
        false ->
            {skip, "set EYSQL_IT=1; integration/run.sh does"};
        _ ->
            {ok, _} = application:ensure_all_started(eysql),
            Config
    end.

end_per_suite(_Config) ->
    ok.

%%%=============================================================================
%%% PostgreSQL
%%%=============================================================================

pg(Overrides) ->
    maps:merge(#{hosts => ["postgres"], port => 5432, username => "eysql",
                 password => "eysql", database => "eysql", pool_size => 3},
               Overrides).

pg_query(_Config) ->
    {ok, Pool} = eysql:start_link(pg(#{})),
    ?assertMatch({ok, _, [{42}]}, eysql:equery(Pool, "SELECT $1::int + 1", [41])),
    stop(Pool).

pg_transaction(_Config) ->
    {ok, Pool} = eysql:start_link(pg(#{})),
    {ok, _, _} = eysql:squery(Pool, "CREATE TABLE IF NOT EXISTS eysql_t (id int PRIMARY KEY)"),
    {ok, _, _} = eysql:squery(Pool, "TRUNCATE eysql_t"),
    Insert = fun(Id) -> fun(C) -> ok_or_error(epgsql:equery(C, "INSERT INTO eysql_t VALUES ($1)", [Id]), Id) end end,
    ?assertEqual({ok, 1}, eysql:transaction(Pool, Insert(1))),
    ?assertEqual({error, {rollback, no}},
                 eysql:transaction(Pool, fun(C) -> {ok, 1} = epgsql:equery(C, "INSERT INTO eysql_t VALUES (2)", []),
                                                  {rollback, no} end)),
    {error, Duplicate} = eysql:transaction(Pool, Insert(1)),
    ?assertEqual(<<"23505">>, eysql_error:code(Duplicate)),
    ?assertMatch({ok, _, [{1}]}, eysql:equery(Pool, "SELECT count(*)::int FROM eysql_t", [])),
    stop(Pool).

%% With load balancing asked for, the first discovery finds no yb_servers()
%% and the pool stays with the configured host.
pg_static(_Config) ->
    {ok, Pool} = eysql:start_link(pg(#{load_balance => true})),
    wait(fun() -> maps:get(mode, eysql:cluster_info(Pool)) =:= static end, static),
    ?assertMatch(#{servers := [#{host := <<"postgres">>, port := 5432}]}, eysql:cluster_info(Pool)),
    stop(Pool).

pg_read_write(_Config) ->
    {ok, Pool} = eysql:start_link(pg(#{target_session_attrs => read_write, load_balance => false})),
    ?assertMatch({ok, _, [{1}]}, eysql:equery(Pool, "SELECT 1", [])),
    stop(Pool).

ok_or_error({ok, _}, Value) -> {ok, Value};
ok_or_error({error, _} = Error, _) -> Error.

%%%=============================================================================
%%% YugabyteDB
%%%=============================================================================

yb(Overrides) ->
    maps:merge(#{hosts => ["yb1"], port => 5433, username => "yugabyte", password => "",
                 database => "yugabyte", pool_size => 6, load_balance => true,
                 failed_host_reconnect_delay_secs => 2, failed_host_max_delay_secs => 4,
                 yb_servers_refresh_interval => 5,
                 rebalance_interval => 1000, rebalance_batch => 2},
               Overrides).

yb_discovery(_Config) ->
    {ok, Pool} = eysql:start_link(yb(#{})),
    wait(fun() -> maps:get(discovered, eysql:cluster_info(Pool)) end, discovered),
    Servers = maps:get(servers, eysql:cluster_info(Pool)),
    ct:pal("yb_servers(): ~p", [Servers]),
    ?assertEqual([<<"yb1">>, <<"yb2">>, <<"yb3">>], lists:sort([H || #{host := H} <- Servers])),
    ?assertEqual(?ZONES, lists:sort([Z || #{zone := Z} <- Servers])),
    stop(Pool).

yb_spread(_Config) ->
    {ok, Pool} = eysql:start_link(yb(#{})),
    wait(fun() -> by_host(Pool) =:= #{key(<<"yb1">>) => 2, key(<<"yb2">>) => 2, key(<<"yb3">>) => 2} end,
         spread),
    stop(Pool).

yb_topology(_Config) ->
    {ok, Pool} = eysql:start_link(yb(#{topology_keys => "gcp.us-east1.us-east1-c:1,gcp.us-east1.*:2"})),
    wait(fun() -> by_host(Pool) =:= #{key(<<"yb2">>) => 6} end, all_on_yb2),
    stop(Pool).

%% Concurrent read-modify-writes under REPEATABLE READ conflict with 40001;
%% transaction/3 retries them until every increment lands.
yb_serialization_retries(_Config) ->
    {ok, Pool} = eysql:start_link(yb(#{pool_size => 10})),
    {ok, _, _} = eysql:squery(Pool, "DROP TABLE IF EXISTS eysql_counter"),
    {ok, _, _} = eysql:squery(Pool, "CREATE TABLE eysql_counter (id int PRIMARY KEY, n int NOT NULL)"),
    {ok, 1} = eysql:equery(Pool, "INSERT INTO eysql_counter VALUES (1, 0)", []),
    Workers = 10,
    PerWorker = 10,
    Increment = fun(C) ->
                        {ok, _, [{N}]} = epgsql:equery(C, "SELECT n FROM eysql_counter WHERE id = 1", []),
                        case epgsql:equery(C, "UPDATE eysql_counter SET n = $1 WHERE id = 1", [N + 1]) of
                            {ok, 1} -> {ok, N + 1};
                            {error, _} = Error -> Error
                        end
                end,
    Options = #{attempts => 50, 'begin' => "BEGIN ISOLATION LEVEL REPEATABLE READ"},
    Self = self(),
    Pids = [spawn_link(fun() ->
                               Results = [eysql:transaction(Pool, Increment, Options) || _ <- lists:seq(1, PerWorker)],
                               Self ! {done, self(), Results}
                       end)
            || _ <- lists:seq(1, Workers)],
    Results = lists:append([receive {done, Pid, R} -> R after 120000 -> error(timeout) end || Pid <- Pids]),
    Failures = [R || R <- Results, element(1, R) =/= ok],
    ct:pal("~b transactions, ~b failed: ~p", [length(Results), length(Failures), lists:sublist(Failures, 3)]),
    ?assertEqual([], Failures),
    ?assertMatch({ok, _, [{100}]}, eysql:equery(Pool, "SELECT n FROM eysql_counter WHERE id = 1", [])),
    stop(Pool).

%% Stop a node under load: connections move off it and queries keep working;
%% start it again: connections come back.
%%
%% While yb3 shuts down it answers new connections 57P03, "shutting down",
%% and once it has stopped it refuses them or its name does not resolve.
%% Either way it is out of new connections, and refreshes, every 5 s, probe
%% it until one finds it taking connections again. So connections come back
%% to it within a refresh or so of its accepting them, not when one reaches
%% max_lifetime. A process of the test's own notes when yb3 first accepts a
%% connection, for the log.
yb_failover(_Config) ->
    {ok, Pool} = eysql:start_link(yb(#{})),
    wait(fun() -> maps:get(key(<<"yb3">>), by_host(Pool), 0) =:= 2 end, balanced),
    Loader = start_load(Pool),
    timer:sleep(1000),
    ct:pal("stopping yb3: ~s", [docker(<<"POST">>, <<"/containers/eysql-yb3/stop?t=10">>)]),
    wait(fun() -> maps:get(key(<<"yb3">>), by_host(Pool), 0) =:= 0 andalso full(Pool) end,
         off_yb3, 90000),
    {Ok1, Err1} = load_counts(Loader),
    ct:pal("while yb3 went down: ~b ok, ~b errors; pool ~p", [Ok1, Err1, eysql:stats(Pool)]),
    timer:sleep(5000),
    {Ok2, Err2} = load_counts(Loader),
    ct:pal("with yb3 down: ~b ok, ~b errors", [Ok2 - Ok1, Err2 - Err1]),
    ?assertEqual(Err1, Err2),
    ?assert(Ok2 > Ok1),
    Started = erlang:monotonic_time(millisecond),
    ct:pal("starting yb3: ~s", [docker(<<"POST">>, <<"/containers/eysql-yb3/start">>)]),
    Poller = accepting(<<"yb3">>, Started),
    wait(fun() -> maps:get(key(<<"yb3">>), by_host(Pool), 0) >= 1 end, back_on_yb3, 180000),
    Back = erlang:monotonic_time(millisecond) - Started,
    Accepted = receive {accepting, Poller, Ms} -> Ms after 1000 -> unknown end,
    unlink(Poller),
    exit(Poller, kill),
    ct:pal("yb3 accepted a connection ~p ms after its start, and the pool had one there after ~b ms; "
           "cluster ~p", [Accepted, Back, maps:with([failed, rejected, placement], eysql:cluster_info(Pool))]),
    {Ok3, Err3} = stop_load(Loader),
    ct:pal("after yb3 returned: ~b ok, ~b errors in total; pool ~p", [Ok3, Err3, eysql:stats(Pool)]),
    stop(Pool).

%% A process that tries `Host' every 200 ms, as the pool would connect, and
%% tells the caller how many ms after `Since' the first connection opened.
accepting(Host, Since) ->
    {ok, #{settings := Settings}} = eysql_config:normalize(yb(#{connect_timeout => 1000})),
    Self = self(),
    spawn_link(fun() -> poll_accepting(Self, Host, Settings, Since) end).

poll_accepting(Parent, Host, Settings, Since) ->
    case eysql_conn:open(Host, 5433, Settings) of
        {ok, Conn} ->
            eysql_conn:close(Conn),
            Parent ! {accepting, self(), erlang:monotonic_time(millisecond) - Since};
        {error, _} ->
            timer:sleep(200),
            poll_accepting(Parent, Host, Settings, Since)
    end.

start_load(Pool) ->
    Self = self(),
    spawn_link(fun() -> load(Pool, Self, 0, 0) end).

load(Pool, Parent, Ok, Err) ->
    receive
        {counts, From} -> From ! {counts, self(), Ok, Err}, load(Pool, Parent, Ok, Err);
        {stop, From} -> From ! {counts, self(), Ok, Err}
    after 5 ->
            case eysql:equery(Pool, "SELECT 1", []) of
                {ok, _, _} -> load(Pool, Parent, Ok + 1, Err);
                _ -> load(Pool, Parent, Ok, Err + 1)
            end
    end.

load_counts(Loader) ->
    Loader ! {counts, self()},
    receive {counts, Loader, Ok, Err} -> {Ok, Err} end.

stop_load(Loader) ->
    Loader ! {stop, self()},
    receive {counts, Loader, Ok, Err} -> {Ok, Err} end.

%%%=============================================================================
%%% Either engine: which connection the next holder gets
%%%=============================================================================

%% with_connection/3 and transaction/3 read the transaction status epgsql
%% last heard in ReadyForQuery, and keep the connection only when it is
%% idle. Each pool here holds one connection, so the next holder gets the
%% same one if it was kept and a new one if it was discarded.

%% BEGIN and COMMIT inside with_connection: the status follows the server
%% from idle to in a transaction and back, and the connection is kept.
idle_connection_kept(Config) ->
    Pool = one(Config),
    First = eysql:with_connection(Pool, fun(C) ->
                                                Id = id(C, Config),
                                                ?assertEqual(idle, eysql_conn:transaction_status(C)),
                                                {ok, _, _} = epgsql:squery(C, "BEGIN"),
                                                ?assertEqual(in_transaction, eysql_conn:transaction_status(C)),
                                                {ok, _, _} = epgsql:squery(C, "COMMIT"),
                                                ?assertEqual(idle, eysql_conn:transaction_status(C)),
                                                Id
                                        end),
    ?assertEqual(First, holder(Pool, Config)),
    stop(Pool).

%% BEGIN with no COMMIT: the connection is closed, which ends the
%% transaction, and the next holder gets a new one.
open_transaction_discarded(Config) ->
    Pool = one(Config),
    Left = eysql:with_connection(Pool, fun(C) ->
                                               Id = id(C, Config),
                                               {ok, _, _} = epgsql:squery(C, "BEGIN"),
                                               ?assertEqual(in_transaction, eysql_conn:transaction_status(C)),
                                               Id
                                       end),
    replaced(Left, Pool, Config),
    stop(Pool).

%% A statement fails inside BEGIN, so the server accepts only ROLLBACK.
failed_transaction_discarded(Config) ->
    Pool = one(Config),
    Left = eysql:with_connection(Pool, fun(C) ->
                                               Id = id(C, Config),
                                               {ok, _, _} = epgsql:squery(C, "BEGIN"),
                                               {error, _} = epgsql:squery(C, "SELECT 1/0"),
                                               ?assertEqual(failed, eysql_conn:transaction_status(C)),
                                               Id
                                       end),
    replaced(Left, Pool, Config),
    stop(Pool).

%% COMMIT, ROLLBACK on request, and ROLLBACK after a failed statement all
%% leave the connection idle, so transaction/2 keeps it each time.
transaction_connection_kept(Config) ->
    Pool = one(Config),
    {ok, First} = eysql:transaction(Pool, fun(C) -> {ok, id(C, Config)} end),
    ?assertEqual(First, holder(Pool, Config)),
    Self = self(),
    ?assertEqual({error, {rollback, no}},
                 eysql:transaction(Pool, fun(C) -> Self ! {id, id(C, Config)}, {rollback, no} end)),
    ?assertEqual({id, First}, receive {id, _} = Id -> Id after 0 -> none end),
    {error, Error} = eysql:transaction(Pool, fun(C) ->
                                                     Self ! {id, id(C, Config)},
                                                     epgsql:squery(C, "SELECT 1/0")
                                             end),
    ?assertEqual(<<"22012">>, eysql_error:code(Error)),
    ?assertEqual({id, First}, receive {id, _} = Id -> Id after 0 -> none end),
    ?assertEqual(First, holder(Pool, Config)),
    stop(Pool).

%% eysql:squery(Pool, "BEGIN") leaves its connection inside a transaction,
%% so the next holder gets a new one. Holders before it get the same one.
squery_begin_discarded(Config) ->
    Pool = one(Config),
    Before = holder(Pool, Config),
    ?assertEqual(Before, holder(Pool, Config)),
    ?assertMatch({ok, _, _}, eysql:squery(Pool, "BEGIN")),
    replaced(Before, Pool, Config),
    stop(Pool).

%% The pool stops, closing its connection, while a query waits on the
%% server: the caller gets the query's error, `{error, closed}' from epgsql,
%% not an exit from asking the stopped pool for its driver.
pool_stops_under_query(Config) ->
    Pool = one(Config),
    unlink(Pool),
    Cluster = eysql_pool:cluster(Pool),
    Self = self(),
    Query = fun() ->
                    Result = try eysql:equery(Pool, "SELECT pg_sleep(10)", [])
                             catch Class:Reason -> {Class, Reason}
                             end,
                    Self ! {result, self(), Result}
            end,
    Querier = spawn(Query),
    wait(fun() -> maps:get(leased, eysql:stats(Pool)) =:= 1 end, leased),
    exit(Cluster, kill),
    receive
        {result, Querier, Result} ->
            ct:pal("query when the pool stopped: ~p", [Result]),
            ?assert(eysql_error:is_connection_lost(Result))
    after 5000 ->
            ct:fail(no_result)
    end.

%% A query that outlasts socket_timeout, as on a server that has stopped
%% answering, is cut off on the client: the call returns connection_lost at
%% about the bound, not when the server finishes. The pool opens another
%% connection, the server is not marked, and the next query works.
socket_timeout_cuts_query(Config) ->
    Pool = one(Config, #{socket_timeout => 500}),
    {Before, _} = holder(Pool, Config),
    {Micros, Result} = timer:tc(fun() -> eysql:equery(Pool, "SELECT pg_sleep(5)", []) end),
    ct:pal("pg_sleep(5) with socket_timeout 500: ~p after ~b ms", [Result, Micros div 1000]),
    ?assertEqual({error, {connection_lost, socket_timeout}}, Result),
    ?assert(Micros >= 500000 andalso Micros < 1500000),
    ?assertNot(is_process_alive(Before)),
    ?assertMatch({ok, _, [{1}]}, eysql:equery(Pool, "SELECT 1", [])),
    {After, _} = holder(Pool, Config),
    ?assertNotEqual(Before, After),
    ?assertMatch(#{failed := [], rejected := [], read_only := []}, eysql:cluster_info(Pool)),
    stop(Pool).

%% Every pooled connection runs after_connect before it serves a query: the
%% first three, and the ones that replace them as each reaches its 1 s
%% lifetime under load. The hook waits a little, so that a connection handed
%% out before its hook had finished would show, then marks its session and
%% records the connection and its backend. Each query checks, as it gets its
%% connection, that the hook has recorded it, and reads the backend and the
%% mark: the same backend, marked. Each connection ran the hook once.
after_connect_prepares(Config) ->
    Table = ets:new(?MODULE, [public, duplicate_bag]),
    Pool = one(Config, #{pool_size => 3, max_lifetime => 1000, lifetime_jitter => 0,
                         rebalance_interval => 200,
                         after_connect => {?MODULE, prepare_session, [Table]}}),
    Serve = fun(C) ->
                    Recorded = ets:lookup(Table, C),
                    {ok, _, [{Backend, Mark}]} =
                        epgsql:equery(C, "SELECT pg_backend_pid(), current_setting('eysql.prepared', true)", []),
                    {C, Backend, Mark, Recorded}
            end,
    Self = self(),
    Until = erlang:monotonic_time(millisecond) + 4000,
    Workers = [spawn_link(fun() -> Self ! {served, self(), serve_until(Pool, Serve, Until, [])} end)
               || _ <- [1, 2, 3]],
    Served = lists:append([receive {served, W, S} -> S after 30000 -> ct:fail(no_result) end || W <- Workers]),
    Conns = lists:usort([C || {C, _, _, _} <- Served]),
    Prepared = ets:tab2list(Table),
    ct:pal("~b queries on ~b connections; the hook ran ~b times", [length(Served), length(Conns), length(Prepared)]),
    ?assertEqual([], [S || S <- Served, tuple_size(S) =/= 4]),
    ?assertEqual([], [S || {C, Backend, Mark, Recorded} = S <- Served,
                           {Mark, Recorded} =/= {<<"yes">>, [{C, Backend}]}]),
    ?assert(length(Conns) > 3),
    ?assertEqual(length(Prepared), length(lists:usort([C || {C, _} <- Prepared]))),
    stop(Pool).

serve_until(Pool, Serve, Until, Acc) ->
    case erlang:monotonic_time(millisecond) < Until of
        true -> serve_until(Pool, Serve, Until, [eysql:with_connection(Pool, Serve, 10000) | Acc]);
        false -> Acc
    end.

prepare_session(Conn, Table) ->
    {ok, _, _} = eysql_conn:squery(Conn, "SELECT pg_sleep(0.2)"),
    {ok, _, [{Backend, <<"yes">>}]} =
        eysql_conn:equery(Conn, "SELECT pg_backend_pid(), set_config('eysql.prepared', 'yes', false)", []),
    true = ets:insert(Table, {Conn, Backend}),
    ok.

%% A hook whose statement the server rejects: no connection joins the pool,
%% a checkout fails with the server's error, and no server is left out.
after_connect_failure_keeps_servers(Config) ->
    Pool = one(Config, #{after_connect => fun(C) -> eysql_conn:squery(C, "SELECT no_such_column") end}),
    {error, {after_connect, Error}} = eysql:checkout(Pool, 10000),
    ?assertEqual(<<"42703">>, eysql_error:code(Error)),
    ?assertMatch(#{idle := 0, leased := 0}, eysql:stats(Pool)),
    ?assertMatch(#{failed := [], rejected := [], read_only := []}, eysql:cluster_info(Pool)),
    stop(Pool).

%% A pool of one connection on this group's engine.
one(Config) ->
    one(Config, #{}).

one(Config, Overrides) ->
    Options = case ?config(engine, Config) of
                  postgres -> pg(maps:merge(#{pool_size => 1}, Overrides));
                  yugabyte -> yb(maps:merge(#{pool_size => 1}, Overrides))
              end,
    {ok, Pool} = eysql:start_link(Options),
    Pool.

%% Which connection this is: its epgsql process and, on PostgreSQL, the
%% server backend's pid as well. On YugabyteDB the epgsql process alone
%% names it: YSQL Connection Manager, which eysql supports though this stack
%% runs without it, can serve a client connection from more than one
%% backend, and backend pids on different nodes can be equal.
id(Conn, Config) ->
    case ?config(engine, Config) of
        postgres ->
            {ok, _, [{Backend}]} = epgsql:equery(Conn, "SELECT pg_backend_pid()", []),
            {Conn, Backend};
        yugabyte ->
            {Conn, undefined}
    end.

%% The connection the next holder gets.
holder(Pool, Config) ->
    eysql:with_connection(Pool, fun(C) -> id(C, Config) end).

rows({ok, _Columns, Rows}) -> Rows.

%% The connection `Left' was closed, and the next holder has another. On
%% PostgreSQL its backend is gone too, and with it the transaction.
replaced({Conn, Backend} = Left, Pool, Config) ->
    {Next, NextBackend} = holder(Pool, Config),
    ?assertNotEqual(Conn, Next),
    wait(fun() -> not is_process_alive(Conn) end, closed),
    case ?config(engine, Config) of
        postgres ->
            ?assertNotEqual(Backend, NextBackend),
            Sessions = "SELECT count(*)::int FROM pg_stat_activity WHERE pid = $1",
            wait(fun() -> rows(eysql:equery(Pool, Sessions, [Backend])) =:= [{0}] end,
                 {backend_gone, Left});
        yugabyte ->
            ok
    end.

%%%=============================================================================
%%% Helpers
%%%=============================================================================

key(Host) -> {Host, 5433}.

by_host(Pool) -> maps:get(by_host, eysql:stats(Pool)).

full(Pool) ->
    #{idle := Idle, leased := Leased, size := Size} = eysql:stats(Pool),
    Idle + Leased =:= Size.

stop(Pool) ->
    unlink(Pool),
    eysql:stop(Pool).

wait(Fun, What) -> wait(Fun, What, 30000).

wait(Fun, What, Timeout) when Timeout =< 0 ->
    ct:fail({timeout_waiting_for, What, Fun()});
wait(Fun, What, Timeout) ->
    case Fun() of
        true -> ok;
        _ -> timer:sleep(200), wait(Fun, What, Timeout - 200)
    end.

%% One request to the Docker Engine API over its Unix socket; returns the
%% status line.
docker(Method, Path) ->
    {ok, Socket} = gen_tcp:connect({local, "/var/run/docker.sock"}, 0,
                                   [binary, {active, false}, {packet, raw}]),
    ok = gen_tcp:send(Socket, [Method, " ", Path, " HTTP/1.1\r\nHost: docker\r\n"
                               "Content-Length: 0\r\nConnection: close\r\n\r\n"]),
    Response = recv_all(Socket, <<>>),
    ok = gen_tcp:close(Socket),
    [Status | _] = binary:split(Response, <<"\r\n">>),
    Status.

recv_all(Socket, Acc) ->
    case gen_tcp:recv(Socket, 0, 60000) of
        {ok, Data} -> recv_all(Socket, <<Acc/binary, Data/binary>>);
        {error, closed} -> Acc
    end.
