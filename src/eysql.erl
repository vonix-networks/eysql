%% Copyright 2026 Vonix Networks
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%% @doc Topology-aware load balancing and failover for epgsql.
%%
%% eysql gives epgsql what the YugabyteDB smart drivers give JDBC, pgx and
%% psycopg: it discovers the cluster's servers with `yb_servers()', opens each
%% connection to the least-loaded server in the preferred zone or region, and
%% skips hosts that fail. It adds a pool that recycles and rebalances
%% connections, and a transaction helper that retries serialization
%% failures. With PostgreSQL it uses the configured hosts.
%%
%% Load balancing is off unless `load_balance' says otherwise, as in the
%% smart drivers: without it, the pool uses the configured hosts only.
%%
%% Connections are ordinary epgsql connections: check one out and call epgsql
%% on it.
%%
%% ```
%% {ok, Pool} = eysql:start_link(#{hosts => ["yb-tservers.db.svc.cluster.local"],
%%                                username => <<"app">>,
%%                                password => <<"secret">>,
%%                                database => <<"app">>,
%%                                load_balance => true,
%%                                topology_keys => "gcp.us-east1.us-east1-b:1,gcp.us-east1.*:2"}),
%% {ok, _, Rows} = eysql:equery(Pool, "SELECT id FROM accounts WHERE realm = $1", [Realm]),
%% eysql:with_connection(Pool, fun(Conn) -> epgsql:equery(Conn, Sql, Params) end).
%% '''
-module(eysql).

-export([start_link/1,
         start_link/2,
         child_spec/2,
         stop/1,
         checkout/1,
         checkout/2,
         checkin/2,
         discard/2,
         with_connection/2,
         with_connection/3,
         equery/3,
         squery/2,
         transaction/2,
         transaction/3,
         stats/1,
         cluster_info/1
        ]).

-export_type([pool/0, transaction_error/0]).

-type pool() :: gen_server:server_ref().

-type transaction_error() :: {rollback, term()}
                           | commit_outcome_unknown
                           | transaction_aborted
                           | checkout_timeout
                           | term().

-define(DEFAULT_TIMEOUT, 5000).
%% Waits between transaction attempts, in ms; see backoff/2.
-define(BACKOFF_BASE, 10).
-define(DEFAULT_MAX_BACKOFF, 500).
%% 10 ms doubled 20 times is about 3 hours, past any sensible cap. Stopping
%% there keeps `bsl' from building a huge integer when `attempts' is large.
-define(BACKOFF_MAX_SHIFT, 20).

%%%=============================================================================
%%% Starting
%%%=============================================================================

%% @doc Start a pool linked to the caller. See {@link eysql_config:defaults/0}
%% for the options.
-spec start_link(eysql_config:options()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    case eysql_config:normalize(Options) of
        {ok, Config} -> eysql_pool:start_link(Config);
        {error, _} = Error -> Error
    end.

%% @doc Start a registered pool, for example `{local, my_db}'.
-spec start_link(gen_server:server_name(), eysql_config:options()) -> {ok, pid()} | {error, term()}.
start_link(Name, Options) ->
    case eysql_config:normalize(Options) of
        {ok, Config} -> eysql_pool:start_link(Name, Config);
        {error, _} = Error -> Error
    end.

%% @doc A child spec for a registered pool, `Name' being a local name.
-spec child_spec(atom(), eysql_config:options()) -> supervisor:child_spec().
child_spec(Name, Options) ->
    #{id => Name,
      start => {?MODULE, start_link, [{local, Name}, Options]},
      type => worker,
      restart => permanent,
      shutdown => 5000,
      modules => [eysql_pool]
     }.

-spec stop(pool()) -> ok.
stop(Pool) ->
    eysql_pool:stop(Pool).

%%%=============================================================================
%%% Connections
%%%=============================================================================

-spec checkout(pool()) -> {ok, pid()} | {error, term()}.
checkout(Pool) ->
    checkout(Pool, ?DEFAULT_TIMEOUT).

%% @doc Take an epgsql connection, waiting up to `Timeout' ms. Return it with
%% {@link checkin/2}, or {@link discard/2} if it may be in a bad state.
%% Until then, the pool's `socket_timeout' bounds the queries this process
%% makes on it with {@link eysql_conn:squery/2} and
%% {@link eysql_conn:equery/3}.
-spec checkout(pool(), timeout()) -> {ok, pid()} | {error, term()}.
checkout(Pool, Timeout) ->
    eysql_pool:checkout(Pool, Timeout).

%% @doc Return a connection for reuse. Only the process that checked it out
%% can return it: the pool ignores a checkin from any other process, including
%% a late one from an earlier holder. If the holder exits instead, the pool
%% closes the connection and opens another.
%%
%% The pool does not look at the connection: the next holder gets it as it
%% is. End any transaction with COMMIT or ROLLBACK first, or call
%% {@link discard/2} instead. {@link eysql_conn:transaction_status/1} tells
%% whether one is still open; {@link with_connection/3} does this for you.
-spec checkin(pool(), pid()) -> ok.
checkin(Pool, Conn) ->
    eysql_pool:checkin(Pool, Conn).

%% @doc Return a connection that must not be reused, for example after an
%% error left it in an unknown state. The pool closes it and opens another.
%% As with {@link checkin/2}, only the process holding the connection can
%% discard it; a discard from any other process is ignored.
-spec discard(pool(), pid()) -> ok.
discard(Pool, Conn) ->
    eysql_pool:discard(Pool, Conn).

%% @equiv with_connection(Pool, Fun, 5000)
-spec with_connection(pool(), fun((pid()) -> Result)) -> Result | {error, term()}.
with_connection(Pool, Fun) ->
    with_connection(Pool, Fun, ?DEFAULT_TIMEOUT).

%% @doc Run `Fun' with a connection and return its result. Returns
%% `{error, Reason}' if no connection could be had.
%%
%% The connection goes back to the pool afterwards only if no transaction is
%% open on it. If `Fun' ran BEGIN without COMMIT or ROLLBACK, or left the
%% connection in a state the driver's `transaction_status/1' cannot read
%% (see {@link eysql_conn:transaction_status/1}), the connection is discarded
%% instead: the pool closes it, which rolls the transaction back, and opens
%% another. The check costs no round trip to the server. If `Fun' raises, the
%% connection is discarded and the exception re-raised.
%%
%% With the pool's `socket_timeout' set, queries `Fun' makes with
%% {@link eysql_conn:squery/2} and {@link eysql_conn:equery/3} wait for the
%% server that long at most, then return
%% `{error, {connection_lost, socket_timeout}}', the connection closed.
%% epgsql's own functions, called on the connection directly, wait for as
%% long as the server takes.
%%
%% If the pool stops while `Fun' runs, the result of `Fun' is still
%% returned. The pool closes its connections as it stops, so that is usually
%% an error that {@link eysql_error:is_connection_lost/1} accepts, such as
%% `{error, closed}'.
-spec with_connection(pool(), fun((pid()) -> Result), timeout()) -> Result | {error, term()}.
with_connection(Pool, Fun, Timeout) ->
    case checkout(Pool, Timeout) of
        {ok, Conn} ->
            %% Ask for the driver now, while the pool has just answered. By
            %% the time Fun returns the pool may have stopped, for example
            %% because its cluster process died, and asking a stopped pool
            %% exits the caller. Giving the connection back is a cast, which
            %% a stopped pool ignores.
            Driver = pool_driver(Pool),
            try Fun(Conn) of
                Result ->
                    settle(Pool, Driver, Conn, false),
                    Result
            catch
                Class:Reason:Stack ->
                    discard(Pool, Conn),
                    erlang:raise(Class, Reason, Stack)
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc `epgsql:equery/3' on a pooled connection, through
%% {@link eysql_conn:equery/3}: bounded by `socket_timeout', and a lost
%% connection is an error rather than an exit.
-spec equery(pool(), iodata(), [term()]) -> term().
equery(Pool, Sql, Params) ->
    with_connection(Pool, fun(Conn) -> eysql_conn:equery(Conn, Sql, Params) end).

%% @doc `epgsql:squery/2' on a pooled connection, as {@link equery/3}.
-spec squery(pool(), iodata()) -> term().
squery(Pool, Sql) ->
    with_connection(Pool, fun(Conn) -> eysql_conn:squery(Conn, Sql) end).

%%%=============================================================================
%%% Transactions
%%%=============================================================================

%% @equiv transaction(Pool, Fun, #{})
-spec transaction(pool(), fun((pid()) -> {ok, T} | {rollback, term()} | {error, term()})) ->
          {ok, T} | {error, transaction_error()}.
transaction(Pool, Fun) ->
    transaction(Pool, Fun, #{}).

%% @doc Run `Fun' inside BEGIN and COMMIT, retrying when that is safe.
%%
%% `Fun' returns `{ok, Value}' to commit, `{rollback, Reason}' to roll back,
%% or `{error, Reason}', usually an epgsql error it got back, to roll back.
%%
%% The whole transaction runs again, on a fresh checkout, when an error is
%% retryable (serialization failure or deadlock, see {@link eysql_error}) or
%% the connection was lost before COMMIT. Nothing was committed in either case.
%% `Fun' must therefore have no side effects outside the database.
%%
%% The pool's `socket_timeout' bounds BEGIN, COMMIT and ROLLBACK, and the
%% queries `Fun' makes with {@link eysql_conn:squery/2} and
%% {@link eysql_conn:equery/3}, as in {@link with_connection/3}. One that
%% runs out loses the connection: before COMMIT, the transaction runs again;
%% during COMMIT, the result is `commit_outcome_unknown'.
%%
%% Results:
%% <ul>
%% <li>`{ok, Value}': committed;</li>
%% <li>`{error, {rollback, Reason}}': `Fun' asked to roll back;</li>
%% <li>`{error, commit_outcome_unknown}': the connection was lost during
%%     COMMIT, so the transaction may or may not have committed. Check the
%%     data, for example by re-reading a row the transaction wrote;</li>
%% <li>`{error, transaction_aborted}': COMMIT rolled back because a statement
%%     failed earlier and `Fun' still returned `{ok, _}';</li>
%% <li>`{error, Reason}': anything else, including the last error once
%%     retries run out.</li>
%% </ul>
%%
%% If no connection can be checked out for a retry, because none came in time
%% or the pool has stopped, the retries stop there and the result is the
%% error that made the last attempt retry, such as the serialization failure,
%% as when the attempts run out. A checkout error such as
%% `{error, checkout_timeout}' therefore means `Fun' never ran.
%%
%% Options:
%% <ul>
%% <li>`attempts': how many times to run the transaction, default 3;</li>
%% <li>`checkout_timeout': ms to wait for a connection, default 5000;</li>
%% <li>`max_backoff': the longest wait before a retry, in ms, default 500.
%%     The wait is random: up to 20 ms before the second attempt, twice that
%%     before the third and so on, but never more than this. 0 retries at
%%     once;</li>
%% <li>`begin': the statement that opens the transaction, default
%%     `"BEGIN"';</li>
%% <li>`driver': the module that runs BEGIN, COMMIT and ROLLBACK, by default
%%     the pool's own `driver' option. Only tests need to set it.</li>
%% </ul>
%%
%% If `Fun' raises, the connection is discarded, which ends the transaction,
%% and the exception is re-raised.
-spec transaction(pool(), fun((pid()) -> {ok, T} | {rollback, term()} | {error, term()}), map()) ->
          {ok, T} | {error, transaction_error()}.
transaction(Pool, Fun, Options) ->
    Attempts = maps:get(attempts, Options, 3),
    Timeout = maps:get(checkout_timeout, Options, ?DEFAULT_TIMEOUT),
    Begin = maps:get('begin', Options, "BEGIN"),
    MaxBackoff = case maps:get(max_backoff, Options, ?DEFAULT_MAX_BACKOFF) of
                     Ms when is_integer(Ms), Ms >= 0 -> Ms;
                     Other -> erlang:error({invalid_option, max_backoff, Other})
                 end,
    Driver = case maps:find(driver, Options) of
                 {ok, Module} -> Module;
                 error -> eysql_pool:driver(Pool)
             end,
    attempt(Pool, Fun, Driver, Begin, Timeout, MaxBackoff, 1, Attempts, none).

%% `Last' is the error that made the previous attempt retry, `none' on the
%% first.
attempt(Pool, Fun, Driver, Begin, Timeout, MaxBackoff, N, Max, Last) ->
    case attempt_checkout(Pool, Timeout, Last) of
        {ok, Conn} ->
            Outcome = try run(Driver, Conn, Fun, Begin)
                      catch
                          Class:Exception:Stack ->
                              discard(Pool, Conn),
                              erlang:raise(Class, Exception, Stack)
                      end,
            case Outcome of
                {committed, Value} ->
                    settle(Pool, Driver, Conn, false),
                    {ok, Value};
                {rolled_back, Reason} ->
                    settle(Pool, Driver, Conn, false),
                    {error, {rollback, Reason}};
                {retry, Error, Broken} ->
                    settle(Pool, Driver, Conn, Broken),
                    case N < Max of
                        true ->
                            backoff(N, MaxBackoff),
                            attempt(Pool, Fun, Driver, Begin, Timeout, MaxBackoff, N + 1, Max, Error);
                        false ->
                            {error, reason(Error)}
                    end;
                {failed, Error, Broken} ->
                    settle(Pool, Driver, Conn, Broken),
                    {error, reason(Error)};
                commit_unknown ->
                    discard(Pool, Conn),
                    {error, commit_outcome_unknown}
            end;
        {error, _} = Error when Last =:= none ->
            Error;
        {error, _} ->
            %% An attempt ran and failed with nothing committed. Say so
            %% rather than why the retry got no connection.
            {error, reason(Last)}
    end.

%% A retry follows an attempt that ran, and the pool may have stopped
%% meanwhile: its connections close as it stops, which is often why the
%% attempt failed. A checkout from a stopped pool exits the caller; for a
%% retry it is just no connection, so the caller gets the error that made it
%% retry. The first checkout exits as checkout/2 does.
attempt_checkout(Pool, Timeout, none) ->
    checkout(Pool, Timeout);
attempt_checkout(Pool, Timeout, _Last) ->
    try checkout(Pool, Timeout)
    catch exit:Reason -> {error, Reason}
    end.

run(Driver, Conn, Fun, Begin) ->
    case Driver:squery(Conn, Begin) of
        {error, _} = Error ->
            before_commit(Error);
        _ ->
            case Fun(Conn) of
                {ok, Value} ->
                    commit(Driver, Conn, Value);
                {rollback, Reason} ->
                    _ = Driver:squery(Conn, "ROLLBACK"),
                    {rolled_back, Reason};
                {error, _} = Error ->
                    _ = Driver:squery(Conn, "ROLLBACK"),
                    before_commit(Error);
                Other ->
                    _ = Driver:squery(Conn, "ROLLBACK"),
                    erlang:error({bad_transaction_result, Other})
            end
    end.

commit(Driver, Conn, Value) ->
    case Driver:squery(Conn, "COMMIT") of
        {error, _} = Error ->
            case eysql_error:classify(Error) of
                connection_lost -> commit_unknown;
                retryable -> {retry, Error, false};
                other -> {failed, Error, false}
            end;
        _ ->
            case Driver:committed(Conn) of
                true -> {committed, Value};
                false -> {failed, {error, transaction_aborted}, false}
            end
    end.

%% Nothing was committed: safe to run the whole transaction again when the
%% error is retryable or the connection is gone.
before_commit(Error) ->
    case eysql_error:classify(Error) of
        retryable -> {retry, Error, false};
        connection_lost -> {retry, Error, true};
        other -> {failed, Error, false}
    end.

%% Give the connection back, unless it is broken or still inside a
%% transaction: a ROLLBACK that failed, or a Fun that left one open, would
%% otherwise hand the next holder someone else's transaction. Closing the
%% connection rolls it back without waiting on the server. With no driver to
%% ask, the pool has stopped and the connection is closing anyway.
%%
%% Nothing here waits on the pool, which may have stopped since the checkout:
%% checkin and discard are casts, and the driver reads the status from the
%% connection.
settle(Pool, Driver, Conn, false) when Driver =/= undefined ->
    case Driver:transaction_status(Conn) of
        idle -> checkin(Pool, Conn);
        _ -> discard(Pool, Conn)
    end;
settle(Pool, _Driver, Conn, _Broken) ->
    discard(Pool, Conn).

%% The pool's driver, or `undefined' if the pool stopped after answering the
%% checkout, in which case eysql_pool:driver/1 exits.
pool_driver(Pool) ->
    try eysql_pool:driver(Pool)
    catch exit:_ -> undefined
    end.

%% Wait before attempt `N + 1': exponential with full jitter, capped, so
%% transactions that conflicted spread out instead of colliding again.
backoff(_N, 0) ->
    ok;
backoff(N, MaxBackoff) ->
    Ceiling = min(MaxBackoff, ?BACKOFF_BASE bsl min(N, ?BACKOFF_MAX_SHIFT)),
    timer:sleep(rand:uniform(Ceiling)).

reason({error, Reason}) -> Reason.

%%%=============================================================================
%%% Introspection
%%%=============================================================================

%% @doc Connections by state and by host.
-spec stats(pool()) -> #{atom() => term()}.
stats(Pool) ->
    eysql_pool:stats(Pool).

%% @doc The cluster's view: servers, where new connections go now
%% (`placement'), which hosts the pool keeps connections on (`allowed'),
%% counts per host, hosts that failed and have not accepted a connection
%% since (`failed'), and standbys that `target_session_attrs => read_write'
%% turned away (`read_only'). See {@link eysql_cluster:snapshot/1}.
-spec cluster_info(pool()) -> #{atom() => term()}.
cluster_info(Pool) ->
    eysql_cluster:snapshot(eysql_pool:cluster(Pool)).
