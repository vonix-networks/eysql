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

%% @doc A fixed-size pool of epgsql connections spread across a cluster.
%%
%% The pool starts its own {@link eysql_cluster} and keeps `pool_size'
%% connections open, each opened in a short-lived process so a slow connect
%% never blocks checkouts. It starts opening them at once: each opener waits
%% in {@link eysql_cluster:open/1} for the cluster's first discovery, so the
%% first connections go to the discovered servers rather than all to the
%% seeds, and checkouts wait for the openers. On top of what a smart driver
%% does, it:
%%
%% <ul>
%% <li>recycles each connection after `max_lifetime' plus up to
%%     `lifetime_jitter', so connections return to hosts that came back. An
%%     idle connection past its lifetime closes at the next rebalance tick,
%%     or when a checkout comes to it first; a leased one, when it comes
%%     back;</li>
%% <li>moves up to `rebalance_batch' connections every `rebalance_interval'
%%     off hosts no longer allowed, or else from the busiest host new
%%     connections may go to, and reopens them where the cluster says. A
%%     host that failed stays allowed, and connections move back towards a
%%     better one only once a connection to it has opened again, the
%%     cluster's probe at a refresh as a rule. Until then it does not count
%%     towards finding the busiest either. With `load_balance' false new
%%     connections go to the first host that works, so there is no busiest
%%     to move from, and connections on a later host move back to the first
%%     once it is found back. A rebalance asks the cluster process only; it sends nothing to
%%     a server.</li>
%% </ul>
%%
%% Like the smart drivers, the pool sends no query of its own: no health
%% check, no ping. A connection that dies is found as its process exits,
%% which epgsql's does when the server closes the socket or the socket
%% fails; and a connection that fails a query with a lost connection is
%% discarded by {@link eysql:with_connection/3} and
%% {@link eysql:transaction/3}, which read its transaction status as
%% `unknown'. Either way the pool opens another, and only if that connect
%% fails is the host marked down. A query error on a live connection marks
%% nothing: a cancelled or slow statement never takes a host out.
%%
%% A server that dies without closing its sockets, or hangs, answers nothing
%% and closes nothing, so a query on it would wait for as long as TCP takes
%% to give up. `socket_timeout' bounds that wait on the client, over the
%% whole call where pgjdbc's `socketTimeout' bounds each read, and sends
%% nothing to the server to do it. A checkout hands the bound to the process
%% that checks the connection out, and {@link eysql_conn:squery/2} and
%% {@link eysql_conn:equery/3} honour it there (see {@link watch/1}). That
%% covers {@link eysql:equery/3}, {@link eysql:squery/2}, the BEGIN, COMMIT
%% and ROLLBACK of {@link eysql:transaction/3}, and those two functions
%% called inside {@link eysql:with_connection/3} or a transaction. A call
%% that overruns has its connection killed, which closes the socket, and
%% returns `{error, {connection_lost, socket_timeout}}'. The pool replaces
%% the connection as it replaces any that dies, and marks no host: a
%% timeout on an open connection is not a failure to connect. epgsql calls
%% made on the connection directly are not bounded.
%%
%% Connections are linked to the pool, which traps exits: a connection that
%% dies is replaced, and if the pool stops its connections and its cluster go
%% with it. A process that dies while holding a connection loses it; the pool
%% closes that connection, since its state is unknown, and opens another. Only
%% the process holding a connection can return it.
%%
%% `after_connect' prepares each connection the pool opens before anyone
%% gets it, replacements included: those for a connection that died, one
%% past its lifetime, or one moved by a rebalance. The opener calls it with
%% the connection once {@link eysql_cluster:open/1} has opened it, in a
%% process of its own, and reports the connection only when it is done.
%% Until then the connection counts as `opening', and checkouts wait for
%% it. `ok', or a tuple whose first element is `ok', lets it into the pool.
%% Anything else fails it: `{error, Reason}', an exception, another value,
%% no answer within `after_connect_timeout', or a connection the hook left
%% closed or inside a transaction. The opener closes the connection and
%% reports the failure.
%%
%% The cluster marks no host for it: a failing hook is the application's,
%% not the server's, and leaving servers out for it would let one bad
%% statement take every server out of rotation. The pool steers its own
%% connections instead, but only between the hosts a pick would choose
%% among anyway ({@link eysql_cluster:pick/3}): a hook failure never changes
%% the tier, topology level, node type or host order. For the cluster's
%% failed-host delay after a hook fails on a host, new connections go to the
%% other hosts the pick offers, if it offers any, and rebalancing moves
%% nothing towards the host. A failure on a host the pool was not keeping
%% off is replaced at once, which then keeps off it. A failure on one it
%% had to take, the pick offering nothing else, as with `load_balance'
%% false or a single host in the preferred level, is a failed open: the
%% pool opens another after its refill delay, and fails waiting checkouts
%% with `{error, {after_connect, Reason}}' when no other connection is
%% coming. After the delay, picks and rebalancing try the host again. The
%% first failure on a host is logged as a warning with the reason, and the
%% end of the run once, at info, as the cluster logs a host's failed
%% connects. The connections that discovery and probes open are the
%% cluster's, and never run the hook.
-module(eysql_pool).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/1,
         start_link/2,
         stop/1,
         checkout/2,
         checkin/2,
         discard/2,
         stats/1,
         cluster/1,
         driver/1,
         watch/1,
         unwatch/2
        ]).

-export_type([watch/0]).

%% A call's clock, from watch/1 to unwatch/2: the timer the pool will kill
%% the connection on, or `unwatched' when there is no bound.
-opaque watch() :: reference() | unwatched.

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         format_status/1
        ]).

-define(REFILL_DELAY, 1000).
-define(ACK_TIMEOUT, 5000).
-define(STOP_TIMEOUT, 1000).

%% A run of after_connect failures on a host ends once a connection there
%% has passed the hook and the run has gone this long without a failure, as
%% a run of failed connects does in eysql_cluster. Tests shorten it with the
%% same `log_window' key in the normalized config; it is not an option.
-define(LOG_WINDOW, 60000).

%% Where the process holding a connection keeps the pool's socket_timeout
%% for it, `{Pool, Ms}', from checkout to checkin or discard, in its process
%% dictionary. Only while a bound is set.
-define(BOUND(Conn), {?MODULE, socket_timeout, Conn}).

-record(conn, {
    pid :: pid(),
    key :: eysql_topology:key(),
    expires_at :: integer()
}).

-record(lease, {
    conn :: #conn{},
    owner :: pid(),
    owner_monitor :: reference(),
    cref :: reference()
}).

-record(waiter, {
    from :: gen_server:from(),
    cref :: reference(),
    deadline :: integer()
}).

-record(state, {
    config :: eysql_config:config(),
    cluster :: pid(),
    idle = [] :: [#conn{}],
    leased = #{} :: #{pid() => #lease{}},
    %% Each lease's owner monitor, to the connection it holds.
    owners = #{} :: #{reference() => pid()},
    %% Each lease's checkout reference, to the connection it holds, for a
    %% checkout that is cancelled after its reply went out.
    crefs = #{} :: #{reference() => pid()},
    openers = #{} :: #{pid() => opening | reported},
    %% How many openers are `opening', so that filling need not count them.
    opening = 0 :: non_neg_integer(),
    waiters = queue:new() :: queue:queue(#waiter{}),
    refill_timer :: undefined | reference(),
    %% Leased connections to close when they come back, because their host
    %% is no longer allowed.
    draining = #{} :: #{pid() => true},
    %% Each host's current run of after_connect failures, for logging: how
    %% many, when the last was, and whether a connection there has passed
    %% the hook since.
    hook_runs = #{} :: #{eysql_topology:key() => {pos_integer(), integer(), boolean()}}
}).

%%%=============================================================================
%%% API
%%%=============================================================================

-spec start_link(eysql_config:config()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

-spec start_link(gen_server:server_name(), eysql_config:config()) -> {ok, pid()} | {error, term()}.
start_link(Name, Config) ->
    gen_server:start_link(Name, ?MODULE, Config, []).

-spec stop(gen_server:server_ref()) -> ok.
stop(Pool) ->
    gen_server:stop(Pool).

%% @doc Take a connection, waiting up to `Timeout' ms for one. With a
%% `socket_timeout', the calling process keeps it for the connection until
%% it checks the connection in or discards it (see {@link watch/1}).
-spec checkout(gen_server:server_ref(), timeout()) -> {ok, pid()} | {error, term()}.
checkout(Pool, Timeout) ->
    CRef = make_ref(),
    try gen_server:call(Pool, {checkout, CRef, Timeout}, Timeout) of
        {ok, Conn, Bound} ->
            keep_bound(Conn, Bound),
            {ok, Conn};
        {error, _} = Error ->
            Error
    catch
        exit:{timeout, _} ->
            gen_server:cast(Pool, {cancel, CRef}),
            {error, checkout_timeout}
    end.

%% @doc Return a connection for reuse. Call it from the process that checked
%% the connection out; the pool ignores a checkin from any other process.
-spec checkin(gen_server:server_ref(), pid()) -> ok.
checkin(Pool, Conn) ->
    _ = erase(?BOUND(Conn)),
    gen_server:cast(Pool, {checkin, Conn, self(), true}).

%% @doc Return a connection that must not be reused, for example after an
%% exception left a transaction open. The pool closes it and opens another.
%% As with {@link checkin/2}, only the process holding it can discard it.
-spec discard(gen_server:server_ref(), pid()) -> ok.
discard(Pool, Conn) ->
    _ = erase(?BOUND(Conn)),
    gen_server:cast(Pool, {checkin, Conn, self(), false}).

%% @doc Start the clock on a call to the server on `Conn', for a driver to
%% call just before the call. It runs only in the process that checked
%% `Conn' out of a pool with a `socket_timeout', and until that process
%% checks it in or discards it; elsewhere this does nothing. Follow with
%% {@link unwatch/2} as soon as the call returns or raises.
%%
%% The clock is a timer to the pool, which kills the connection if the
%% call is still waiting when it runs out: the socket closes, and the call
%% fails as on any connection that dies. That sends nothing to the server
%% and costs a timer, started and cancelled, on each call. The pool has to
%% be the one to act, because the calling process is blocked in the call.
%% {@link eysql_conn} runs every query it makes on a connection this way.
-spec watch(pid()) -> watch().
watch(Conn) ->
    case get(?BOUND(Conn)) of
        undefined -> unwatched;
        {Pool, Ms} -> erlang:start_timer(Ms, Pool, {socket_timeout, Conn})
    end.

%% @doc Stop the clock that {@link watch/1} started. `expired' if it ran out
%% first: the pool is killing the connection, or has, even if the call got
%% its answer at the last moment. This kills it as well, so that it is dead
%% before anything else this process sends it, such as the status read
%% that decides whether it goes back to the pool. The caller should report
%% the call as `{error, {connection_lost, socket_timeout}}'.
-spec unwatch(watch(), pid()) -> ok | expired.
unwatch(unwatched, _Conn) ->
    ok;
unwatch(Timer, Conn) ->
    case erlang:cancel_timer(Timer) of
        false ->
            exit(Conn, kill),
            expired;
        _Left ->
            ok
    end.

keep_bound(Conn, infinity) ->
    _ = erase(?BOUND(Conn)),
    ok;
keep_bound(Conn, Bound) ->
    _ = put(?BOUND(Conn), Bound),
    ok.

-spec stats(gen_server:server_ref()) -> #{atom() => term()}.
stats(Pool) ->
    gen_server:call(Pool, stats).

-spec cluster(gen_server:server_ref()) -> pid().
cluster(Pool) ->
    gen_server:call(Pool, cluster).

%% @doc The driver the pool opens its connections with, the `driver' option.
%% It is fixed for the pool's life. A pool on this node keeps it in
%% persistent_term, so this makes no call to the pool; a pool on another
%% node is asked.
-spec driver(gen_server:server_ref()) -> module().
driver(Pool) ->
    case persistent_term:get(driver_key(whereis_pool(Pool)), undefined) of
        undefined -> gen_server:call(Pool, driver);
        Driver -> Driver
    end.

whereis_pool(Pid) when is_pid(Pid) -> Pid;
whereis_pool(Name) when is_atom(Name) -> whereis(Name);
whereis_pool({global, Name}) -> global:whereis_name(Name);
whereis_pool({via, Module, Name}) -> Module:whereis_name(Name);
whereis_pool({Name, Node}) when is_atom(Name), Node =:= node() -> whereis(Name);
whereis_pool(_Remote) -> undefined.

driver_key(Pool) -> {?MODULE, driver, Pool}.

%%%=============================================================================
%%% gen_server
%%%=============================================================================

init(Config) ->
    process_flag(trap_exit, true),
    case eysql_cluster:start_link(Config) of
        {ok, Cluster} ->
            State = #state{config = Config, cluster = Cluster},
            %% driver/1, which every transaction calls, reads the driver
            %% from persistent_term. A put copies the node's table of
            %% persistent terms, in time proportional to their number, and
            %% terminate/2 erases the term again; with an atom for the
            %% value, erasing frees it at once instead of starting a global
            %% garbage collection. Both happen once per pool. A pool killed
            %% outright runs no terminate/2 and leaves the term behind.
            persistent_term:put(driver_key(self()), driver_of(State)),
            schedule_rebalance(State),
            {ok, fill(State)};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({checkout, CRef, Timeout}, From, State) ->
    Waiter = #waiter{from = From, cref = CRef, deadline = deadline(Timeout)},
    State1 = State#state{waiters = queue:in(Waiter, State#state.waiters)},
    {noreply, serve(fill(State1))};
handle_call(stats, _From, State) ->
    {reply, stats_of(State), State};
handle_call(cluster, _From, State) ->
    {reply, State#state.cluster, State};
handle_call(driver, _From, State) ->
    {reply, driver_of(State), State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({checkin, Pid, Owner, Reusable}, State) ->
    %% A late or repeated checkin from an earlier holder must not return the
    %% lease someone else now holds.
    case maps:find(Pid, State#state.leased) of
        {ok, #lease{owner = Owner}} -> {noreply, release(Pid, Reusable, State)};
        _ -> {noreply, State}
    end;
handle_cast({cancel, CRef}, State) ->
    Waiters = queue:filter(fun(#waiter{cref = R}) -> R =/= CRef end, State#state.waiters),
    State1 = State#state{waiters = Waiters},
    %% The reply may already have gone out as the caller gave up: take the
    %% lease back.
    case maps:find(CRef, State1#state.crefs) of
        {ok, Pid} -> {noreply, release(Pid, true, State1)};
        error -> {noreply, State1}
    end;
handle_cast(_Message, State) ->
    {noreply, State}.

handle_info({opened, Opener, {ok, Pid, Key}}, State) ->
    true = link(Pid),
    Opener ! {ack, self()},
    Conn = #conn{pid = Pid, key = Key, expires_at = eysql_util:now_ms() + lifetime(State)},
    State1 = hook_worked(Key, reported(Opener, State)),
    {noreply, serve(State1#state{idle = [Conn | State1#state.idle]})};
handle_info({opened, Opener, {error, _} = Error}, State) ->
    {noreply, open_failed(Opener, Error, State)};
%% The connection opened, and failed after_connect; the opener has closed
%% it. The host is not marked. When the opener went to that host as to any
%% other, others may well pass the hook: a replacement opens at once, and
%% keeps off the host, as new connections now do for a while, so that no
%% waiter fails for one host's failure. When the opener had to take the
%% host because the pick offered no other, the open failed as a failed
%% connect does.
handle_info({after_connect_failed, Opener, Key, Reason, Stack, Avoided}, State) ->
    State1 = hook_failed(Key, Reason, Stack, State),
    case Avoided of
        false -> {noreply, serve(open_one(reported(Opener, State1)))};
        true -> {noreply, open_failed(Opener, {error, {after_connect, Reason}}, State1)}
    end;
handle_info(refill, State) ->
    {noreply, serve(fill(State#state{refill_timer = undefined}))};
%% Idle connections past their lifetime close on this tick too: the pool
%% has no other timer, and a pool that nobody checks out from would keep
%% them for ever.
handle_info(rebalance, State) ->
    schedule_rebalance(State),
    {noreply, serve(fill(rebalance(expire_idle(State))))};
%% A call on a leased connection outlived socket_timeout (see watch/1). The
%% connection's exit, which follows, brings its replacement. One that is no
%% longer leased needs nothing: its holder has died, and the pool closed
%% it, or stopped the clock too late and killed it itself.
handle_info({timeout, _Timer, {socket_timeout, Pid}}, State) ->
    _ = maps:is_key(Pid, State#state.leased) andalso exit(Pid, kill),
    {noreply, State};
handle_info({timeout, _Timer, {after_connect_run_end, Key, Last}}, State) ->
    {noreply, end_hook_run(Key, Last, State)};
handle_info({'EXIT', Cluster, Reason}, #state{cluster = Cluster} = State) ->
    {stop, {cluster_exit, Reason}, State};
handle_info({'EXIT', Pid, _Reason}, State) ->
    {noreply, serve(fill(forget(Pid, State)))};
handle_info({'DOWN', Monitor, process, Pid, _Reason}, State) ->
    {noreply, serve(fill(down(Monitor, Pid, State)))};
handle_info(_Message, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    _ = persistent_term:erase(driver_key(self())),
    stop_cluster(State#state.cluster),
    Driver = driver_of(State),
    lists:foreach(fun(#conn{pid = Pid}) -> Driver:close(Pid) end, conns(State)).

%% sys:get_status/1 and crash reports print the state, whose config holds
%% the password and TLS options. A crash reason can carry the state too, in a
%% stack trace.
format_status(Status) ->
    maps:map(fun(_Key, Value) -> eysql_util:redact(Value) end, Status).

%%%=============================================================================
%%% Checkout
%%%=============================================================================

serve(State) ->
    case queue:out(State#state.waiters) of
        {empty, _} ->
            State;
        {{value, #waiter{deadline = Deadline} = Waiter}, Waiters} ->
            case eysql_util:now_ms() > Deadline of
                true ->
                    serve(State#state{waiters = Waiters});
                false ->
                    case take_idle(State) of
                        {ok, Conn, State1} ->
                            serve(lease(Waiter, Conn, State1#state{waiters = Waiters}));
                        {none, State1} ->
                            State1
                    end
            end
    end.

take_idle(#state{idle = []} = State) ->
    {none, State};
take_idle(#state{idle = [Conn | Idle]} = State) ->
    case expired(Conn, eysql_util:now_ms()) of
        true -> take_idle(fill(close(Conn, State#state{idle = Idle})));
        false -> {ok, Conn, State#state{idle = Idle}}
    end.

lease(#waiter{from = {Owner, _} = From, cref = CRef}, #conn{pid = Pid} = Conn, State) ->
    Monitor = erlang:monitor(process, Owner),
    Lease = #lease{conn = Conn, owner = Owner, owner_monitor = Monitor, cref = CRef},
    gen_server:reply(From, {ok, Pid, bound(State)}),
    State#state{leased = maps:put(Pid, Lease, State#state.leased),
                owners = maps:put(Monitor, Pid, State#state.owners),
                crefs = maps:put(CRef, Pid, State#state.crefs)}.

%% Take a lease back without asking who returns it: a checkin has matched the
%% owner already, and a cancel finds the lease by its checkout reference.
release(Pid, Reusable, State) ->
    case unlease(Pid, State) of
        {#lease{conn = Conn}, State0} ->
            {Draining, State1} = take_draining(Pid, State0),
            case Reusable andalso not Draining andalso not expired(Conn, eysql_util:now_ms()) of
                true -> serve(State1#state{idle = [Conn | State1#state.idle]});
                false -> serve(fill(close(Conn, State1)))
            end;
        error ->
            State
    end.

%% End a lease: drop it, its owner monitor and its checkout reference.
unlease(Pid, State) ->
    case maps:take(Pid, State#state.leased) of
        {#lease{owner_monitor = Monitor, cref = CRef} = Lease, Leased} ->
            erlang:demonitor(Monitor, [flush]),
            {Lease, State#state{leased = Leased,
                                owners = maps:remove(Monitor, State#state.owners),
                                crefs = maps:remove(CRef, State#state.crefs)}};
        error ->
            error
    end.

fail_waiters(Reply, State) ->
    lists:foreach(fun(#waiter{from = From}) -> gen_server:reply(From, Reply) end,
                  queue:to_list(State#state.waiters)),
    State#state{waiters = queue:new()}.

%%%=============================================================================
%%% Opening and closing
%%%=============================================================================

%% Open connections until the pool is at its size, unless a recent failure
%% set the refill timer. Before the cluster's first discovery has finished,
%% the openers wait for it in eysql_cluster:open/1.
fill(#state{refill_timer = Timer} = State) when Timer =/= undefined ->
    State;
fill(State) ->
    #{pool_size := Size} = State#state.config,
    case Size - total(State) of
        Missing when Missing > 0 -> fill(open_one(State));
        _ -> State
    end.

total(State) ->
    length(State#state.idle) + maps:size(State#state.leased) + State#state.opening.

%% The opener takes the hosts after_connect has just failed on, to keep the
%% connection off them where the pick offers others (see
%% eysql_cluster:pick/3), and says, when the hook fails, whether it had to
%% take one of them.
open_one(#state{cluster = Cluster, config = Config} = State) ->
    Pool = self(),
    Driver = driver_of(State),
    %% A config normalized by an eysql before 0.1.2, as across a hot code
    %% upgrade, or built by hand, may have neither key.
    Hook = maps:get(after_connect, Config, undefined),
    HookTimeout = maps:get(after_connect_timeout, Config,
                           maps:get(after_connect_timeout, eysql_config:defaults())),
    Avoid = hook_avoided(State),
    {Opener, _Monitor} =
        spawn_monitor(
          fun() ->
                  case eysql_cluster:open(Cluster, Avoid) of
                      {ok, Pid, Key} ->
                          case prepare(Hook, HookTimeout, Pool, Driver, Pid) of
                              ok ->
                                  Pool ! {opened, self(), {ok, Pid, Key}},
                                  await_ack(Pool, Driver, Pid);
                              {failed, Reason, Stack} ->
                                  Pool ! {after_connect_failed, self(), Key, Reason, Stack,
                                          lists:member(Key, Avoid)};
                              pool_down ->
                                  ok
                          end;
                      {error, _} = Error ->
                          Pool ! {opened, self(), Error}
                  end
          end),
    State#state{openers = maps:put(Opener, opening, State#state.openers),
                opening = State#state.opening + 1}.

%% An open failed: the connect, or after_connect. Waiters fail only when no
%% connection can reach them: none idle and none opening.
open_failed(Opener, Error, State) ->
    State1 = reported(Opener, State),
    State2 = case State1#state.idle =:= [] andalso State1#state.opening =:= 0 of
                 true -> fail_waiters(Error, State1);
                 false -> State1
             end,
    schedule_refill(State2).

%% Run after_connect on a connection that has just opened, in the opener,
%% to which the connection is linked. The hook runs in a process of its
%% own, linked to the opener, so that the opener can stop it when it
%% overruns or the pool goes, and so that it dies if the opener is killed.
%% Meanwhile the opener traps exits, so that neither the hook's process nor
%% the connection can take it down before it reports. `pool_down' when the
%% pool has gone, since there is no one left to report to; a pool already
%% gone gets no hook run at all. On anything but `ok' the connection is
%% closed here.
prepare(undefined, _Timeout, _Pool, _Driver, _Conn) ->
    ok;
prepare(Hook, Timeout, Pool, Driver, Conn) ->
    Trapping = process_flag(trap_exit, true),
    PoolMonitor = erlang:monitor(process, Pool),
    %% The monitor's 'DOWN' for a pool already gone can still be on its way;
    %% is_process_alive/1 answers at once for a process on this node, as the
    %% pool always is.
    Outcome = case is_process_alive(Pool) of
                  false -> pool_down;
                  true -> run_prepare(Hook, Timeout, Pool, PoolMonitor, Driver, Conn)
              end,
    erlang:demonitor(PoolMonitor, [flush]),
    case Outcome of
        ok ->
            _ = process_flag(trap_exit, Trapping),
            ok;
        _ ->
            %% Close before trapping stops, so that the connection's exit
            %% cannot take the opener down before it reports.
            unlink(Conn),
            Driver:close(Conn),
            _ = process_flag(trap_exit, Trapping),
            receive {'EXIT', Conn, _} -> ok after 0 -> ok end,
            Outcome
    end.

run_prepare(Hook, Timeout, Pool, PoolMonitor, Driver, Conn) ->
    Opener = self(),
    {Runner, RunnerMonitor} =
        spawn_opt(fun() -> Opener ! {after_connect, self(), run_hook(Hook, Conn)} end, [link, monitor]),
    Outcome = receive
                  {after_connect, Runner, Result} ->
                      checked(Result, Driver, Conn);
                  {'DOWN', RunnerMonitor, process, Runner, Reason} ->
                      %% Killed by an exit signal: run_hook/2 catches the rest.
                      {failed, {exit, Reason}, []};
                  {'DOWN', PoolMonitor, process, Pool, _} ->
                      pool_down
              after Timeout ->
                      %% Not `timeout', which a hook can return itself.
                      {failed, {after_connect_timeout, Timeout}, []}
              end,
    unlink(Runner),
    exit(Runner, kill),
    erlang:demonitor(RunnerMonitor, [flush]),
    receive {'EXIT', Runner, _} -> ok after 0 -> ok end,
    Outcome.

%% Runs in the hook's process. `ok', or a tuple tagged `ok' such as epgsql's
%% query results, lets the connection in. An exception's stack trace is
%% kept for the log: the hook's own frames, without the arguments a frame
%% can carry, which may be the hook's data.
run_hook(Hook, Conn) ->
    try call_hook(Hook, Conn) of
        ok -> ok;
        Ok when tuple_size(Ok) >= 2, element(1, Ok) =:= ok -> ok;
        {error, Reason} -> {failed, Reason, []};
        Other -> {failed, {bad_return, Other}, []}
    catch
        Class:Reason:Stack ->
            Own = lists:takewhile(fun(Frame) -> element(1, Frame) =/= ?MODULE end, Stack),
            {failed, {Class, Reason}, [without_args(Frame) || Frame <- Own]}
    end.

call_hook(Fun, Conn) when is_function(Fun, 1) -> Fun(Conn);
call_hook({Module, Function, Args}, Conn) -> apply(Module, Function, [Conn | Args]).

without_args({Module, Function, Args, Location}) when is_list(Args) ->
    {Module, Function, length(Args), Location};
without_args(Frame) ->
    Frame.

%% A hook that says `ok' must leave the connection open, and outside a
%% transaction, as eysql:with_connection/3 checks one before it goes back
%% to the pool. The status is read without a round trip.
checked(ok, Driver, Conn) ->
    receive
        {'EXIT', Conn, Why} -> {failed, {connection_lost, Why}, []}
    after 0 ->
            case Driver:transaction_status(Conn) of
                idle -> ok;
                Status -> {failed, {transaction_status, Status}, []}
            end
    end;
checked(Failed, _Driver, _Conn) ->
    Failed.

%% An opener has reported how its connect went. It stays in `openers' until
%% its monitor fires.
reported(Opener, State) ->
    case maps:find(Opener, State#state.openers) of
        {ok, opening} ->
            State#state{openers = maps:put(Opener, reported, State#state.openers),
                        opening = State#state.opening - 1};
        _ ->
            State
    end.

%% The connection is linked to the opener. Wait for the pool to link it too
%% before exiting, so it is never unowned; close it if the pool is gone.
await_ack(Pool, Driver, Pid) ->
    receive
        {ack, Pool} -> ok
    after ?ACK_TIMEOUT ->
            Driver:close(Pid)
    end.

%% The connection is unlinked, so no EXIT follows to forget it: drop it from
%% `draining' here.
close(#conn{pid = Pid}, State) ->
    unlink(Pid),
    (driver_of(State)):close(Pid),
    State#state{draining = maps:remove(Pid, State#state.draining)}.

%% A connection's process exited: epgsql's does when its socket closes or
%% fails. Nothing tells the cluster: the connect that replaces it finds out
%% whether the host is still there.
forget(Pid, State) ->
    Idle = [Conn || #conn{pid = P} = Conn <- State#state.idle, P =/= Pid],
    State1 = case unlease(Pid, State) of
                 {_Lease, Unleased} -> Unleased;
                 error -> State
             end,
    State1#state{idle = Idle, draining = maps:remove(Pid, State1#state.draining)}.

%% A monitored process went down: an opener or a lease owner.
down(Monitor, Pid, State) ->
    case maps:take(Pid, State#state.openers) of
        {opening, Openers} ->
            %% Died before reporting.
            schedule_refill(State#state{openers = Openers, opening = State#state.opening - 1});
        {reported, Openers} ->
            State#state{openers = Openers};
        error ->
            owner_down(Monitor, State)
    end.

owner_down(Monitor, State) ->
    case maps:find(Monitor, State#state.owners) of
        {ok, Pid} ->
            {#lease{conn = Conn}, State1} = unlease(Pid, State),
            close(Conn, State1);
        error ->
            State
    end.

schedule_refill(#state{refill_timer = undefined} = State) ->
    State#state{refill_timer = erlang:send_after(?REFILL_DELAY, self(), refill)};
schedule_refill(State) ->
    State.

%% Stop the cluster whatever the pool's reason, and wait so that it is gone,
%% any probe it has running with it, when stop/1 returns. The cluster traps
%% exits and takes an exit signal from the pool, its parent, as an order to
%% stop. Safe when it is already dead.
stop_cluster(Cluster) ->
    unlink(Cluster),
    Monitor = erlang:monitor(process, Cluster),
    exit(Cluster, shutdown),
    receive
        {'DOWN', Monitor, process, Cluster, _} -> ok
    after ?STOP_TIMEOUT ->
            exit(Cluster, kill),
            _ = erlang:demonitor(Monitor, [flush]),
            ok
    end.

%%%=============================================================================
%%% Lifetime and rebalancing
%%%=============================================================================

expire_idle(State) ->
    Now = eysql_util:now_ms(),
    {Expired, Fresh} = lists:partition(fun(Conn) -> expired(Conn, Now) end, State#state.idle),
    lists:foldl(fun close/2, State#state{idle = Fresh}, Expired).

%% Move up to `rebalance_batch' connections. First strays, those on hosts
%% the cluster no longer allows: idle ones close now, and leased ones are
%% marked to close when they come back. Each counts as one move, but one
%% already marked does not count again. With no strays, idle connections on
%% the busiest host new connections may go to close, when it holds at least
%% two more than the quietest. Replacements go wherever the cluster picks.
%%
%% The cluster keeps a host allowed once it fails, and counts it as up again
%% only once a connection to it opens: as a rule its probe's, at the first
%% refresh after its delay (see eysql_cluster:snapshot/1). So a failure
%% alone moves nothing, and nor does the mere end of a delay: connections
%% move towards a host only when it has shown it takes them.
%%
%% Failed hosts, down, standbys or rejected, take no part in finding the
%% busiest and the quietest either. They hold no connections while they are
%% out, or few, so connections closed to make room on them would only
%% reopen on the hosts they left, every interval. New connections leave
%% failed hosts out, so the placement holds one only in the cluster's last
%% resort, the first seed regardless of its failures; then every host has
%% failed, and nothing moves this way until one is found back.
%%
%% With `load_balance' false, and wherever new connections go to the seeds,
%% the placement is the one seed that takes them, in order, so there is
%% nothing to spread: connections gather on the first host that works.
%%
%% A host where after_connect has just failed, which new connections keep
%% off (see hook_avoided/1), takes no part in finding the busiest either,
%% though the cluster has not marked it. Otherwise it would be the quietest,
%% and each rebalance would close good connections to make room on a host
%% where the hook fails. Once that delay is over it takes part again, so a
%% rebalance tries it with up to `rebalance_batch' connections: if the hook
%% still fails, they reopen elsewhere and the delay starts again; if it
%% passes, connections move back to the host.
rebalance(State) ->
    #{rebalance_batch := Batch} = State#state.config,
    try eysql_cluster:snapshot(State#state.cluster) of
        #{allowed := []} ->
            State;
        #{allowed := Allowed, placement := Placement, counts := Counts,
          failed := Failed, read_only := ReadOnly, rejected := Rejected} ->
            case strays(Allowed, State) of
                {[], []} ->
                    Out = Failed ++ ReadOnly ++ Rejected ++ hook_avoided(State),
                    Busiest = busiest(Placement -- Out, Counts, State#state.idle),
                    close_idle(lists:sublist(Busiest, Batch), State#state{draining = #{}});
                {IdleStrays, BusyStrays} ->
                    %% A mark on a connection whose host is allowed again
                    %% lapses.
                    Marked = maps:with(BusyStrays, State#state.draining),
                    Close = lists:sublist(IdleStrays, Batch),
                    Unmarked = [Pid || Pid <- BusyStrays, not maps:is_key(Pid, Marked)],
                    Mark = lists:sublist(Unmarked, Batch - length(Close)),
                    Draining = maps:merge(Marked, maps:from_keys(Mark, true)),
                    close_idle(Close, State#state{draining = Draining})
            end
    catch
        exit:_ -> State
    end.

strays(Allowed, State) ->
    Stray = fun(#conn{key = Key}) -> not lists:member(Key, Allowed) end,
    Idle = [Conn || Conn <- State#state.idle, Stray(Conn)],
    Busy = [Pid || #lease{conn = #conn{pid = Pid} = Conn} <- maps:values(State#state.leased), Stray(Conn)],
    {Idle, Busy}.

take_draining(Pid, State) ->
    case maps:take(Pid, State#state.draining) of
        {true, Draining} -> {true, State#state{draining = Draining}};
        error -> {false, State}
    end.

busiest([], _Counts, _Idle) ->
    [];
busiest(Hosts, Counts, Idle) ->
    Loads = [{maps:get(Key, Counts, 0), Key} || Key <- Hosts],
    {Max, MaxKey} = lists:max(Loads),
    {Min, _} = lists:min(Loads),
    case Max - Min > 1 of
        true -> [Conn || #conn{key = Key} = Conn <- Idle, Key =:= MaxKey];
        false -> []
    end.

close_idle(Conns, State) ->
    Pids = [Pid || #conn{pid = Pid} <- Conns],
    Idle = [Conn || #conn{pid = Pid} = Conn <- State#state.idle, not lists:member(Pid, Pids)],
    lists:foldl(fun close/2, State#state{idle = Idle}, Conns).

%%%=============================================================================
%%% Logging after_connect's failures
%%%=============================================================================

%% A hook that fails, fails on every connection as a rule, so only the first
%% failure of a run on each host is logged, with its reason, by the rules
%% eysql_cluster logs a host's failed connects by. The run ends only once a
%% connection to the host has passed the hook since its last failure and
%% the window has passed since that failure. Per host, since a hook can
%% fail on one server only, as on one that is slow to answer.
hook_failed(Key, Reason, Stack, #state{hook_runs = Runs} = State) ->
    Now = eysql_util:now_ms(),
    case maps:find(Key, Runs) of
        {ok, {Count, _Last, _Worked}} ->
            State#state{hook_runs = maps:put(Key, {Count + 1, Now, false}, Runs)};
        error ->
            log_hook_failure(Key, Reason, Stack, State),
            State#state{hook_runs = maps:put(Key, {1, Now, false}, Runs)}
    end.

%% A connection to `Key' passed the hook (or opened, with no hook set). The
%% first since the run's last failure ends it once the window has passed
%% since that failure: now, or on a timer.
hook_worked(Key, #state{hook_runs = Runs} = State) ->
    case maps:find(Key, Runs) of
        {ok, {Count, Last, false}} ->
            State1 = State#state{hook_runs = maps:put(Key, {Count, Last, true}, Runs)},
            case Last + log_window(State) - eysql_util:now_ms() of
                Wait when Wait > 0 ->
                    _ = erlang:start_timer(Wait, self(), {after_connect_run_end, Key, Last}),
                    State1;
                _ ->
                    end_hook_run(Key, Last, State1)
            end;
        _ ->
            State
    end.

%% The window has passed since `Key''s failure at `Last'. The run ends if
%% that is still its last failure and a connection has passed the hook
%% since; a timer left over from an earlier failure finds neither.
end_hook_run({Host, Port} = Key, Last, #state{hook_runs = Runs} = State) ->
    case maps:find(Key, Runs) of
        {ok, {Count, Last, true}} ->
            ?LOG_INFO("eysql: after_connect works on connections to ~s:~b again after ~b failures, "
                      "with none in the last ~b ms", [Host, Port, Count, log_window(State)]),
            State#state{hook_runs = maps:remove(Key, Runs)};
        _ ->
            State
    end.

log_hook_failure({Host, Port}, Reason, Stack, State) ->
    ?LOG_WARNING("eysql: after_connect failed on a new connection to ~s:~b~ts~ts; closing the connection, "
                 "keeping the pool's new connections off ~s:~b for ~b ms where it has another server of "
                 "the same choice, without marking it failed, and not logging after_connect's failures "
                 "there again until it has gone ~b ms without one",
                 [Host, Port, hook_why(Reason, State), raised_at(Stack), Host, Port, avoid_delay(State),
                  log_window(State)]).

%% The reason, with any password in it hidden, as for a failed connect.
hook_why({after_connect_timeout, Ms}, _State) ->
    io_lib:format(" (still running after ~p ms, its after_connect_timeout)", [Ms]);
hook_why(Reason, _State) ->
    io_lib:format(" (~0tP)", [eysql_util:redact_reason(Reason), 30]).

%% Where an exception came from: the innermost frames, without arguments.
raised_at([]) -> "";
raised_at(Stack) -> io_lib:format(", raised at ~0tP", [lists:sublist(Stack, 3), 20]).

log_window(#state{config = Config}) ->
    maps:get(log_window, Config, ?LOG_WINDOW).

%% The hosts where after_connect failed last, with no connection passing it
%% there since, and that failure recent: new connections keep off them where
%% the pick has other hosts to choose among, and rebalancing moves nothing
%% towards them. Recent is the cluster's failed-host delay,
%% `failed_host_reconnect_delay_secs', but at least the refill delay, so
%% that the replacement opened at once after a failure keeps off its host.
%% After that a new connection may go there again, and find out whether
%% the hook passes there now.
hook_avoided(#state{hook_runs = Runs} = State) ->
    Since = eysql_util:now_ms() - avoid_delay(State),
    [Key || {Key, {_Count, Last, false}} <- maps:to_list(Runs), Last > Since].

avoid_delay(#state{config = #{failed_host_delay := Delay}}) ->
    max(Delay, ?REFILL_DELAY).

%%%=============================================================================
%%% Helpers
%%%=============================================================================

lifetime(#state{config = #{max_lifetime := Max, lifetime_jitter := Jitter}}) ->
    Max + rand:uniform(Jitter + 1) - 1.

expired(#conn{expires_at = At}, Now) -> Now >= At.

schedule_rebalance(#state{config = #{rebalance_interval := Interval}}) ->
    erlang:send_after(Interval, self(), rebalance).

deadline(infinity) -> infinity_deadline();
deadline(Timeout) -> eysql_util:now_ms() + Timeout.

infinity_deadline() -> eysql_util:now_ms() + 1 bsl 40.

driver_of(#state{config = #{driver := Driver}}) -> Driver.

%% What a checkout hands the holder for watch/1: the pool, which kills a
%% connection whose call overruns, and the bound.
bound(#state{config = #{socket_timeout := infinity}}) -> infinity;
bound(#state{config = #{socket_timeout := Ms}}) -> {self(), Ms}.

conns(State) ->
    State#state.idle ++ [Conn || #lease{conn = Conn} <- maps:values(State#state.leased)].

stats_of(State) ->
    ByHost = lists:foldl(fun(#conn{key = Key}, Acc) -> eysql_util:incr(Key, Acc) end, #{}, conns(State)),
    #{size => maps:get(pool_size, State#state.config),
      idle => length(State#state.idle),
      leased => maps:size(State#state.leased),
      draining => maps:size(State#state.draining),
      opening => State#state.opening,
      waiting => queue:len(State#state.waiters),
      by_host => ByHost
     }.
