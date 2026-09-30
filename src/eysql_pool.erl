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
-module(eysql_pool).

-behaviour(gen_server).

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
    draining = #{} :: #{pid() => true}
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
    State1 = reported(Opener, State),
    {noreply, serve(State1#state{idle = [Conn | State1#state.idle]})};
handle_info({opened, Opener, {error, Reason}}, State) ->
    State1 = reported(Opener, State),
    %% Waiters fail only when no connection can reach them: none idle and
    %% none opening.
    State2 = case State1#state.idle =:= [] andalso State1#state.opening =:= 0 of
                 true -> fail_waiters({error, Reason}, State1);
                 false -> State1
             end,
    {noreply, schedule_refill(State2)};
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

open_one(#state{cluster = Cluster} = State) ->
    Pool = self(),
    Driver = driver_of(State),
    {Opener, _Monitor} =
        spawn_monitor(
          fun() ->
                  case eysql_cluster:open(Cluster) of
                      {ok, Pid, Key} ->
                          Pool ! {opened, self(), {ok, Pid, Key}},
                          await_ack(Pool, Driver, Pid);
                      {error, _} = Error ->
                          Pool ! {opened, self(), Error}
                  end
          end),
    State#state{openers = maps:put(Opener, opening, State#state.openers),
                opening = State#state.opening + 1}.

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
rebalance(State) ->
    #{rebalance_batch := Batch} = State#state.config,
    try eysql_cluster:snapshot(State#state.cluster) of
        #{allowed := []} ->
            State;
        #{allowed := Allowed, placement := Placement, counts := Counts,
          failed := Failed, read_only := ReadOnly, rejected := Rejected} ->
            case strays(Allowed, State) of
                {[], []} ->
                    Out = Failed ++ ReadOnly ++ Rejected,
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
