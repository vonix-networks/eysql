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

%% @doc One process per database cluster: which servers exist, how many
%% connections this node holds to each, and which hosts have failed.
%%
%% It refreshes on a timer, early after any connection failure, and at
%% start unless `load_balance' is false. A pick also starts a refresh once
%% the refresh interval has passed, without waiting for it; with an interval
%% of 0 every pick does, and there is no timer. A pick that finds no host
%% starts none: as in the JDBC smart driver, only the interval, a connection
%% failure or a failed discovery makes a refresh due. A refresh discovers
%% servers with `yb_servers()', and probes the hosts that have failed (see
%% below). On PostgreSQL, where that function does not exist, and with
%% `load_balance' false, it stays with the configured hosts, and a refresh
%% only probes. {@link connect/1} asks it for a host and opens the
%% connection in the calling process, so a slow connect never blocks the
%% cluster process.
%%
%% A discovery asks the configured hosts, the seeds, first, each once and in
%% the order given, and then the discovered servers that are not down, as
%% the JDBC driver's checkAndRefresh dials the hosts in its URL before the
%% servers its getAllAvailableHosts lists. It never dials a server that is
%% down.
%%
%% A discovery that fails leaves the refresh due, as the JDBC driver's
%% failed refresh leaves its clock where it was: the next pick starts
%% another, though not within a second of the last refresh, and the timer
%% comes back after the full interval, not sooner. So while discovery fails,
%% it is tried again at most once a second while connections are being
%% opened, and once per interval while none are, and each of those
%% refreshes probes the failed hosts that are due. The driver would retry
%% on every connection, and un-mark its down hosts only in a refresh that
%% succeeded; this keeps a busy pool from hammering a dead cluster, and
%% still brings connections back within seconds of a full outage. A
%% discovery that succeeds probes at once the failed hosts that are due
%% and have no probe running, so that they come back with the cluster
%% rather than at the next refresh. A run of failed discoveries is logged once, as
%% a warning, and its end once, at info, by the rules for a host's failures
%% below.
%%
%% {@link connect/1} waits for the first discovery to finish, either way, as
%% the JDBC driver runs its first refresh before it picks a server. It then
%% tries each host a pick offers, at most once each, until one opens: every
%% eligible server, least loaded first, a tie going to one at random, and
%% then the seeds, as the driver's getConnection tries every eligible server
%% and its caller then makes a plain connection to the hosts in its URL.
%%
%% The seeds are taken in the order given, as that plain connection takes
%% the hosts in the URL (pgjdbc's loadBalanceHosts is false by default): a
%% connection goes to the first seed that is working, and to the next only
%% when that one fails it. With `load_balance' false the seeds are all there
%% is, so every connection goes to the first host that works; with
%% `target_session_attrs => read_write', to the first that accepts writes.
%%
%% Each server has two addresses, its host and its public IP, and which one
%% connections go to is decided as the JDBC driver's refresh decides it
%% ({@link eysql_topology:address_column/3}). The discovery process resolves
%% the name it dialled and each server's two names, and compares addresses:
%% the first server whose host is the address that answered, or whose
%% public IP is, or whose two names are one address, decides for the life
%% of the cluster process. A client outside the cluster's network reaches
%% it at a public IP, and so connects to each server's. Until a discovery
%% decides, each connects to the public IPs when every server has one and
%% every one resolves, and to the hosts otherwise.
%%
%% While no discovered server is working, a pick takes the first seed that
%% is; and while no seed is working either, the seeds regardless, as a
%% smart driver falls back to a plain connection to the hosts in its URL. It
%% does neither in the modes where the JDBC driver refuses instead of
%% falling back: `only_primary', `only_rr', and
%% `fallback_to_topology_keys_only' with topology keys under `true' or
%% `any'. There, once servers are discovered, a pick that finds no eligible
%% server fails with `{no_node_available, primary | read_replica | cluster}',
%% as the driver's getLeastLoadedServer throws "No node available", whether
%% or not the connect has tried servers already. Before a discovery has succeeded, and
%% on PostgreSQL, the seeds are all there is, and every mode takes them,
%% whatever their node type: the driver makes a plain connection to its URL
%% hosts when its refresh fails, or finds no `yb_servers()'.
%%
%% Only a failure to connect marks a host down: the failures pgjdbc reports
%% as SQLSTATE 08001, such as a refused or timed-out connect or a name that
%% does not resolve ({@link eysql_error:connect_failure/1}). A host that
%% answers but fails the connection otherwise, with a failed login, a
%% missing database, too many connections, a server starting up or shutting
%% down, or a failed TLS handshake, is not down; it is kept apart as
%% `rejected'. Either way, and for a standby (below), the host is left out
%% of picks until a refresh's probe, or a connection to it that opens,
%% finds it back. The smart drivers do that for a host that is down: they
%% mark it down, and un-mark it only at a refresh once its delay has
%% passed. A rejected host they skip only for the rest of that one
%% connection, and their next connection tries it again. Here it stays out,
%% so that a server that turns connections away, as one does while it
%% restarts, takes no new connection until a probe finds it taking them,
%% and takes them again from that refresh on. Only the seeds' last resort
%% above takes a failed host, and then only a seed, once per connection.
%% Nothing that happens on a connection once it is open marks its host: not
%% a query error, not a cancelled or slow statement, not one that
%% `socket_timeout' cuts off, and not the connection's death. Only
%% `open_failed' and the probes below mark hosts, and both follow a
%% connect.
%%
%% At each refresh the cluster probes every failed host, down, standby or
%% rejected, whose delay has passed: it connects in a separate process, with
%% the same `target_session_attrs' check as a pick's connect, and closes the
%% connection. A probe that succeeds ends the failure, as does any
%% connection to the host that opens. One that fails, or takes longer than
%% twice `connect_timeout', starts another delay, as a failure of the kind
%% its reason makes it, and starts no refresh: a server that answers 57P03
%% while it shuts down, and then refuses connections, turns from rejected
%% to down. So a host that stays failed is tried once per refresh, not once
%% per delay. There is at most one probe per host, it is not counted as a
%% connection, and it dies with the cluster. The cluster traps exits for
%% that: probes are linked to it, and it kills them when it stops.
%%
%% A standby that `target_session_attrs => read_write' turns away is
%% reachable, but has the wrong role. It is left out and probed like a host
%% that is down, so a promotion is found at the next refresh, or at once
%% through the seeds when the primary fails. {@link snapshot/1} reports it
%% apart from the hosts that are down.
%%
%% A host that fails is logged once, as a warning with the reason when the
%% caller gives one, and not again for the rest of that run of failures. A
%% standby is logged the same way, at info. The run ends once a connection to
%% the host, a probe's included, has opened since its last failure and a
%% minute has passed since that failure; that is logged at info. A failure
%% of another kind starts a new run: down, standby and rejected are each
%% logged in their own words, so that a failed login never reads as a host
%% that is down.
%%
%% A discovery that succeeds forgets the failures, runs and probes of hosts
%% that are neither servers nor seeds any more.
%%
%% Counts are per cluster process, like a smart driver's are per client:
%% connections opened through {@link connect/1} are monitored and counted
%% until they close. A host picked for a connection counts as pending until
%% the picking process reports how the connect went, or exits.
-module(eysql_cluster).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/1,
         stop/1,
         connect/1,
         open/1,
         pick/1,
         pick/2,
         opened/3,
         open_failed/2,
         open_failed/3,
         cancel/2,
         snapshot/1,
         refresh/1,
         when_ready/1
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         format_status/1
        ]).

-export_type([cluster/0, connect_spec/0, reservation/0, pick_error/0]).

-type cluster() :: pid().

%% A host picked for one connection, from {@link pick/1} until the picker
%% reports the outcome.
-type reservation() :: {reference(), eysql_topology:key()}.

-type connect_spec() :: #{driver := module(),
                          settings := map(),
                          target_session_attrs := any | read_write
                         }.

%% Why a pick found no host: none is left that the mode allows, or, in a
%% mode where the JDBC driver refuses rather than fall back to the seeds,
%% no eligible server is, which names the node type it looked for.
-type pick_error() :: no_server_available
                    | {no_node_available, primary | read_replica | cluster}.

%% Why a host is left out of picks, and not trusted with a pool's
%% connections: it is `down', it cannot be connected to; it is a standby
%% that `target_session_attrs => read_write' turned away (`read_only'); or
%% it answered and failed the connection otherwise (`rejected').
-type failure() :: down | read_only | rejected.

-define(CALL_TIMEOUT, 5000).
-define(MIN_REFRESH_GAP, 1000).

%% A run of failures ends once its host has accepted a connection and then
%% gone this long without failing. That is an order of magnitude above the
%% default back-off, so a host that keeps failing now and then stays in one
%% run. It is also how long YugabyteDB takes by default to declare a tablet
%% server dead (`tserver_unresponsive_timeout_ms'), so a server's restart is
%% one run.
%% Incidents a few minutes apart are still logged apart. Tests shorten it
%% with a `log_window' key in the normalized config; it is not an option.
-define(LOG_WINDOW, 60000).

-record(state, {
    config :: eysql_config:config(),
    servers :: [eysql_topology:server()],
    discovered = false :: boolean(),
    mode = discovering :: discovering | static,
    counts = #{} :: #{eysql_topology:key() => non_neg_integer()},
    pending = #{} :: #{eysql_topology:key() => non_neg_integer()},
    %% Picks not yet reported, and the monitor on each picker.
    reservations = #{} :: #{reference() => {eysql_topology:key(), reference()}},
    %% Each picker's monitor, to its reservation, for when the picker exits.
    pickers = #{} :: #{reference() => reference()},
    %% Hosts that failed and have not been connected to since: why, the
    %% failures in a row, and when a refresh may probe the host again.
    failures = #{} :: #{eysql_topology:key() => {failure(), pos_integer(), integer()}},
    %% The probe running for a host in `failures', and the timer that stops
    %% it. A host has at most one.
    probes = #{} :: #{eysql_topology:key() => {pid(), reference()}},
    %% Each host's current run of failures, for logging: of which kind, how
    %% many, when the last was, and whether a connection has opened since.
    %% Discovery keeps its run here too, under `discovery', as `down'.
    streaks = #{} :: #{eysql_topology:key() | discovery =>
                           {failure(), pos_integer(), integer(), boolean()}},
    conns = #{} :: #{pid() => {eysql_topology:key(), reference()}},
    %% Which address of each discovered server to connect to, as
    %% eysql_topology:address_column/3 chose it at the last discovery: the
    %% decision, which stands once made, the column in use, and why, or
    %% `none' before any discovery.
    address = {undecided, host, none} :: {eysql_topology:decision(), eysql_topology:column(),
                                         eysql_topology:address_basis() | none},
    %% The discovery running: its process, its monitor and the timer that
    %% stops it.
    refresh :: undefined | {pid(), reference(), reference()},
    refresh_timer :: undefined | reference(),
    %% When the last refresh started.
    last_refresh = undefined :: undefined | integer(),
    %% The last discovery failed, so the refresh is still due.
    stale = false :: boolean(),
    %% Ready once the first discovery has finished, either way.
    ready = false :: boolean(),
    watchers = [] :: [pid()]
}).

%%%=============================================================================
%%% API
%%%=============================================================================

%% @doc Start a cluster process from a normalized config ({@link eysql_config}).
-spec start_link(eysql_config:config()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

-spec stop(cluster()) -> ok.
stop(Cluster) ->
    gen_server:stop(Cluster).

%% @doc Open a connection to the host this cluster picks. Runs in the
%% caller; the connection is linked to the caller, as with epgsql.
%%
%% Waits first for the cluster's first discovery to finish, either way, as
%% the JDBC driver runs its first refresh before it picks; the discovery's
%% own timeout bounds the wait. Then tries hosts until one opens, each at
%% most once: every server a pick allows, least loaded first, then the
%% seeds in the order given, where the mode allows them. That is the JDBC
%% driver's getConnection loop, which tries every eligible server, followed
%% by its plain connection to the hosts in its URL.
%%
%% Returns the last connect error when every host tried failed, and
%% `{error, no_server_available}' when no host was left to try. In the modes
%% where the driver refuses rather than fall back to the seeds, once servers
%% are discovered, it returns `{error, {no_node_available, Type}}' when no
%% eligible server is left, whether or not it tried some first, as the
%% driver's last getLeastLoadedServer throws.
-spec connect(cluster()) -> {ok, pid()} | {error, term()}.
connect(Cluster) ->
    case open(Cluster) of
        {ok, Conn, _Key} -> {ok, Conn};
        {error, _} = Error -> Error
    end.

%% @doc As {@link connect/1}, also returning the host the connection went to.
-spec open(cluster()) -> {ok, pid(), eysql_topology:key()} | {error, term()}.
open(Cluster) ->
    await_ready(Cluster),
    open(Cluster, [], {error, no_server_available}).

%% `Tried' holds the hosts this open has tried, as getConnection's
%% failedHosts do: a host that failed is left out of picks anyway, but the
%% seeds' last resort takes failed seeds, and none may be tried twice.
open(Cluster, Tried, LastError) ->
    case pick(Cluster, Tried) of
        {ok, #{host := Host, port := Port}, Spec, Reservation} ->
            Result = try connect_to(Host, Port, Spec)
                     catch
                         Class:Reason:Stack ->
                             cancel(Cluster, Reservation),
                             erlang:raise(Class, Reason, Stack)
                     end,
            case Result of
                {ok, Conn} ->
                    opened(Cluster, Reservation, Conn),
                    {ok, Conn, {Host, Port}};
                {error, Why} = Error ->
                    open_failed(Cluster, Reservation, Why),
                    open(Cluster, [{Host, Port} | Tried], Error)
            end;
        {error, {no_node_available, _}} = Refused ->
            Refused;
        {error, no_server_available} when Tried =/= [] ->
            LastError;
        {error, _} = Error ->
            Error
    end.

%% Wait for the first discovery. A cluster that dies meanwhile lets the
%% caller go on to the pick, which exits as a call to it would.
await_ready(Cluster) ->
    case when_ready(Cluster) of
        ready ->
            ok;
        pending ->
            Monitor = erlang:monitor(process, Cluster),
            receive
                {eysql_cluster_ready, Cluster} -> erlang:demonitor(Monitor, [flush]), ok;
                {'DOWN', Monitor, process, Cluster, _} -> ok
            end
    end.

%% Open a connection, closing it again if its session is not the kind asked
%% for.
connect_to(Host, Port, #{driver := Driver, settings := Settings, target_session_attrs := Attrs}) ->
    case Driver:open(Host, Port, Settings) of
        {ok, Conn} ->
            case session_ok(Driver, Conn, Attrs) of
                ok ->
                    {ok, Conn};
                {error, _} = Error ->
                    unlink(Conn),
                    Driver:close(Conn),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

session_ok(_Driver, _Conn, any) ->
    ok;
session_ok(Driver, Conn, read_write) ->
    case Driver:is_primary(Conn) of
        {ok, true} -> ok;
        {ok, false} -> {error, read_only_server};
        {error, _} = Error -> Error
    end.

%% @doc Reserve the best host for a new connection. The caller must follow
%% with {@link opened/3}, {@link open_failed/2} or {@link cancel/2};
%% {@link connect/1} does. Until then the host counts as pending, unless the
%% caller exits first. As {@link pick/2} with nothing tried yet.
-spec pick(cluster()) ->
          {ok, eysql_topology:server(), connect_spec(), reservation()}
        | {error, pick_error()}.
pick(Cluster) ->
    pick(Cluster, []).

%% @doc As {@link pick/1}, leaving out the hosts in `Tried', those the caller
%% has tried already for this connection. `{error, no_server_available}'
%% when no host is left that the mode allows; in a mode that refuses rather
%% than fall back to the seeds, once servers are discovered,
%% `{error, {no_node_available, Type}}' instead, `Type' being `primary'
%% under `only_primary', `read_replica' under `only_rr', and `cluster'
%% under `fallback_to_topology_keys_only'.
-spec pick(cluster(), [eysql_topology:key()]) ->
          {ok, eysql_topology:server(), connect_spec(), reservation()}
        | {error, pick_error()}.
pick(Cluster, Tried) ->
    Ref = make_ref(),
    try gen_server:call(Cluster, {pick, Ref, Tried}, ?CALL_TIMEOUT) of
        {ok, Server, Spec} -> {ok, Server, Spec, {Ref, eysql_topology:key(Server)}};
        {error, _} = Error -> Error
    catch
        exit:{timeout, _} = Reason:Stack ->
            %% The cluster may still reserve a host for this call; release it.
            gen_server:cast(Cluster, {cancel, Ref}),
            erlang:raise(exit, Reason, Stack)
    end.

%% @doc The connection for `Reservation' is open; count it until its
%% process exits.
-spec opened(cluster(), reservation(), pid()) -> ok.
opened(Cluster, Reservation, Conn) ->
    gen_server:cast(Cluster, {opened, Reservation, Conn}).

%% @doc The connection for `Reservation' could not be made; picks leave its
%% host out until a refresh finds it back, as after a reason that
%% {@link eysql_error:connect_failure/1} calls `unreachable'. As
%% {@link open_failed/3}, with no reason to log.
-spec open_failed(cluster(), reservation()) -> ok.
open_failed(Cluster, Reservation) ->
    open_failed(Cluster, Reservation, undefined).

%% @doc As {@link open_failed/2}, failed with `Reason', which decides what
%% the host counts as. An `unreachable' reason
%% ({@link eysql_error:connect_failure/1}) marks it down,
%% `read_only_server', a standby turned away by
%% `target_session_attrs => read_write', marks it a standby, and any other
%% reason, such as a failed login, marks it rejected. Whichever it is, picks
%% leave the host out until a refresh's probe, or a connection to it that
%% opens, finds it back. The first failure of a run is logged with the
%% reason, a standby at info and the others as warnings.
-spec open_failed(cluster(), reservation(), term()) -> ok.
open_failed(Cluster, Reservation, Reason) ->
    gen_server:cast(Cluster, {open_failed, Reservation, Reason}).

%% @doc Give `Reservation' back with no outcome, for example because the
%% connect raised. Its host is not left out.
-spec cancel(cluster(), reservation()) -> ok.
cancel(Cluster, {Ref, _Key}) ->
    gen_server:cast(Cluster, {cancel, Ref}).

%% @doc The cluster's current view.
%%
%% <ul>
%% <li>`placement': the hosts a new connection may go to now, as a pick
%%     chooses them: failed hosts left out, of any kind, node type and
%%     topology preference applied; the first working seed while no
%%     discovered server is working; and, while no seed is working either,
%%     the first seed regardless. With `load_balance' false, the first host
%%     that is working. In the modes that refuse the seeds once servers are
%%     discovered, it is empty while no eligible server is working;</li>
%% <li>`allowed': the hosts a pool may keep connections on, so it can drain
%%     the others. A failure alone never takes a host out: a failed host
%%     counts as up again once a connection to it has opened, a probe's
%%     included. A host is allowed while it is in the level new connections
%%     would use on that reckoning, or in the level they would use if no
%%     host had failed (see {@link eysql_topology:allowed/5}). The seeds,
%%     taken in order, are allowed while no discovered server is up on that
%%     reckoning, unless the mode refuses them: the first seed, and the
%%     first seed that is up, or every seed while none is up. A host
%%     discovery no longer lists is in neither;</li>
%% <li>`failed': the hosts that are down: they could not be connected to,
%%     and have not been since;</li>
%% <li>`read_only': the standbys that `target_session_attrs => read_write'
%%     turned away and that have not been connected to since. They are left
%%     out as the hosts in `failed' are;</li>
%% <li>`rejected': the hosts that answered and failed a connection
%%     otherwise, such as with a failed login, and that no connection has
%%     opened to since. They are left out and probed as the hosts in
%%     `failed' are;</li>
%% <li>`counts': open connections per host, and `pending' picks per host not
%%     yet reported. A probe's connection is in neither.</li>
%% </ul>
-spec snapshot(cluster()) -> #{atom() => term()}.
snapshot(Cluster) ->
    gen_server:call(Cluster, snapshot, ?CALL_TIMEOUT).

%% @doc Refresh now: read `yb_servers()' again, unless on PostgreSQL, and
%% probe the failed hosts whose delay has passed.
-spec refresh(cluster()) -> ok.
refresh(Cluster) ->
    Cluster ! refresh,
    ok.

%% @doc `ready' if the first discovery has finished (or there is none to do);
%% otherwise `pending', and the caller gets `{eysql_cluster_ready, Cluster}'
%% when it finishes. {@link open/1} waits for this, so that a pick follows
%% the mode's rules for discovered servers, and a pool's first connections
%% go to the servers rather than all to the seeds.
-spec when_ready(cluster()) -> ready | pending.
when_ready(Cluster) ->
    gen_server:call(Cluster, {when_ready, self()}, ?CALL_TIMEOUT).

%%%=============================================================================
%%% gen_server
%%%=============================================================================

init(Config) ->
    %% Probes are linked to the cluster so that they die with it; trapping
    %% keeps a probe's death from taking the cluster with it.
    process_flag(trap_exit, true),
    #{seeds := Seeds, load_balance := LoadBalance} = Config,
    State = #state{config = Config, servers = Seeds},
    case LoadBalance of
        false ->
            %% Nothing to discover, but failed hosts are probed on the same
            %% schedule.
            Static = State#state{mode = static, ready = true, last_refresh = eysql_util:now_ms()},
            {ok, schedule_refresh(Static)};
        _ ->
            self() ! refresh,
            {ok, State}
    end.

handle_call({pick, Ref, Tried}, {Picker, _}, State0) ->
    State = refresh_if_due(State0),
    case choose(State, Tried) of
        {ok, Server} ->
            Key = eysql_topology:key(Server),
            %% Released when the picker reports, or if it exits first.
            Monitor = erlang:monitor(process, Picker),
            Reservations = maps:put(Ref, {Key, Monitor}, State#state.reservations),
            State1 = State#state{pending = eysql_util:incr(Key, State#state.pending),
                                 reservations = Reservations,
                                 pickers = maps:put(Monitor, Ref, State#state.pickers)},
            {reply, {ok, Server, connect_spec(State)}, State1};
        {error, _} = Error ->
            %% Nothing is brought forward: the JDBC driver throws here
            %% without forcing a refresh.
            {reply, Error, State}
    end;
handle_call(snapshot, _From, State) ->
    {reply, snapshot_of(State), State};
handle_call({when_ready, _Pid}, _From, #state{ready = true} = State) ->
    {reply, ready, State};
handle_call({when_ready, Pid}, _From, State) ->
    {reply, pending, State#state{watchers = [Pid | State#state.watchers]}};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({opened, {Ref, Key}, Conn}, State) ->
    State1 = recovered(Key, release(Ref, State)),
    Monitor = erlang:monitor(process, Conn),
    {noreply, State1#state{counts = eysql_util:incr(Key, State1#state.counts),
                           conns = maps:put(Conn, {Key, Monitor}, State1#state.conns)
                          }};
handle_cast({open_failed, {Ref, Key}, Reason}, State) ->
    %% Any failed connect forces a refresh, as the driver's getConnection
    %% sets forceRefreshOnce for every SQLException, before it looks at the
    %% state.
    {noreply, maybe_refresh(mark_failed(Key, kind(Reason), Reason, release(Ref, State)))};
handle_cast({cancel, Ref}, State) ->
    {noreply, release(Ref, State)};
handle_cast(_Message, State) ->
    {noreply, State}.

%% Asked for by refresh/1, or at start. A scheduled refresh stays scheduled
%% until this one schedules the next: when its discovery finishes, or at
%% once with nothing to discover.
handle_info(refresh, State) ->
    {noreply, start_refresh(State)};
handle_info({timeout, Timer, refresh}, #state{refresh_timer = Timer} = State) ->
    {noreply, start_refresh(State#state{refresh_timer = undefined})};
handle_info({discovered, Pid, Result}, #state{refresh = {Pid, Monitor, Timer}} = State) ->
    erlang:demonitor(Monitor, [flush]),
    _ = erlang:cancel_timer(Timer),
    {noreply, discovered(Result, State#state{refresh = undefined})};
handle_info({refresh_timeout, Pid}, #state{refresh = {Pid, Monitor, _}} = State) ->
    erlang:demonitor(Monitor, [flush]),
    exit(Pid, kill),
    {noreply, discovered({error, timeout}, State#state{refresh = undefined})};
handle_info({'DOWN', Monitor, process, Pid, Reason}, #state{refresh = {Pid, Monitor, Timer}} = State) ->
    _ = erlang:cancel_timer(Timer),
    {noreply, discovered({error, Reason}, State#state{refresh = undefined})};
handle_info({'DOWN', Monitor, process, Pid, _Reason}, State) ->
    case maps:take(Pid, State#state.conns) of
        {{Key, _}, Conns} ->
            {noreply, State#state{conns = Conns, counts = eysql_util:decr(Key, State#state.counts)}};
        error ->
            {noreply, abandoned(Monitor, State)}
    end;
handle_info({probed, Pid, Key, Result}, State) ->
    case maps:find(Key, State#state.probes) of
        {ok, {Pid, Timer}} ->
            _ = erlang:cancel_timer(Timer),
            {noreply, probed(Key, Result, State#state{probes = maps:remove(Key, State#state.probes)})};
        _ ->
            {noreply, State}
    end;
handle_info({timeout, Timer, {probe_timeout, Key}}, State) ->
    case maps:find(Key, State#state.probes) of
        {ok, {_Pid, Timer}} ->
            {noreply, probed(Key, {error, probe_timeout}, stop_probe(Key, State))};
        _ ->
            {noreply, State}
    end;
%% A probe died before it reported, for example with its connection. One
%% that reported, or was stopped, is no longer in `probes'.
handle_info({'EXIT', Pid, Reason}, State) ->
    case [Key || {Key, {P, _}} <- maps:to_list(State#state.probes), P =:= Pid] of
        [Key] -> {noreply, probed(Key, {error, {probe_exit, Reason}}, stop_probe(Key, State))};
        [] -> {noreply, State}
    end;
handle_info({timeout, _Timer, {streak_end, Key, Last}}, State) ->
    {noreply, end_streak(Key, Last, State)};
handle_info(_Message, State) ->
    {noreply, State}.

%% Probes are linked to the cluster, so they die with it when it exits with
%% any reason but `normal', which gen_server:stop/1 uses; stop them here for
%% that. The discovery process holds a connection too.
terminate(_Reason, State) ->
    _ = lists:foldl(fun stop_probe/2, State, maps:keys(State#state.probes)),
    case State#state.refresh of
        {Pid, _Monitor, _Timer} -> exit(Pid, kill);
        undefined -> ok
    end,
    ok.

%% sys:get_status/1 and crash reports print the state, whose config holds
%% the password and TLS options. A crash reason can carry the state too, in a
%% stack trace, and a sys log the connect specs that picks return.
format_status(Status) ->
    maps:map(fun(_Key, Value) -> eysql_util:redact(Value) end, Status).

%%%=============================================================================
%%% Host selection
%%%=============================================================================

%% The least loaded candidate, a tie going to one at random. With none, the
%% modes that refuse the seeds say so as the JDBC driver does, naming the
%% node type they looked for.
choose(State, Tried) ->
    case eysql_topology:choose(placement(State, Tried), fun(Server) -> load(Server, State) end) of
        {ok, _} = Chosen -> Chosen;
        {error, no_server_available} = None -> refusal(State, None)
    end.

%% getLeastLoadedServer throws "No node available in the given placements
%% for the primary cluster", or read-replica, or entire, in these modes
%% (TopologyAwareLoadBalancer; ClusterAwareLoadBalancer throws the same
%% without the placements for the first two).
refusal(State, None) ->
    case seeds_allowed(State) of
        true -> None;
        false -> {error, {no_node_available, refused_type(State#state.config)}}
    end.

refused_type(#{load_balance := only_primary}) -> primary;
refused_type(#{load_balance := only_rr}) -> read_replica;
refused_type(_Config) -> cluster.

%% The candidates for a new connection, leaving out the hosts in `Tried',
%% from the first of these tiers that has any: the discovered servers that
%% are working; the first seed that is working, since a client that cannot
%% reach the addresses servers advertise may still reach a seed; and last,
%% the first seed whatever its failures. The seeds are the smart drivers'
%% plain connection to the hosts in their URL, which they fall back to when
%% no server they would pick is up. The last tier keeps work going after a
%% cluster-wide blip, and it is how a lone PostgreSQL host that comes back,
%% or a standby promoted when the primary fails, takes connections before a
%% refresh has probed it. See tiers/1 for when the seeds take part.
%%
%% A host that has failed, whatever the kind, is never a candidate
%% otherwise, even once its delay has passed: only a refresh's probe, or a
%% connection that opens, brings it back. So a client that reaches the
%% servers only through a load balancer among the seeds spends no connect
%% attempt on the servers' addresses once they have failed, until a
%% refresh finds them back; and a server that turns connections away while
%% it restarts takes none until a probe finds it taking them.
placement(#state{config = #{seeds := Seeds}} = State, Tried) ->
    Untried = fun(Server) -> not lists:member(eysql_topology:key(Server), Tried) end,
    Working = working(State),
    Available = fun(Server) -> Untried(Server) andalso Working(Server) end,
    LastResort = case seeds_allowed(State) of
                     true -> [{Seeds, plain, Untried}];
                     false -> []
                 end,
    Tiers = [{Hosts, Filter, Available} || {Hosts, Filter} <- tiers(State)] ++ LastResort,
    first_candidates(Tiers, State).

first_candidates([], _State) ->
    [];
first_candidates([{Hosts, Filter, Available} | Rest], State) ->
    case candidates(Hosts, Filter, Available, State) of
        [] -> first_candidates(Rest, State);
        Candidates -> Candidates
    end.

%% The hosts new connections may go to, in the order they are tried, each
%% with how it is narrowed: `typed' by `load_balance' and the topology keys,
%% `plain' not at all, but taken in order. The discovered servers come
%% first, once there are any. The seeds follow where seeds_allowed/1 lets
%% them: the smart drivers' plain connection to their URL hosts looks at
%% neither node type nor zone, and a seed has neither. Before discovery, and
%% on PostgreSQL, the seeds are all there is.
tiers(#state{discovered = Discovered, servers = Servers, config = #{seeds := Seeds}} = State) ->
    Typed = case Discovered of
                true -> [{Servers, typed}];
                false -> []
            end,
    case seeds_allowed(State) of
        true -> Typed ++ [{Seeds, plain}];
        false -> Typed
    end.

%% Whether the seeds may take connections: always before a discovery has
%% succeeded, since the JDBC driver makes a plain connection to its URL
%% hosts whenever its refresh fails or finds no yb_servers(); after one,
%% unless the mode is one the driver refuses in.
seeds_allowed(#state{discovered = false}) -> true;
seeds_allowed(#state{config = Config}) -> not refuses(Config).

%% The modes in which the JDBC driver refuses a connection rather than make
%% a plain one to the hosts in its URL: getLeastLoadedServer throws
%% IllegalStateException when it finds no server under only-primary or
%% only-rr, and under any (which `true' also means) with
%% fallback-to-topology-keys-only. The last is honoured only by the
%% TopologyAwareLoadBalancer, the one topology keys select; without keys,
%% the ClusterAwareLoadBalancer ignores it, and the driver falls back.
refuses(#{load_balance := LoadBalance}) when LoadBalance =:= only_primary;
                                              LoadBalance =:= only_rr ->
    true;
refuses(#{load_balance := LoadBalance, fallback_to_topology_keys_only := true,
          topology_keys := [_ | _]}) when LoadBalance =:= true; LoadBalance =:= any ->
    true;
refuses(_Config) ->
    false.

%% The hosts a pool keeps connections on, a host being up while it is
%% working (see working/1). A host whose delay has merely run out has not
%% shown that it takes connections again; if it still does not, a working
%% connection closed to move towards it is lost, or reopens where it was
%% after failing there. It takes a connection that opens, a probe's at a
%% refresh as a rule, to bring connections back to a host.
%%
%% The tiers are walked as placement/2 walks them, but for the last resort.
%% Each tier with no host up keeps its second level, the one
%% eysql_topology:allowed/5 keeps whatever is up, and the first tier with a
%% host up keeps both of its levels; for the seeds, in order, those are the
%% first seed and the first seed that is up, and every seed while none is.
%% So after a cluster-wide blip the servers keep their connections and those
%% opened on the seeds meanwhile stay, until a server is up again; while no
%% host at all is up, nothing moves; and once an earlier seed is up again,
%% connections on a later one are no longer kept. In a mode that refuses the
%% seeds, connections on them are not kept once servers are discovered.
allowed(State) ->
    Allowed = allowed_tiers(tiers(State), working(State), State),
    lists:usort([eysql_topology:key(Server) || Server <- Allowed]).

allowed_tiers([], _Up, _State) ->
    [];
allowed_tiers([{Hosts, Filter} | Rest], Up, State) ->
    Kept = allowed(Hosts, Filter, Up, State),
    case candidates(Hosts, Filter, Up, State) of
        [] -> Kept ++ allowed_tiers(Rest, Up, State);
        _ -> Kept
    end.

allowed(Hosts, plain, Up, _State) ->
    case [Host || Host <- Hosts, Up(Host)] of
        [] -> Hosts;
        [FirstUp | _] -> [hd(Hosts), FirstUp]
    end;
allowed(Hosts, typed, Up, #state{config = Config}) ->
    #{load_balance := LoadBalance, topology_keys := Keys,
      fallback_to_topology_keys_only := FallbackOnly} = Config,
    eysql_topology:allowed(Hosts, Up, LoadBalance, Keys, FallbackOnly).

%% The seeds are taken in order, as a plain connection takes the hosts in
%% its URL: only the first that is available is a candidate.
candidates(Hosts, plain, Available, _State) ->
    case [Host || Host <- Hosts, Available(Host)] of
        [First | _] -> [First];
        [] -> []
    end;
candidates(Hosts, typed, Available, #state{config = Config}) ->
    #{load_balance := LoadBalance, topology_keys := Keys,
      fallback_to_topology_keys_only := FallbackOnly} = Config,
    eysql_topology:candidates(Hosts, Available, LoadBalance, Keys, FallbackOnly).

%% Hosts that have not failed, of any kind, since a connection to them last
%% opened: those picks may go to, and those a pool counts as up. A rejected
%% host is not one, though the driver skips it only for the connection it
%% failed: a server that turns connections away while it restarts takes
%% none until a probe finds it taking them.
working(#state{failures = Failures}) ->
    fun(Server) -> not maps:is_key(eysql_topology:key(Server), Failures) end.

load(Server, #state{counts = Counts, pending = Pending}) ->
    Key = eysql_topology:key(Server),
    maps:get(Key, Counts, 0) + maps:get(Key, Pending, 0).

%% Settle a reservation: its host is no longer pending and its picker no
%% longer watched. A reservation already settled is ignored.
release(Ref, #state{reservations = Reservations} = State) ->
    case maps:take(Ref, Reservations) of
        {{Key, Monitor}, Rest} ->
            erlang:demonitor(Monitor, [flush]),
            State#state{reservations = Rest,
                        pickers = maps:remove(Monitor, State#state.pickers),
                        pending = eysql_util:decr(Key, State#state.pending)};
        error ->
            State
    end.

%% A process that picked a host exited before reporting how the connect went.
abandoned(Monitor, #state{pickers = Pickers} = State) ->
    case maps:find(Monitor, Pickers) of
        {ok, Ref} -> release(Ref, State);
        error -> State
    end.

%% A host failed, of the kind given: it is left out of picks, and the first
%% refresh once the delay has passed probes it. A host whose failures
%% change kind, such as one that is down and comes back as a standby, takes
%% the kind of its latest. The count, which a doubling delay follows, goes
%% on either way: the host has not taken a connection since.
mark_failed(Key, Kind, Reason, #state{config = Config, failures = Failures} = State) ->
    #{failed_host_delay := Base, failed_host_max_delay := Max} = Config,
    Count = case maps:find(Key, Failures) of
                {ok, {_Kind, N, _RetryAt}} -> N + 1;
                error -> 1
            end,
    Delay = eysql_topology:backoff(Count, Base, Max),
    Failure = {Kind, Count, eysql_util:now_ms() + Delay},
    log_failure(Key, Kind, Reason, Delay, State#state{failures = maps:put(Key, Failure, Failures)}).

%% What a failed connect makes of its host. open_failed/2 gives no reason:
%% its caller says only that the connection could not be made.
kind(undefined) ->
    down;
kind(read_only_server) ->
    read_only;
kind(Reason) ->
    case eysql_error:connect_failure(Reason) of
        unreachable -> down;
        rejected -> rejected
    end.

%% A connection to `Key' opened, a pick's or a probe's: it is up again, and
%% a probe of it has nothing left to tell.
recovered(Key, State) ->
    State1 = stop_probe(Key, State#state{failures = maps:remove(Key, State#state.failures)}),
    log_success(Key, State1).

connect_spec(#state{config = Config}) ->
    maps:with([driver, settings, target_session_attrs], Config).

snapshot_of(#state{servers = Servers, failures = Failures} = State) ->
    #{mode => State#state.mode,
      discovered => State#state.discovered,
      servers => Servers,
      placement => [eysql_topology:key(Server) || Server <- placement(State, [])],
      allowed => allowed(State),
      counts => State#state.counts,
      pending => State#state.pending,
      failed => failed(down, Failures),
      read_only => failed(read_only, Failures),
      rejected => failed(rejected, Failures)
     }.

failed(Kind, Failures) ->
    lists:sort([Key || {Key, {K, _Count, _RetryAt}} <- maps:to_list(Failures), K =:= Kind]).

%%%=============================================================================
%%% Probes
%%%=============================================================================

%% Probe each failed host whose delay has passed and that has no probe
%% running. A refresh calls this, and so does a discovery that succeeds:
%% there is no timer per host, so a host that stays down is probed once per
%% refresh, and a standby that stays one is not reconnected to every few
%% seconds.
probe_due(State) ->
    lists:foldl(fun start_probe/2, State, due(State)).

%% Every kind is probed, a rejected host too: it is out of picks, so only a
%% probe finds it back, short of the seeds' last resort. After a password
%% rotation that is one failing login per host per refresh.
%%
%% A host that is neither a server nor a seed, such as one whose pooled
%% connection failed after discovery dropped it, is not probed: a discovery
%% will either forget it or list it again.
due(#state{failures = Failures, probes = Probes} = State) ->
    Now = eysql_util:now_ms(),
    [Key || {Key, {_Kind, _Count, RetryAt}} <- maps:to_list(Failures),
            RetryAt =< Now, not maps:is_key(Key, Probes), known(Key, State)].

%% Connect to `Key' in a process of its own, as a pick's connect would, and
%% close the connection. The driver gives the connect `connect_timeout', and
%% the session check gets as long again; a probe still running then is
%% stopped and counts as a failure to connect.
start_probe({Host, Port} = Key, #state{config = Config} = State) ->
    Spec = connect_spec(State),
    Cluster = self(),
    Pid = spawn_link(fun() -> Cluster ! {probed, self(), Key, probe(Host, Port, Spec)} end),
    #{settings := #{connect_timeout := Timeout}} = Config,
    Timer = erlang:start_timer(2 * Timeout, self(), {probe_timeout, Key}),
    State#state{probes = maps:put(Key, {Pid, Timer}, State#state.probes)}.

%% Runs in the probe. It catches what the driver raises, so that it always
%% reports; its connection is linked to it, though, and one that dies can
%% take the probe with it, which the cluster hears of as an exit.
probe(Host, Port, #{driver := Driver} = Spec) ->
    try connect_to(Host, Port, Spec) of
        {ok, Conn} ->
            unlink(Conn),
            Driver:close(Conn),
            ok;
        {error, _} = Error ->
            Error
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

%% A failed probe starts another delay but no refresh, unlike a failed
%% connect: the host was failing already, and a refresh that probes a host
%% must not lead to another. Its reason decides the host's kind, as a
%% pick's connect would: a host that was down and now answers with a failed
%% login is rejected, and one that answered 57P03 while it shut down and
%% now refuses connections is down. It stays out of picks either way.
probed(Key, ok, State) ->
    recovered(Key, State);
probed(Key, {error, Reason}, State) ->
    mark_failed(Key, kind(Reason), Reason, State).

%% Stop `Key''s probe, unlinked first, so that its death is not taken for a
%% failure.
stop_probe(Key, #state{probes = Probes} = State) ->
    case maps:take(Key, Probes) of
        {{Pid, Timer}, Rest} ->
            _ = erlang:cancel_timer(Timer),
            unlink(Pid),
            exit(Pid, kill),
            State#state{probes = Rest};
        error ->
            State
    end.

known(Key, #state{servers = Servers, config = #{seeds := Seeds}}) ->
    lists:any(fun(Server) -> eysql_topology:key(Server) =:= Key end, Servers ++ Seeds).

%%%=============================================================================
%%% Logging
%%%=============================================================================

%% During an outage a host fails every connect tried to it and every probe,
%% so only the first failure of a run is logged, with its reason. The run
%% ends only once a connection to the host has opened since its last
%% failure and the window has passed since that failure. A host that
%% connects and then fails a connect again within the window is still in
%% the same run; and a host that goes the window untried, as with a long
%% back-off, has not shown it works, so its run goes on too. This is kept
%% apart from `failures', which any connection that opens ends at once.
%%
%% A standby is expected rather than a fault, so its run starts at info. A
%% failure of another kind ends a run and starts one of its own: a standby
%% that goes down warns, a host that comes back as a standby says so, and a
%% host that answers with a failed login after being down warns again, in
%% words that do not call it down.
%%
%% Discovery's failures form runs by the same rules, under the key
%% `discovery'. During an outage every refresh fails to discover, and a
%% warning for each, as the JDBC driver logs one, would come once a second
%% while connections are being opened. A discovery that succeeds counts as
%% the connection that opened.
log_failure(Key, Kind, Reason, Delay, #state{streaks = Streaks} = State) ->
    Now = eysql_util:now_ms(),
    case maps:find(Key, Streaks) of
        {ok, {Kind, Count, _Last, _Connected}} ->
            State#state{streaks = maps:put(Key, {Kind, Count + 1, Now, false}, Streaks)};
        _ ->
            log_start(Key, Kind, Reason, Delay, State),
            State#state{streaks = maps:put(Key, {Kind, 1, Now, false}, Streaks)}
    end.

log_start(discovery, down, Reason, _Delay, State) ->
    ?LOG_WARNING("eysql: server discovery failed~ts; keeping the hosts known, trying again when a "
                 "connection is next opened or at the next refresh, and not logging its failures again "
                 "until it has gone ~b ms without one",
                 [why(Reason), log_window(State)]);
log_start({Host, Port}, down, Reason, Delay, State) ->
    ?LOG_WARNING("eysql: could not connect to ~s:~b~ts; leaving it out until a refresh at least ~b ms "
                 "from now finds it back, and not logging its failures again until it has gone ~b ms "
                 "without one",
                 [Host, Port, why(Reason), Delay, log_window(State)]);
log_start({Host, Port}, read_only, _Reason, Delay, State) ->
    ?LOG_INFO("eysql: ~s:~b is a standby; leaving it out until a refresh at least ~b ms from now "
              "finds it writable, and not logging this again until it has been writable for ~b ms",
              [Host, Port, Delay, log_window(State)]);
log_start({Host, Port}, rejected, Reason, Delay, State) ->
    ?LOG_WARNING("eysql: ~s:~b answered, but the connection failed~ts; leaving it out until a refresh at "
                 "least ~b ms from now finds it taking connections, and not logging its failures again "
                 "until it has gone ~b ms without one",
                 [Host, Port, why(Reason), Delay, log_window(State)]).

%% A connection to `Key' opened. The first since its last failure ends the
%% run once the window has passed since that failure: now, or on a timer.
log_success(Key, #state{streaks = Streaks} = State) ->
    case maps:find(Key, Streaks) of
        {ok, {Kind, Count, Last, false}} ->
            State1 = State#state{streaks = maps:put(Key, {Kind, Count, Last, true}, Streaks)},
            case Last + log_window(State) - eysql_util:now_ms() of
                Wait when Wait > 0 ->
                    _ = erlang:start_timer(Wait, self(), {streak_end, Key, Last}),
                    State1;
                _ ->
                    end_streak(Key, Last, State1)
            end;
        _ ->
            State
    end.

%% The window has passed since `Key''s failure at `Last'. The run ends if
%% that is still its last failure and a connection has opened since; a timer
%% left over from an earlier failure finds neither.
end_streak(discovery, Last, #state{streaks = Streaks} = State) ->
    case maps:find(discovery, Streaks) of
        {ok, {down, Count, Last, true}} ->
            ?LOG_INFO("eysql: server discovery works again after ~b failures, with none in the last ~b ms",
                      [Count, log_window(State)]),
            State#state{streaks = maps:remove(discovery, Streaks)};
        _ ->
            State
    end;
end_streak({Host, Port} = Key, Last, #state{streaks = Streaks} = State) ->
    case maps:find(Key, Streaks) of
        {ok, {Kind, Count, Last, true}} ->
            log_end(Host, Port, Kind, Count, log_window(State)),
            State#state{streaks = maps:remove(Key, Streaks)};
        _ ->
            State
    end.

log_end(Host, Port, down, Count, Window) ->
    ?LOG_INFO("eysql: ~s:~b recovered after ~b failures, with none in the last ~b ms",
              [Host, Port, Count, Window]);
log_end(Host, Port, read_only, _Count, Window) ->
    ?LOG_INFO("eysql: ~s:~b accepts writes, with no failure in the last ~b ms", [Host, Port, Window]);
log_end(Host, Port, rejected, Count, Window) ->
    ?LOG_INFO("eysql: ~s:~b takes connections again after ~b failures, with none in the last ~b ms",
              [Host, Port, Count, Window]).

log_window(#state{config = Config}) ->
    maps:get(log_window, Config, ?LOG_WINDOW).

%% The reason for the warning, if the caller gave one. A connect error can
%% echo options, as an ssl option error does the option it rejects, so
%% passwords and configs in it are hidden.
why(undefined) -> "";
why(Reason) -> io_lib:format(" (~0tP)", [hide(eysql_util:redact(Reason)), 30]).

%% A password sits in a proplist as `{password, _}', and in a map, such as
%% epgsql's connect options, under a `password' key, atom or binary.
hide({password, _}) -> {password, redacted};
hide([Head | Tail]) -> [hide(Head) | hide(Tail)];
hide(Tuple) when is_tuple(Tuple) -> list_to_tuple(hide(tuple_to_list(Tuple)));
hide(Map) when is_map(Map) -> maps:map(fun hide/2, Map);
hide(Term) -> Term.

hide(Key, _Value) when Key =:= password; Key =:= <<"password">> -> redacted;
hide(_Key, Value) -> hide(Value).

%%%=============================================================================
%%% Refreshes and discovery
%%%=============================================================================

%% A refresh: probe the failed hosts whose delay has passed, and read
%% yb_servers() again unless there is nothing to discover or a discovery is
%% running. A discovery schedules the next refresh when it finishes; with
%% nothing to discover, the refresh schedules it, so that failed hosts are
%% probed on the same schedule either way.
start_refresh(State0) ->
    State = probe_due(State0#state{last_refresh = eysql_util:now_ms()}),
    case State#state.mode of
        static -> schedule_refresh(State);
        discovering -> start_discovery(State)
    end.

%% A pick starts a refresh once the interval has passed since the last one
%% started, as the JDBC driver's getConnection does through needsRefresh.
%% It does not wait for it: this pick uses the hosts known now. With an
%% interval of 0 every pick starts one.
%%
%% After a discovery that failed, the refresh stays due, whether or not the
%% pick finds a host: the driver's lastRefreshTime moves only when a refresh
%% succeeds, so its next getConnection refreshes again. Here that is the
%% next pick, though not within a second of the last refresh, as after a
%% connection failure; the driver's refreshes wait on a lock instead.
refresh_if_due(#state{config = #{refresh_interval := Interval}, last_refresh = Last} = State) ->
    case Last =:= undefined orelse eysql_util:now_ms() - Last >= Interval of
        true -> start_refresh(State);
        false when State#state.stale -> maybe_refresh(State);
        false -> State
    end.

%% After a failure, refresh early, as the JDBC driver does by setting
%% forceRefreshOnce; but not within a second of the last refresh, however
%% many connects fail. This rarely probes the host that just failed, whose
%% delay has only begun, unless that delay is 0.
maybe_refresh(#state{last_refresh = Last} = State) ->
    case Last =:= undefined orelse eysql_util:now_ms() - Last >= ?MIN_REFRESH_GAP of
        true -> start_refresh(State);
        false -> State
    end.

%% The discovery asks each target in turn, as a connect would, so it gets
%% `connect_timeout' for each, a few seconds for its query, and another
%% `connect_timeout' for the name lookups that follow (see addresses/3).
start_discovery(#state{refresh = {_, _, _}} = State) ->
    State;
start_discovery(#state{config = Config, address = {Decision, _, _}} = State) ->
    #{driver := Driver, settings := #{connect_timeout := ConnectTimeout} = Settings} = Config,
    Targets = refresh_targets(State),
    Lookup = addresses(maps:get(resolve, Config, fun resolve/1), Decision, ConnectTimeout),
    Parent = self(),
    {Pid, Monitor} =
        spawn_monitor(fun() ->
                              Parent ! {discovered, self(), discover(Driver, Settings, Lookup, Targets)}
                      end),
    Timeout = ConnectTimeout * (length(Targets) + 1) + 5000,
    Timer = erlang:send_after(Timeout, self(), {refresh_timeout, Pid}),
    State#state{refresh = {Pid, Monitor, Timer}}.

%% Hosts to ask: the seeds, in the order given, whatever their failures,
%% and then the known servers that are not down, each once. The JDBC
%% driver's checkAndRefresh dials the hosts in its URL first, and after them
%% only the servers getAllAvailableHosts lists, those not marked down. A
%% seed behind a load balancer thus answers before any server's address is
%% tried, and a server that cannot be reached costs a discovery no
%% connect_timeout until a probe or a connection finds it back.
refresh_targets(#state{servers = Servers, config = #{seeds := Seeds}, failures = Failures}) ->
    SeedKeys = [eysql_topology:key(Seed) || Seed <- Seeds],
    Dialled = fun(Server) ->
                      Key = eysql_topology:key(Server),
                      not lists:member(Key, SeedKeys) andalso not is_down(Key, Failures)
              end,
    Seeds ++ [Server || Server <- Servers, Dialled(Server)].

is_down(Key, Failures) ->
    case maps:find(Key, Failures) of
        {ok, {down, _Count, _RetryAt}} -> true;
        _ -> false
    end.

%% Runs in the discovery process, with the name lookups, so that none of
%% them holds up the cluster process.
discover(_Driver, _Settings, _Lookup, []) ->
    {error, no_server_reachable};
discover(Driver, Settings, Lookup, [#{host := Host, port := Port} | Rest]) ->
    case Driver:open(Host, Port, Settings) of
        {ok, Conn} ->
            Result = Driver:discover(Conn),
            unlink(Conn),
            Driver:close(Conn),
            case Result of
                {ok, Servers} ->
                    %% Which address answered decides the column to use.
                    case Lookup(Host, Servers) of
                        {ok, Addresses} -> {ok, Servers, Host, Addresses};
                        error -> discover(Driver, Settings, Lookup, Rest)
                    end;
                {error, Reason} ->
                    case eysql_error:code(Reason) of
                        <<"42883">> -> {error, not_yugabytedb};
                        _ -> discover(Driver, Settings, Lookup, Rest)
                    end
            end;
        {error, _} ->
            discover(Driver, Settings, Lookup, Rest)
    end.

%% The lookups pgjdbc's refresh makes with InetAddress.getByName, for
%% eysql_topology:address_column/3: the name the discovery dialled, as
%% getConnectedInetAddress resolves it, and, while the column is undecided,
%% each server's host and public IP. Once it is decided, the servers' names
%% no longer count, and are not looked up. The name dialled still is: the
%% driver fails the refresh when it does not resolve ("Unexpected
%% UnknownHostException"), and this moves on to the next target, as the
%% driver's checkAndRefresh does, rather than decide from nothing.
addresses(Resolve, Decision, Timeout) ->
    fun(Answered, Servers) ->
            Names = case Decision of
                        undecided -> [Answered | lists:append([names(S) || S <- Servers])];
                        _ -> [Answered]
                    end,
            Found = resolve_all(Resolve, lists:usort(Names), Timeout),
            case maps:get(Answered, Found) of
                {ok, Address} when Decision =:= undecided ->
                    {ok, {Address, [resolved(Server, Found) || Server <- Servers]}};
                {ok, Address} ->
                    {ok, {Address, []}};
                error ->
                    error
            end
    end.

names(#{host := Host} = Server) ->
    case maps:get(public_ip, Server, <<>>) of
        <<>> -> [Host];
        PublicIp -> [Host, PublicIp]
    end.

resolved(#{host := Host} = Server, Found) ->
    Public = case maps:get(public_ip, Server, <<>>) of
                 <<>> -> none;
                 PublicIp -> maps:get(PublicIp, Found)
             end,
    {maps:get(Host, Found), Public}.

%% Look the names up all at once, each in a process of its own, where the
%% driver looks them up one after another. A lookup that has not answered
%% within `Timeout' counts as a name that does not resolve, as the
%% UnknownHostException the driver would get in the end, so that a slow DNS
%% server costs a discovery that long at most, not that long per name. The
%% lookups are linked to the discovery process, and die with it.
resolve_all(Resolve, Names, Timeout) ->
    Self = self(),
    Lookups = maps:from_list([{spawn_link(fun() -> Self ! {resolved, self(), lookup(Resolve, Name)} end),
                               Name}
                              || Name <- Names]),
    collect(Lookups, #{}, eysql_util:now_ms() + Timeout).

collect(Lookups, Found, _Deadline) when map_size(Lookups) =:= 0 ->
    Found;
collect(Lookups, Found, Deadline) ->
    receive
        {resolved, Pid, Result} when is_map_key(Pid, Lookups) ->
            {Name, Rest} = maps:take(Pid, Lookups),
            collect(Rest, Found#{Name => Result}, Deadline)
    after max(0, Deadline - eysql_util:now_ms()) ->
            maps:foreach(fun(Pid, _Name) -> unlink(Pid), exit(Pid, kill) end, Lookups),
            maps:merge(Found, maps:from_list([{Name, error} || Name <- maps:values(Lookups)]))
    end.

lookup(Resolve, Name) ->
    try Resolve(Name) of
        {ok, Address} -> {ok, Address};
        _ -> error
    catch
        _:_ -> error
    end.

%% The address Java's InetAddress.getByName gives a name, the one pgjdbc
%% compares: the first IPv4 address, or else the first IPv6 one, an
%% IPv4-mapped IPv6 address being the IPv4 address it maps, as in Java.
%% Tests replace this with a `resolve' key in the normalized config; it is
%% not an option.
resolve(Name) ->
    Host = unicode:characters_to_list(Name),
    case inet:getaddrs(Host, inet) of
        {ok, [Address | _]} ->
            {ok, Address};
        _ ->
            case inet:getaddrs(Host, inet6) of
                {ok, [{0, 0, 0, 0, 0, 16#ffff, AB, CD} | _]} ->
                    {ok, {AB bsr 8, AB band 16#ff, CD bsr 8, CD band 16#ff}};
                {ok, [Address | _]} ->
                    {ok, Address};
                {error, _} ->
                    error
            end
    end.

discovered(Result, State) ->
    set_ready(apply_discovery(Result, State)).

%% A discovery that failed schedules the next refresh after the full
%% interval, as one that succeeded does, and leaves the refresh due for the
%% next pick (see refresh_if_due/1). An empty answer is not a failure: the
%% JDBC driver moves its clock on for it too. Servers are stored with their
%% placement case-folded, so that topology keys match it whatever its case.
apply_discovery({ok, [_ | _] = Found, Answered, {Address, Resolved}},
                #state{address = {Decision, _, _} = Previous} = State) ->
    {_, Column, _} = Chosen = eysql_topology:address_column(Decision, Address, Resolved),
    log_address(Answered, Chosen, Previous),
    Servers = case Column of
                  host -> Found;
                  public_ip -> eysql_topology:use_public_ip(Found)
              end,
    State1 = State#state{servers = eysql_topology:casefold_placement(Servers), discovered = true,
                         address = Chosen},
    schedule_refresh(probe_due(discovery_worked(forget_gone(State1))));
apply_discovery({ok, [], _Answered, _Addresses}, State) ->
    schedule_refresh(probe_due(discovery_worked(State)));
apply_discovery({error, not_yugabytedb}, State) ->
    ?LOG_INFO("eysql: yb_servers() is not available; using the configured hosts"),
    schedule_refresh(discovery_worked(State#state{mode = static}));
apply_discovery({error, Reason}, State) ->
    State1 = log_failure(discovery, down, Reason, undefined, State#state{stale = true}),
    schedule_refresh(State1).

discovery_worked(State) ->
    log_success(discovery, State#state{stale = false}).

%% Forget hosts that are neither servers nor seeds any more: their failures,
%% runs of failures and probes. Only a connection to the same host removed a
%% failure, which never comes for a host that is gone. Where yb_servers()
%% reports addresses that change on restart, each rolling restart would leave
%% an entry per old address; and a host that left and came back under the
%% same address would carry its old run on, its next failure unlogged.
%% Discovery's own run of failures is not a host's, and stays.
forget_gone(#state{servers = Servers, config = #{seeds := Seeds}} = State) ->
    Known = [discovery | [eysql_topology:key(Server) || Server <- Servers ++ Seeds]],
    Kept = maps:keys(State#state.failures) ++ maps:keys(State#state.streaks)
        ++ maps:keys(State#state.probes),
    Gone = lists:usort(Kept) -- Known,
    State1 = lists:foldl(fun stop_probe/2, State, Gone),
    State1#state{failures = maps:without(Gone, State1#state.failures),
                 streaks = maps:without(Gone, State1#state.streaks)}.

%% Logged when the choice changes: once when it is decided, and while it is
%% not, each time the guess changes. The driver logs its decision at info,
%% and, while it has none, warns at every refresh that it is using host
%% addresses, and once that the public IPs it would guess do not resolve.
%% Here only the second is a warning: short of that, with no decision the
%% host is the only address every server has.
log_address(_Answered, Same, Same) ->
    ok;
log_address(Answered, {Column, Column, decided}, _Previous) ->
    ?LOG_INFO("eysql: ~s answered discovery; connecting to each server's ~s from now on",
              [Answered, Column]);
log_address(Answered, {undecided, public_ip, all_public}, _Previous) ->
    ?LOG_INFO("eysql: ~s answered discovery at an address that is no server's host or public_ip; "
              "every server has a public_ip that resolves, so connecting to each server's public_ip "
              "until a discovery tells which to use", [Answered]);
log_address(_Answered, {undecided, host, unresolved_public}, _Previous) ->
    ?LOG_WARNING("eysql: not connecting to the servers' public_ip addresses: every server has one, "
                 "but not every one resolves; connecting to each server's host");
log_address(Answered, {undecided, host, unknown}, _Previous) ->
    ?LOG_INFO("eysql: ~s answered discovery at an address that is no server's host or public_ip; "
              "connecting to each server's host until a discovery tells which to use", [Answered]).

set_ready(#state{ready = true} = State) ->
    State;
set_ready(#state{watchers = Watchers} = State) ->
    lists:foreach(fun(Pid) -> Pid ! {eysql_cluster_ready, self()} end, Watchers),
    State#state{ready = true, watchers = []}.

%% With an interval of 0 picks start every refresh, so there is no timer.
schedule_refresh(#state{config = #{refresh_interval := 0}} = State) ->
    cancel_refresh_timer(State);
schedule_refresh(#state{config = #{refresh_interval := Interval}} = State) ->
    schedule_refresh(State, Interval).

schedule_refresh(State, After) ->
    State1 = cancel_refresh_timer(State),
    State1#state{refresh_timer = erlang:start_timer(After, self(), refresh)}.

cancel_refresh_timer(#state{refresh_timer = undefined} = State) ->
    State;
cancel_refresh_timer(#state{refresh_timer = Timer} = State) ->
    _ = erlang:cancel_timer(Timer),
    State#state{refresh_timer = undefined}.
