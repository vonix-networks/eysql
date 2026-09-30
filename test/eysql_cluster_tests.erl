-module(eysql_cluster_tests).

-include_lib("eunit/include/eunit.hrl").

-import(eysql_test_util, [config/1, three/0, key/1, wait_until/2, wait_until/3,
                          refreshing_until/3, fail/2, fail/3, reserve/2]).

%% A logger handler, for the test that reads what the cluster logs.
-export([log/2]).

cluster_test_() ->
    {foreach,
     fun() -> eysql_fake_driver:start() end,
     fun(_) -> eysql_fake_driver:stop() end,
     [{"connections spread evenly across discovered servers", fun spreads_evenly/0},
      {"topology keys prefer a zone, then fall back", fun prefers_zone/0},
      {"fallback_to_topology_keys_only refuses other zones", fun fallback_only/0},
      {"prefer_primary ignores fallback_to_topology_keys_only", fun prefer_ignores_fallback_only/0},
      {"a failed host is skipped until a refresh finds it back", fun failed_host_backs_off/0},
      {"a host that fails again waits the same delay for its next probe", fun fixed_delay/0},
      {"failed_host_max_delay_secs doubles the delay", {timeout, 15, fun doubling_delay/0}},
      {"a closed connection stops counting", fun conn_death_decrements/0},
      {"PostgreSQL: no yb_servers(), configured hosts only", fun postgres_static/0},
      {"PostgreSQL: a lone host that fails and comes back is used again", fun pg_single_host_back/0},
      {"PostgreSQL: a promoted standby takes over through the last resort", fun pg_failover/0},
      {"target_session_attrs read_write skips standbys", fun read_write/0},
      {"a server that leaves discovery is no longer allowed", fun removed_server/0},
      {"when_ready reports the first discovery", fun when_ready/0},
      {"while the preferred host is failed, it and the fallback level are allowed",
       {timeout, 15, fun allowed_falls_back/0}},
      {"a failing host in the chosen level stays allowed", fun failing_host_stays_allowed/0},
      {"after a cluster-wide blip the servers stay allowed beside the seeds", fun blip_keeps_servers/0},
      {"a failed host is no longer allowed once discovery drops it", fun dropped_while_failing/0},
      {"a failing host is logged once, and the end of its run once", fun logs_once/0},
      {"the warning gives the reason, without passwords", fun logs_reason/0},
      {"the warning hides passwords under map keys", fun hides_map_passwords/0},
      {"a standby is logged at info, listed as read_only, and moves between the lists",
       {timeout, 20, fun standby_read_only/0}},
      {"discovery forgets hosts that are gone", fun forgets_gone_hosts/0},
      {"a probe stops when discovery drops its host", fun probe_stops_when_pruned/0},
      {"a failed host is probed once discovery lists it again", {timeout, 15, fun relisted_host_probed/0}},
      {"a probe blocks nothing, runs alone, times out and stops with the cluster",
       {timeout, 20, fun probe_times_out/0}},
      {"a probe closes its connection and is not counted", fun probe_closes/0},
      {"a probe checks target_session_attrs as a connect does", {timeout, 20, fun probe_checks_session/0}},
      {"a host that fails for good is probed once per refresh, and not between",
       {timeout, 30, fun probed_once_per_refresh/0}},
      {"a host back after a short outage takes picks from the next refresh, not before",
       {timeout, 15, fun recovery_follows_refresh/0}},
      {"with nothing to discover, refreshes still come on their own schedule",
       {timeout, 15, fun static_refreshes/0}},
      {"a load-balancer seed takes every open once the servers have failed",
       {timeout, 15, fun lb_seed_skips_failed_servers/0}},
      {"a pick that finds no host brings nothing forward, past a failed host's delay too",
       {timeout, 15, fun stranded_pick_waits/0}},
      {"the modes the JDBC driver refuses in leave the seeds out once servers are discovered",
       fun refusing_modes/0},
      {"one open tries every eligible server once, then the seeds", fun tries_every_server/0},
      {"one open tries a dead seed once", fun dead_seed_once/0},
      {"a connect waits for the first discovery", fun connect_waits_for_discovery/0},
      {"before a discovery has succeeded, every mode takes the seeds", fun seeds_before_discovery/0},
      {"a failed discovery is tried again at the next pick or refresh, not after a delay",
       {timeout, 15, fun discovery_failure_cadence/0}},
      {"failed discoveries are logged once per run, and the end of the run once",
       {timeout, 15, fun logs_discovery_once/0}},
      {"a picker that exits without reporting leaves nothing pending", fun abandoned_pick/0},
      {"every outcome settles the pick and its monitor", fun picks_settle/0},
      {"seeds are tried while no discovered server is reachable", fun seeds_fallback/0},
      {"an answer at a public IP switches to public IPs", fun public_ips/0},
      {"a seed whose name resolves to a server's public IP: public IPs", fun dns_seed_public_ip/0},
      {"a seed whose name resolves to a server's host: hosts, though public IPs would do",
       fun dns_seed_host/0},
      {"the column is decided once, and stands", fun address_decided_once/0},
      {"undecided, public IPs are a guess, made again at each discovery", fun address_guess/0},
      {"a name that does not resolve", {timeout, 15, fun address_unresolved/0}},
      {"the resolver the driver's InetAddress.getByName stands for", fun default_resolver/0},
      {"a manual refresh keeps one scheduled refresh", fun manual_refresh/0},
      {"refresh interval 0: each pick refreshes, without a timer or a wait", fun refresh_each_pick/0},
      {"a discovery that succeeds probes the failed hosts that are due, at once",
       {timeout, 15, fun probes_after_discovery/0}},
      {"only a failure to connect marks a host down; a failed login marks it rejected, out of picks",
       fun rejected_not_down/0},
      {"a probe that finds a down host answering, but failing the connection, marks it rejected",
       {timeout, 15, fun probe_finds_rejected/0}},
      {"a rejected host is probed at the first refresh after its delay, and a probe brings it back",
       {timeout, 15, fun rejected_probed/0}},
      {"a rejected seed is passed over by every open, and probed like one that is down",
       {timeout, 15, fun rejected_seed_probed/0}},
      {"a graceful restart: 57P03, then refused, then accepted; out of picks until the refresh after",
       {timeout, 20, fun graceful_restart/0}},
      {"discovery dials the seeds first, each once, then the servers that are not down",
       {timeout, 15, fun discovery_targets/0}},
      {"topology keys match cloud, region and zone whatever their case", fun keys_ignore_case/0},
      {"failed_host_reconnect_delay_secs 0: the refresh a failure brings forward probes the host",
       {timeout, 15, fun zero_delay/0}},
      {"with load_balance false, hosts are taken in the order given", {timeout, 15, fun static_in_order/0}},
      {"the seeds are taken in order in every mode", fun seeds_in_order/0}
     ]}.

start(Overrides) ->
    {ok, Cluster} = eysql_cluster:start_link(config(Overrides)),
    Cluster.

%% A cluster whose runs of failures end after `Window' ms, not a minute.
start_windowed(Overrides, Window) ->
    {ok, Cluster} = eysql_cluster:start_link(maps:put(log_window, Window, config(Overrides))),
    Cluster.

now_ms() ->
    erlang:monotonic_time(millisecond).

discovered(Cluster) ->
    wait_until(fun() -> maps:get(discovered, eysql_cluster:snapshot(Cluster)) end, discovered).

counts(Cluster) ->
    maps:get(counts, eysql_cluster:snapshot(Cluster)).

pending(Cluster) ->
    maps:get(pending, eysql_cluster:snapshot(Cluster)).

failed(Cluster) ->
    maps:get(failed, eysql_cluster:snapshot(Cluster)).

read_only(Cluster) ->
    maps:get(read_only, eysql_cluster:snapshot(Cluster)).

rejected(Cluster) ->
    maps:get(rejected, eysql_cluster:snapshot(Cluster)).

allowed(Cluster) ->
    lists:sort(maps:get(allowed, eysql_cluster:snapshot(Cluster))).

placement(Cluster) ->
    lists:sort(maps:get(placement, eysql_cluster:snapshot(Cluster))).

hosts(Cluster) ->
    [Host || #{host := Host} <- maps:get(servers, eysql_cluster:snapshot(Cluster))].

opens(Keys) when is_list(Keys) ->
    [eysql_fake_driver:opens(Key) || Key <- Keys];
opens(Key) ->
    eysql_fake_driver:opens(Key).

monitored(Cluster, Pid) ->
    {monitors, Monitors} = erlang:process_info(Cluster, monitors),
    lists:member({process, Pid}, Monitors).

open_n(Cluster, N) ->
    [begin {ok, Conn, Key} = eysql_cluster:open(Cluster), {Conn, Key} end || _ <- lists:seq(1, N)].

spreads_evenly() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{}),
    discovered(Cluster),
    _ = open_n(Cluster, 9),
    wait_until(fun() -> maps:size(counts(Cluster)) =:= 3 end, three_hosts),
    ?assertEqual(#{key(<<"a">>) => 3, key(<<"b">>) => 3, key(<<"c">>) => 3}, counts(Cluster)),
    eysql_cluster:stop(Cluster).

prefers_zone() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{topology_keys => "gcp.us-east1.us-east1-b:1,gcp.us-east1.*:2"}),
    discovered(Cluster),
    Opened = open_n(Cluster, 4),
    ?assertEqual([key(<<"a">>)], lists:usort([Key || {_, Key} <- Opened])),
    eysql_fake_driver:down(key(<<"a">>)),
    {ok, _, Key} = eysql_cluster:open(Cluster),
    ?assert(lists:member(Key, [key(<<"b">>), key(<<"c">>)])),
    ?assert(lists:member(key(<<"a">>), maps:get(failed, eysql_cluster:snapshot(Cluster)))),
    eysql_cluster:stop(Cluster).

%% a, alone in the listed zone, refuses: the open tries it, and then finds
%% no eligible server, and fails as the JDBC driver's getLeastLoadedServer
%% throws, rather than with a's error.
fallback_only() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{topology_keys => "gcp.us-east1.us-east1-b",
                      fallback_to_topology_keys_only => true}),
    discovered(Cluster),
    eysql_fake_driver:down(key(<<"a">>)),
    ?assertEqual({error, {no_node_available, cluster}}, eysql_cluster:connect(Cluster)),
    eysql_cluster:stop(Cluster).

%% As fallback_only, but prefer_primary: the drivers ignore the option then,
%% and take a primary anywhere in the cluster.
prefer_ignores_fallback_only() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{topology_keys => "gcp.us-east1.us-east1-b",
                      fallback_to_topology_keys_only => true,
                      load_balance => prefer_primary}),
    discovered(Cluster),
    eysql_fake_driver:down(key(<<"a">>)),
    {ok, _, Key} = eysql_cluster:open(Cluster),
    ?assert(lists:member(Key, [key(<<"b">>), key(<<"c">>)])),
    eysql_cluster:stop(Cluster).

%% a refuses, and new connections go to b and c. Once it answers again, a
%% refresh after its delay probes it, and the next connection goes to a, the
%% least loaded.
failed_host_backs_off() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{yb_servers_refresh_interval => 1}),
    discovered(Cluster),
    eysql_fake_driver:down(key(<<"a">>)),
    _ = open_n(Cluster, 6),
    ?assertEqual(#{key(<<"b">>) => 3, key(<<"c">>) => 3}, maps:remove(key(<<"a">>), counts(Cluster))),
    ?assertEqual(0, maps:get(key(<<"a">>), counts(Cluster), 0)),
    eysql_fake_driver:up(key(<<"a">>)),
    wait_until(fun() -> failed(Cluster) =:= [] end, found_back),
    {ok, _, Key} = eysql_cluster:open(Cluster),
    ?assertEqual(key(<<"a">>), Key),
    eysql_cluster:stop(Cluster).

%% a fails, and fails again when a refresh first probes it, once its delay
%% (1 s) has passed; from then on it answers. Refreshes come every 20 ms
%% here, so each probe comes as soon as the delay allows. Returns how long
%% after the first failure a probe found it back, which is 1 s plus the
%% second delay. The cluster's seed must not be a, or each refresh's
%% discovery would connect to it too.
back_after(Cluster) ->
    A = key(<<"a">>),
    Opens = opens(A),
    eysql_fake_driver:down(A),
    Start = now_ms(),
    fail(Cluster, A),
    refreshing_until(Cluster, fun() -> opens(A) > Opens end, first_probe),
    ?assert(now_ms() - Start >= 950),
    ?assert(lists:member(A, failed(Cluster))),
    eysql_fake_driver:up(A),
    refreshing_until(Cluster, fun() -> failed(Cluster) =:= [] end, back),
    now_ms() - Start.

fixed_delay() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{hosts => [{<<"lb">>, 5433}]}),
    discovered(Cluster),
    Back = back_after(Cluster),
    ?assert(Back >= 1900 andalso Back < 2700),
    eysql_cluster:stop(Cluster).

doubling_delay() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{hosts => [{<<"lb">>, 5433}], failed_host_max_delay_secs => 4}),
    discovered(Cluster),
    %% The second failure keeps a out for 2 s.
    Back = back_after(Cluster),
    ?assert(Back >= 2900 andalso Back < 3700),
    eysql_cluster:stop(Cluster).

conn_death_decrements() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{}),
    discovered(Cluster),
    [{Conn, Key} | _] = open_n(Cluster, 3),
    wait_until(fun() -> maps:get(Key, counts(Cluster), 0) =:= 1 end, counted),
    unlink(Conn),
    eysql_fake_driver:kill_conn(Conn),
    wait_until(fun() -> maps:get(Key, counts(Cluster), 0) =:= 0 end, uncounted),
    {ok, _, Again} = eysql_cluster:open(Cluster),
    ?assertEqual(Key, Again),
    eysql_cluster:stop(Cluster).

postgres_static() ->
    Cluster = start(#{hosts => ["pg"], port => 5432}),
    wait_until(fun() -> maps:get(mode, eysql_cluster:snapshot(Cluster)) =:= static end, static),
    {ok, _, Key} = eysql_cluster:open(Cluster),
    ?assertEqual({<<"pg">>, 5432}, Key),
    eysql_cluster:stop(Cluster).

%% A lone PostgreSQL host goes down, with a delay longer than the test. With
%% no other host, opens try it regardless of its failure, as the smart
%% drivers make a plain connection to the host in their URL; the first
%% connection that opens once it is back ends the failure.
pg_single_host_back() ->
    Pg = {<<"pg">>, 5432},
    Cluster = start(#{hosts => ["pg"], port => 5432, failed_host_reconnect_delay_secs => 30}),
    wait_until(fun() -> maps:get(mode, eysql_cluster:snapshot(Cluster)) =:= static end, static),
    eysql_fake_driver:down(Pg),
    ?assertEqual({error, econnrefused}, eysql_cluster:connect(Cluster)),
    ?assertEqual([Pg], failed(Cluster)),
    ?assertEqual([Pg], placement(Cluster)),
    eysql_fake_driver:up(Pg),
    ?assertMatch({ok, _, Pg}, eysql_cluster:open(Cluster)),
    ?assertEqual([], failed(Cluster)),
    eysql_cluster:stop(Cluster).

%% PostgreSQL, read_write: pg1, listed first, is a standby, which the first
%% open finds and picks then leave out, with a delay longer than the test;
%% pg2 is the primary. pg2 goes down and pg1 is promoted. With no host
%% working, the last resort takes the seeds regardless, in order, and pg1
%% takes the connection at once, before any refresh.
pg_failover() ->
    Pg1 = {<<"pg1">>, 5432},
    Pg2 = {<<"pg2">>, 5432},
    eysql_fake_driver:set_standby(Pg1),
    Cluster = start(#{hosts => ["pg1", "pg2"], port => 5432, load_balance => false,
                      target_session_attrs => read_write, failed_host_reconnect_delay_secs => 30}),
    ?assertEqual([Pg2, Pg2], [Key || {_, Key} <- open_n(Cluster, 2)]),
    ?assertEqual({[], [Pg1]}, {failed(Cluster), read_only(Cluster)}),
    eysql_fake_driver:down(Pg2),
    eysql_fake_driver:set_primary(Pg1),
    ?assertMatch({ok, _, Pg1}, eysql_cluster:open(Cluster)),
    ?assertEqual({[Pg2], []}, {failed(Cluster), read_only(Cluster)}),
    ?assertEqual([Pg1], placement(Cluster)),
    eysql_cluster:stop(Cluster).

%% pg1 is a standby, and every connection goes to pg2, the first primary in
%% the order given, none to pg3.
read_write() ->
    eysql_fake_driver:set_standby({<<"pg1">>, 5432}),
    Cluster = start(#{hosts => ["pg1", "pg2", "pg3"], port => 5432, load_balance => false,
                      target_session_attrs => read_write}),
    Keys = [Key || {_, Key} <- open_n(Cluster, 4)],
    ?assertEqual([{<<"pg2">>, 5432}], lists:usort(Keys)),
    ?assertEqual({[], [{<<"pg1">>, 5432}]}, {failed(Cluster), read_only(Cluster)}),
    eysql_cluster:stop(Cluster).

removed_server() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{}),
    discovered(Cluster),
    ?assertEqual(3, length(maps:get(allowed, eysql_cluster:snapshot(Cluster)))),
    eysql_fake_driver:set_servers(lists:sublist(three(), 2)),
    eysql_cluster:refresh(Cluster),
    wait_until(fun() -> length(maps:get(allowed, eysql_cluster:snapshot(Cluster))) =:= 2 end, removed),
    ?assertNot(lists:member(key(<<"c">>), maps:get(allowed, eysql_cluster:snapshot(Cluster)))),
    eysql_cluster:stop(Cluster).

when_ready() ->
    Static = start(#{load_balance => false}),
    ?assertEqual(ready, eysql_cluster:when_ready(Static)),
    eysql_cluster:stop(Static),
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{}),
    case eysql_cluster:when_ready(Cluster) of
        ready -> ok;
        pending -> receive {eysql_cluster_ready, Cluster} -> ok after 2000 -> error(not_ready) end
    end,
    ?assert(maps:get(discovered, eysql_cluster:snapshot(Cluster))),
    eysql_cluster:stop(Cluster).

%% a is alone in the preferred zone. While it is failed, new connections go
%% to b and c, and a pool keeps connections on all three: a's, since a
%% failure alone moves nothing, and those on b and c, since new ones go
%% there. That holds past a's delay, while the probes refreshes start wait
%% out connect_timeout on an address that has become unreachable: a has not
%% shown it takes connections again. Once a probe finds it back, only a is
%% allowed.
allowed_falls_back() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{topology_keys => "gcp.us-east1.us-east1-b:1,"
                                       "gcp.us-east1.us-east1-c:2,gcp.us-east1.us-east1-d:2",
                      connect_timeout => 500, yb_servers_refresh_interval => 1}),
    discovered(Cluster),
    ?assertEqual([key(<<"a">>)], allowed(Cluster)),
    ?assertEqual([key(<<"a">>)], placement(Cluster)),
    eysql_fake_driver:down(key(<<"a">>)),
    {ok, _, Key} = eysql_cluster:open(Cluster),
    ?assert(lists:member(Key, [key(<<"b">>), key(<<"c">>)])),
    ?assert(lists:member(key(<<"a">>), failed(Cluster))),
    ?assertEqual([key(<<"b">>), key(<<"c">>)], placement(Cluster)),
    ?assertEqual([key(<<"a">>), key(<<"b">>), key(<<"c">>)], allowed(Cluster)),
    eysql_fake_driver:unreachable(key(<<"a">>)),
    %% Past the delay (1 s), with a refresh's probe of a under way or done.
    timer:sleep(1500),
    ?assertEqual([key(<<"a">>)], failed(Cluster)),
    ?assertEqual([key(<<"b">>), key(<<"c">>)], placement(Cluster)),
    ?assertEqual([key(<<"a">>), key(<<"b">>), key(<<"c">>)], allowed(Cluster)),
    eysql_fake_driver:up(key(<<"a">>)),
    wait_until(fun() -> failed(Cluster) =:= [] end, a_back, 8000),
    ?assertEqual([key(<<"a">>)], placement(Cluster)),
    ?assertEqual([key(<<"a">>)], allowed(Cluster)),
    eysql_cluster:stop(Cluster).

failing_host_stays_allowed() ->
    Server = fun(Host, Zone) -> eysql_fake_driver:server(Host, <<"gcp">>, <<"us-east1">>, Zone) end,
    eysql_fake_driver:set_servers([Server(<<"a">>, <<"us-east1-b">>),
                                  Server(<<"a2">>, <<"us-east1-b">>),
                                  Server(<<"b">>, <<"us-east1-c">>)]),
    Cluster = start(#{topology_keys => "gcp.us-east1.us-east1-b:1,gcp.us-east1.*:2"}),
    discovered(Cluster),
    eysql_fake_driver:down(key(<<"a">>)),
    Opened = open_n(Cluster, 2),
    ?assertEqual([key(<<"a2">>)], lists:usort([Key || {_, Key} <- Opened])),
    ?assert(lists:member(key(<<"a">>), failed(Cluster))),
    %% a2 keeps the level; one failed connect to a must not drain a.
    ?assertEqual([key(<<"a2">>)], placement(Cluster)),
    ?assertEqual([key(<<"a">>), key(<<"a2">>)], allowed(Cluster)),
    eysql_cluster:stop(Cluster).

%% Every discovered server fails at once, and new connections go to the
%% seed. A pool keeps its connections to the servers all the same, so the
%% blip moves nothing; once refreshes find the servers back, connections on
%% the seed are the ones to move.
blip_keeps_servers() ->
    eysql_fake_driver:set_servers(three()),
    Seed = {<<"seed">>, 5433},
    Cluster = start(#{hosts => [Seed], yb_servers_refresh_interval => 1}),
    discovered(Cluster),
    Servers = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    ?assertEqual(Servers, allowed(Cluster)),
    [fail(Cluster, Key) || Key <- Servers],
    ?assertEqual([Seed], placement(Cluster)),
    ?assertEqual(Servers ++ [Seed], allowed(Cluster)),
    wait_until(fun() -> failed(Cluster) =:= [] end, back),
    ?assertEqual(Servers, placement(Cluster)),
    ?assertEqual(Servers, allowed(Cluster)),
    eysql_cluster:stop(Cluster).

%% a is alone in the preferred zone and is left out for longer than the
%% test. It stays allowed until discovery stops listing it.
dropped_while_failing() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{topology_keys => "gcp.us-east1.us-east1-b:1,"
                                       "gcp.us-east1.us-east1-c:2,gcp.us-east1.us-east1-d:2",
                      failed_host_reconnect_delay_secs => 30}),
    discovered(Cluster),
    fail(Cluster, key(<<"a">>)),
    ?assertEqual([key(<<"a">>), key(<<"b">>), key(<<"c">>)], allowed(Cluster)),
    eysql_fake_driver:set_servers(tl(three())),
    %% A refresh asked for while one runs is dropped; ask until one reads the
    %% new list.
    wait_until(fun() -> eysql_cluster:refresh(Cluster), hosts(Cluster) =:= [<<"b">>, <<"c">>] end,
               dropped),
    ?assertEqual([key(<<"a">>)], failed(Cluster)),
    ?assertEqual([key(<<"b">>), key(<<"c">>)], allowed(Cluster)),
    eysql_cluster:stop(Cluster).

%% Run `Test' with the cluster's log events sent to this process.
with_log(Test) ->
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => #{pid => self()}}),
    ok = logger:set_module_level(eysql_cluster, info),
    try
        Test()
    after
        logger:unset_module_level(eysql_cluster),
        logger:remove_handler(?MODULE)
    end.

%% a fails three times in a row: one warning. The first refresh once its
%% one-second delay has passed finds it back, and nothing opens a connection
%% to it after that, as for a host in a fallback zone. Its run ends all the
%% same, at info, once it has gone the window (1.5 s) without failing, and
%% not before. A failure after that starts a new run, and warns again.
logs_once() ->
    with_log(fun logs_once_run/0).

logs_once_run() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start_windowed(#{}, 1500),
    discovered(Cluster),
    A = key(<<"a">>),
    Start = now_ms(),
    %% Three connects to a, each picked while it was working.
    Picked = [reserve(Cluster, A) || _ <- [1, 2, 3]],
    [eysql_cluster:open_failed(Cluster, Reservation, econnrefused) || Reservation <- Picked],
    %% A call returns after the casts before it, and after what they logged.
    ?assert(lists:member(A, failed(Cluster))),
    ?assertEqual([warning], logged("a:5433")),
    refreshing_until(Cluster, fun() -> failed(Cluster) =:= [] end, a_back),
    ?assertEqual([], logged("a:5433")),
    ?assertEqual(info, next_logged("a:5433", 2000)),
    ?assert(now_ms() - Start >= 1500),
    ?assertEqual(0, maps:get(A, counts(Cluster), 0)),
    fail(Cluster, A),
    _ = failed(Cluster),
    ?assertEqual([warning], logged("a:5433")),
    eysql_cluster:stop(Cluster).

%% The warning that starts a run says why: here the refused connect. A
%% reason that carries a password, as an ssl option error can, is logged
%% without it.
logs_reason() ->
    with_log(fun logs_reason_run/0).

logs_reason_run() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{}),
    discovered(Cluster),
    eysql_fake_driver:down(key(<<"a">>)),
    _ = open_n(Cluster, 3),
    ?assert(lists:member(key(<<"a">>), failed(Cluster))),
    [{warning, Refused}] = messages("a:5433"),
    ?assertNotEqual(nomatch, string:find(Refused, "econnrefused")),
    fail(Cluster, key(<<"b">>), {tls_alert, [{password, "k3y"}]}),
    _ = failed(Cluster),
    [{warning, Hidden}] = messages("b:5433"),
    ?assertEqual(nomatch, string:find(Hidden, "k3y")),
    ?assertNotEqual(nomatch, string:find(Hidden, "tls_alert")),
    eysql_cluster:stop(Cluster).

%% A reason can carry options as a map, such as epgsql's connect options,
%% with the password under an atom or a binary key.
hides_map_passwords() ->
    with_log(fun hides_map_passwords_run/0).

hides_map_passwords_run() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{}),
    discovered(Cluster),
    Reasons = [{key(<<"a">>), {bad_options, #{password => <<"secret">>, username => <<"app">>}}},
               {key(<<"b">>), {bad_options, [#{<<"password">> => <<"secret">>, <<"host">> => <<"b">>}]}}],
    [begin
         fail(Cluster, {Host, Port}, Reason),
         _ = failed(Cluster),
         [{warning, Text}] = messages(binary_to_list(Host) ++ ":" ++ integer_to_list(Port)),
         ?assertEqual(nomatch, string:find(Text, "secret")),
         ?assertNotEqual(nomatch, string:find(Text, "bad_options")),
         ?assertNotEqual(nomatch, string:find(Text, "redacted"))
     end || {{Host, Port}, Reason} <- Reasons],
    eysql_cluster:stop(Cluster).

%% PostgreSQL, read_write. pg1 is a standby: an open finds it so, which is
%% logged at info rather than as a warning, and cluster_info lists it under
%% read_only, not failed. It then goes down, comes back still a standby, and
%% is promoted. The refresh after each change puts it in the right list,
%% and each change starts a run of its own, logged once: a warning for down,
%% info for the rest.
standby_read_only() ->
    with_log(fun standby_read_only_run/0).

standby_read_only_run() ->
    Pg1 = {<<"pg1">>, 5432},
    Pg2 = {<<"pg2">>, 5432},
    eysql_fake_driver:set_standby(Pg1),
    Cluster = start_windowed(#{hosts => ["pg1", "pg2"], port => 5432, load_balance => false,
                               target_session_attrs => read_write}, 500),
    %% The first open tries pg1.
    _ = open_n(Cluster, 2),
    ?assertEqual({[], [Pg1]}, {failed(Cluster), read_only(Cluster)}),
    ?assertEqual([Pg2], placement(Cluster)),
    [{info, Standby}] = messages("pg1:5432"),
    ?assertNotEqual(nomatch, string:find(Standby, "standby")),
    eysql_fake_driver:down(Pg1),
    refreshing_until(Cluster, fun() -> failed(Cluster) =:= [Pg1] end, down),
    ?assertEqual([], read_only(Cluster)),
    [{warning, Down}] = messages("pg1:5432"),
    ?assertNotEqual(nomatch, string:find(Down, "econnrefused")),
    eysql_fake_driver:up(Pg1),
    refreshing_until(Cluster, fun() -> read_only(Cluster) =:= [Pg1] end, standby_again),
    ?assertEqual([], failed(Cluster)),
    ?assertEqual([info], logged("pg1:5432")),
    ?assertEqual([Pg2], placement(Cluster)),
    eysql_fake_driver:set_primary(Pg1),
    refreshing_until(Cluster, fun() -> read_only(Cluster) =:= [] end, promoted),
    ?assertEqual([], failed(Cluster)),
    %% The first primary in the order given takes new connections again.
    ?assertEqual([Pg1], placement(Cluster)),
    ?assertEqual(info, next_logged("pg1:5432", 2000)),
    eysql_cluster:stop(Cluster).

%% The levels of the events logged so far whose text contains `Text'.
logged(Text) ->
    [Level || {Level, _Message} <- messages(Text)].

%% The events logged so far whose text contains `Text'.
messages(Text) ->
    receive
        {eysql_log, Level, Message} ->
            case string:find(Message, Text) of
                nomatch -> messages(Text);
                _ -> [{Level, Message} | messages(Text)]
            end
    after 0 ->
            []
    end.

%% The level of the next event whose text contains `Text', if one comes
%% within `Timeout' ms, or `none'.
next_logged(Text, Timeout) ->
    receive
        {eysql_log, Level, Message} ->
            case string:find(Message, Text) of
                nomatch -> next_logged(Text, Timeout);
                _ -> Level
            end
    after Timeout ->
            none
    end.

%% yb_servers() reports addresses that change on restart. Three rolling
%% restarts each fail a server that then comes back at a new address:
%% discovery forgets the old one, so nothing gone stays failed. A host that
%% leaves and comes back under the same address starts a new run, and its
%% next failure warns again. The seed is a name of its own, which is kept.
forgets_gone_hosts() ->
    with_log(fun forgets_gone_hosts_run/0).

forgets_gone_hosts_run() ->
    Server = fun(Host) -> eysql_fake_driver:server(Host, <<"gcp">>, <<"us-east1">>, <<"us-east1-b">>) end,
    eysql_fake_driver:set_servers([Server(<<"10.0.0.1">>), Server(<<"10.0.0.2">>)]),
    Cluster = start(#{hosts => [{<<"seed">>, 5433}], failed_host_reconnect_delay_secs => 30}),
    discovered(Cluster),
    Discover = fun(Hosts) ->
                       eysql_fake_driver:set_servers([Server(Host) || Host <- Hosts]),
                       wait_until(fun() -> eysql_cluster:refresh(Cluster), hosts(Cluster) =:= Hosts end,
                                  {discovered, Hosts})
               end,
    Restart = fun(N, [Old, Other]) ->
                      fail(Cluster, {Old, 5433}),
                      ?assertEqual([{Old, 5433}], failed(Cluster)),
                      New = <<"10.0.1.", (integer_to_binary(N))/binary>>,
                      Discover([Other, New]),
                      ?assertEqual([], failed(Cluster)),
                      [Other, New]
              end,
    [Stay, Back] = lists:foldl(Restart, [<<"10.0.0.1">>, <<"10.0.0.2">>], [1, 2, 3]),
    fail(Cluster, key(Back)),
    _ = failed(Cluster),
    Text = binary_to_list(Back),
    ?assertEqual([warning], logged(Text)),
    Discover([Stay]),
    Discover([Stay, Back]),
    fail(Cluster, key(Back)),
    _ = failed(Cluster),
    ?assertEqual([warning], logged(Text)),
    fail(Cluster, {<<"seed">>, 5433}),
    Discover([Back]),
    ?assertEqual([key(Back), {<<"seed">>, 5433}], failed(Cluster)),
    eysql_cluster:stop(Cluster).

%% c's probe hangs, and discovery drops c: the probe is stopped, and c
%% forgotten.
probe_stops_when_pruned() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{hosts => [{<<"seed">>, 5433}], connect_timeout => 60000}),
    discovered(Cluster),
    C = key(<<"c">>),
    eysql_fake_driver:open_hangs(C),
    fail(Cluster, C),
    refreshing_until(Cluster, fun() -> length(eysql_fake_driver:hung(C)) =:= 1 end, probing),
    [Probe] = eysql_fake_driver:hung(C),
    eysql_fake_driver:set_servers(lists:sublist(three(), 2)),
    wait_until(fun() -> eysql_cluster:refresh(Cluster), hosts(Cluster) =:= [<<"a">>, <<"b">>] end,
               dropped),
    wait_until(fun() -> not is_process_alive(Probe) end, probe_stopped),
    ?assertEqual([], failed(Cluster)),
    eysql_cluster:stop(Cluster).

%% A connect to a was picked while a was a server, and fails once discovery
%% has dropped a and fails for now. a is not probed while no discovery lists
%% it; once one does, the next refresh's probe finds it back.
relisted_host_probed() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{hosts => [{<<"seed">>, 5433}]}),
    discovered(Cluster),
    A = key(<<"a">>),
    Picked = reserve(Cluster, A),
    eysql_fake_driver:set_servers(tl(three())),
    wait_until(fun() -> eysql_cluster:refresh(Cluster), length(hosts(Cluster)) =:= 2 end, dropped),
    %% The last refresh asked for started a discovery; let it finish, or it
    %% would forget a's failure below.
    timer:sleep(100),
    eysql_fake_driver:set_discover_error(timeout),
    Opens = opens(A),
    eysql_cluster:open_failed(Cluster, Picked, econnrefused),
    timer:sleep(1300),
    ?assertEqual(Opens, opens(A)),
    ?assertEqual([A], failed(Cluster)),
    eysql_fake_driver:set_servers(three()),
    wait_until(fun() -> eysql_cluster:refresh(Cluster), length(hosts(Cluster)) =:= 3 end, relisted),
    %% Nothing picks here, and discovery asks the seed first; only a probe
    %% connects to a.
    refreshing_until(Cluster, fun() -> failed(Cluster) =:= [] end, probed),
    eysql_cluster:stop(Cluster).

%% a's connects hang. The first refresh after its delay probes it, and the
%% cluster still answers at once. Another failure while the probe runs, and
%% refreshes past that failure's delay, start no second probe. The probe is
%% stopped after twice connect_timeout, 1.6 s, and a stays failed. Past its
%% next delay, nothing probes it until a refresh does; that probe stops with
%% the cluster, whose reason is `normal'.
probe_times_out() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{hosts => [{<<"lb">>, 5433}], connect_timeout => 800}),
    discovered(Cluster),
    A = key(<<"a">>),
    %% Two connects to a, both picked while it was working.
    [First, Second] = [reserve(Cluster, A) || _ <- [1, 2]],
    eysql_fake_driver:open_hangs(A),
    eysql_cluster:open_failed(Cluster, First, timeout),
    refreshing_until(Cluster, fun() -> length(eysql_fake_driver:hung(A)) =:= 1 end, probing),
    Started = now_ms(),
    [Probe] = eysql_fake_driver:hung(A),
    {Micros, _} = timer:tc(fun() -> eysql_cluster:snapshot(Cluster) end),
    ?assert(Micros < 100000),
    eysql_cluster:open_failed(Cluster, Second, timeout),
    [begin timer:sleep(100), eysql_cluster:refresh(Cluster) end || _ <- lists:seq(1, 12)],
    ?assertEqual([Probe], eysql_fake_driver:hung(A)),
    wait_until(fun() -> not is_process_alive(Probe) end, probe_stopped),
    ?assert(now_ms() - Started >= 1500),
    ?assertEqual([A], failed(Cluster)),
    timer:sleep(1300),
    ?assertEqual([], eysql_fake_driver:hung(A)),
    refreshing_until(Cluster, fun() -> length(eysql_fake_driver:hung(A)) =:= 1 end, probing_again),
    [Next] = eysql_fake_driver:hung(A),
    eysql_cluster:stop(Cluster),
    wait_until(fun() -> not is_process_alive(Next) end, stopped_with_cluster).

%% The probe that finds c back opened one connection, closed it, and never
%% counted it. With nothing to discover, the refreshes connect nowhere else.
probe_closes() ->
    Cluster = start(#{load_balance => false}),
    C = key(<<"c">>),
    Opens = opens(C),
    fail(Cluster, C),
    refreshing_until(Cluster, fun() -> failed(Cluster) =:= [] end, probed),
    ?assertEqual(Opens + 1, opens(C)),
    wait_until(fun() -> eysql_fake_driver:conns_to(C) =:= [] end, closed),
    ?assertEqual(#{}, counts(Cluster)),
    ?assertEqual(#{}, pending(Cluster)),
    eysql_cluster:stop(Cluster).

%% PostgreSQL with a standby, and read_write. Probes of the standby at
%% refreshes connect, find it read-only and close the connection, so it
%% stays out. Once it is promoted, a probe brings it back.
probe_checks_session() ->
    Pg1 = {<<"pg1">>, 5432},
    eysql_fake_driver:set_standby(Pg1),
    Cluster = start(#{hosts => ["pg1", "pg2"], port => 5432, load_balance => false,
                      target_session_attrs => read_write}),
    %% The second open, at the latest, tries pg1.
    _ = open_n(Cluster, 2),
    ?assertEqual([Pg1], read_only(Cluster)),
    Opens = opens(Pg1),
    refreshing_until(Cluster, fun() -> opens(Pg1) >= Opens + 2 end, two_probes),
    ?assertEqual({[], [Pg1]}, {failed(Cluster), read_only(Cluster)}),
    wait_until(fun() -> eysql_fake_driver:conns_to(Pg1) =:= [] end, closed),
    eysql_fake_driver:set_primary(Pg1),
    refreshing_until(Cluster, fun() -> read_only(Cluster) =:= [] end, promoted),
    ?assertEqual([], failed(Cluster)),
    eysql_cluster:stop(Cluster).

%% `Key' fails for good, with a delay (1 s) half the refresh interval (2 s).
%% Once a refresh has probed it, nothing tries it again for most of an
%% interval, well past its delay; then the next refresh probes it once.
%% Twice over.
probed_at_refreshes(Key) ->
    Opens = opens(Key),
    wait_until(fun() -> opens(Key) > Opens end, first_probe, 4000),
    lists:foreach(fun(_) ->
                          N = opens(Key),
                          timer:sleep(1500),
                          ?assertEqual(N, opens(Key)),
                          wait_until(fun() -> opens(Key) =:= N + 1 end, {next_probe, N}, 1500)
                  end,
                  [1, 2]).

%% c refuses from here on; discovery asks a first. Then PostgreSQL with a
%% standby that stays one, under read_write, where there is nothing to
%% discover and the refreshes only probe.
probed_once_per_refresh() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{yb_servers_refresh_interval => 2}),
    discovered(Cluster),
    C = key(<<"c">>),
    eysql_fake_driver:down(C),
    fail(Cluster, C),
    probed_at_refreshes(C),
    ?assertEqual([C], failed(Cluster)),
    eysql_cluster:stop(Cluster),
    Pg1 = {<<"pg1">>, 5432},
    eysql_fake_driver:set_standby(Pg1),
    Pg = start(#{hosts => ["pg1", "pg2"], port => 5432, load_balance => false,
                 target_session_attrs => read_write, yb_servers_refresh_interval => 2}),
    _ = open_n(Pg, 2),
    ?assertEqual([Pg1], read_only(Pg)),
    probed_at_refreshes(Pg1),
    ?assertEqual([Pg1], read_only(Pg)),
    eysql_cluster:stop(Pg).

%% a is preferred, fails once and is back at once, right after a refresh.
%% Past its delay (1 s), picks still leave it out, and nothing has tried it:
%% no refresh has probed it yet. The next refresh, 3 s after the last,
%% finds it back, and picks go to it again.
recovery_follows_refresh() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{topology_keys => "gcp.us-east1.us-east1-b:1,gcp.us-east1.*:2",
                      yb_servers_refresh_interval => 3}),
    discovered(Cluster),
    A = key(<<"a">>),
    Start = now_ms(),
    Opens = opens(A),
    fail(Cluster, A),
    {ok, _, Other} = eysql_cluster:open(Cluster),
    ?assertNotEqual(A, Other),
    timer:sleep(1500),
    {ok, _, Still} = eysql_cluster:open(Cluster),
    ?assertNotEqual(A, Still),
    ?assertEqual(Opens, opens(A)),
    ?assertEqual([A], failed(Cluster)),
    wait_until(fun() -> failed(Cluster) =:= [] end, found_back),
    ?assert(now_ms() - Start >= 2500),
    ?assertMatch({ok, _, A}, eysql_cluster:open(Cluster)),
    eysql_cluster:stop(Cluster).

%% With load_balance false there is nothing to discover, but refreshes still
%% probe. a fails once and is back at once, and nothing picks: past its
%% delay it is still failed, and the refresh on the timer, 2 s after the
%% cluster started, finds it back. With an interval of 0 there is no timer:
%% a stays failed until a pick starts a refresh.
static_refreshes() ->
    A = key(<<"a">>),
    Cluster = start(#{load_balance => false, yb_servers_refresh_interval => 2}),
    Start = now_ms(),
    fail(Cluster, A),
    timer:sleep(1500),
    ?assertEqual([A], failed(Cluster)),
    wait_until(fun() -> failed(Cluster) =:= [] end, found_back),
    ?assert(now_ms() - Start >= 1900),
    eysql_cluster:stop(Cluster),
    OnPicks = start(#{load_balance => false, yb_servers_refresh_interval => 0}),
    fail(OnPicks, A),
    timer:sleep(1500),
    ?assertEqual([A], failed(OnPicks)),
    {ok, _, Other} = eysql_cluster:open(OnPicks),
    ?assertNotEqual(A, Other),
    wait_until(fun() -> failed(OnPicks) =:= [] end, found_back_on_pick),
    eysql_cluster:stop(OnPicks).

%% The client reaches the cluster only through a load balancer among the
%% seeds, and the addresses the servers advertise drop packets from here.
%% The first open tries each server once, waiting out its connect timeout,
%% and then the load balancer. From then on, even past the servers' delay,
%% every open goes straight to the load balancer and none waits out a
%% server's connect timeout, until a refresh. The refresh probes each server
%% once, finds it still unreachable, and changes nothing.
lb_seed_skips_failed_servers() ->
    eysql_fake_driver:set_servers(three()),
    Servers = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    [eysql_fake_driver:unreachable(Key) || Key <- Servers],
    Lb = {<<"lb">>, 5433},
    Cluster = start(#{hosts => [Lb], connect_timeout => 300}),
    discovered(Cluster),
    ?assertMatch({ok, _, Lb}, eysql_cluster:open(Cluster)),
    ?assertEqual(Servers, failed(Cluster)),
    Opens = opens(Servers),
    ?assertMatch({ok, _, Lb}, eysql_cluster:open(Cluster)),
    timer:sleep(1200),
    Fast = fun() ->
                   {Micros, Result} = timer:tc(fun() -> eysql_cluster:open(Cluster) end),
                   ?assertMatch({ok, _, Lb}, Result),
                   ?assert(Micros < 250000)
           end,
    [Fast() || _ <- [1, 2, 3]],
    ?assertEqual(Opens, opens(Servers)),
    eysql_cluster:refresh(Cluster),
    Probed = [N + 1 || N <- Opens],
    wait_until(fun() -> opens(Servers) =:= Probed end, probed),
    %% The probes wait out connect_timeout, then fail.
    timer:sleep(500),
    ?assertEqual(Servers, failed(Cluster)),
    [Fast() || _ <- [1, 2, 3]],
    timer:sleep(1200),
    ?assertEqual(Probed, opens(Servers)),
    eysql_cluster:stop(Cluster).

%% fallback_to_topology_keys_only rules the seeds out, so while a, alone in
%% the listed zone, is failed, a pick finds no host at all, and fails as the
%% JDBC driver's getLeastLoadedServer does. The driver throws there without
%% forcing a refresh, and so these picks start nothing: no discovery and no
%% probe, not even past a's delay (1 s), until the refresh scheduled 3 s
%% after the last one probes a and finds it back.
stranded_pick_waits() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{topology_keys => "gcp.us-east1.us-east1-b",
                      fallback_to_topology_keys_only => true,
                      yb_servers_refresh_interval => 3}),
    discovered(Cluster),
    A = key(<<"a">>),
    %% Past the second after the first refresh, a's failure brings one
    %% forward; its discovery ends before the picks start.
    timer:sleep(1100),
    Refreshed = eysql_fake_driver:discoveries(),
    fail(Cluster, A),
    Failed = now_ms(),
    wait_until(fun() -> eysql_fake_driver:discoveries() =:= Refreshed + 1 end, refreshed),
    _ = failed(Cluster),
    Opens = opens(A),
    Discoveries = eysql_fake_driver:discoveries(),
    [begin
         ?assertEqual({error, {no_node_available, cluster}}, eysql_cluster:connect(Cluster)),
         timer:sleep(100)
     end || _ <- lists:seq(1, 17)],
    ?assert(now_ms() - Failed >= 1500),
    ?assertEqual({Opens, Discoveries}, {opens(A), eysql_fake_driver:discoveries()}),
    ?assertEqual([A], failed(Cluster)),
    wait_until(fun() -> failed(Cluster) =:= [] end, found_back),
    ?assert(now_ms() - Failed >= 2500),
    ?assertMatch({ok, _, A}, eysql_cluster:open(Cluster)),
    eysql_cluster:stop(Cluster).

%% Every server refuses, and the seed, a load balancer, answers. In the
%% modes where the JDBC driver's getLeastLoadedServer throws rather than let
%% the driver fall back to its URL hosts, the seed takes nothing once
%% servers are discovered: an open tries the eligible servers and then
%% fails as the driver throws, naming the node type it looked for, whether
%% or not it tried any; and the pool keeps nothing on the seed. only_primary
%% is one: a seed would count as a primary, but a load balancer can lead to
%% a read replica. The other modes take the seed once every eligible server
%% has failed; fallback_to_topology_keys_only without keys is one of them,
%% as the driver's ClusterAwareLoadBalancer ignores it.
refusing_modes() ->
    eysql_fake_driver:set_servers(three()),
    Servers = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Lb = {<<"lb">>, 5433},
    Keys = "gcp.us-east1.us-east1-b",
    Start = fun(Options) ->
                    [eysql_fake_driver:up(Key) || Key <- Servers],
                    Cluster = start(maps:merge(#{hosts => [Lb]}, Options)),
                    discovered(Cluster),
                    [eysql_fake_driver:down(Key) || Key <- Servers],
                    Cluster
            end,
    Refusing = [{#{load_balance => only_primary}, primary},
                {#{load_balance => only_rr}, read_replica},
                {#{load_balance => any, topology_keys => Keys, fallback_to_topology_keys_only => true},
                 cluster},
                {#{load_balance => true, topology_keys => Keys, fallback_to_topology_keys_only => true},
                 cluster}],
    FallingBack = [#{load_balance => any},
                   #{load_balance => prefer_primary},
                   #{load_balance => prefer_rr},
                   #{load_balance => any, fallback_to_topology_keys_only => true},
                   #{load_balance => prefer_primary, topology_keys => Keys,
                     fallback_to_topology_keys_only => true}],
    lists:foreach(
      fun({Options, Type}) ->
              Cluster = Start(Options),
              ?assertEqual({error, {no_node_available, Type}}, eysql_cluster:connect(Cluster)),
              ?assertEqual({error, {no_node_available, Type}}, eysql_cluster:connect(Cluster)),
              ?assertEqual([], placement(Cluster)),
              ?assertNot(lists:member(Lb, allowed(Cluster))),
              eysql_cluster:stop(Cluster)
      end,
      Refusing),
    lists:foreach(
      fun(Options) ->
              Cluster = Start(Options),
              ?assertMatch({ok, _, Lb}, eysql_cluster:open(Cluster)),
              ?assertEqual([Lb], placement(Cluster)),
              eysql_cluster:stop(Cluster)
      end,
      FallingBack).

%% Five servers refuse and the seed answers. One open tries each server
%% once and then the seed, as the JDBC driver's getConnection tries every
%% eligible server before its caller connects to the hosts in its URL.
tries_every_server() ->
    Hosts = [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>],
    eysql_fake_driver:set_servers([eysql_fake_driver:server(H, <<"gcp">>, <<"us-east1">>, <<"us-east1-b">>)
                                   || H <- Hosts]),
    Seed = {<<"seed">>, 5433},
    Cluster = start(#{hosts => [Seed]}),
    discovered(Cluster),
    Servers = [key(H) || H <- Hosts],
    [eysql_fake_driver:down(Key) || Key <- Servers],
    Before = opens(Servers),
    ?assertMatch({ok, _, Seed}, eysql_cluster:open(Cluster)),
    ?assertEqual([N + 1 || N <- Before], opens(Servers)),
    ?assertEqual(Servers, failed(Cluster)),
    eysql_cluster:stop(Cluster).

%% Every server refuses, and so does the one seed. One open tries each
%% server once and the seed once, then returns the last error. The next,
%% with every host failed, has only the last resort, and tries the seed
%% once, where it used to try it on each of its attempts. With nothing to
%% discover, a lone host that refuses is tried once per open too.
dead_seed_once() ->
    eysql_fake_driver:set_servers(three()),
    Seed = {<<"seed">>, 5433},
    Servers = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Cluster = start(#{hosts => [Seed]}),
    discovered(Cluster),
    [eysql_fake_driver:down(Key) || Key <- [Seed | Servers]],
    Before = opens([Seed | Servers]),
    ?assertEqual({error, econnrefused}, eysql_cluster:connect(Cluster)),
    ?assertEqual([N + 1 || N <- Before], opens([Seed | Servers])),
    ?assertEqual({error, econnrefused}, eysql_cluster:connect(Cluster)),
    ?assertEqual([N + 1 || N <- tl(Before)], opens(Servers)),
    ?assertEqual(hd(Before) + 2, opens(Seed)),
    eysql_cluster:stop(Cluster),
    Pg = {<<"pg">>, 5432},
    Static = start(#{hosts => ["pg"], port => 5432, load_balance => false}),
    eysql_fake_driver:down(Pg),
    PgOpens = opens(Pg),
    ?assertEqual({error, econnrefused}, eysql_cluster:connect(Static)),
    ?assertEqual(PgOpens + 1, opens(Pg)),
    eysql_cluster:stop(Static).

%% A connect before the first discovery has finished waits for it, as the
%% JDBC driver runs its first refresh before it picks. Under only_rr it
%% then goes to the read replica discovery finds, rather than fail for want
%% of one among the seeds.
connect_waits_for_discovery() ->
    Rr = eysql_fake_driver:server(<<"r">>, <<"gcp">>, <<"us-east1">>, <<"us-east1-b">>, read_replica),
    eysql_fake_driver:set_servers([hd(three()), Rr]),
    eysql_fake_driver:set_discover_delay(300),
    Cluster = start(#{hosts => [key(<<"a">>)], load_balance => only_rr}),
    ?assertMatch({ok, _, {<<"r">>, 5433}}, eysql_cluster:open(Cluster)),
    eysql_cluster:stop(Cluster).

%% Until a discovery has succeeded the seeds are all there is, and every
%% mode takes them, whatever their node type, as the JDBC driver makes a
%% plain connection to the hosts in its URL when its refresh fails or finds
%% no yb_servers(). Here under only_rr, which a seed, counted as a primary,
%% would not pass: on PostgreSQL, and after a first discovery that failed.
seeds_before_discovery() ->
    Pg = start(#{hosts => ["pg"], port => 5432, load_balance => only_rr}),
    ?assertMatch({ok, _, {<<"pg">>, 5432}}, eysql_cluster:open(Pg)),
    ?assertEqual(static, maps:get(mode, eysql_cluster:snapshot(Pg))),
    eysql_cluster:stop(Pg),
    eysql_fake_driver:set_discover_error(timeout),
    Cluster = start(#{load_balance => only_rr}),
    {ok, _, Key} = eysql_cluster:open(Cluster),
    ?assert(lists:member(Key, [key(<<"a">>), key(<<"b">>), key(<<"c">>)])),
    ?assertNot(maps:get(discovered, eysql_cluster:snapshot(Cluster))),
    eysql_cluster:stop(Cluster).

%% Discovery fails from here on, and a refresh finds out; each failed
%% discovery asks all three servers. As the JDBC driver's failed refresh
%% leaves its clock where it was, the refresh stays due. Nothing tries
%% again on a timer before the next scheduled refresh, 5 s on, not even
%% once a failed host's delay (1 s) has passed; but the next pick does,
%% and no other pick in the second after it.
discovery_failure_cadence() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{yb_servers_refresh_interval => 5}),
    discovered(Cluster),
    eysql_fake_driver:set_discover_error(timeout),
    Start = eysql_fake_driver:discoveries(),
    eysql_cluster:refresh(Cluster),
    wait_until(fun() -> eysql_fake_driver:discoveries() =:= Start + 3 end, failed_once),
    timer:sleep(2000),
    ?assertEqual(Start + 3, eysql_fake_driver:discoveries()),
    {ok, _, _} = eysql_cluster:open(Cluster),
    {ok, _, _} = eysql_cluster:open(Cluster),
    wait_until(fun() -> eysql_fake_driver:discoveries() =:= Start + 6 end, failed_again),
    timer:sleep(300),
    ?assertEqual(Start + 6, eysql_fake_driver:discoveries()),
    eysql_cluster:stop(Cluster).

%% Discovery fails three times in a row: one warning, with the reason. Once
%% discovery works again, its run ends at info when the window (1 s) has
%% passed since the last failure, and not at once.
logs_discovery_once() ->
    with_log(fun logs_discovery_once_run/0).

logs_discovery_once_run() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start_windowed(#{}, 1000),
    discovered(Cluster),
    eysql_fake_driver:set_discover_error(timeout),
    Start = eysql_fake_driver:discoveries(),
    refreshing_until(Cluster, fun() -> eysql_fake_driver:discoveries() >= Start + 9 end,
                     three_failures),
    %% Let the last discovery reach the cluster.
    timer:sleep(100),
    _ = failed(Cluster),
    LastFailure = now_ms(),
    [{warning, Failed}] = messages("server discovery"),
    ?assertNotEqual(nomatch, string:find(Failed, "no_server_reachable")),
    eysql_fake_driver:set_servers(three()),
    Before = eysql_fake_driver:discoveries(),
    eysql_cluster:refresh(Cluster),
    wait_until(fun() -> eysql_fake_driver:discoveries() > Before end, discovered_again),
    timer:sleep(50),
    _ = failed(Cluster),
    ?assertEqual([], logged("server discovery")),
    ?assertEqual(info, next_logged("server discovery", 2000)),
    ?assert(now_ms() - LastFailure >= 700),
    eysql_cluster:stop(Cluster).

log(#{level := Level, msg := Msg}, #{config := #{pid := Pid}}) ->
    Pid ! {eysql_log, Level, lists:flatten(message(Msg))}.

message({string, String}) -> unicode:characters_to_list(String);
message({report, Report}) -> io_lib:format("~p", [Report]);
message({Format, Args}) -> io_lib:format(Format, Args).

abandoned_pick() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{}),
    discovered(Cluster),
    Self = self(),
    Picker = spawn(fun() ->
                           Self ! {picked, self(), eysql_cluster:pick(Cluster)},
                           receive _ -> ok end
                   end),
    Picked = receive {picked, Picker, P} -> P after 2000 -> error(no_pick) end,
    {ok, Server, _Spec, _Reservation} = Picked,
    ?assertEqual(#{eysql_topology:key(Server) => 1}, pending(Cluster)),
    exit(Picker, kill),
    wait_until(fun() -> pending(Cluster) =:= #{} end, released),
    eysql_cluster:stop(Cluster).

picks_settle() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{}),
    discovered(Cluster),
    %% Opened, and failed then opened.
    eysql_fake_driver:down(key(<<"a">>)),
    _ = open_n(Cluster, 3),
    ?assertEqual(#{}, pending(Cluster)),
    ?assertNot(monitored(Cluster, self())),
    %% A connect that raises gives its pick back without failing the host.
    [eysql_fake_driver:crash_open(key(H)) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    ?assertError({fake_crash, _}, eysql_cluster:open(Cluster)),
    ?assertEqual(#{}, pending(Cluster)),
    ?assertEqual([], failed(Cluster) -- [key(<<"a">>)]),
    ?assertNot(monitored(Cluster, self())),
    eysql_cluster:stop(Cluster).

seeds_fallback() ->
    eysql_fake_driver:set_servers(three()),
    [eysql_fake_driver:down(key(H)) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Seed = {<<"seed">>, 5433},
    Cluster = start(#{hosts => [Seed], connect_timeout => 1000}),
    discovered(Cluster),
    %% a, b and c refuse and are left out; the seed answers.
    ?assertMatch({ok, _, Seed}, eysql_cluster:open(Cluster)),
    ?assertMatch({ok, _, Seed}, eysql_cluster:open(Cluster)),
    ?assertEqual([Seed], placement(Cluster)),
    %% A pool keeps its connections to a, b and c: a failure alone moves
    %% nothing.
    All = [key(<<"a">>), key(<<"b">>), key(<<"c">>), Seed],
    ?assertEqual(All, allowed(Cluster)),
    %% Nor does the end of their delay, with their addresses now
    %% unreachable: nothing has found them back, and the seed keeps its
    %% connections.
    [eysql_fake_driver:unreachable(key(H)) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    timer:sleep(1500),
    ?assertEqual(All, allowed(Cluster)),
    ?assertEqual([Seed], placement(Cluster)),
    %% With the seed failed too, nothing has shown it works, and nothing is
    %% moved. New connections still try the seed, the last resort.
    fail(Cluster, Seed),
    ?assertEqual(All, allowed(Cluster)),
    ?assertEqual([Seed], placement(Cluster)),
    eysql_cluster:stop(Cluster).

public_ips() ->
    Private = [<<"10.0.0.1">>, <<"10.0.0.2">>, <<"10.0.0.3">>],
    Public = [<<"pub-a">>, <<"pub-b">>, <<"pub-c">>],
    Zones = [<<"us-east1-b">>, <<"us-east1-c">>, <<"us-east1-d">>],
    Server = fun(Host, Zone, PublicIp) ->
                     eysql_fake_driver:server(Host, <<"gcp">>, <<"us-east1">>, Zone, primary, PublicIp)
             end,
    Servers = [Server(H, Z, P) || {H, Z, P} <- lists:zip3(Private, Zones, Public)],
    eysql_fake_driver:set_servers(Servers),
    %% The addresses the servers advertise are not reachable from here.
    [eysql_fake_driver:down(key(H)) || H <- Private],
    Cluster = start(#{hosts => [{<<"pub-a">>, 5433}, {<<"yb">>, 5433}]}),
    discovered(Cluster),
    ?assertEqual(Public, hosts(Cluster)),
    {ok, _, {Host, 5433}} = eysql_cluster:open(Cluster),
    ?assert(lists:member(Host, Public)),
    %% An answer from a name that is neither host nor public IP keeps them.
    [eysql_fake_driver:down(key(H)) || H <- Public],
    eysql_fake_driver:set_servers(Servers ++ [Server(<<"10.0.0.4">>, <<"us-east1-b">>, <<"pub-d">>)]),
    eysql_cluster:refresh(Cluster),
    wait_until(fun() -> length(hosts(Cluster)) =:= 4 end, refreshed),
    ?assertEqual(Public ++ [<<"pub-d">>], hosts(Cluster)),
    eysql_cluster:stop(Cluster).

%% Three servers, each with a host on the cluster's network and a public
%% IP, as yb_servers() reports them.
external() ->
    [eysql_fake_driver:server(<<"10.0.0.", N>>, <<"gcp">>, <<"us-east1">>, Zone, primary, <<"34.0.0.", N>>)
     || {N, Zone} <- [{$1, <<"us-east1-b">>}, {$2, <<"us-east1-c">>}, {$3, <<"us-east1-d">>}]].

private_ips() -> [<<"10.0.0.1">>, <<"10.0.0.2">>, <<"10.0.0.3">>].

public_ips_of() -> [<<"34.0.0.1">>, <<"34.0.0.2">>, <<"34.0.0.3">>].

%% A cluster whose names resolve as `Names' says, and IP addresses to
%% themselves, as a resolver would; any other name does not resolve. A name
%% that `Names' maps to `error' does not resolve either, and one it maps to
%% `hang' never answers.
start_resolving(Overrides, Names) ->
    Resolve = fun(Name) ->
                      case maps:find(Name, Names) of
                          {ok, hang} ->
                              timer:sleep(60000);
                          {ok, error} ->
                              error;
                          {ok, Address} ->
                              {ok, Address};
                          error ->
                              case inet:parse_address(binary_to_list(Name)) of
                                  {ok, Address} -> {ok, Address};
                                  {error, _} -> error
                              end
                      end
              end,
    {ok, Cluster} = eysql_cluster:start_link(maps:put(resolve, Resolve, config(Overrides))),
    Cluster.

%% The seed is a DNS name, such as a load balancer's, that resolves to b's
%% public IP: the client is outside the cluster's network. As text the name
%% is neither a host nor a public IP; resolved, as the JDBC driver resolves
%% it, it is b's public IP, so connections go to each server's.
dns_seed_public_ip() ->
    eysql_fake_driver:set_servers(external()),
    Cluster = start_resolving(#{hosts => [{<<"yb.example.com">>, 5433}]},
                              #{<<"yb.example.com">> => {34, 0, 0, 2}}),
    discovered(Cluster),
    ?assertEqual(public_ips_of(), hosts(Cluster)),
    {ok, _, {Host, 5433}} = eysql_cluster:open(Cluster),
    ?assert(lists:member(Host, public_ips_of())),
    eysql_cluster:stop(Cluster).

%% Every server has a public IP that resolves, so undecided, the driver
%% would guess them. But the seed resolves to b's host, which decides: the
%% hosts. The servers' hosts are DNS names here, compared by address too.
dns_seed_host() ->
    Servers = [S#{host := <<"yb-", N, ".svc">>} || {N, S} <- lists:zip("123", external())],
    eysql_fake_driver:set_servers(Servers),
    Cluster = start_resolving(#{hosts => [{<<"yb.svc">>, 5433}]},
                              #{<<"yb.svc">> => {10, 0, 0, 2}, <<"yb-1.svc">> => {10, 0, 0, 1},
                                <<"yb-2.svc">> => {10, 0, 0, 2}, <<"yb-3.svc">> => {10, 0, 0, 3}}),
    discovered(Cluster),
    ?assertEqual([<<"yb-1.svc">>, <<"yb-2.svc">>, <<"yb-3.svc">>], hosts(Cluster)),
    eysql_cluster:stop(Cluster).

%% The first discovery answers at a's host, which decides the hosts, as the
%% driver keeps its useHostColumn for the cluster once set. A later one
%% answers at b's public IP, as when the first seed has gone and the second
%% is a public address, and the hosts stand.
address_decided_once() ->
    eysql_fake_driver:set_servers(external()),
    First = {<<"yb-a.svc">>, 5433},
    Second = {<<"34.0.0.2">>, 5433},
    Cluster = start_resolving(#{hosts => [First, Second]}, #{<<"yb-a.svc">> => {10, 0, 0, 1}}),
    discovered(Cluster),
    ?assertEqual(private_ips(), hosts(Cluster)),
    eysql_fake_driver:down(First),
    Opens = opens(Second),
    refreshes(Cluster, 1),
    ?assertEqual(Opens + 1, opens(Second)),
    ?assertEqual(private_ips(), hosts(Cluster)),
    eysql_cluster:stop(Cluster).

%% The seed resolves to an address that is neither a host nor a public IP,
%% as a load balancer's may: nothing decides, and the driver guesses at
%% each refresh. Every server has a public IP that resolves: public IPs.
%% One server has none: hosts. One's does not resolve: hosts. All resolve
%% again: public IPs, the decision still open.
address_guess() ->
    [A, B, C] = external(),
    eysql_fake_driver:set_servers([A, B, C]),
    Cluster = start_resolving(#{hosts => [{<<"lb">>, 5433}]}, #{<<"lb">> => {192, 168, 0, 1}}),
    discovered(Cluster),
    ?assertEqual(public_ips_of(), hosts(Cluster)),
    Rediscover = fun(Servers) ->
                         eysql_fake_driver:set_servers(Servers),
                         refreshes(Cluster, 1),
                         hosts(Cluster)
                 end,
    ?assertEqual(private_ips(), Rediscover([A, B, maps:remove(public_ip, C)])),
    ?assertEqual(private_ips(), Rediscover([A, B, C#{public_ip := <<"gone.example.com">>}])),
    ?assertEqual(public_ips_of(), Rediscover([A, B, C])),
    eysql_cluster:stop(Cluster).

%% A name that does not resolve. The seed that answered discovery: the
%% driver fails that refresh, and the discovery moves on to the next seed,
%% which resolves to a's public IP. A server's name counts as no address:
%% here a's host does not resolve, and its public IP still decides. And a
%% lookup that is still running after connect_timeout counts as a name that
%% does not resolve, so that no guess of public IPs is made on it.
address_unresolved() ->
    eysql_fake_driver:set_servers(external()),
    Gone = {<<"gone.example.com">>, 5433},
    Cluster = start_resolving(#{hosts => [Gone, {<<"yb.example.com">>, 5433}]},
                              #{<<"yb.example.com">> => {34, 0, 0, 1}, <<"10.0.0.1">> => error}),
    discovered(Cluster),
    ?assertEqual(1, opens(Gone)),
    ?assertEqual(public_ips_of(), hosts(Cluster)),
    eysql_cluster:stop(Cluster),
    Slow = start_resolving(#{hosts => [{<<"lb">>, 5433}], connect_timeout => 300},
                           #{<<"lb">> => {192, 168, 0, 1}, <<"34.0.0.3">> => hang}),
    Start = now_ms(),
    discovered(Slow),
    ?assert(now_ms() - Start < 2000),
    ?assertEqual(private_ips(), hosts(Slow)),
    eysql_cluster:stop(Slow).

%% Without a resolver of the test's, names resolve as Java's
%% InetAddress.getByName resolves them: the seed "localhost" to 127.0.0.1,
%% and a's public IP, IPv4-mapped IPv6, to the IPv4 address it maps. So a's
%% public IP answered, and connections go to public IPs: a's, and b's and
%% c's hosts, since they have none.
default_resolver() ->
    [A, B, C] = [maps:remove(public_ip, S) || S <- external()],
    eysql_fake_driver:set_servers([A#{public_ip => <<"::ffff:127.0.0.1">>}, B, C]),
    {ok, Cluster} = eysql_cluster:start_link(maps:remove(resolve, config(#{hosts => [{<<"localhost">>, 5433}]}))),
    discovered(Cluster),
    ?assertEqual([<<"::ffff:127.0.0.1">>, <<"10.0.0.2">>, <<"10.0.0.3">>], hosts(Cluster)),
    eysql_cluster:stop(Cluster).

manual_refresh() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{yb_servers_refresh_interval => 2}),
    discovered(Cluster),
    %% The next discovery is due 2 s after the first. Refresh halfway.
    timer:sleep(1000),
    Before = eysql_fake_driver:discoveries(),
    eysql_cluster:refresh(Cluster),
    wait_until(fun() -> eysql_fake_driver:discoveries() > Before end, manual),
    %% The next is now due 2 s after this one. A timer left running would
    %% also discover at the 2 s mark.
    timer:sleep(1500),
    ?assertEqual(Before + 1, eysql_fake_driver:discoveries()),
    eysql_cluster:stop(Cluster).

refresh_each_pick() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{yb_servers_refresh_interval => 0}),
    discovered(Cluster),
    %% No timer: nothing discovers while nothing picks.
    Start = eysql_fake_driver:discoveries(),
    timer:sleep(300),
    ?assertEqual(Start, eysql_fake_driver:discoveries()),
    _ = open_n(Cluster, 1),
    wait_until(fun() -> eysql_fake_driver:discoveries() =:= Start + 1 end, refreshed),
    %% Let it finish before discoveries slow down.
    timer:sleep(100),
    %% A slow discovery holds up no pick, and runs once however many picks
    %% come while it does.
    eysql_fake_driver:set_discover_delay(1000),
    Before = erlang:monotonic_time(millisecond),
    _ = open_n(Cluster, 4),
    ?assert(erlang:monotonic_time(millisecond) - Before < 500),
    wait_until(fun() -> eysql_fake_driver:discoveries() =:= Start + 2 end, slow_refresh),
    _ = open_n(Cluster, 2),
    ?assertEqual(Start + 2, eysql_fake_driver:discoveries()),
    %% Once it has finished, the next pick starts another.
    eysql_fake_driver:set_discover_delay(0),
    timer:sleep(1300),
    _ = open_n(Cluster, 1),
    wait_until(fun() -> eysql_fake_driver:discoveries() =:= Start + 3 end, next_refresh),
    eysql_cluster:stop(Cluster).

%% a fails and is back at once. The next refresh starts before a's delay
%% (1 s) has passed, so it does not probe a, and its discovery takes 1.5 s.
%% By the time that discovery succeeds a is due, and it is probed then,
%% rather than at the next refresh, 300 s on. The seed is a name of its own,
%% so that no discovery connects to a.
probes_after_discovery() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{hosts => [{<<"lb">>, 5433}]}),
    discovered(Cluster),
    A = key(<<"a">>),
    fail(Cluster, A),
    ?assertEqual([A], failed(Cluster)),
    Opens = opens(A),
    Discoveries = eysql_fake_driver:discoveries(),
    eysql_fake_driver:set_discover_delay(1500),
    Start = now_ms(),
    eysql_cluster:refresh(Cluster),
    wait_until(fun() -> failed(Cluster) =:= [] end, probed_after_discovery, 3000),
    ?assert(now_ms() - Start >= 1400),
    ?assertEqual(Opens + 1, opens(A)),
    ?assertEqual(Discoveries + 1, eysql_fake_driver:discoveries()),
    eysql_cluster:stop(Cluster).

%% a, alone in the preferred zone, answers but fails each login. Only a
%% failure to connect, pgjdbc's 08001, marks a host down; the driver skips
%% any other for the rest of that one connection. The first open tries a
%% and moves on to b or c. a is listed as rejected, not failed, and it is
%% left out of picks all the same, so the opens after it go straight to b
%% or c; a pool keeps every level, since a has not shown it takes
%% connections. The run is logged once, in words that do not call a down.
%% A server error, a server starting up and a failed TLS handshake do the
%% same, each tried here on a cluster of its own, while a name that does
%% not resolve marks a down. The seed is a name of its own, so that no
%% discovery connects to a.
rejected_not_down() ->
    with_log(fun rejected_not_down_run/0).

rejected_not_down_run() ->
    eysql_fake_driver:set_servers(three()),
    [A, B, C] = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Start = fun() ->
                    Cluster = start(#{hosts => [{<<"lb">>, 5433}],
                                      topology_keys => "gcp.us-east1.us-east1-b:1,gcp.us-east1.*:2"}),
                    discovered(Cluster),
                    Cluster
            end,
    Elsewhere = fun(Cluster) ->
                        {ok, _, Key} = eysql_cluster:open(Cluster),
                        ?assert(lists:member(Key, [B, C]))
                end,
    Cluster = Start(),
    eysql_fake_driver:reject(A, invalid_password),
    Opens = opens(A),
    [Elsewhere(Cluster) || _ <- [1, 2, 3]],
    ?assertEqual(Opens + 1, opens(A)),
    ?assertEqual({[], [A], [B, C]}, {failed(Cluster), rejected(Cluster), placement(Cluster)}),
    ?assertEqual([A, B, C], allowed(Cluster)),
    [{warning, Text}] = messages("a:5433"),
    ?assertNotEqual(nomatch, string:find(Text, "invalid_password")),
    ?assertEqual(nomatch, string:find(Text, "could not connect")),
    eysql_cluster:stop(Cluster),
    Answered = [eysql_fake_driver:pg_error(<<"53300">>), eysql_fake_driver:pg_error(<<"3D000">>),
                eysql_fake_driver:pg_error(<<"57P03">>), {ssl_negotiation_failed, closed}],
    lists:foreach(fun(Reason) ->
                          Fresh = Start(),
                          eysql_fake_driver:reject(A, Reason),
                          N = opens(A),
                          [Elsewhere(Fresh) || _ <- [1, 2]],
                          ?assertEqual(N + 1, opens(A)),
                          ?assertEqual({[], [A], [B, C]},
                                       {failed(Fresh), rejected(Fresh), placement(Fresh)}),
                          [{warning, Rejected}] = messages("a:5433"),
                          ?assertEqual(nomatch, string:find(Rejected, "could not connect")),
                          eysql_cluster:stop(Fresh)
                  end,
                  Answered),
    Unresolved = Start(),
    eysql_fake_driver:reject(A, nxdomain),
    Elsewhere(Unresolved),
    ?assertEqual({[A], []}, {failed(Unresolved), rejected(Unresolved)}),
    [{warning, Down}] = messages("a:5433"),
    ?assertNotEqual(nomatch, string:find(Down, "could not connect")),
    eysql_cluster:stop(Unresolved).

%% a is down. It restarts, and first answers as a server starting up does:
%% the refresh's probe finds it rejected, not down. It stays out of picks,
%% and no open tries it. Once it takes connections, a refresh's probe
%% brings it back.
probe_finds_rejected() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{hosts => [{<<"lb">>, 5433}]}),
    discovered(Cluster),
    A = key(<<"a">>),
    eysql_fake_driver:down(A),
    fail(Cluster, A),
    ?assertEqual({[A], []}, {failed(Cluster), rejected(Cluster)}),
    ?assertNot(lists:member(A, placement(Cluster))),
    eysql_fake_driver:up(A),
    eysql_fake_driver:reject(A, eysql_fake_driver:pg_error(<<"57P03">>)),
    refreshing_until(Cluster, fun() -> rejected(Cluster) =:= [A] end, starting_up),
    ?assertEqual([], failed(Cluster)),
    ?assertNot(lists:member(A, placement(Cluster))),
    Opens = opens(A),
    ?assertNot(lists:member(A, [K || {_, K} <- open_n(Cluster, 3)])),
    ?assertEqual(Opens, opens(A)),
    eysql_fake_driver:up(A),
    refreshing_until(Cluster, fun() -> rejected(Cluster) =:= [] end, up),
    ?assertEqual([], failed(Cluster)),
    ?assert(lists:member(A, placement(Cluster))),
    eysql_cluster:stop(Cluster).

%% a, alone in the preferred zone, answers but fails each login, as after a
%% password rotation. The first open tries it and moves on, and from then
%% on a is out of picks: the opens after it go straight to b or c. A
%% refresh before its delay (1 s) has passed does not probe it. The first
%% refresh after does, once, and finds its login still failing: a stays
%% rejected. Once the login works, the first refresh after a's next delay
%% probes it, and that probe brings it back.
rejected_probed() ->
    eysql_fake_driver:set_servers(three()),
    A = key(<<"a">>),
    Cluster = start(#{hosts => [{<<"lb">>, 5433}],
                      topology_keys => "gcp.us-east1.us-east1-b:1,gcp.us-east1.*:2"}),
    discovered(Cluster),
    eysql_fake_driver:reject(A, invalid_password),
    {ok, _, Elsewhere} = eysql_cluster:open(Cluster),
    ?assertNotEqual(A, Elsewhere),
    ?assertEqual({[], [A]}, {failed(Cluster), rejected(Cluster)}),
    Opens = opens(A),
    ?assertNot(lists:member(A, [K || {_, K} <- open_n(Cluster, 3)])),
    ?assertNot(lists:member(A, placement(Cluster))),
    refreshes(Cluster, 1),
    ?assertEqual(Opens, opens(A)),
    timer:sleep(1100),
    refreshes(Cluster, 1),
    wait_until(fun() -> opens(A) =:= Opens + 1 end, probed),
    timer:sleep(100),
    ?assertEqual(Opens + 1, opens(A)),
    ?assertEqual({[], [A]}, {failed(Cluster), rejected(Cluster)}),
    ?assertNot(lists:member(A, placement(Cluster))),
    eysql_fake_driver:up(A),
    timer:sleep(1100),
    refreshes(Cluster, 1),
    wait_until(fun() -> rejected(Cluster) =:= [] end, probed_back),
    ?assertEqual(Opens + 2, opens(A)),
    ?assertEqual({[], [A]}, {failed(Cluster), placement(Cluster)}),
    ?assertMatch({ok, _, A}, eysql_cluster:open(Cluster)),
    eysql_cluster:stop(Cluster).

%% With load_balance false the hosts are taken in order. a is down and b
%% fails logins. The first open tries a and b and moves on to c; the next
%% goes straight to c, since a and b are both out of picks. The first
%% refresh after their delay (1 s) probes both. Once b's login works, the
%% first refresh after its next delay finds it back, and b, now the first
%% host that works, takes the next open.
rejected_seed_probed() ->
    [A, B, C] = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    eysql_fake_driver:down(A),
    eysql_fake_driver:reject(B, invalid_password),
    Cluster = start(#{load_balance => false, hosts => [A, B, C]}),
    ?assertEqual([C, C], [K || {_, K} <- open_n(Cluster, 2)]),
    ?assertEqual({[A], [B]}, {failed(Cluster), rejected(Cluster)}),
    ?assertEqual({1, 1}, {opens(A), opens(B)}),
    ?assertEqual([C], placement(Cluster)),
    timer:sleep(1100),
    eysql_cluster:refresh(Cluster),
    wait_until(fun() -> {opens(A), opens(B)} =:= {2, 2} end, probed),
    timer:sleep(100),
    ?assertEqual({[A], [B]}, {failed(Cluster), rejected(Cluster)}),
    ?assertEqual([C], placement(Cluster)),
    eysql_fake_driver:up(B),
    timer:sleep(1100),
    eysql_cluster:refresh(Cluster),
    wait_until(fun() -> rejected(Cluster) =:= [] end, b_back),
    ?assertEqual({[A], [B]}, {failed(Cluster), placement(Cluster)}),
    ?assertMatch({ok, _, B}, eysql_cluster:open(Cluster)),
    eysql_cluster:stop(Cluster).

%% A server restarted gracefully, as yb_failover restarts one: a, alone in
%% the preferred zone, first answers 57P03 while it shuts down, then
%% refuses connections while it is stopped, then accepts them again. It is
%% out of picks throughout: after the first open, which tries it, no open
%% goes to it or tries it. Refreshes past its delay (1 s) probe it, once
%% each, and each probe sorts it by its reason: rejected while it shuts
%% down, down once it refuses. Each run is logged once, however many
%% probes fail in it. Once a accepts connections again, the first refresh
%% after its delay brings it back, and picks go to it.
graceful_restart() ->
    with_log(fun graceful_restart_run/0).

graceful_restart_run() ->
    eysql_fake_driver:set_servers(three()),
    [A, B, C] = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Cluster = start(#{hosts => [{<<"lb">>, 5433}],
                      topology_keys => "gcp.us-east1.us-east1-b:1,gcp.us-east1.*:2"}),
    discovered(Cluster),
    Out = fun() ->
                  ?assertEqual([B, C], placement(Cluster)),
                  N = opens(A),
                  ?assertNot(lists:member(A, [K || {_, K} <- open_n(Cluster, 3)])),
                  ?assertEqual(N, opens(A))
          end,
    %% One refresh once a's delay has passed, and its one probe of a.
    Probe = fun() ->
                    N = opens(A),
                    timer:sleep(1100),
                    refreshes(Cluster, 1),
                    wait_until(fun() -> opens(A) =:= N + 1 end, probed),
                    timer:sleep(100),
                    ?assertEqual(N + 1, opens(A))
            end,
    eysql_fake_driver:reject(A, eysql_fake_driver:pg_error(<<"57P03">>)),
    {ok, _, First} = eysql_cluster:open(Cluster),
    ?assertNotEqual(A, First),
    ?assertEqual({[], [A]}, {failed(Cluster), rejected(Cluster)}),
    Out(),
    Probe(),
    ?assertEqual({[], [A]}, {failed(Cluster), rejected(Cluster)}),
    Out(),
    eysql_fake_driver:down(A),
    Probe(),
    ?assertEqual({[A], []}, {failed(Cluster), rejected(Cluster)}),
    Out(),
    Probe(),
    ?assertEqual({[A], []}, {failed(Cluster), rejected(Cluster)}),
    Out(),
    [{warning, ShuttingDown}, {warning, Refused}] = messages("a:5433"),
    ?assertNotEqual(nomatch, string:find(ShuttingDown, "57P03")),
    ?assertEqual(nomatch, string:find(ShuttingDown, "could not connect")),
    ?assertNotEqual(nomatch, string:find(Refused, "could not connect")),
    ?assertNotEqual(nomatch, string:find(Refused, "econnrefused")),
    eysql_fake_driver:up(A),
    Probe(),
    ?assertEqual({[], [], [A]}, {failed(Cluster), rejected(Cluster), placement(Cluster)}),
    ?assertMatch({ok, _, A}, eysql_cluster:open(Cluster)),
    eysql_cluster:stop(Cluster).

%% Refresh `N' times, each once the last one's discovery has run.
refreshes(Cluster, N) ->
    lists:foreach(fun(_) ->
                          Before = eysql_fake_driver:discoveries(),
                          eysql_cluster:refresh(Cluster),
                          wait_until(fun() -> eysql_fake_driver:discoveries() > Before end, refreshed),
                          timer:sleep(50)
                  end,
                  lists:seq(1, N)).

%% As the JDBC driver's checkAndRefresh, a discovery dials the seed first,
%% and then only the servers that are not down, each once. a is down, with a
%% delay longer than the test, so that no probe connects to it either.
discovery_targets() ->
    eysql_fake_driver:set_servers(three()),
    Lb = {<<"lb">>, 5433},
    [A, B, C] = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Cluster = start(#{hosts => [Lb], failed_host_reconnect_delay_secs => 30}),
    discovered(Cluster),
    fail(Cluster, A),
    %% Whatever discovery the failure brought forward has finished.
    timer:sleep(200),
    Discover = fun() ->
                       Before = opens([Lb, A, B, C]),
                       N = eysql_fake_driver:discoveries(),
                       eysql_cluster:refresh(Cluster),
                       wait_until(fun() -> eysql_fake_driver:discoveries() > N end, {discovered, N}),
                       timer:sleep(50),
                       [After - Was || {After, Was} <- lists:zip(opens([Lb, A, B, C]), Before)]
               end,
    %% The seed answers.
    ?assertEqual([1, 0, 0, 0], Discover()),
    %% The seed refuses: b answers, and c is not asked. The order of the
    %% servers is yb_servers()' own.
    eysql_fake_driver:down(Lb),
    ?assertEqual([1, 0, 1, 0], Discover()),
    %% b refuses too: c answers. a, down, is never asked.
    eysql_fake_driver:down(B),
    ?assertEqual([1, 0, 1, 1], Discover()),
    ?assertEqual([A], failed(Cluster)),
    eysql_cluster:stop(Cluster).

%% yb_servers() reports the placement in capitals, and the keys name it in
%% lower case; then the other way round. Either way a is the only server in
%% the preferred zone, as the JDBC driver compares with equalsIgnoreCase.
keys_ignore_case() ->
    Upper = [eysql_fake_driver:server(H, <<"GCP">>, <<"US-East1">>, Z)
             || {H, Z} <- [{<<"a">>, <<"US-EAST1-B">>}, {<<"b">>, <<"us-east1-C">>},
                           {<<"c">>, <<"Us-East1-D">>}]],
    lists:foreach(
      fun({Servers, Keys}) ->
              eysql_fake_driver:set_servers(Servers),
              Cluster = start(#{topology_keys => Keys}),
              discovered(Cluster),
              ?assertEqual([key(<<"a">>)], lists:usort([K || {_, K} <- open_n(Cluster, 4)])),
              ?assertEqual({[key(<<"a">>)], [key(<<"a">>)]}, {placement(Cluster), allowed(Cluster)}),
              eysql_cluster:stop(Cluster)
      end,
      [{Upper, "gcp.us-east1.us-east1-b:1,gcp.us-east1.*:2"},
       {three(), "GCP.US-EAST1.US-East1-B:1,Gcp.Us-east1.*:2"}]).

%% With failed_host_reconnect_delay_secs 0 a failed host is due at once, as
%% the JDBC driver un-marks it at its next refresh. That refresh is the one
%% its failure brings forward, a second or more after the last, and its
%% probe finds a back.
zero_delay() ->
    eysql_fake_driver:set_servers(three()),
    Cluster = start(#{hosts => [{<<"lb">>, 5433}], failed_host_reconnect_delay_secs => 0}),
    discovered(Cluster),
    timer:sleep(1100),
    A = key(<<"a">>),
    Opens = opens(A),
    fail(Cluster, A),
    wait_until(fun() -> failed(Cluster) =:= [] end, probed_at_once, 500),
    ?assertEqual(Opens + 1, opens(A)),
    eysql_cluster:stop(Cluster).

%% With load_balance false, hosts are taken in the order given, as pgjdbc
%% takes the hosts in its URL with loadBalanceHosts off, its default: every
%% connection goes to the first that works, and the next is tried only when
%% it fails. A pool keeps connections on the first host and the one that
%% took over from it, and no other. Once a refresh's probe finds the first
%% back, it takes new connections again, and it alone is kept.
static_in_order() ->
    [A, B, C] = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Cluster = start(#{load_balance => false, hosts => [A, B, C], yb_servers_refresh_interval => 1}),
    ?assertEqual([A], lists:usort([K || {_, K} <- open_n(Cluster, 4)])),
    ?assertEqual({[A], [A]}, {placement(Cluster), allowed(Cluster)}),
    eysql_fake_driver:down(A),
    ?assertEqual([B, B], [K || {_, K} <- open_n(Cluster, 2)]),
    ?assertEqual({[A], [B], [A, B]}, {failed(Cluster), placement(Cluster), allowed(Cluster)}),
    eysql_fake_driver:down(B),
    ?assertMatch({ok, _, C}, eysql_cluster:open(Cluster)),
    ?assertEqual({[C], [A, C]}, {placement(Cluster), allowed(Cluster)}),
    eysql_fake_driver:up(A),
    wait_until(fun() -> failed(Cluster) =:= [B] end, a_back),
    ?assertEqual({[A], [A]}, {placement(Cluster), allowed(Cluster)}),
    ?assertMatch({ok, _, A}, eysql_cluster:open(Cluster)),
    eysql_cluster:stop(Cluster).

%% The seeds are the drivers' plain connection to the hosts in their URL,
%% which pgjdbc takes in order in every mode: before a discovery has
%% succeeded, and when no discovered server is working. lb1 takes every
%% connection until it fails, and then lb2.
seeds_in_order() ->
    Lbs = [Lb1, Lb2] = [{<<"lb1">>, 5433}, {<<"lb2">>, 5433}],
    eysql_fake_driver:set_discover_error(timeout),
    Undiscovered = start(#{hosts => Lbs}),
    ?assertEqual([Lb1, Lb1, Lb1], [K || {_, K} <- open_n(Undiscovered, 3)]),
    eysql_cluster:stop(Undiscovered),
    eysql_fake_driver:set_servers(three()),
    [eysql_fake_driver:down(key(H)) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Cluster = start(#{hosts => Lbs}),
    discovered(Cluster),
    ?assertEqual([Lb1, Lb1, Lb1], [K || {_, K} <- open_n(Cluster, 3)]),
    ?assertEqual([Lb1], placement(Cluster)),
    eysql_fake_driver:down(Lb1),
    ?assertEqual([Lb2, Lb2], [K || {_, K} <- open_n(Cluster, 2)]),
    ?assertEqual({[Lb2], [Lb1, Lb2]}, {placement(Cluster), allowed(Cluster) -- three_keys()}),
    eysql_cluster:stop(Cluster).

three_keys() ->
    [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]].
