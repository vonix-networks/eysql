-module(eysql_pool_tests).

-include_lib("eunit/include/eunit.hrl").

-import(eysql_test_util, [config/1, three/0, key/1, wait_until/2, wait_until/3,
                          refreshing_until/3, fail/2]).

pool_test_() ->
    {foreach,
     fun() -> eysql_fake_driver:start() end,
     fun(_) -> eysql_fake_driver:stop() end,
     [{"fills to its size across the discovered servers", fun fills/0},
      {"opens at once, its openers waiting for the first discovery", fun opens_at_once/0},
      {"without load_balance, the configured hosts only, in order, with no discovery",
       fun static_by_default/0},
      {"without load_balance, connections gather on the first host, and go back to it",
       {timeout, 15, fun static_moves_back/0}},
      {"a preferred server that fails logins causes no churn, and takes the rest back once probed",
       {timeout, 20, fun rejected_no_churn/0}},
      {"checkout and checkin", fun checkout_checkin/0},
      {"a waiting checkout is served by the next checkin", fun waits_for_checkin/0},
      {"a checkout that times out leaks nothing", fun checkout_timeout/0},
      {"a connection held by a process that dies is replaced", fun owner_death/0},
      {"a connection that dies is replaced", fun conn_death/0},
      {"connections are recycled after max_lifetime", fun lifetime/0},
      {"an idle connection that dies is replaced, its host left in", fun idle_death/0},
      {"a connection whose socket closes is replaced; an idle pool sends nothing",
       {timeout, 20, fun socket_close/0}},
      {"a query error marks no host, starts no refresh, and the connection is reused",
       {timeout, 10, fun query_error_marks_nothing/0}},
      {"a connection lost under a query is replaced, its host left in", fun lost_under_query/0},
      {"connections rebalance when a server comes back", fun rebalance/0},
      {"no churn while an allowed server backs off", fun no_churn_while_down/0},
      {"a preferred server that fails keeps its connections, and takes back the rest at the next refresh",
       {timeout, 15, fun preferred_backs_off/0}},
      {"a cluster-wide blip drains nothing", fun blip_keeps_connections/0},
      {"a load-balancer seed keeps its connections while the servers are unreachable",
       {timeout, 30, fun lb_seed_keeps_connections/0}},
      {"with the servers unreachable, a replacement goes straight to a load-balancer seed",
       {timeout, 30, fun lb_seed_replaces_at_once/0}},
      {"a preferred server down for long leaves the fallback alone, then takes it back",
       {timeout, 30, fun preferred_down_long/0}},
      {"stopping the pool stops a probe its cluster has running", fun stop_stops_probe/0},
      {"connections leave a server that discovery drops", fun drains_removed/0},
      {"each rebalance moves at most rebalance_batch, idle or busy", fun batch_caps_drains/0},
      {"a draining connection whose holder dies is no longer draining", fun drain_ends_with_holder/0},
      {"no connection is available: checkouts fail with the reason", fun all_down/0},
      {"a failed open leaves waiters for a connection still opening", fun waits_for_opener/0},
      {"only the holder can check a connection in", fun checkin_by_holder/0},
      {"a cancel that follows the reply takes the lease back", fun cancel_after_reply/0},
      {"stopping the pool stops its cluster", fun stop_stops_cluster/0},
      {"status and crash reports show no secrets", fun redacts_status/0},
      {"driver/1 makes no call to the pool", fun driver_without_call/0}
     ]}.

start(Overrides) ->
    eysql_fake_driver:set_servers(three()),
    {ok, Pool} = eysql_pool:start_link(config(Overrides)),
    Pool.

stop(Pool) ->
    unlink(Pool),
    eysql_pool:stop(Pool).

stat(Pool, Name) -> maps:get(Name, eysql_pool:stats(Pool)).

by_host(Pool) -> stat(Pool, by_host).

snapshot(Pool) -> eysql_cluster:snapshot(eysql_pool:cluster(Pool)).

failed(Pool) -> lists:sort(maps:get(failed, snapshot(Pool))).

rejected(Pool) -> lists:sort(maps:get(rejected, snapshot(Pool))).

alive(Pids) -> [Pid || Pid <- Pids, is_process_alive(Pid)].

%% Rebalance now, and return once the pool has.
rebalance_now(Pool) ->
    Pool ! rebalance,
    _ = eysql_pool:stats(Pool),
    ok.

%% Check out `N' connections, report them, and check in what the test says.
hold(Pool, N, Parent) ->
    Conns = [begin {ok, C} = eysql_pool:checkout(Pool, 1000), C end || _ <- lists:seq(1, N)],
    Parent ! {holding, self(), Conns},
    hold_loop(Pool).

hold_loop(Pool) ->
    receive
        {checkin, Conn} -> eysql_pool:checkin(Pool, Conn), hold_loop(Pool)
    end.

full(Pool, Size) ->
    wait_until(fun() -> stat(Pool, idle) + stat(Pool, leased) =:= Size end, {full, Size}).

fills() ->
    Pool = start(#{pool_size => 6}),
    full(Pool, 6),
    ?assertEqual(#{key(<<"a">>) => 2, key(<<"b">>) => 2, key(<<"c">>) => 2}, by_host(Pool)),
    stop(Pool).

%% Discovery takes half a second, and the seed is a name of its own. The pool
%% starts its openers at once, and they wait in eysql_cluster:open/1 for the
%% first discovery, so its connections go to the servers, none to the seed.
opens_at_once() ->
    eysql_fake_driver:set_discover_delay(500),
    Pool = start(#{hosts => [{<<"lb">>, 5433}], pool_size => 3}),
    ?assertEqual(3, stat(Pool, opening)),
    ?assertEqual(0, stat(Pool, idle)),
    full(Pool, 3),
    ?assertEqual(#{key(<<"a">>) => 1, key(<<"b">>) => 1, key(<<"c">>) => 1}, by_host(Pool)),
    stop(Pool).

%% load_balance is off unless set, as in the smart drivers: the pool takes
%% the hosts it was given in order, as pgjdbc does with loadBalanceHosts off,
%% so every connection goes to the first. It never reads yb_servers(),
%% though the cluster has a fourth server to offer.
static_by_default() ->
    eysql_fake_driver:set_servers(three() ++ [eysql_fake_driver:server(<<"d">>, <<"gcp">>, <<"us-east1">>,
                                                                       <<"us-east1-b">>)]),
    {ok, Config} = eysql_config:normalize(#{hosts => [key(<<"a">>), key(<<"b">>), key(<<"c">>)],
                                            driver => eysql_fake_driver, pool_size => 6,
                                            rebalance_interval => 100}),
    {ok, Pool} = eysql_pool:start_link(Config),
    full(Pool, 6),
    ?assertEqual(#{key(<<"a">>) => 6}, by_host(Pool)),
    %% Rebalancing spreads nothing to b or c.
    timer:sleep(300),
    ?assertEqual(#{key(<<"a">>) => 6}, by_host(Pool)),
    ?assertMatch(#{mode := static, discovered := false}, snapshot(Pool)),
    ?assertEqual(0, eysql_fake_driver:discoveries()),
    stop(Pool).

checkout_checkin() ->
    Pool = start(#{pool_size => 3}),
    full(Pool, 3),
    %% No health checks, so no connection is ever out for one.
    ?assertNot(maps:is_key(checking, eysql_pool:stats(Pool))),
    {ok, Conn} = eysql_pool:checkout(Pool, 1000),
    ?assertEqual(1, stat(Pool, leased)),
    ?assertEqual(2, stat(Pool, idle)),
    eysql_pool:checkin(Pool, Conn),
    wait_until(fun() -> stat(Pool, idle) =:= 3 end, back),
    stop(Pool).

waits_for_checkin() ->
    Pool = start(#{pool_size => 1}),
    full(Pool, 1),
    {ok, Conn} = eysql_pool:checkout(Pool, 1000),
    Self = self(),
    spawn_link(fun() -> Self ! {got, eysql_pool:checkout(Pool, 2000)} end),
    wait_until(fun() -> stat(Pool, waiting) =:= 1 end, waiting),
    eysql_pool:checkin(Pool, Conn),
    receive {got, Result} -> ?assertEqual({ok, Conn}, Result)
    after 2000 -> error(not_served)
    end,
    stop(Pool).

checkout_timeout() ->
    Pool = start(#{pool_size => 1}),
    full(Pool, 1),
    {ok, Conn} = eysql_pool:checkout(Pool, 1000),
    ?assertEqual({error, checkout_timeout}, eysql_pool:checkout(Pool, 100)),
    eysql_pool:checkin(Pool, Conn),
    wait_until(fun() -> stat(Pool, idle) =:= 1 andalso stat(Pool, waiting) =:= 0 end, no_leak),
    ?assertEqual(0, stat(Pool, leased)),
    stop(Pool).

owner_death() ->
    Pool = start(#{pool_size => 2}),
    full(Pool, 2),
    Self = self(),
    Owner = spawn(fun() -> {ok, C} = eysql_pool:checkout(Pool, 1000), Self ! {conn, C} end),
    Conn = receive {conn, C} -> C after 2000 -> error(no_checkout) end,
    _ = Owner,
    wait_until(fun() -> not is_process_alive(Conn) end, closed),
    full(Pool, 2),
    ?assertEqual(0, stat(Pool, leased)),
    stop(Pool).

conn_death() ->
    Pool = start(#{pool_size => 3}),
    full(Pool, 3),
    [Victim | _] = eysql_fake_driver:conns(),
    eysql_fake_driver:kill_conn(Victim),
    wait_until(fun() -> not lists:member(Victim, eysql_fake_driver:conns()) end, dead),
    full(Pool, 3),
    stop(Pool).

lifetime() ->
    Pool = start(#{pool_size => 3, max_lifetime => 200}),
    full(Pool, 3),
    Before = eysql_fake_driver:conns(),
    wait_until(fun() -> lists:all(fun(P) -> not is_process_alive(P) end, Before) end, recycled, 3000),
    full(Pool, 3),
    stop(Pool).

%% a's connection dies while idle, as when its server closes it. The pool
%% replaces it, and a is not marked: only a failure to connect does that, so
%% the replacement goes to a, the least loaded, again.
idle_death() ->
    Pool = start(#{pool_size => 3}),
    full(Pool, 3),
    A = key(<<"a">>),
    [Dead] = eysql_fake_driver:conns_to(A),
    Opens = eysql_fake_driver:opens(A),
    eysql_fake_driver:kill_conn(Dead),
    wait_until(fun() -> stat(Pool, idle) =:= 3 andalso length(eysql_fake_driver:conns_to(A)) =:= 1 end,
               replaced),
    ?assertEqual({[], []}, {failed(Pool), rejected(Pool)}),
    ?assertEqual(#{A => 1, key(<<"b">>) => 1, key(<<"c">>) => 1}, by_host(Pool)),
    ?assertEqual(Opens + 1, eysql_fake_driver:opens(A)),
    stop(Pool).

%% The default driver, against a server of the test's own on loopback that
%% completes the handshake. The pool holds its connection idle and sends
%% nothing on it: no health check, no ping. The server then closes the
%% socket, as one that goes away does; epgsql's connection process exits
%% with it, and the pool opens another, whose connect succeeds, so the host
%% is not marked.
socket_close() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {packet, raw}, {ip, loopback}]),
    {ok, Port} = inet:port(Listen),
    Host = {<<"127.0.0.1">>, Port},
    {ok, Config} = eysql_config:normalize(#{hosts => [Host], pool_size => 1, rebalance_interval => 100,
                                            epgsql_opts => #{codecs => []}}),
    {ok, Pool} = eysql_pool:start_link(Config),
    First = accept_session(Listen),
    wait_until(fun() -> stat(Pool, idle) =:= 1 end, first),
    %% Longer than the 5 s between the health checks the pool used to run,
    %% fifty rebalance ticks, and nothing on the wire.
    ?assertEqual({error, timeout}, gen_tcp:recv(First, 0, 5500)),
    ok = gen_tcp:close(First),
    Second = accept_session(Listen),
    wait_until(fun() -> stat(Pool, idle) =:= 1 end, second),
    ?assertEqual({#{Host => 1}, []}, {by_host(Pool), failed(Pool)}),
    stop(Pool),
    gen_tcp:close(Second),
    gen_tcp:close(Listen).

%% Accept a connection and complete its handshake: no password, and a
%% session ready for queries.
accept_session(Listen) ->
    {ok, Sock} = gen_tcp:accept(Listen, 5000),
    {ok, <<Length:32>>} = gen_tcp:recv(Sock, 4, 5000),
    {ok, _Startup} = gen_tcp:recv(Sock, Length - 4, 5000),
    ok = gen_tcp:send(Sock, [pg_msg($R, <<0:32>>), pg_msg($S, ["integer_datetimes", 0, "on", 0]),
                             pg_msg($K, <<1:32, 2:32>>), pg_msg($Z, "I")]),
    Sock.

pg_msg(Type, Payload) ->
    [Type, <<(iolist_size(Payload) + 4):32>>, Payload].

%% A statement the server cancels, 57014 as for statement_timeout, fails
%% only itself. Through eysql:equery/3, with_connection/2 and transaction/2
%% alike, the connection goes back to the pool and is reused, no host is
%% marked, and neither a refresh nor a probe starts, though a second has
%% passed since the last refresh and one could: only a failure to connect
%% marks a host, as in the drivers.
query_error_marks_nothing() ->
    Pool = start(#{pool_size => 1, hosts => [{<<"lb">>, 5433}]}),
    full(Pool, 1),
    wait_until(fun() -> length(eysql_fake_driver:conns()) =:= 1 end, settled),
    [Conn] = eysql_fake_driver:conns(),
    Canceled = eysql_fake_driver:pg_error(<<"57014">>),
    eysql_fake_driver:set_epgsql_reply({error, Canceled}),
    eysql_fake_driver:script("SELECT slow", [{error, Canceled}]),
    Slow = fun(C) -> eysql_fake_driver:squery(C, "SELECT slow") end,
    timer:sleep(1100),
    Hosts = [{<<"lb">>, 5433} | [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]]],
    Before = {eysql_fake_driver:discoveries(), [eysql_fake_driver:opens(K) || K <- Hosts]},
    ?assertEqual({error, Canceled}, eysql:equery(Pool, "SELECT pg_sleep(10)", [])),
    ?assertEqual({error, Canceled}, eysql:with_connection(Pool, Slow)),
    ?assertEqual({error, Canceled}, eysql:transaction(Pool, Slow)),
    ?assertEqual({error, Canceled}, eysql:equery(Pool, "SELECT pg_sleep(10)", [])),
    timer:sleep(200),
    ?assertEqual([Conn], eysql_fake_driver:conns()),
    ?assertEqual({1, 0}, {stat(Pool, idle), stat(Pool, leased)}),
    ?assertEqual(Before, {eysql_fake_driver:discoveries(), [eysql_fake_driver:opens(K) || K <- Hosts]}),
    ?assertMatch(#{failed := [], rejected := [], read_only := []}, snapshot(Pool)),
    stop(Pool).

%% The connection dies under a query, as when its server goes away. The
%% query fails with connection_lost, with_connection reads the connection's
%% status as unknown and discards it, and the pool opens another. The host
%% is not marked: connects to it still succeed, and with load_balance false
%% the replacement goes to it again. The connections are epgsql's own
%% processes, read through eysql_conn.
lost_under_query() ->
    {ok, Config} = eysql_config:normalize(#{driver => eysql_sock_driver, pool_size => 1,
                                            hosts => [key(<<"a">>), key(<<"b">>)]}),
    {ok, Pool} = eysql_pool:start_link(Config),
    full(Pool, 1),
    Self = self(),
    Result = eysql:with_connection(Pool, fun(C) ->
                                                 Self ! {conn, C},
                                                 exit(C, kill),
                                                 eysql_conn:equery(C, "SELECT 1", [])
                                         end),
    ?assertMatch({error, {connection_lost, _}}, Result),
    Dead = receive {conn, C} -> C after 0 -> error(no_conn) end,
    wait_until(fun() -> stat(Pool, idle) =:= 1 andalso stat(Pool, leased) =:= 0 end, replaced),
    {ok, Next} = eysql_pool:checkout(Pool, 1000),
    ?assertNotEqual(Dead, Next),
    eysql_pool:checkin(Pool, Next),
    ?assertEqual({#{key(<<"a">>) => 1}, []}, {by_host(Pool), failed(Pool)}),
    stop(Pool).

%% a refuses at first, and the pool fills on b and c. Once a answers, a
%% refresh after its delay finds it back, and connections move to it.
rebalance() ->
    eysql_fake_driver:down(key(<<"a">>)),
    Pool = start(#{pool_size => 6, yb_servers_refresh_interval => 1}),
    full(Pool, 6),
    ?assertEqual(0, maps:get(key(<<"a">>), by_host(Pool), 0)),
    eysql_fake_driver:up(key(<<"a">>)),
    wait_until(fun() -> by_host(Pool) =:= #{key(<<"a">>) => 2, key(<<"b">>) => 2, key(<<"c">>) => 2} end,
               balanced, 8000),
    stop(Pool).

%% a is allowed, but down, with a delay longer than the test runs. b and c
%% hold three each; counting a's 0 as the quietest would close two
%% connections every 100 ms, only for them to reopen on b and c.
no_churn_while_down() ->
    eysql_fake_driver:down(key(<<"a">>)),
    Pool = start(#{pool_size => 6, failed_host_reconnect_delay_secs => 30}),
    full(Pool, 6),
    ?assertEqual(#{key(<<"b">>) => 3, key(<<"c">>) => 3}, by_host(Pool)),
    #{allowed := Allowed, failed := Failed} = eysql_cluster:snapshot(eysql_pool:cluster(Pool)),
    ?assert(lists:member(key(<<"a">>), Allowed)),
    ?assert(lists:member(key(<<"a">>), Failed)),
    %% A discovery started by a's failure may still hold a connection.
    wait_until(fun() -> length(eysql_fake_driver:conns()) =:= 6 end, settled),
    Before = lists:sort(eysql_fake_driver:conns()),
    %% Six rebalance intervals.
    timer:sleep(600),
    ?assertEqual(Before, lists:sort(eysql_fake_driver:conns())),
    ?assertEqual(#{key(<<"b">>) => 3, key(<<"c">>) => 3}, by_host(Pool)),
    stop(Pool).

%% a is alone in the preferred zone and holds every connection. Right after
%% a refresh, one of them dies while a refuses connections, and a is back at
%% once. The connection's replacement fails to connect to a, which leaves a
%% out, and opens on b or c; a keeps the other three: a failure alone moves
%% nothing. Nor does the end of a's delay (1 s): the replacement stays until
%% the next refresh, 2 s after the last, probes a and finds it back. Then
%% the replacement is a stray and moves to a.
preferred_backs_off() ->
    A = key(<<"a">>),
    Pool = start(#{pool_size => 4, yb_servers_refresh_interval => 2,
                   topology_keys => "gcp.us-east1.us-east1-b:1,gcp.us-east1.us-east1-c:2,"
                                    "gcp.us-east1.us-east1-d:2"}),
    full(Pool, 4),
    ?assertEqual(#{A => 4}, by_host(Pool)),
    wait_until(fun() -> length(eysql_fake_driver:conns()) =:= 4 end, settled),
    Before = eysql_fake_driver:conns_to(A),
    eysql_cluster:refresh(eysql_pool:cluster(Pool)),
    %% The refresh's discovery asks a, the first seed, before a refuses.
    timer:sleep(50),
    Start = erlang:monotonic_time(millisecond),
    eysql_fake_driver:down(A),
    eysql_fake_driver:kill_conn(hd(Before)),
    wait_until(fun() -> failed(Pool) =:= [A] end, a_failed),
    eysql_fake_driver:up(A),
    wait_until(fun() -> length(alive(Before)) =:= 3 andalso stat(Pool, idle) =:= 4 end, replaced),
    Kept = alive(Before),
    %% Past a's delay, and fifteen rebalance intervals.
    timer:sleep(max(0, Start + 1500 - erlang:monotonic_time(millisecond))),
    ?assertEqual(Kept, alive(Kept)),
    ?assertEqual(0, stat(Pool, draining)),
    ?assertEqual(3, maps:get(A, by_host(Pool))),
    ?assertEqual([A], failed(Pool)),
    wait_until(fun() -> by_host(Pool) =:= #{A => 4} end, moved_back),
    ?assert(erlang:monotonic_time(millisecond) - Start >= 1900),
    ?assertEqual(Kept, alive(Kept)),
    stop(Pool).

%% Every server refuses connections for a moment while the seed still
%% answers, and one connection on each dies. Their replacements fail to
%% connect to the servers, which leaves all three out, and open on the
%% seed; the servers keep their other connections. Once refreshes find them
%% back, the seed's connections move to them.
blip_keeps_connections() ->
    Servers = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Pool = start(#{hosts => [{<<"seed">>, 5433}], pool_size => 6, yb_servers_refresh_interval => 1}),
    full(Pool, 6),
    ?assertEqual(maps:from_list([{Key, 2} || Key <- Servers]), by_host(Pool)),
    wait_until(fun() -> length(eysql_fake_driver:conns()) =:= 6 end, settled),
    Before = eysql_fake_driver:conns(),
    [eysql_fake_driver:down(Key) || Key <- Servers],
    [eysql_fake_driver:kill_conn(hd(eysql_fake_driver:conns_to(Key))) || Key <- Servers],
    wait_until(fun() -> failed(Pool) =:= Servers end, all_failed),
    ?assertEqual([{<<"seed">>, 5433}], maps:get(placement, snapshot(Pool))),
    [eysql_fake_driver:up(Key) || Key <- Servers],
    wait_until(fun() -> length(alive(Before)) =:= 3 andalso stat(Pool, idle) =:= 6 end, three_replaced),
    Kept = alive(Before),
    timer:sleep(500),
    ?assertEqual(Kept, alive(Kept)),
    ?assertEqual(0, stat(Pool, draining)),
    wait_until(fun() -> by_host(Pool) =:= maps:from_list([{Key, 2} || Key <- Servers]) end, moved_back),
    ?assertEqual(Kept, alive(Kept)),
    stop(Pool).

%% The seed is a load balancer the client reaches, and the addresses the
%% servers advertise drop packets from here. The pool settles on the seed.
%% Over three delays and thirty rebalances it closes none of the seed's
%% connections: their replacements would have nowhere better to go. And
%% nothing tries the servers again before the next refresh: no probe, and no
%% pick.
lb_seed_keeps_connections() ->
    Servers = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    [eysql_fake_driver:unreachable(Key) || Key <- Servers],
    Lb = {<<"lb">>, 5433},
    Pool = start(#{hosts => [Lb], pool_size => 4, connect_timeout => 300}),
    wait_until(fun() ->
                       by_host(Pool) =:= #{Lb => 4} andalso stat(Pool, idle) =:= 4
                           andalso length(eysql_fake_driver:conns_to(Lb)) =:= 4
               end, on_lb, 20000),
    Before = lists:sort(eysql_fake_driver:conns_to(Lb)),
    Opens = [eysql_fake_driver:opens(Key) || Key <- Servers],
    timer:sleep(3000),
    ?assertEqual(Before, lists:sort(eysql_fake_driver:conns_to(Lb))),
    ?assertEqual(Servers, failed(Pool)),
    ?assertEqual(Opens, [eysql_fake_driver:opens(Key) || Key <- Servers]),
    stop(Pool).

%% As above, with a single connection, so that a checkout waits for each
%% replacement. Past the servers' delay, every connection discarded is
%% replaced on the load balancer at once, and the checkout waiting for it
%% gets it: no replacement spends its connect attempts, each waiting out
%% connect_timeout, on the servers first, and no checkout fails.
lb_seed_replaces_at_once() ->
    Servers = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    [eysql_fake_driver:unreachable(Key) || Key <- Servers],
    Lb = {<<"lb">>, 5433},
    Pool = start(#{hosts => [Lb], pool_size => 1, connect_timeout => 300}),
    wait_until(fun() -> by_host(Pool) =:= #{Lb => 1} andalso stat(Pool, idle) =:= 1 end, on_lb, 20000),
    ?assertEqual(Servers, failed(Pool)),
    Opens = [eysql_fake_driver:opens(Key) || Key <- Servers],
    timer:sleep(1200),
    [begin
         {Micros, {ok, Conn}} = timer:tc(fun() -> eysql_pool:checkout(Pool, 2000) end),
         ?assert(Micros < 250000),
         ?assert(lists:member(Conn, eysql_fake_driver:conns_to(Lb))),
         eysql_pool:discard(Pool, Conn)
     end || _ <- lists:seq(1, 5)],
    ?assertMatch({ok, _}, eysql_pool:checkout(Pool, 2000)),
    ?assertEqual(Opens, [eysql_fake_driver:opens(Key) || Key <- Servers]),
    stop(Pool).

%% a is alone in the preferred zone and holds every connection, then goes
%% for good: its address drops packets, and its connections die and reopen
%% on b and c, their connects to a timing out. For four delays and sixteen
%% rebalances, with refreshes every second probing a and waiting out
%% connect_timeout, none of those moves. Once a answers again, a probe finds
%% it within two refresh intervals (a is due at every other refresh) and a
%% connect_timeout, still moving nothing; the next rebalance moves
%% rebalance_batch connections to a, and the one after that the rest.
preferred_down_long() ->
    A = key(<<"a">>),
    Pool = start(#{pool_size => 4, rebalance_batch => 2, rebalance_interval => 60000,
                   connect_timeout => 300, yb_servers_refresh_interval => 1,
                   topology_keys => "gcp.us-east1.us-east1-b:1,gcp.us-east1.us-east1-c:2,"
                                    "gcp.us-east1.us-east1-d:2"}),
    full(Pool, 4),
    ?assertEqual(#{A => 4}, by_host(Pool)),
    eysql_fake_driver:unreachable(A),
    [eysql_fake_driver:kill_conn(Pid) || Pid <- eysql_fake_driver:conns_to(A)],
    wait_until(fun() -> maps:get(A, by_host(Pool), 0) =:= 0 andalso stat(Pool, idle) =:= 4 end, off_a),
    OnFallback = fun() ->
                         lists:sort(eysql_fake_driver:conns_to(key(<<"b">>))
                                    ++ eysql_fake_driver:conns_to(key(<<"c">>)))
                 end,
    wait_until(fun() -> length(OnFallback()) =:= 4 end, settled),
    Fallback = OnFallback(),
    [begin timer:sleep(250), rebalance_now(Pool) end || _ <- lists:seq(1, 16)],
    ?assertEqual(Fallback, OnFallback()),
    ?assertEqual([A], failed(Pool)),
    eysql_fake_driver:up(A),
    Up = erlang:monotonic_time(millisecond),
    wait_until(fun() -> failed(Pool) =:= [] end, a_found),
    ?assert(erlang:monotonic_time(millisecond) - Up < 2 * 1000 + 300 + 500),
    ?assertEqual(Fallback, OnFallback()),
    rebalance_now(Pool),
    wait_until(fun() -> stat(Pool, idle) =:= 4 andalso maps:get(A, by_host(Pool), 0) =:= 2 end,
               two_moved),
    ?assertEqual(2, length(OnFallback())),
    rebalance_now(Pool),
    wait_until(fun() -> by_host(Pool) =:= #{A => 4} end, all_moved),
    stop(Pool).

%% With load_balance false every connection goes to the first host that
%% works. a refuses, and its connections die: they reopen on b, and while a
%% is down rebalancing spreads nothing from b to c, since new connections
%% would go to b alone. Once a refresh's probe finds a back, b's connections
%% are strays, and move to a rebalance_batch at a time.
static_moves_back() ->
    [A, B, C] = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Pool = start(#{load_balance => false, hosts => [A, B, C], pool_size => 4, rebalance_batch => 2,
                   rebalance_interval => 60000, yb_servers_refresh_interval => 1}),
    full(Pool, 4),
    ?assertEqual(#{A => 4}, by_host(Pool)),
    eysql_fake_driver:down(A),
    [eysql_fake_driver:kill_conn(Pid) || Pid <- eysql_fake_driver:conns_to(A)],
    wait_until(fun() -> by_host(Pool) =:= #{B => 4} andalso stat(Pool, idle) =:= 4 end, on_b),
    [rebalance_now(Pool) || _ <- [1, 2, 3]],
    ?assertEqual(#{B => 4}, by_host(Pool)),
    ?assertEqual(0, eysql_fake_driver:opens(C)),
    eysql_fake_driver:up(A),
    wait_until(fun() -> failed(Pool) =:= [] end, a_found),
    ?assertEqual(#{B => 4}, by_host(Pool)),
    rebalance_now(Pool),
    wait_until(fun() -> stat(Pool, idle) =:= 4 andalso by_host(Pool) =:= #{A => 2, B => 2} end, two_moved),
    rebalance_now(Pool),
    wait_until(fun() -> by_host(Pool) =:= #{A => 4} end, all_moved),
    stop(Pool).

%% a, alone in the preferred zone, answers but fails every login, as with
%% too many connections. It is not down, but new connections leave it out
%% all the same once one has failed there, and open on b and c. The pool
%% does not count a as up: it keeps the connections on b and c, and moves
%% none towards a, where each would fail and reopen where it was.
%% Refreshes, every second, probe a once its delay has passed, and find it
%% failing still. Once a takes connections again, the next refresh's probe
%% finds so, and the connections move to a. The seed is a name of its own,
%% so that no discovery connects to a.
rejected_no_churn() ->
    [A, B, C] = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    eysql_fake_driver:reject(A, eysql_fake_driver:pg_error(<<"53300">>)),
    Pool = start(#{pool_size => 4, yb_servers_refresh_interval => 1, hosts => [{<<"lb">>, 5433}],
                   topology_keys => "gcp.us-east1.us-east1-b:1,gcp.us-east1.*:2"}),
    full(Pool, 4),
    ?assertEqual(0, maps:get(A, by_host(Pool), 0)),
    #{rejected := Rejected, failed := Failed, placement := Placement} = snapshot(Pool),
    ?assertEqual({[A], [], [B, C]}, {Rejected, Failed, lists:sort(Placement)}),
    Pooled = fun() -> lists:sort(eysql_fake_driver:conns_to(B) ++ eysql_fake_driver:conns_to(C)) end,
    wait_until(fun() -> length(Pooled()) =:= 4 end, settled),
    Before = Pooled(),
    Opens = eysql_fake_driver:opens(A),
    %% Twenty-five rebalance intervals, with refreshes every second, past
    %% a's delay.
    timer:sleep(2500),
    ?assertEqual(Before, Pooled()),
    ?assert(eysql_fake_driver:opens(A) > Opens),
    ?assertEqual([A], rejected(Pool)),
    eysql_fake_driver:up(A),
    wait_until(fun() -> by_host(Pool) =:= #{A => 4} end, moved_to_a, 8000),
    ?assertEqual([], rejected(Pool)),
    stop(Pool).

%% The pool stops its cluster, and the cluster the probe it has running.
stop_stops_probe() ->
    Pool = start(#{pool_size => 1, connect_timeout => 60000}),
    full(Pool, 1),
    Cluster = eysql_pool:cluster(Pool),
    C = key(<<"c">>),
    eysql_fake_driver:open_hangs(C),
    fail(Cluster, C),
    refreshing_until(Cluster, fun() -> length(eysql_fake_driver:hung(C)) =:= 1 end, probing),
    [Probe] = eysql_fake_driver:hung(C),
    stop(Pool),
    ?assertNot(is_process_alive(Cluster)),
    wait_until(fun() -> not is_process_alive(Probe) end, probe_stopped).

drains_removed() ->
    Pool = start(#{pool_size => 6}),
    full(Pool, 6),
    eysql_fake_driver:set_servers(lists:sublist(three(), 2)),
    eysql_cluster:refresh(eysql_pool:cluster(Pool)),
    wait_until(fun() -> maps:get(key(<<"c">>), by_host(Pool), 0) =:= 0 end, drained, 5000),
    full(Pool, 6),
    stop(Pool).

%% The pool unlinks a connection before closing it, so no EXIT comes to forget
%% it: closing it must take it out of `draining'.
drain_ends_with_holder() ->
    Pool = start(#{pool_size => 3}),
    full(Pool, 3),
    ?assertEqual(#{key(<<"a">>) => 1, key(<<"b">>) => 1, key(<<"c">>) => 1}, by_host(Pool)),
    Self = self(),
    Holder = spawn(fun() ->
                           [{ok, _} = eysql_pool:checkout(Pool, 1000) || _ <- [1, 2, 3]],
                           Self ! holding,
                           receive stop -> ok end
                   end),
    receive holding -> ok after 3000 -> error(no_checkout) end,
    eysql_fake_driver:set_servers(lists:sublist(three(), 2)),
    eysql_cluster:refresh(eysql_pool:cluster(Pool)),
    wait_until(fun() -> stat(Pool, draining) =:= 1 end, draining),
    exit(Holder, kill),
    wait_until(fun() -> stat(Pool, leased) =:= 0 end, closed),
    ?assertEqual(0, stat(Pool, draining)),
    full(Pool, 3),
    ?assertEqual(0, maps:get(key(<<"c">>), by_host(Pool), 0)),
    stop(Pool).

%% b and c leave discovery while a holder has all six connections, and it
%% returns one on b: four strays, one idle and three leased. With
%% rebalance_batch 2, the first rebalance closes the idle one and marks one
%% leased one to close when it comes back, the second marks the other two,
%% and the third has nothing left to move.
batch_caps_drains() ->
    Pool = start(#{pool_size => 6, rebalance_batch => 2,
                   rebalance_interval => 60000}),
    full(Pool, 6),
    Self = self(),
    Holder = spawn(fun() -> hold(Pool, 6, Self) end),
    Conns = receive {holding, Holder, Cs} -> Cs after 3000 -> error(no_checkout) end,
    Cluster = eysql_pool:cluster(Pool),
    eysql_fake_driver:set_servers(lists:sublist(three(), 1)),
    wait_until(fun() ->
                       eysql_cluster:refresh(Cluster),
                       maps:get(allowed, eysql_cluster:snapshot(Cluster)) =:= [key(<<"a">>)]
               end, dropped),
    [OnB | _] = [C || C <- Conns, lists:member(C, eysql_fake_driver:conns_to(key(<<"b">>)))],
    Holder ! {checkin, OnB},
    wait_until(fun() -> stat(Pool, idle) =:= 1 end, returned),
    %% Strays closed, and strays marked to close.
    Moved = fun() ->
                    #{by_host := ByHost, draining := Draining} = eysql_pool:stats(Pool),
                    4 - maps:get(key(<<"b">>), ByHost, 0) - maps:get(key(<<"c">>), ByHost, 0) + Draining
            end,
    ?assertEqual(0, Moved()),
    rebalance_now(Pool),
    ?assertEqual(2, Moved()),
    ?assertEqual(1, stat(Pool, draining)),
    rebalance_now(Pool),
    ?assertEqual(4, Moved()),
    ?assertEqual(3, stat(Pool, draining)),
    rebalance_now(Pool),
    ?assertEqual(4, Moved()),
    exit(Holder, kill),
    wait_until(fun() -> stat(Pool, leased) =:= 0 end, released),
    ?assertEqual(0, stat(Pool, draining)),
    stop(Pool).

all_down() ->
    [eysql_fake_driver:down(key(H)) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Pool = start(#{pool_size => 2}),
    ?assertEqual({error, econnrefused}, eysql_pool:checkout(Pool, 3000)),
    stop(Pool).

%% Both connections are leased. One is discarded and its replacement's
%% connect hangs; then every host refuses, and a waiter comes. The other is
%% discarded, and its replacement fails: the waiter keeps waiting for the
%% one still opening.
waits_for_opener() ->
    Hosts = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Pool = start(#{pool_size => 2}),
    full(Pool, 2),
    {ok, First} = eysql_pool:checkout(Pool, 1000),
    {ok, Second} = eysql_pool:checkout(Pool, 1000),
    [eysql_fake_driver:open_hangs(Key) || Key <- Hosts],
    eysql_pool:discard(Pool, First),
    wait_until(fun() -> length(lists:append([eysql_fake_driver:hung(Key) || Key <- Hosts])) =:= 1 end,
               hanging),
    %% The hanging connect goes on as it began.
    [eysql_fake_driver:up(Key) || Key <- Hosts],
    [eysql_fake_driver:down(Key) || Key <- Hosts],
    Waiter = spawn(fun() -> eysql_pool:checkout(Pool, 3000) end),
    wait_until(fun() -> stat(Pool, waiting) =:= 1 end, waiting),
    eysql_pool:discard(Pool, Second),
    wait_until(fun() -> stat(Pool, opening) =:= 1 andalso length(failed(Pool)) =:= 3 end, open_failed),
    ?assertEqual(1, stat(Pool, waiting)),
    exit(Waiter, kill),
    stop(Pool).

%% A checkin from a process other than the holder, including a late one from
%% an earlier holder, leaves the lease alone.
checkin_by_holder() ->
    Pool = start(#{pool_size => 1}),
    full(Pool, 1),
    {ok, Conn} = eysql_pool:checkout(Pool, 1000),
    Self = self(),
    %% Stats from the same process return after the pool has had its checkin.
    spawn(fun() -> eysql_pool:checkin(Pool, Conn), Self ! {other, eysql_pool:stats(Pool)} end),
    receive {other, #{leased := Leased}} -> ?assertEqual(1, Leased)
    after 2000 -> error(no_stats)
    end,
    eysql_pool:checkin(Pool, Conn),
    ?assertEqual(0, stat(Pool, leased)),
    Holder = spawn(fun() ->
                           {ok, C} = eysql_pool:checkout(Pool, 1000),
                           Self ! {holding, C},
                           receive release -> eysql_pool:checkin(Pool, C) end
                   end),
    receive {holding, Conn} -> ok after 2000 -> error(no_checkout) end,
    eysql_pool:checkin(Pool, Conn),
    ?assertEqual(1, stat(Pool, leased)),
    Holder ! release,
    wait_until(fun() -> stat(Pool, leased) =:= 0 end, released),
    stop(Pool).

%% The caller gave up as the reply went out: the cancel it sends then finds
%% the lease by its checkout reference.
cancel_after_reply() ->
    Pool = start(#{pool_size => 1}),
    full(Pool, 1),
    CRef = make_ref(),
    {ok, _Conn, infinity} = gen_server:call(Pool, {checkout, CRef, 1000}),
    ?assertEqual(1, stat(Pool, leased)),
    gen_server:cast(Pool, {cancel, CRef}),
    ?assertEqual(0, stat(Pool, leased)),
    ?assertEqual(1, stat(Pool, idle)),
    stop(Pool).

stop_stops_cluster() ->
    Pool = start(#{pool_size => 1}),
    Cluster = eysql_pool:cluster(Pool),
    stop(Pool),
    ?assertNot(is_process_alive(Cluster)).

%% Neither the pool nor its cluster shows the password or the TLS options:
%% not in sys:get_status/1, and not in a crash reason that carries the
%% config, for example in a stack trace.
redacts_status() ->
    Config = config(#{pool_size => 1, password => <<"s3cret">>, ssl_opts => [{password, "k3y"}]}),
    eysql_fake_driver:set_servers(three()),
    {ok, Pool} = eysql_pool:start_link(Config),
    full(Pool, 1),
    Cluster = eysql_pool:cluster(Pool),
    Reason = {function_clause, [{eysql_pool, fill, [{state, Config}], [{line, 1}]}]},
    Printed = [sys:get_status(Pool),
               sys:get_status(Cluster),
               eysql_pool:format_status(#{reason => Reason}),
               eysql_cluster:format_status(#{reason => Reason})],
    [begin
         Text = lists:flatten(io_lib:format("~p", [Status])),
         ?assertEqual(nomatch, string:find(Text, "s3cret")),
         ?assertEqual(nomatch, string:find(Text, "k3y")),
         ?assertNotEqual(nomatch, string:find(Text, "redacted"))
     end || Status <- Printed],
    %% The pool still connects with what it holds.
    {ok, Conn} = eysql_pool:checkout(Pool, 1000),
    eysql_pool:checkin(Pool, Conn),
    stop(Pool).

%% The pool keeps its driver in persistent_term: driver/1 answers while the
%% pool is suspended and could answer no call, by pid and by name. Stopping
%% the pool erases the term.
driver_without_call() ->
    eysql_fake_driver:set_servers(three()),
    Name = eysql_pool_tests_pool,
    {ok, Pool} = eysql_pool:start_link({local, Name}, config(#{pool_size => 1})),
    ?assertMatch([_], terms_of(Pool)),
    ok = sys:suspend(Pool),
    try
        ?assertEqual(eysql_fake_driver, eysql_pool:driver(Pool)),
        ?assertEqual(eysql_fake_driver, eysql_pool:driver(Name)),
        ?assertEqual(eysql_fake_driver, eysql_pool:driver({Name, node()}))
    after
        sys:resume(Pool)
    end,
    stop(Pool),
    ?assertEqual([], terms_of(Pool)),
    ?assertExit({noproc, _}, eysql_pool:driver(Name)).

terms_of(Pool) ->
    [Key || {Key, _} <- persistent_term:get(), is_tuple(Key), lists:member(Pool, tuple_to_list(Key))].
