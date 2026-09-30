-module(eysql_conn_tests).

-include_lib("eunit/include/eunit.hrl").

dead() ->
    Pid = spawn(fun() -> ok end),
    Ref = erlang:monitor(process, Pid),
    receive {'DOWN', Ref, process, Pid, _} -> Pid end.

%% epgsql exits the caller when the connection process is gone; eysql_conn
%% returns an error instead.
dead_connection_test_() ->
    [?_assertMatch({error, {connection_lost, _}}, eysql_conn:equery(dead(), "SELECT 1", [])),
     ?_assertMatch({error, {connection_lost, _}}, eysql_conn:squery(dead(), "SELECT 1")),
     ?_assert(eysql_error:is_connection_lost(eysql_conn:squery(dead(), "SELECT 1"))),
     ?_assertEqual(false, eysql_conn:committed(dead())),
     ?_assertEqual(ok, eysql_conn:close(dead())),
     ?_assertEqual(unknown, eysql_conn:transaction_status(dead()))
    ].

refused_test() ->
    %% Nothing listens on port 1; the caller must survive the failed connect.
    Result = eysql_conn:open(<<"127.0.0.1">>, 1, #{username => <<"u">>, password => <<>>,
                                                 connect_timeout => 1000}),
    ?assertMatch({error, _}, Result),
    ?assert(is_process_alive(self())).

%%%=============================================================================
%%% transaction_status/1
%%%=============================================================================

%% Each state is set with epgsql's own setters on a real epgsql connection
%% process, so these fail if an epgsql release moves the fields
%% transaction_status/1 reads.
transaction_status_test_() ->
    [?_assertEqual(idle, status_after(fun(_) -> ok end)),
     ?_assertEqual(in_transaction, status_after(fun(C) -> eysql_sock_driver:set_txstatus(C, $T) end)),
     ?_assertEqual(failed, status_after(fun(C) -> eysql_sock_driver:set_txstatus(C, $E) end)),
     {"waiting for a sync after an error", ?_assertEqual(unknown, status_after(fun eysql_sock_driver:set_sync_required/1))},
     {"in COPY mode", ?_assertEqual(unknown, status_after(fun eysql_sock_driver:set_copy_mode/1))},
     {"before any ReadyForQuery", ?_assertEqual(unknown, never_ready())},
     {"not an epgsql connection", ?_assertEqual(unknown, not_epgsql())},
     {"does not answer", ?_assertEqual(unknown, silent())}
    ].

%% A connection as eysql_sock_driver opens it, idle, then changed.
status_after(Change) ->
    {ok, Conn} = eysql_sock_driver:open(<<"h">>, 5433, #{}),
    ok = Change(Conn),
    Status = eysql_conn:transaction_status(Conn),
    unlink(Conn),
    eysql_sock_driver:close(Conn),
    Status.

never_ready() ->
    {ok, Conn} = epgsql_sock:start_link(),
    Status = eysql_conn:transaction_status(Conn),
    unlink(Conn),
    gen_server:stop(Conn),
    Status.

not_epgsql() ->
    {ok, Pid} = gen_event:start(),
    Status = eysql_conn:transaction_status(Pid),
    gen_event:stop(Pid),
    Status.

%% Ignores the request, so the read times out.
silent() ->
    Pid = spawn(fun() -> receive stop -> ok end end),
    Status = eysql_conn:transaction_status(Pid),
    Pid ! stop,
    Status.

%% The status is read on every checkin. A connected epgsql process's state is
%% about 11 KB, mostly type codecs, and none of it may be copied to the
%% caller: only the answer comes back. The codec here stands in for the real
%% one, several times its size so that a copy cannot go unnoticed.
status_copies_no_state_test() ->
    {ok, Conn} = eysql_sock_driver:open(<<"h">>, 5433, #{}),
    ok = eysql_sock_driver:set_codec(Conn, lists:seq(1, 5000)),
    ok = eysql_sock_driver:set_txstatus(Conn, $T),
    Self = self(),
    {Caller, Monitor} = spawn_monitor(fun() ->
                                              receive go -> ok end,
                                              Self ! {status, eysql_conn:transaction_status(Conn)}
                                      end),
    1 = erlang:trace(Caller, true, ['receive']),
    Caller ! go,
    ?assertEqual(in_transaction, receive {status, S} -> S after 5000 -> error(no_status) end),
    receive {'DOWN', Monitor, process, Caller, _} -> ok end,
    Delivered = erlang:trace_delivered(Caller),
    receive {trace_delivered, Caller, Delivered} -> ok end,
    Received = received(Caller),
    %% `go' and the answer, at least.
    ?assertMatch([_, _ | _], Received),
    ?assert(erts_debug:flat_size(sys:get_state(Conn)) > 10000),
    ?assertEqual([], [Size || Size <- Received, Size > 100]),
    unlink(Conn),
    eysql_sock_driver:close(Conn).

%% The flat size of each message `Pid' received while traced.
received(Pid) ->
    receive
        {trace, Pid, 'receive', Message} -> [erts_debug:flat_size(Message) | received(Pid)]
    after 0 ->
            []
    end.

%% Reading the status must leave the connection exactly as it was.
status_changes_nothing_test_() ->
    [?_assert(unchanged(fun(_) -> ok end)),
     ?_assert(unchanged(fun(C) -> eysql_sock_driver:set_txstatus(C, $T) end)),
     ?_assert(unchanged(fun(C) -> eysql_sock_driver:set_txstatus(C, $E) end)),
     ?_assert(unchanged(fun eysql_sock_driver:set_sync_required/1)),
     ?_assert(unchanged(fun eysql_sock_driver:set_copy_mode/1))
    ].

unchanged(Change) ->
    {ok, Conn} = eysql_sock_driver:open(<<"h">>, 5433, #{}),
    ok = Change(Conn),
    Before = sys:get_state(Conn),
    _ = eysql_conn:transaction_status(Conn),
    After = sys:get_state(Conn),
    unlink(Conn),
    eysql_sock_driver:close(Conn),
    Before =:= After.

%% A connection that does not answer in time, here a suspended one, is given
%% up on after a second, and its late answer does not reach the caller. The
%% reads run in a fresh process, whose mailbox holds only what they leave.
status_timeout_test() ->
    {ok, Conn} = eysql_sock_driver:open(<<"h">>, 5433, #{}),
    Self = self(),
    Reader = fun() ->
                     true = erlang:suspend_process(Conn),
                     {Micros, Status} = timer:tc(fun() -> eysql_conn:transaction_status(Conn) end),
                     true = erlang:resume_process(Conn),
                     %% Answered after the late one, which is gone by now.
                     Again = eysql_conn:transaction_status(Conn),
                     Self ! {read, Micros, Status, Again, process_info(self(), messages)}
             end,
    _ = spawn_link(Reader),
    receive
        {read, Micros, Status, Again, Messages} ->
            ?assertEqual(unknown, Status),
            ?assert(Micros > 900000 andalso Micros < 2000000),
            ?assertEqual(idle, Again),
            ?assertEqual({messages, []}, Messages)
    after 5000 ->
            error(no_read)
    end,
    unlink(Conn),
    eysql_sock_driver:close(Conn).

%%%=============================================================================
%%% open/3
%%%=============================================================================

%% The server answers the last statement open/3 sends, SET statement_timeout,
%% then shuts the connection down. The caller is held until the connection
%% process has died, so it links to a dead process. open/3 returns an error,
%% and the caller lives.
dies_before_link_test() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {packet, raw}, {ip, loopback}]),
    {ok, Port} = inet:port(Listen),
    Self = self(),
    Caller = spawn(fun() ->
                           Result = eysql_conn:open(<<"127.0.0.1">>, Port,
                                                    #{username => <<"u">>, password => <<>>,
                                                      statement_timeout => 1000,
                                                      epgsql_opts => #{codecs => []}}),
                           Self ! {opened, self(), Result}
                   end),
    CallerRef = erlang:monitor(process, Caller),
    {ok, Sock} = gen_tcp:accept(Listen, 5000),
    _Startup = recv_startup(Sock),
    ok = gen_tcp:send(Sock, [msg($R, <<0:32>>),
                             msg($S, ["integer_datetimes", 0, "on", 0]),
                             msg($K, <<1:32, 2:32>>),
                             msg($Z, "I")]),
    {$Q, _Set} = recv_message(Sock),
    %% The caller is waiting for the answer.
    true = erlang:suspend_process(Caller),
    Conn = conn_of(Caller),
    ConnRef = erlang:monitor(process, Conn),
    ok = gen_tcp:send(Sock, [msg($C, ["SET", 0]), msg($Z, "I"),
                             msg($E, [$S, "FATAL", 0, $C, "57P01", 0, $M, "shutting down", 0, 0])]),
    ok = gen_tcp:close(Sock),
    receive {'DOWN', ConnRef, process, Conn, _} -> ok after 5000 -> error(connection_alive) end,
    true = erlang:resume_process(Caller),
    receive
        {opened, Caller, Result} -> ?assertEqual({error, closed}, Result);
        {'DOWN', CallerRef, process, Caller, Reason} -> error({caller_died, Reason})
    after 5000 ->
            error(no_result)
    end,
    ok = gen_tcp:close(Listen).

msg(Type, Payload) ->
    [Type, <<(iolist_size(Payload) + 4):32>>, Payload].

recv_startup(Sock) ->
    {ok, <<Length:32>>} = gen_tcp:recv(Sock, 4, 5000),
    {ok, Body} = gen_tcp:recv(Sock, Length - 4, 5000),
    Body.

recv_message(Sock) ->
    {ok, <<Type, Length:32>>} = gen_tcp:recv(Sock, 5, 5000),
    {ok, Body} = gen_tcp:recv(Sock, Length - 4, 5000),
    {Type, Body}.

%% The epgsql connection process `Caller' started.
conn_of(Caller) ->
    [Conn] = [P || P <- processes(),
                   {dictionary, D} <- [process_info(P, dictionary)],
                   proplists:get_value('$ancestors', D) =:= [Caller],
                   proplists:get_value('$initial_call', D) =:= {epgsql_sock, init, 1}],
    Conn.
