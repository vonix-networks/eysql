%% socket_timeout: a call on a pooled connection waits that long for the
%% server at most. Most of these run the default driver against a server of
%% the test's own on loopback, which can stop answering at any statement, as
%% a server that has died without closing its sockets does.
-module(eysql_socket_timeout_tests).

-include_lib("eunit/include/eunit.hrl").

-import(eysql_test_util, [config/1, three/0, key/1, wait_until/2]).

socket_timeout_test_() ->
    [{"a query on a server that stops answering returns within the bound; the connection is "
      "replaced, its host left in", {timeout, 20, fun silent_server/0}},
     {"off by default, and never for epgsql's own calls", {timeout, 20, fun unbounded/0}},
     {"a transaction whose statement times out runs again on a fresh connection",
      {timeout, 20, fun transaction_before_commit/0}},
     {"a transaction whose COMMIT times out reports commit_outcome_unknown",
      {timeout, 20, fun transaction_during_commit/0}},
     {"a fast query leaves no timer and no message behind", {timeout, 20, fun no_stray/0}}
    ].

fake_test_() ->
    {foreach,
     fun() -> eysql_fake_driver:start() end,
     fun(_) -> eysql_fake_driver:stop() end,
     [{"with load balancing, a timeout marks no host and starts no refresh",
       {timeout, 20, fun marks_nothing/0}}]}.

%% The server stops answering eysql:equery/3, whose extended protocol it
%% never answers, and "SELECT sleep". Each call returns once the bound has
%% passed, the connection's socket closed, and the pool opens another to
%% the same host, which answers.
silent_server() ->
    {Server, Port} = server(fun(_Session, Sql) -> answer_unless(<<"SELECT sleep">>, Sql) end),
    Pool = pool(Port, #{socket_timeout => 300}),
    lists:foreach(
      fun({Session, Query}) ->
              {Micros, Result} = timed(Query),
              ?assertEqual({error, {connection_lost, socket_timeout}}, Result),
              ?assert(Micros >= 300000 andalso Micros < 1000000),
              closed(Session),
              wait_until(fun() -> maps:get(idle, eysql:stats(Pool)) =:= 1 end, replaced)
      end,
      [{1, fun() -> eysql:equery(Pool, "SELECT 1", []) end},
       {2, fun() -> eysql:squery(Pool, "SELECT sleep") end},
       {3, fun() -> eysql:with_connection(Pool, fun(C) -> eysql_conn:squery(C, "SELECT sleep") end) end}]),
    ?assertEqual({ok, 1}, eysql:squery(Pool, "UPDATE t")),
    Host = {<<"127.0.0.1">>, Port},
    ?assertMatch(#{failed := [], rejected := [], read_only := [], counts := #{Host := 1}},
                 eysql:cluster_info(Pool)),
    stop(Pool, Server).

%% Without socket_timeout, a query on a server that stops answering waits
%% until the socket closes. So does a call to epgsql itself, bound or not:
%% only eysql_conn's calls are bounded.
unbounded() ->
    {Server, Port} = server(fun(_Session, Sql) -> answer_unless(<<"SELECT sleep">>, Sql) end),
    Default = pool(Port, #{}),
    Bounded = pool(Port, #{socket_timeout => 200}),
    Self = self(),
    Queries = [fun() -> eysql:squery(Default, "SELECT sleep") end,
               fun() -> eysql:with_connection(Bounded, fun(C) -> epgsql:squery(C, "SELECT sleep") end) end],
    Queriers = [spawn(fun() -> Self ! {result, self(), Query()} end) || Query <- Queries],
    timer:sleep(800),
    receive {result, _, Early} -> error({returned, Early}) after 0 -> ok end,
    %% The server goes away, closing its sockets. epgsql's connections
    %% report their crash, and the pools that they cannot reconnect; keep
    %% that out of the output.
    #{level := Level} = logger:get_primary_config(),
    ok = logger:set_primary_config(level, none),
    try
        stop_server(Server),
        [receive
             {result, Querier, Result} -> ?assert(eysql_error:is_connection_lost(Result))
         after 5000 ->
                 error(still_waiting)
         end || Querier <- Queriers],
        stop(Default),
        stop(Bounded)
    after
        logger:set_primary_config(level, Level)
    end.

%% The first connection stops answering at the transaction's statement,
%% before COMMIT: nothing was committed, and the transaction runs again on
%% a fresh connection, which answers.
transaction_before_commit() ->
    {Server, Port} = server(fun(1, <<"UPDATE t">>) -> silent;
                               (_Session, _Sql) -> ok
                            end),
    Pool = pool(Port, #{socket_timeout => 300}),
    Attempts = counters:new(1, []),
    Update = fun(C) ->
                     counters:add(Attempts, 1, 1),
                     case eysql_conn:squery(C, "UPDATE t") of
                         {ok, 1} -> {ok, updated};
                         {error, _} = Error -> Error
                     end
             end,
    {_Micros, Result} = timed(fun() -> eysql:transaction(Pool, Update, #{max_backoff => 0}) end),
    ?assertEqual({ok, updated}, Result),
    ?assertEqual(2, counters:get(Attempts, 1)),
    closed(1),
    stop(Pool, Server).

%% The connection stops answering at COMMIT, which the server may or may not
%% have carried out: the transaction reports that, and does not run again.
transaction_during_commit() ->
    {Server, Port} = server(fun(1, <<"COMMIT">>) -> silent;
                               (_Session, _Sql) -> ok
                            end),
    Pool = pool(Port, #{socket_timeout => 300}),
    Attempts = counters:new(1, []),
    Update = fun(C) ->
                     counters:add(Attempts, 1, 1),
                     {ok, 1} = eysql_conn:squery(C, "UPDATE t"),
                     {ok, updated}
             end,
    {Micros, Result} = timed(fun() -> eysql:transaction(Pool, Update, #{max_backoff => 0}) end),
    ?assertEqual({error, commit_outcome_unknown}, Result),
    ?assert(Micros >= 300000 andalso Micros < 1000000),
    ?assertEqual(1, counters:get(Attempts, 1)),
    closed(1),
    stop(Pool, Server).

%% Queries that answer in time, through each way in, leave the caller's
%% mailbox empty and nothing kept for the connection in its process
%% dictionary once it is given back. The clocks were stopped: well past the
%% bound, no timeout has come to the pool, which would otherwise kill the
%% connection, and the same connection still serves. The caller is a
%% process of its own, so that its mailbox holds only what the queries
%% leave, and not the trace of what the pool receives.
no_stray() ->
    {Server, Port} = server(fun(_Session, _Sql) -> ok end),
    Pool = pool(Port, #{socket_timeout => 200}),
    1 = erlang:trace(Pool, true, ['receive']),
    Test = self(),
    Caller = spawn(fun() ->
                           First = eysql:with_connection(Pool, fun(C) ->
                                                                       {ok, 1} = eysql_conn:squery(C, "UPDATE t"),
                                                                       C
                                                               end),
                           {ok, 1} = eysql:squery(Pool, "UPDATE t"),
                           {ok, x} = eysql:transaction(Pool, fun(C) ->
                                                                     {ok, 1} = eysql_conn:squery(C, "UPDATE t"),
                                                                     {ok, x}
                                                             end),
                           {ok, Conn} = eysql:checkout(Pool),
                           Held = kept(),
                           {ok, 1} = eysql_conn:squery(Conn, "UPDATE t"),
                           eysql:checkin(Pool, Conn),
                           Test ! {done, self(), {First, Conn, Held, kept()}},
                           receive look -> Test ! {mailbox, self(), process_info(self(), messages)} end
                   end),
    {First, Conn, Held, Kept} = receive {done, Caller, Done} -> Done after 5000 -> error(no_result) end,
    ?assertMatch([_], Held),
    ?assertEqual([], Kept),
    timer:sleep(500),
    Caller ! look,
    ?assertEqual({messages, []}, receive {mailbox, Caller, Mailbox} -> Mailbox after 5000 -> error(no_mailbox) end),
    ?assertEqual(First, Conn),
    ?assert(is_process_alive(Conn)),
    Delivered = erlang:trace_delivered(Pool),
    receive {trace_delivered, Pool, Delivered} -> ok end,
    erlang:trace(Pool, false, ['receive']),
    ?assertEqual([], [M || {trace, P, 'receive', {timeout, _, _} = M} <- flush(), P =:= Pool]),
    stop(Pool, Server).

%% What this process keeps for connections it holds.
kept() ->
    [Key || {{eysql_pool, socket_timeout, _} = Key, _} <- get()].

flush() ->
    receive Message -> [Message | flush()]
    after 0 -> []
    end.

%% The fake driver's connections never answer epgsql's calls. One times out,
%% the pool kills it and opens another, and that is all: no host is marked,
%% no refresh starts, though a second has passed since the last one and a
%% failed connect would start one, and no other connect is made.
marks_nothing() ->
    eysql_fake_driver:set_servers(three()),
    {ok, Pool} = eysql_pool:start_link(config(#{pool_size => 3, socket_timeout => 200})),
    wait_until(fun() -> maps:get(idle, eysql:stats(Pool)) =:= 3
                            andalso length(eysql_fake_driver:conns()) =:= 3 end, full),
    timer:sleep(1100),
    Hosts = [key(H) || H <- [<<"a">>, <<"b">>, <<"c">>]],
    Opens = fun() -> lists:sum([eysql_fake_driver:opens(K) || K <- Hosts]) end,
    Before = {eysql_fake_driver:discoveries(), Opens()},
    eysql_fake_driver:set_epgsql_reply(no_reply),
    {Micros, Result} = timed(fun() -> eysql:equery(Pool, "SELECT 1", []) end),
    ?assertEqual({error, {connection_lost, socket_timeout}}, Result),
    ?assert(Micros >= 200000 andalso Micros < 1000000),
    wait_until(fun() -> maps:get(idle, eysql:stats(Pool)) =:= 3
                            andalso length(eysql_fake_driver:conns()) =:= 3 end, replaced),
    timer:sleep(200),
    ?assertEqual({element(1, Before), element(2, Before) + 1}, {eysql_fake_driver:discoveries(), Opens()}),
    ?assertMatch(#{failed := [], rejected := [], read_only := []}, eysql:cluster_info(Pool)),
    unlink(Pool),
    eysql:stop(Pool).

%%%=============================================================================
%%% Helpers
%%%=============================================================================

%% Run `Fun' in a process of its own, as a caller of the pool, and return how
%% long it took in microseconds and what it returned. Fails, rather than
%% hang, if it has not returned in three seconds, well past any bound here.
timed(Fun) ->
    Test = self(),
    Caller = spawn(fun() -> Test ! {timed, self(), timer:tc(Fun)} end),
    receive
        {timed, Caller, Timed} -> Timed
    after 3000 ->
            exit(Caller, kill),
            error(still_waiting)
    end.

%% A pool of one connection to the test's server, with no discovery.
pool(Port, Overrides) ->
    Options = maps:merge(#{hosts => [{<<"127.0.0.1">>, Port}], pool_size => 1, rebalance_interval => 60000,
                           epgsql_opts => #{codecs => []}},
                         Overrides),
    {ok, Config} = eysql_config:normalize(Options),
    {ok, Pool} = eysql_pool:start_link(Config),
    wait_until(fun() -> maps:get(idle, eysql:stats(Pool)) =:= 1 end, open),
    Pool.

stop(Pool) ->
    unlink(Pool),
    eysql:stop(Pool).

stop(Pool, Server) ->
    stop(Pool),
    stop_server(Server).

answer_unless(Silent, Silent) -> silent;
answer_unless(_Silent, _Sql) -> ok.

%% The server's session `N', counting connections from 1, saw its socket
%% close.
closed(N) ->
    receive {session_closed, N} -> ok
    after 5000 -> error({socket_open, N})
    end.

%% A PostgreSQL server on loopback. Each connection completes the handshake,
%% then answers each simple query as `Answer(Session, Sql)' says: `ok'
%% answers with the statement's command tag, and `silent' leaves it, and
%% everything after it, unanswered. The extended protocol is never
%% answered. Each session tells the test when its socket closes. The
%% sessions are linked to the server, and stop_server/1 kills them with it,
%% closing their sockets.
server(Answer) ->
    Test = self(),
    Server = spawn(fun() ->
                           {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {packet, raw},
                                                             {ip, loopback}]),
                           {ok, Port} = inet:port(Listen),
                           Test ! {port, self(), Port},
                           accept(Listen, Answer, Test, 1)
                   end),
    receive {port, Server, Port} -> {Server, Port} end.

stop_server(Server) ->
    Monitor = erlang:monitor(process, Server),
    exit(Server, kill),
    receive {'DOWN', Monitor, process, Server, _} -> ok end.

accept(Listen, Answer, Test, N) ->
    {ok, Sock} = gen_tcp:accept(Listen),
    Session = spawn_link(fun() -> receive go -> session(Sock, Answer, Test, N) end end),
    ok = gen_tcp:controlling_process(Sock, Session),
    Session ! go,
    accept(Listen, Answer, Test, N + 1).

session(Sock, Answer, Test, N) ->
    case handshake(Sock) of
        ok -> serve(Sock, fun(Sql) -> Answer(N, Sql) end, $I);
        _Closed -> ok
    end,
    Test ! {session_closed, N}.

%% No password, and a session ready for queries.
handshake(Sock) ->
    case gen_tcp:recv(Sock, 4) of
        {ok, <<Length:32>>} ->
            case gen_tcp:recv(Sock, Length - 4) of
                {ok, _Startup} ->
                    gen_tcp:send(Sock, [msg($R, <<0:32>>), msg($S, ["integer_datetimes", 0, "on", 0]),
                                        msg($K, <<1:32, 2:32>>), msg($Z, "I")]);
                {error, _} ->
                    closed
            end;
        {error, _} ->
            closed
    end.

serve(Sock, Answer, Status) ->
    case recv(Sock) of
        {$Q, Body} ->
            [Sql | _] = binary:split(Body, <<0>>),
            case Answer(Sql) of
                ok ->
                    Next = status(Sql, Status),
                    ok = gen_tcp:send(Sock, [msg($C, [tag(Sql), 0]), msg($Z, [Next])]),
                    serve(Sock, Answer, Next);
                silent ->
                    drain(Sock)
            end;
        {$X, _} ->
            gen_tcp:close(Sock);
        {_Extended, _} ->
            drain(Sock);
        closed ->
            ok
    end.

recv(Sock) ->
    case gen_tcp:recv(Sock, 5) of
        {ok, <<Type, Length:32>>} ->
            case gen_tcp:recv(Sock, Length - 4) of
                {ok, Body} -> {Type, Body};
                {error, _} -> closed
            end;
        {error, _} ->
            closed
    end.

%% Read, and answer nothing, until the client closes the socket.
drain(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, _} -> drain(Sock);
        {error, _} -> ok
    end.

status(<<"BEGIN">>, _Status) -> $T;
status(<<"COMMIT">>, _Status) -> $I;
status(<<"ROLLBACK">>, _Status) -> $I;
status(_Sql, Status) -> Status.

%% epgsql:squery/2 returns `{ok, 1}' for an UPDATE, and `{ok, [], []}'
%% for BEGIN, COMMIT and ROLLBACK.
tag(Sql) ->
    case hd(binary:split(Sql, <<" ">>)) of
        <<"UPDATE">> -> "UPDATE 1";
        Word -> Word
    end.

msg(Type, Payload) ->
    [Type, <<(iolist_size(Payload) + 4):32>>, Payload].
