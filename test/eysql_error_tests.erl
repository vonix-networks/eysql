-module(eysql_error_tests).

-include_lib("eunit/include/eunit.hrl").

e(Code) -> {error, eysql_fake_driver:pg_error(Code)}.

classify_test_() ->
    [?_assertEqual(retryable, eysql_error:classify(e(<<"40001">>))),
     ?_assertEqual(retryable, eysql_error:classify(e(<<"40P01">>))),
     ?_assertEqual(connection_lost, eysql_error:classify(e(<<"57P01">>))),
     ?_assertEqual(connection_lost, eysql_error:classify(e(<<"57P02">>))),
     ?_assertEqual(connection_lost, eysql_error:classify(e(<<"57P03">>))),
     ?_assertEqual(connection_lost, eysql_error:classify(e(<<"08006">>))),
     ?_assertEqual(connection_lost, eysql_error:classify({error, closed})),
     ?_assertEqual(connection_lost, eysql_error:classify({error, sock_closed})),
     ?_assertEqual(connection_lost, eysql_error:classify({error, {connection_lost, noproc}})),
     ?_assertEqual(other, eysql_error:classify(e(<<"23505">>))),
     ?_assertEqual(other, eysql_error:classify({error, whatever})),
     ?_assert(eysql_error:is_retryable(e(<<"40001">>))),
     ?_assertNot(eysql_error:is_retryable(e(<<"23505">>))),
     ?_assert(eysql_error:is_connection_lost({error, closed})),
     ?_assertEqual(<<"42883">>, eysql_error:code(e(<<"42883">>))),
     ?_assertEqual(undefined, eysql_error:code({error, closed}))
    ].

%% Only what pgjdbc reports as 08001, a connection that could not be made,
%% marks a host down (LoadBalanceService.getConnection). Its
%% ConnectionFactoryImpl gives that state to every I/O failure while
%% connecting; a server that answers with an error keeps its own SQLSTATE,
%% and a failed TLS handshake is 08006 (MakeSSL.convert).
connect_failure_test_() ->
    Unreachable = [econnrefused, timeout, etimedout, nxdomain, ehostunreach, ehostdown, enetunreach,
                   enetdown, econnreset, econnaborted, epipe, enotconn, eaddrnotavail, closed,
                   sock_closed, {sock_error, econnreset}, {connection_lost, noproc}, probe_timeout,
                   {probe_exit, killed}, eysql_fake_driver:pg_error(<<"08001">>)],
    Rejected = [invalid_password, invalid_authorization_specification,
                eysql_fake_driver:pg_error(<<"28P01">>), eysql_fake_driver:pg_error(<<"3D000">>),
                eysql_fake_driver:pg_error(<<"53300">>), eysql_fake_driver:pg_error(<<"57P03">>),
                eysql_fake_driver:pg_error(<<"08006">>), eysql_fake_driver:pg_error(<<"08004">>),
                ssl_not_available, {ssl_negotiation_failed, closed},
                {ssl_negotiation_failed, {tls_alert, {bad_certificate, "x"}}},
                {unsupported_auth_method, gss}, {sasl_server_final, x}, read_only_server,
                {exit, killed}, whatever],
    [{"cannot connect: " ++ lists:flatten(io_lib:format("~0p", [R])),
      ?_assertEqual(unreachable, eysql_error:connect_failure(R))} || R <- Unreachable]
    ++ [{"answered: " ++ lists:flatten(io_lib:format("~0P", [R, 4])),
         ?_assertEqual(rejected, eysql_error:connect_failure(R))} || R <- Rejected]
    ++ [{"inside {error, _}", ?_assertEqual(unreachable, eysql_error:connect_failure({error, nxdomain}))}].
