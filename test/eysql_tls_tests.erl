-module(eysql_tls_tests).

%% Which name OTP checks a server's certificate against. Each test answers
%% one connection with a TLS server of its own that speaks just enough of
%% the protocol to take epgsql's SSLRequest, so the handshake, and with it
%% the certificate check, is real.

-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

tls_test_() ->
    {setup,
     fun() -> {ok, Apps} = application:ensure_all_started(ssl), Apps end,
     fun(Apps) -> [application:stop(App) || App <- lists:reverse(Apps)] end,
     [{"a server dialled by name is checked against that name",
       ?_assertEqual(ok, handshake(<<"localhost">>, [{dNSName, "localhost"}], []))},
      {"a server dialled by address is checked against the address",
       ?_assertEqual(hostname_check_failed, handshake(<<"127.0.0.1">>, [{dNSName, "localhost"}], []))},
      {"a certificate that lists the address passes when dialled by address",
       ?_assertEqual(ok, handshake(<<"127.0.0.1">>, [{iPAddress, <<127, 0, 0, 1>>}], []))},
      {"a certificate that lists only the address fails when dialled by name",
       ?_assertEqual(hostname_check_failed, handshake(<<"localhost">>, [{iPAddress, <<127, 0, 0, 1>>}], []))},
      {"a name the caller gives is checked on every server",
       ?_assertEqual(hostname_check_failed,
                     handshake(<<"localhost">>, [{dNSName, "localhost"}], [{server_name_indication, "db.test"}]))},
      {"disable checks the chain only, as before",
       ?_assertEqual(ok, handshake(<<"127.0.0.1">>, [{dNSName, "localhost"}], [{server_name_indication, disable}]))},
      {"a certificate from another CA fails whatever the name",
       ?_assertEqual(unknown_ca, handshake_other_ca(<<"localhost">>))}
     ]}.

%% Connects to a server whose certificate lists `Names', trusting its CA,
%% and answers `ok' when the handshake succeeded or the reason it failed.
handshake(Host, Names, ExtraSslOpts) ->
    #{server_config := ServerOpts, client_config := ClientOpts} = chain(Names),
    connect(Host, ServerOpts, [{cacerts, proplists:get_value(cacerts, ClientOpts)} | ExtraSslOpts]).

handshake_other_ca(Host) ->
    #{server_config := ServerOpts} = chain([{dNSName, "localhost"}]),
    #{client_config := Other} = chain([{dNSName, "localhost"}]),
    connect(Host, ServerOpts, [{cacerts, proplists:get_value(cacerts, Other)}]).

connect(Host, ServerOpts, SslOpts) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {ip, loopback}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listen),
    Server = serve(Listen, ServerOpts),
    Result = eysql_conn:open(Host, Port, #{username => <<"u">>,
                                          password => <<"p">>,
                                          ssl => required,
                                          ssl_opts => SslOpts,
                                          connect_timeout => 5000}),
    _ = case Result of
            {ok, Conn} -> eysql_conn:close(Conn);
            {error, _} -> ok
        end,
    gen_tcp:close(Listen),
    receive
        {Server, Handshake} -> outcome(Handshake, Result)
    after 5000 ->
        error(server_silent)
    end.

%% The server's view says whether the handshake succeeded; the client's says
%% why it failed.
outcome(ok, {error, {ssl_negotiation_failed, Reason}}) ->
    error({client_failed_after_server_succeeded, Reason});
outcome(ok, _) ->
    ok;
outcome({error, _}, {error, {ssl_negotiation_failed, {tls_alert, {Alert, Text}}}}) ->
    Words = string:lowercase(Text),
    case string:find(Words, "hostname_check_failed") of
        nomatch -> Alert;
        _ -> hostname_check_failed
    end;
outcome({error, _} = Server, Client) ->
    error({unexpected, Server, Client}).

%% Answers one connection: the SSLRequest with `S', then the handshake, and
%% the startup message that follows a good handshake with a failed login.
serve(Listen, ServerOpts) ->
    Test = self(),
    spawn_link(
      fun() ->
              {ok, Socket} = gen_tcp:accept(Listen, 5000),
              {ok, <<8:32, 80877103:32>>} = gen_tcp:recv(Socket, 8, 5000),
              ok = gen_tcp:send(Socket, <<"S">>),
              Handshake = case ssl:handshake(Socket, ServerOpts, 5000) of
                              {ok, Tls} ->
                                  {ok, _Startup} = ssl:recv(Tls, 0, 5000),
                                  ok = ssl:send(Tls, login_failed()),
                                  ssl:close(Tls),
                                  ok;
                              {error, _} = Error ->
                                  Error
                          end,
              gen_tcp:close(Socket),
              Test ! {self(), Handshake}
      end).

%% An ErrorResponse: FATAL, 28P01, so the client gets an ordinary error.
login_failed() ->
    Fields = <<$S, "FATAL", 0, $C, "28P01", 0, $M, "no logins here", 0, 0>>,
    <<$E, (byte_size(Fields) + 4):32, Fields/binary>>.

%% A root, and a server certificate under it that lists `Names', with the
%% server's root among the server's own CAs, as a server sends its chain.
%% ECDSA over SHA-256, which TLS 1.3 accepts; OTP's defaults here are not.
chain(Names) ->
    SubjectAltName = #'Extension'{extnID = ?'id-ce-subjectAltName',
                                  critical = false,
                                  extnValue = Names},
    Key = [{digest, sha256}, {key, {namedCurve, secp256r1}}],
    #{server_config := Server, client_config := Client} = Chain =
        public_key:pkix_test_data(#{server_chain => #{root => Key, intermediates => [],
                                                      peer => [{extensions, [SubjectAltName]} | Key]},
                                    client_chain => #{root => Key, intermediates => [], peer => Key}}),
    Chain#{server_config := lists:keystore(cacerts, 1, Server, lists:keyfind(cacerts, 1, Client))}.
