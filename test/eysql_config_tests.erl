-module(eysql_config_tests).

-include_lib("eunit/include/eunit.hrl").

%% The options with a counterpart in the JDBC smart driver default as there:
%% LoadBalanceProperties for the smart-driver options, PGProperty and
%% Driver.parseURL for the rest.
defaults_test() ->
    {ok, Config} = eysql_config:normalize(#{}),
    ?assertMatch([#{host := <<"localhost">>, port := 5433}], maps:get(seeds, Config)),
    ?assertEqual(300000, maps:get(refresh_interval, Config)),
    ?assertEqual(5000, maps:get(failed_host_delay, Config)),
    ?assertEqual(undefined, maps:get(failed_host_max_delay, Config)),
    ?assertEqual(false, maps:get(load_balance, Config)),
    ?assertEqual([], maps:get(topology_keys, Config)),
    ?assertEqual(false, maps:get(fallback_to_topology_keys_only, Config)),
    ?assertEqual(any, maps:get(target_session_attrs, Config)),
    ?assertMatch(#{connect_timeout := 10000, database := <<"yugabyte">>},
                 maps:get(settings, Config)),
    ?assertEqual(eysql_conn, maps:get(driver, Config)).

%% With no database given, the one named after the user, as in pgjdbc.
database_test_() ->
    Database = fun(Options) ->
                       {ok, #{settings := #{database := D}}} = eysql_config:normalize(Options),
                       D
               end,
    [{"the username's", ?_assertEqual(<<"app">>, Database(#{username => "app"}))},
     {"as given", ?_assertEqual(<<"db">>, Database(#{username => "app", database => "db"}))},
     {"not text", ?_assertEqual({error, {invalid_option, database, 42}},
                                eysql_config:normalize(#{database => 42}))}
    ].

%% The pool has no health checks, as the smart drivers have none, so their
%% options are gone, and naming one is an error rather than silently
%% nothing.
health_check_options_test_() ->
    [?_assertEqual({error, {unknown_options, [health_check_interval]}},
                   eysql_config:normalize(#{health_check_interval => 5000})),
     ?_assertEqual({error, {unknown_options, [health_check_interval, health_check_timeout]}},
                   eysql_config:normalize(#{health_check_timeout => 2000, health_check_interval => 5000})),
     ?_assertNot(lists:any(fun(Key) -> lists:member(Key, [health_check_interval, health_check_timeout]) end,
                           maps:keys(eysql_config:defaults())))].

%% A connection tries every eligible server, as in the JDBC driver, so there
%% is no cap to set.
connect_attempts_test() ->
    ?assertEqual({error, {unknown_options, [connect_attempts]}},
                 eysql_config:normalize(#{connect_attempts => 3})).

hosts_test_() ->
    Seeds = fun(Hosts) ->
                    {ok, #{seeds := S}} = eysql_config:normalize(#{hosts => Hosts, port => 5432}),
                    [{H, P} || #{host := H, port := P} <- S]
            end,
    [?_assertEqual([{<<"db">>, 5432}], Seeds("db")),
     ?_assertEqual([{<<"db">>, 5432}], Seeds(<<"db">>)),
     ?_assertEqual([{<<"a">>, 5432}, {<<"b">>, 5432}], Seeds(["a", <<"b">>])),
     ?_assertEqual([{<<"a">>, 6000}, {<<"b">>, 5432}], Seeds([{"a", 6000}, b]))
    ].

load_balance_test_() ->
    Mode = fun(Value) ->
                   {ok, #{load_balance := M}} = eysql_config:normalize(#{load_balance => Value}),
                   M
           end,
    Invalid = fun(Value) -> {error, {invalid_option, load_balance, Value}} end,
    [{"the drivers' spellings, as strings",
      ?_assertEqual([true, false, any, only_primary, only_rr, prefer_primary, prefer_rr],
                    [Mode(V) || V <- ["true", "false", "any", "only-primary", "only-rr",
                                      "prefer-primary", "prefer-rr"]])},
     {"and as binaries",
      ?_assertEqual([true, false, any, only_primary, only_rr, prefer_primary, prefer_rr],
                    [Mode(V) || V <- [<<"true">>, <<"false">>, <<"any">>, <<"only-primary">>,
                                      <<"only-rr">>, <<"prefer-primary">>, <<"prefer-rr">>]])},
     {"in any case, as the JDBC driver reads them",
      ?_assertEqual([prefer_primary, any], [Mode(<<"Prefer-Primary">>), Mode("ANY")])},
     {"atoms as before", ?_assertEqual(only_rr, Mode(only_rr))},
     {"unknown text", ?_assertEqual(Invalid("sometimes"), eysql_config:normalize(#{load_balance => "sometimes"}))},
     {"the atoms' spelling as text", ?_assertEqual(Invalid(<<"only_primary">>),
                                                  eysql_config:normalize(#{load_balance => <<"only_primary">>}))},
     {"empty", ?_assertEqual(Invalid(""), eysql_config:normalize(#{load_balance => ""}))},
     {"not text", ?_assertEqual(Invalid([-1]), eysql_config:normalize(#{load_balance => [-1]}))}
    ].

refresh_interval_test_() ->
    Interval = fun(Secs) ->
                       case eysql_config:normalize(#{yb_servers_refresh_interval => Secs}) of
                           {ok, #{refresh_interval := Ms}} -> Ms;
                           {error, _} = Error -> Error
                       end
               end,
    Invalid = fun(Value) -> {error, {invalid_option, yb_servers_refresh_interval, Value}} end,
    [{"0 refreshes on each pick", ?_assertEqual(0, Interval(0))},
     {"600 at most", ?_assertEqual(600000, Interval(600))},
     {"above 600", ?_assertEqual(Invalid(601), Interval(601))},
     {"below 0", ?_assertEqual(Invalid(-1), Interval(-1))},
     {"not an integer", ?_assertEqual(Invalid(1.5), Interval(1.5))}
    ].

%% 0 to 60 seconds, as the JDBC driver's LoadBalanceProperties takes
%% failed-host-reconnect-delay-secs.
failed_host_reconnect_delay_test_() ->
    Delay = fun(Secs) ->
                    case eysql_config:normalize(#{failed_host_reconnect_delay_secs => Secs}) of
                        {ok, #{failed_host_delay := Ms}} -> Ms;
                        {error, _} = Error -> Error
                    end
            end,
    Invalid = fun(Value) -> {error, {invalid_option, failed_host_reconnect_delay_secs, Value}} end,
    [{"5 by default", ?_assertEqual(5000, Delay(5))},
     {"0: due at the next refresh", ?_assertEqual(0, Delay(0))},
     {"60 at most", ?_assertEqual(60000, Delay(60))},
     {"above 60", ?_assertEqual(Invalid(61), Delay(61))},
     {"below 0", ?_assertEqual(Invalid(-1), Delay(-1))},
     {"not an integer", ?_assertEqual(Invalid(0.5), Delay(0.5))}
    ].

failed_host_delay_test_() ->
    Max = fun(Options) ->
                  case eysql_config:normalize(Options) of
                      {ok, #{failed_host_max_delay := Ms}} -> Ms;
                      {error, _} = Error -> Error
                  end
          end,
    Invalid = fun(Value) -> {error, {invalid_option, failed_host_max_delay_secs, Value}} end,
    [{"fixed by default", ?_assertEqual(undefined, Max(#{}))},
     %% 0 doubled is 0: a cap would suggest a delay that grows, and change
     %% nothing.
     {"a delay of 0 takes no cap",
      [?_assertEqual(Invalid(10), Max(#{failed_host_reconnect_delay_secs => 0, failed_host_max_delay_secs => 10})),
       ?_assertEqual(Invalid(0), Max(#{failed_host_reconnect_delay_secs => 0, failed_host_max_delay_secs => 0})),
       ?_assertEqual(undefined, Max(#{failed_host_reconnect_delay_secs => 0}))]},
     {"a cap above 60 seconds, the delay's own limit",
      ?_assertEqual(600000, Max(#{failed_host_reconnect_delay_secs => 60, failed_host_max_delay_secs => 600}))},
     {"undefined is fixed", ?_assertEqual(undefined, Max(#{failed_host_max_delay_secs => undefined}))},
     {"a cap turns doubling on", ?_assertEqual(60000, Max(#{failed_host_max_delay_secs => 60}))},
     {"a cap equal to the delay",
      ?_assertEqual(10000, Max(#{failed_host_reconnect_delay_secs => 10, failed_host_max_delay_secs => 10}))},
     {"a cap below the delay",
      ?_assertEqual(Invalid(5), Max(#{failed_host_reconnect_delay_secs => 10, failed_host_max_delay_secs => 5}))},
     {"not an integer", ?_assertEqual(Invalid(forever), Max(#{failed_host_max_delay_secs => forever}))}
    ].

topology_keys_test() ->
    {ok, Config} = eysql_config:normalize(#{topology_keys => "gcp.us-east1.*"}),
    ?assertEqual([{<<"gcp">>, <<"us-east1">>, '*', 1}], maps:get(topology_keys, Config)).

%% Whatever eysql_topology:parse_keys/1 makes of a value, normalize/1 returns
%% an error for it rather than raise.
invalid_topology_keys_test_() ->
    Keys = fun(Value) -> eysql_config:normalize(#{topology_keys => Value}) end,
    [{"text that is not keys", ?_assertMatch({error, {invalid_topology_key, _}}, Keys("nope"))},
     {"an atom", ?_assertEqual({error, {invalid_topology_key, nope}}, Keys(nope))},
     {"an integer", ?_assertMatch({error, {invalid_topology_key, _}}, Keys(42))},
     {"a list with a non-character", ?_assertMatch({error, {invalid_topology_key, _}}, Keys([-1]))},
     {"text with a key that is no text",
      ?_assertMatch({error, {invalid_topology_key, _}}, Keys(["gcp.us-east1.*", undefined]))},
     {"text beyond Latin-1, parsed or refused",
      ?_assert(case Keys("gcp.us-east1.z\x{100}") of
                   {ok, _} -> true;
                   {error, {invalid_topology_key, _}} -> true
               end)}
    ].

%% The settings hold the password as it was given, text as a binary, and the
%% caller's own fun as it is: a fun made in eysql_config would fail with
%% badfun once that module had been reloaded twice. The pool and cluster
%% redact it when they print their state.
password_test_() ->
    Stored = fun(Value) ->
                     {ok, #{settings := #{password := P}}} = eysql_config:normalize(#{password => Value}),
                     P
             end,
    Own = fun() -> <<"mine">> end,
    External = fun erlang:node/0,
    Redacted = {error, {invalid_option, password, redacted}},
    Invalid = fun(Value) -> eysql_config:normalize(#{password => Value}) end,
    [{"empty by default",
      ?_assertMatch({ok, #{settings := #{password := <<>>}}}, eysql_config:normalize(#{}))},
     {"a binary as it is", ?_assertEqual(<<"s3cret">>, Stored(<<"s3cret">>))},
     {"a string as a binary", ?_assertEqual(<<"s3cret">>, Stored("s3cret"))},
     {"a Unicode string as UTF-8", ?_assertEqual(<<"pässwörd"/utf8>>, Stored("pässwörd"))},
     {"a binary that is not UTF-8, as it is: a password need not be text",
      ?_assertEqual(<<"p", 233, "ss">>, Stored(<<"p", 233, "ss">>))},
     {"the caller's fun as it is", ?_assertEqual(Own, Stored(Own))},
     {"an external fun as it is", ?_assertEqual(External, Stored(External))},
     {"not text, and not echoed", ?_assertEqual(Redacted, Invalid(123456))},
     {"a list with a surrogate", ?_assertEqual(Redacted, Invalid("s3cret" ++ [16#D800]))},
     {"a truncated UTF-8 binary in a list", ?_assertEqual(Redacted, Invalid(["s3cret", <<195>>]))},
     {"a list with a non-character", ?_assertEqual(Redacted, Invalid(["s3cret", -1]))},
     {"a fun of the wrong arity", ?_assertEqual(Redacted, Invalid(fun(_) -> <<"s3cret">> end))}
    ].

%% Text options follow eysql_util:text/1: a string, a UTF-8 binary or a mix.
%% unicode:characters_to_binary/1 returns an error tuple for some lists that
%% are not text; that must be refused, not stored. So must a binary that is
%% not UTF-8, which the server would refuse at every connect instead.
text_options_test_() ->
    Settings = fun(Options) ->
                       {ok, #{settings := S}} = eysql_config:normalize(Options),
                       S
               end,
    Invalid = fun(Name, Value) -> {error, {invalid_option, Name, Value}} end,
    Surrogate = "app" ++ [16#D800],
    Truncated = ["app", <<195>>],
    [{"strings as binaries",
      ?_assertMatch(#{username := <<"app">>, database := <<"db">>, application_name := <<"svc">>},
                    Settings(#{username => "app", database => "db", application_name => "svc"}))},
     {"UTF-8 binaries as they are",
      ?_assertMatch(#{username := <<"app">>, database := <<"données"/utf8>>},
                    Settings(#{username => <<"app">>, database => <<"données"/utf8>>}))},
     {"Unicode as UTF-8",
      ?_assertMatch(#{database := <<"données"/utf8>>}, Settings(#{database => "données"}))},
     {"a string and a binary mixed",
      ?_assertMatch(#{application_name := <<"svc-1">>}, Settings(#{application_name => ["svc", <<"-1">>]}))},
     {"a binary that is not UTF-8", ?_assertEqual(Invalid(username, <<"caf", 233>>),
                                                  eysql_config:normalize(#{username => <<"caf", 233>>}))},
     {"a host binary that is not UTF-8", ?_assertEqual(Invalid(hosts, <<"db", 255>>),
                                                       eysql_config:normalize(#{hosts => [<<"db", 255>>]}))},
     {"an empty host", ?_assertEqual(Invalid(hosts, <<>>), eysql_config:normalize(#{hosts => [<<>>]}))},
     {"a surrogate", ?_assertEqual(Invalid(username, Surrogate),
                                   eysql_config:normalize(#{username => Surrogate}))},
     {"a truncated UTF-8 binary", ?_assertEqual(Invalid(database, Truncated),
                                                eysql_config:normalize(#{database => Truncated}))},
     {"a non-character", ?_assertEqual(Invalid(application_name, [-1]),
                                       eysql_config:normalize(#{application_name => [-1]}))},
     {"not a list", ?_assertEqual(Invalid(username, app), eysql_config:normalize(#{username => app}))},
     {"a host with a surrogate", ?_assertEqual(Invalid(hosts, [16#D800]),
                                               eysql_config:normalize(#{hosts => [[16#D800]]}))},
     {"load_balance with a surrogate", ?_assertEqual(Invalid(load_balance, [16#D800]),
                                                     eysql_config:normalize(#{load_balance => [16#D800]}))}
    ].

%% ssl_opts reach epgsql exactly as given, whatever `ssl' is. eysql never
%% turns certificate verification off: with a CA and no `verify', OTP 26 and
%% later verify the server's certificate, and with no CA they refuse to
%% connect. Only the caller's own `{verify, verify_none}' skips the check.
ssl_opts_test_() ->
    SslOpts = fun(Options) ->
                      {ok, #{settings := #{ssl_opts := O}}} = eysql_config:normalize(Options),
                      O
              end,
    Versions = {versions, ['tlsv1.3']},
    CaFile = {cacertfile, "/etc/ssl/ca.pem"},
    CaCerts = {cacerts, [<<"a DER certificate">>]},
    Peer = [{verify, verify_peer}, CaFile],
    None = [{verify, verify_none}],
    [{"off: nothing given, nothing added", ?_assertEqual([], SslOpts(#{}))},
     {"off: the options as given", ?_assertEqual([Versions], SslOpts(#{ssl_opts => [Versions]}))},
     {"on, nothing given: nothing added", ?_assertEqual([], SslOpts(#{ssl => true}))},
     {"required, nothing given: nothing added", ?_assertEqual([], SslOpts(#{ssl => required}))},
     {"a cacertfile only, as given",
      ?_assertEqual([CaFile], SslOpts(#{ssl => required, ssl_opts => [CaFile]}))},
     {"cacerts only, as given",
      ?_assertEqual([Versions, CaCerts], SslOpts(#{ssl => true, ssl_opts => [Versions, CaCerts]}))},
     {"other options only, as given",
      ?_assertEqual([Versions], SslOpts(#{ssl => true, ssl_opts => [Versions]}))},
     {"verify_peer as given", ?_assertEqual(Peer, SslOpts(#{ssl => true, ssl_opts => Peer}))},
     {"verify_none as given, the caller's own choice",
      ?_assertEqual(None, SslOpts(#{ssl => required, ssl_opts => None}))},
     {"ssl itself unchanged",
      ?_assertMatch({ok, #{settings := #{ssl := required}}}, eysql_config:normalize(#{ssl => required}))},
     {"an invalid ssl", ?_assertEqual({error, {invalid_option, ssl, yes}}, eysql_config:normalize(#{ssl => yes}))},
     {"invalid ssl_opts, not echoed",
      ?_assertEqual({error, {invalid_option, ssl_opts, redacted}},
                    eysql_config:normalize(#{ssl => true, ssl_opts => #{password => <<"secret">>}}))},
     {"invalid epgsql_opts, not echoed",
      ?_assertEqual({error, {invalid_option, epgsql_opts, redacted}},
                    eysql_config:normalize(#{epgsql_opts => [{password, <<"secret">>}]}))}
    ].

errors_test_() ->
    [?_assertEqual({error, {unknown_options, [pool_szie]}},
                   eysql_config:normalize(#{pool_szie => 5})),
     ?_assertEqual({error, {invalid_option, pool_size, 0}},
                   eysql_config:normalize(#{pool_size => 0})),
     ?_assertEqual({error, {invalid_option, load_balance, sometimes}},
                   eysql_config:normalize(#{load_balance => sometimes})),
     ?_assertEqual({error, {invalid_option, hosts, []}},
                   eysql_config:normalize(#{hosts => []})),
     ?_assertMatch({error, {invalid_topology_key, _}},
                   eysql_config:normalize(#{topology_keys => "nope"})),
     ?_assertMatch({error, {invalid_options, _}}, eysql_config:normalize([]))
    ].

%% Off unless set, as pgjdbc's socketTimeout is 0 by default; milliseconds
%% otherwise, like eysql's other durations. A bound of 0 would fail every
%% call, so off is `infinity'.
socket_timeout_test_() ->
    Bound = fun(Options) ->
                    {ok, #{socket_timeout := B}} = eysql_config:normalize(Options),
                    B
            end,
    [{"off by default", ?_assertEqual(infinity, Bound(#{}))},
     {"milliseconds", ?_assertEqual(500, Bound(#{socket_timeout => 500}))},
     {"infinity", ?_assertEqual(infinity, Bound(#{socket_timeout => infinity}))},
     [?_assertEqual({error, {invalid_option, socket_timeout, Value}},
                    eysql_config:normalize(#{socket_timeout => Value}))
      || Value <- [0, -1, 1.5, "500", undefined]]
    ].
