-module(eysql_util_tests).

-include_lib("eunit/include/eunit.hrl").

counters_test_() ->
    [{"incr starts a count at 1", ?_assertEqual(#{a => 1}, eysql_util:incr(a, #{}))},
     {"incr adds one", ?_assertEqual(#{a => 3, b => 1}, eysql_util:incr(a, #{a => 2, b => 1}))},
     {"decr takes one", ?_assertEqual(#{a => 1}, eysql_util:decr(a, #{a => 2}))},
     {"decr removes a count that reaches 0", ?_assertEqual(#{b => 1}, eysql_util:decr(a, #{a => 1, b => 1}))},
     {"decr leaves a missing key missing", ?_assertEqual(#{b => 1}, eysql_util:decr(a, #{b => 1}))}
    ].

now_ms_test() ->
    Before = erlang:monotonic_time(millisecond),
    Now = eysql_util:now_ms(),
    ?assert(Now >= Before),
    ?assert(Now =< erlang:monotonic_time(millisecond)).

redact_config_test_() ->
    Settings = #{username => <<"app">>,
                 password => <<"s3cret">>,
                 database => <<"db">>,
                 ssl_opts => [{password, "k3y"}],
                 epgsql_opts => #{codecs => []}},
    Config = #{seeds => [#{host => <<"a">>, port => 5433}], settings => Settings},
    #{settings := Redacted} = eysql_util:redact_config(Config),
    Empty = #{settings => #{password => fun() -> <<"s3cret">> end, ssl_opts => [], epgsql_opts => #{}}},
    [{"the password goes", ?_assertEqual(redacted, maps:get(password, Redacted))},
     {"ssl_opts and epgsql_opts go whole",
      ?_assertEqual({redacted, redacted}, {maps:get(ssl_opts, Redacted), maps:get(epgsql_opts, Redacted)})},
     {"other settings stay",
      ?_assertEqual({<<"app">>, <<"db">>}, {maps:get(username, Redacted), maps:get(database, Redacted)})},
     {"the rest of the config stays",
      ?_assertEqual(maps:get(seeds, Config), maps:get(seeds, eysql_util:redact_config(Config)))},
     {"a password fun goes too; empty options stay",
      ?_assertEqual(#{settings => #{password => redacted, ssl_opts => [], epgsql_opts => #{}}},
                    eysql_util:redact_config(Empty))},
     {"a map without settings is left alone", ?_assertEqual(#{a => 1}, eysql_util:redact_config(#{a => 1}))},
     {"an after_connect {Module, Function, Args} loses its Args",
      ?_assertMatch(#{after_connect := {app_db, warm, redacted}},
                    eysql_util:redact_config(Config#{after_connect => {app_db, warm, [<<"s3cret">>]}}))},
     {"an after_connect fun stays, and undefined too",
      [?_assertEqual(Hook, maps:get(after_connect, eysql_util:redact_config(Config#{after_connect => Hook})))
       || Hook <- [fun erlang:is_process_alive/1, undefined]]}
    ].

%% A config, or a connect spec, anywhere in a term: in a record, a stack
%% trace's arguments, a map's values or an improper list.
redact_test_() ->
    Settings = #{password => <<"s3cret">>, ssl_opts => [{password, "k3y"}], epgsql_opts => #{}},
    Config = #{seeds => [], settings => Settings},
    Spec = #{driver => eysql_conn, settings => Settings, target_session_attrs => any},
    Term = {state, Config, [{eysql_pool, fill, [Config], []} | Spec], #{spec => Spec}},
    Text = lists:flatten(io_lib:format("~p", [eysql_util:redact(Term)])),
    Plain = {state, [1, 2 | 3], #{a => <<"x">>}, "text"},
    [{"no secret is left", ?_assertEqual({nomatch, nomatch}, {string:find(Text, "s3cret"), string:find(Text, "k3y")})},
     {"the shape stays",
      ?_assertMatch({state, #{settings := #{password := redacted}},
                     [{eysql_pool, fill, [#{settings := #{ssl_opts := redacted}}], []} | #{driver := eysql_conn}],
                     #{spec := #{settings := #{password := redacted}}}},
                    eysql_util:redact(Term))},
     {"a term with no config is unchanged", ?_assertEqual(Plain, eysql_util:redact(Plain))}
    ].

%% The one rule for text: Unicode, as a string, a UTF-8 binary or a mix.
text_test_() ->
    Ok = fun(Binary) -> {ok, Binary} end,
    [{"a string as UTF-8", ?_assertEqual(Ok(<<"zōne"/utf8>>), eysql_util:text("zōne"))},
     {"a UTF-8 binary as it is", ?_assertEqual(Ok(<<"zōne"/utf8>>), eysql_util:text(<<"zōne"/utf8>>))},
     {"a string and binaries mixed", ?_assertEqual(Ok(<<"abc">>), eysql_util:text(["a", [<<"b">>], $c]))},
     {"a binary tail", ?_assertEqual(Ok(<<"ab">>), eysql_util:text([$a | <<"b">>]))},
     {"empty", ?_assertEqual({Ok(<<>>), Ok(<<>>)}, {eysql_util:text(""), eysql_util:text(<<>>)})},
     {"Latin-1 bytes above 127", ?_assertEqual(error, eysql_util:text(<<"caf", 233>>))},
     {"truncated UTF-8", ?_assertEqual(error, eysql_util:text(<<"a", 195>>))},
     {"a binary that is not UTF-8 in a list", ?_assertEqual(error, eysql_util:text(["a", <<233>>]))},
     {"a surrogate", ?_assertEqual(error, eysql_util:text([16#D800]))},
     {"a code point beyond Unicode", ?_assertEqual(error, eysql_util:text([16#110000]))},
     {"a negative integer", ?_assertEqual(error, eysql_util:text([-1]))},
     {"an improper list", ?_assertEqual(error, eysql_util:text([$a | $b]))},
     {"a list with a float", ?_assertEqual(error, eysql_util:text([$a, 1.0]))},
     {"a bitstring", ?_assertEqual({error, error}, {eysql_util:text(<<1:1>>), eysql_util:text([<<1:1>>])})},
     {"an atom", ?_assertEqual(error, eysql_util:text(app))},
     {"a number", ?_assertEqual(error, eysql_util:text(42))}
    ].
