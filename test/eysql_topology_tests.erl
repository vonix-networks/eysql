-module(eysql_topology_tests).

-include_lib("eunit/include/eunit.hrl").

s(Host, Zone) -> s(Host, Zone, primary).
s(Host, Zone, Type) ->
    #{host => Host, port => 5433, node_type => Type,
      cloud => <<"gcp">>, region => region(Zone), zone => Zone}.

region(<<"us-west1", _/binary>>) -> <<"us-west1">>;
region(_) -> <<"us-east1">>.

all(_) -> true.
none(_) -> 0.
nothing(_) -> false.

%% Available unless the host is one of `Hosts'.
up_except(Hosts) -> fun(#{host := H}) -> not lists:member(H, Hosts) end.

parse_keys_test_() ->
    [?_assertEqual({ok, [{<<"gcp">>, <<"us-east1">>, <<"us-east1-b">>, 1},
                         {<<"gcp">>, <<"us-east1">>, '*', 2}]},
                   eysql_topology:parse_keys("gcp.us-east1.us-east1-b:1, gcp.us-east1.*:2")),
     ?_assertEqual({ok, [{<<"aws">>, <<"r">>, <<"z">>, 1}]},
                   eysql_topology:parse_keys(<<"aws.r.z">>)),
     ?_assertEqual({ok, []}, eysql_topology:parse_keys("")),
     ?_assertEqual({ok, []}, eysql_topology:parse_keys([])),
     ?_assertMatch({error, {invalid_topology_key, <<"gcp.us-east1">>}},
                   eysql_topology:parse_keys("gcp.us-east1")),
     ?_assertMatch({error, {invalid_topology_key, _}},
                   eysql_topology:parse_keys("gcp.r.z:0")),
     ?_assertMatch({error, {invalid_topology_key, _}},
                   eysql_topology:parse_keys("gcp.r.z:11")),
     ?_assertMatch({error, {invalid_topology_key, _}},
                   eysql_topology:parse_keys("gcp.r.z:x")),
     ?_assertEqual({ok, [{<<"a">>, <<"b">>, <<"c">>, 3}]},
                   eysql_topology:parse_keys([{<<"a">>, <<"b">>, <<"c">>, 3}]))
    ].

%% The JDBC driver's CloudPlacement compares names with equalsIgnoreCase.
%% Keys are case-folded once, as they are parsed, and so are discovered
%% servers, as the cluster stores them; a match is then a plain comparison.
case_test_() ->
    {ok, Upper} = eysql_topology:parse_keys("GCP.US-East1.US-East1-B:1,Gcp.US-EAST1.*:2"),
    {ok, [Tuple]} = eysql_topology:parse_keys([{'GCP', "US-East1", <<"*">>, 1}]),
    {ok, [Unicode]} = eysql_topology:parse_keys("gcp.RÉGION.STRASSE:1"),
    Server = s(<<"b">>, <<"us-east1-b">>),
    [Folded] = eysql_topology:casefold_placement([Server#{cloud := <<"GCP">>, region := <<"US-EAST1">>,
                                                          zone := <<"Us-East1-B">>}]),
    [{"names as text are folded",
      ?_assertEqual([{<<"gcp">>, <<"us-east1">>, <<"us-east1-b">>, 1}, {<<"gcp">>, <<"us-east1">>, '*', 2}],
                    Upper)},
     {"names in tuples are folded", ?_assertEqual({<<"gcp">>, <<"us-east1">>, '*', 1}, Tuple)},
     {"a Unicode fold, not a lowercasing",
      ?_assertEqual({<<"gcp">>, <<"région"/utf8>>, <<"strasse">>, 1}, Unicode)},
     {"a folded key matches a server in another case once the server is folded",
      [?_assert(eysql_topology:matches(hd(Upper), Folded)),
       ?_assert(eysql_topology:matches(Tuple, Folded))]},
     {"a folded server is the same server in lower case",
      ?_assertEqual(Server, Folded)},
     {"sharp s folds as SS does", ?_assertEqual({ok, [Unicode]}, eysql_topology:parse_keys("GCP.région.Straße"))}
    ].

%% Text that is not text is an error, not an exception out of
%% eysql_config:normalize/1. Unicode text is fine.
parse_keys_text_test_() ->
    Invalid = fun(Keys) -> {error, {invalid_topology_key, Keys}} end,
    Unicode = {ok, [{<<"gcp">>, <<"région"/utf8>>, <<"zōne-1"/utf8>>, 2}]},
    [{"an atom", ?_assertEqual(Invalid(nope), eysql_topology:parse_keys(nope))},
     {"a number", ?_assertEqual(Invalid(42), eysql_topology:parse_keys(42))},
     {"a string with characters above 255",
      ?_assertEqual(Unicode, eysql_topology:parse_keys("gcp.région.zōne-1:2"))},
     {"the same as UTF-8", ?_assertEqual(Unicode, eysql_topology:parse_keys(<<"gcp.région.zōne-1:2"/utf8>>))},
     {"strings and binaries mixed",
      ?_assertEqual({ok, [{<<"gcp">>, <<"r">>, <<"z">>, 1}, {<<"aws">>, <<"r">>, <<"z">>, 2}]},
                    eysql_topology:parse_keys(["gcp.r.z", <<",aws.r.z:2">>]))},
     {"invalid UTF-8", ?_assertEqual(Invalid(<<"gcp.r.", 255>>), eysql_topology:parse_keys(<<"gcp.r.", 255>>))},
     {"a code point beyond Unicode", ?_assertEqual(Invalid([16#110000]), eysql_topology:parse_keys([16#110000]))},
     {"a tuple after text",
      ?_assertEqual(Invalid(["gcp.r.z", {a, b, c, 1}]), eysql_topology:parse_keys(["gcp.r.z", {a, b, c, 1}]))},
     {"through the config, an atom",
      ?_assertEqual({error, {invalid_topology_key, nope}}, eysql_config:normalize(#{topology_keys => nope}))},
     {"through the config, a charlist above 255",
      ?_assertMatch({ok, #{topology_keys := [{_, <<"région"/utf8>>, <<"zōne-1"/utf8>>, 2}]}},
                    eysql_config:normalize(#{topology_keys => "gcp.région.zōne-1:2"}))}
    ].

parsed_keys_test_() ->
    Invalid = fun(Key) -> {error, {invalid_topology_key, Key}} end,
    {ok, [Parsed]} = eysql_topology:parse_keys([{"gcp", "us-east1", "us-east1-b", 1}]),
    [{"strings and atoms become binaries",
      ?_assertEqual({ok, [{<<"gcp">>, <<"us-east1">>, <<"us-east1-b">>, 1},
                          {<<"aws">>, <<"us-west-2">>, <<"us-west-2a">>, 2}]},
                    eysql_topology:parse_keys([{"gcp", "us-east1", "us-east1-b", 1},
                                              {aws, 'us-west-2', <<"us-west-2a">>, 2}]))},
     {"zone * in any form is the wildcard",
      ?_assertEqual({ok, [{<<"gcp">>, <<"r">>, '*', 1},
                          {<<"gcp">>, <<"r">>, '*', 2},
                          {<<"gcp">>, <<"r">>, '*', 3}]},
                    eysql_topology:parse_keys([{<<"gcp">>, <<"r">>, '*', 1},
                                              {"gcp", "r", "*", 2},
                                              {gcp, r, <<"*">>, 3}]))},
     {"a key given as strings matches servers",
      ?_assert(eysql_topology:matches(Parsed, s(<<"b">>, <<"us-east1-b">>)))},
     {"preference below 1", ?_assertEqual(Invalid({"a", "b", "c", 0}),
                                          eysql_topology:parse_keys([{"a", "b", "c", 0}]))},
     {"preference above 10", ?_assertEqual(Invalid({"a", "b", "c", 11}),
                                           eysql_topology:parse_keys([{"a", "b", "c", 11}]))},
     {"preference not an integer", ?_assertEqual(Invalid({"a", "b", "c", "1"}),
                                                 eysql_topology:parse_keys([{"a", "b", "c", "1"}]))},
     {"empty name", ?_assertEqual(Invalid({"a", "", "c", 1}),
                                  eysql_topology:parse_keys([{"a", "", "c", 1}]))},
     {"name not text", ?_assertEqual(Invalid({"a", 42, "c", 1}),
                                     eysql_topology:parse_keys([{"a", 42, "c", 1}]))},
     %% Names follow the rule for text that keys given as text follow.
     {"a name that is not UTF-8, refused in both forms",
      ?_assertEqual({Invalid({<<"gcp">>, <<"r", 255>>, <<"z">>, 1}), Invalid(<<"gcp.r", 255, ".z">>)},
                    {eysql_topology:parse_keys([{<<"gcp">>, <<"r", 255>>, <<"z">>, 1}]),
                     eysql_topology:parse_keys(<<"gcp.r", 255, ".z">>)})},
     {"a UTF-8 name", ?_assertEqual({ok, [{<<"gcp">>, <<"région"/utf8>>, <<"z">>, 1}]},
                                    eysql_topology:parse_keys([{gcp, <<"région"/utf8>>, "z", 1}]))},
     {"a name as a string and a binary mixed, as text may be",
      ?_assertEqual({ok, [{<<"gcp">>, <<"r">>, <<"z">>, 1}]},
                    eysql_topology:parse_keys([{["g", <<"cp">>], "r", "z", 1}]))},
     {"a name with a surrogate", ?_assertEqual(Invalid({"a", [16#D800], "c", 1}),
                                               eysql_topology:parse_keys([{"a", [16#D800], "c", 1}]))},
     {"wrong shape", ?_assertEqual(Invalid({"a", "b", "c"}),
                                   eysql_topology:parse_keys([{"a", "b", "c"}]))},
     {"an invalid key after a valid one",
      ?_assertEqual(Invalid(nope), eysql_topology:parse_keys([{"a", "b", "c", 1}, nope]))}
    ].

matches_test_() ->
    B = s(<<"b">>, <<"us-east1-b">>),
    [?_assert(eysql_topology:matches({<<"gcp">>, <<"us-east1">>, <<"us-east1-b">>, 1}, B)),
     ?_assert(eysql_topology:matches({<<"gcp">>, <<"us-east1">>, '*', 1}, B)),
     ?_assertNot(eysql_topology:matches({<<"gcp">>, <<"us-east1">>, <<"us-east1-c">>, 1}, B)),
     ?_assertNot(eysql_topology:matches({<<"aws">>, <<"us-east1">>, '*', 1}, B))
    ].

load_balance_modes_test_() ->
    P = s(<<"p">>, <<"us-east1-b">>),
    R = s(<<"r">>, <<"us-east1-b">>, read_replica),
    Both = [P, R],
    C = fun(LB, Servers) -> eysql_topology:candidates(Servers, fun all/1, LB, [], false) end,
    [?_assertEqual(Both, C(true, Both)),
     ?_assertEqual(Both, C(any, Both)),
     ?_assertEqual([P], C(only_primary, Both)),
     ?_assertEqual([R], C(only_rr, Both)),
     ?_assertEqual([P], C(prefer_primary, Both)),
     ?_assertEqual([R], C(prefer_primary, [R])),
     ?_assertEqual([R], C(prefer_rr, Both)),
     ?_assertEqual([P], C(prefer_rr, [P])),
     ?_assertEqual([], C(only_rr, [P]))
    ].

topology_levels_test_() ->
    B = s(<<"b">>, <<"us-east1-b">>),
    C = s(<<"c">>, <<"us-east1-c">>),
    W = s(<<"w">>, <<"us-west1-a">>),
    Servers = [B, C, W],
    {ok, Keys} = eysql_topology:parse_keys("gcp.us-east1.us-east1-b:1,gcp.us-east1.*:2"),
    Without = fun(Host) -> fun(#{host := H}) -> H =/= Host end end,
    Cand = fun(Available, Fallback) ->
                   eysql_topology:candidates(Servers, Available, any, Keys, Fallback)
           end,
    [{"the preferred zone wins", ?_assertEqual([B], Cand(fun all/1, false))},
     {"then the rest of the region",
      ?_assertEqual([C], Cand(Without(<<"b">>), false))},
     {"then anything, without fallback_only",
      ?_assertEqual([W], Cand(fun(#{host := H}) -> H =:= <<"w">> end, false))},
     {"and nothing with fallback_only",
      ?_assertEqual([], Cand(fun(#{host := H}) -> H =:= <<"w">> end, true))}
    ].

%% A server is allowed while it is in the level new connections use now or
%% in the level they would use with nothing backing off.
allowed_test_() ->
    B1 = s(<<"b1">>, <<"us-east1-b">>),
    B2 = s(<<"b2">>, <<"us-east1-b">>),
    C = s(<<"c">>, <<"us-east1-c">>),
    W = s(<<"w">>, <<"us-west1-a">>),
    Servers = [B1, B2, C, W],
    {ok, Keys} = eysql_topology:parse_keys("gcp.us-east1.us-east1-b:1,gcp.us-east1.us-east1-c:2"),
    Allowed = fun(Available, Fallback) ->
                      eysql_topology:allowed(Servers, Available, any, Keys, Fallback)
              end,
    [{"a host backing off in the chosen level stays allowed",
      ?_assertEqual([B1, B2], Allowed(up_except([<<"b1">>]), false))},
     {"while the preferred level backs off, it and the next are allowed",
      ?_assertEqual([B1, B2, C], Allowed(up_except([<<"b1">>, <<"b2">>]), false))},
     {"once the preferred level is back, only it is",
      ?_assertEqual([B1, B2], Allowed(fun all/1, false))},
     {"a server no longer listed is not, while the others back off or not",
      [?_assertEqual([B1, B2], eysql_topology:allowed([B1, B2], fun nothing/1, any, Keys, false)),
       ?_assertEqual([B1, B2], eysql_topology:allowed([B1, B2], fun all/1, any, Keys, false))]},
     {"when no level has an available server, every server is",
      ?_assertEqual(Servers, Allowed(up_except([<<"b1">>, <<"b2">>, <<"c">>]), false))},
     {"with fallback_only, the keys' servers, backing off as they are",
      ?_assertEqual([B1, B2], Allowed(up_except([<<"b1">>, <<"b2">>, <<"c">>]), true))},
     {"nothing available: back-off is ignored, as when picking",
      ?_assertEqual([B1, B2], Allowed(fun nothing/1, false))},
     {"without keys, every server, backing off or not",
      ?_assertEqual(Servers, eysql_topology:allowed(Servers, up_except([<<"b1">>]), any, [], false))},
     {"fallback_only with no server in the keys allows nothing",
      ?_assertEqual([], eysql_topology:allowed([W], fun all/1, any, Keys, true))}
    ].

allowed_types_test_() ->
    P1 = s(<<"p1">>, <<"us-east1-b">>),
    P2 = s(<<"p2">>, <<"us-east1-b">>),
    R = s(<<"r">>, <<"us-east1-b">>, read_replica),
    Servers = [P1, P2, R],
    Allowed = fun(LoadBalance, Available) ->
                      eysql_topology:allowed(Servers, Available, LoadBalance, [], false)
              end,
    [{"prefer_primary keeps a failing primary while another is up",
      ?_assertEqual([P1, P2], Allowed(prefer_primary, up_except([<<"p1">>])))},
     {"prefer_primary keeps its primaries and adds replicas while no primary is up",
      ?_assertEqual([P1, P2, R], Allowed(prefer_primary, up_except([<<"p1">>, <<"p2">>])))},
     {"and drops the replicas once a primary is up",
      ?_assertEqual([P1, P2], Allowed(prefer_primary, up_except([<<"p1">>])))},
     {"prefer_rr keeps its replica and adds primaries while the replica backs off",
      ?_assertEqual([P1, P2, R], Allowed(prefer_rr, up_except([<<"r">>])))},
     {"and drops the primaries once it is back",
      ?_assertEqual([R], Allowed(prefer_rr, fun all/1))},
     {"only_rr keeps its only replica while it backs off",
      ?_assertEqual([R], Allowed(only_rr, up_except([<<"r">>])))}
    ].

%% One server per zone and the local zone preferred, as in the finding: one
%% slow health check must not move the local server's connections away, nor,
%% while it lasts, the ones that went elsewhere meanwhile.
allowed_one_per_zone_test_() ->
    A = s(<<"a">>, <<"us-east1-b">>),
    B = s(<<"b">>, <<"us-east1-c">>),
    C = s(<<"c">>, <<"us-east1-d">>),
    Servers = [A, B, C],
    {ok, Keys} = eysql_topology:parse_keys("gcp.us-east1.us-east1-b:1,gcp.us-east1.us-east1-c:2,"
                                           "gcp.us-east1.us-east1-d:2"),
    Allowed = fun(Available) -> eysql_topology:allowed(Servers, Available, any, Keys, false) end,
    Candidates = fun(Available) -> eysql_topology:candidates(Servers, Available, any, Keys, false) end,
    [{"new connections go to the fallback while the local server backs off",
      ?_assertEqual([B, C], Candidates(up_except([<<"a">>])))},
     {"while every server is kept",
      ?_assertEqual([A, B, C], Allowed(up_except([<<"a">>])))},
     {"a blip that backs every server off keeps the local one",
      ?_assertEqual([A], Allowed(fun nothing/1))},
     {"once the local server is back, the fallback is not kept",
      ?_assertEqual([A], Allowed(fun all/1))}
    ].

%% The smart drivers' order for prefer-primary and prefer-rr: the preferred
%% type by topology level, then that type anywhere, then the other type
%% anywhere. fallback_to_topology_keys_only does not apply.
prefer_test_() ->
    PB = s(<<"pb">>, <<"us-east1-b">>),
    PC = s(<<"pc">>, <<"us-east1-c">>),
    RB = s(<<"rb">>, <<"us-east1-b">>, read_replica),
    RW = s(<<"rw">>, <<"us-west1-a">>, read_replica),
    Servers = [PB, PC, RB, RW],
    {ok, Keys} = eysql_topology:parse_keys("gcp.us-east1.us-east1-b:1"),
    Cand = fun(LoadBalance, Available, Fallback) ->
                   eysql_topology:candidates(Servers, Available, LoadBalance, Keys, Fallback)
           end,
    [{"prefer_primary takes the preferred zone first",
      ?_assertEqual([PB], Cand(prefer_primary, fun all/1, true))},
     {"prefer_primary ignores fallback_only: a primary outside the keys",
      ?_assertEqual([PC], Cand(prefer_primary, up_except([<<"pb">>]), true))},
     {"only_primary keeps fallback_only",
      ?_assertEqual([], Cand(only_primary, up_except([<<"pb">>]), true))},
     {"prefer_primary with no primary up: replicas anywhere, whatever the keys",
      ?_assertEqual([RB, RW], Cand(prefer_primary, up_except([<<"pb">>, <<"pc">>]), false))},
     {"and the same with fallback_only",
      ?_assertEqual([RB, RW], Cand(prefer_primary, up_except([<<"pb">>, <<"pc">>]), true))},
     {"prefer_rr takes the preferred zone first",
      ?_assertEqual([RB], Cand(prefer_rr, fun all/1, true))},
     {"prefer_rr ignores fallback_only: a replica outside the keys",
      ?_assertEqual([RW], Cand(prefer_rr, up_except([<<"rb">>]), true))},
     {"only_rr keeps fallback_only",
      ?_assertEqual([], Cand(only_rr, up_except([<<"rb">>]), true))},
     {"prefer_rr with no replica up: primaries anywhere, whatever the keys",
      ?_assertEqual([PB, PC], Cand(prefer_rr, up_except([<<"rb">>, <<"rw">>]), true))},
     {"allowed follows: primaries anywhere while the preferred zone's backs off",
      ?_assertEqual([PB, PC], eysql_topology:allowed(Servers, up_except([<<"pb">>]), prefer_primary,
                                                    Keys, true))}
    ].

%% Which of a server's addresses to use, from the addresses the names
%% resolved to, as the JDBC driver's LoadBalanceService.refresh decides it:
%% the first server, in yb_servers() order, whose host or public IP is the
%% address that answered, or whose two are one address, decides for good.
%% Until one does, public IPs are a guess, taken only when every server has
%% one that resolves. The addresses here are atoms: they are only compared.
address_test_() ->
    Col = fun eysql_topology:address_column/3,
    A = (s(<<"10.0.0.1">>, <<"us-east1-b">>))#{public_ip => <<"34.0.0.1">>},
    B = (s(<<"10.0.0.2">>, <<"us-east1-c">>))#{public_ip => <<>>},
    C = s(<<"10.0.0.3">>, <<"us-east1-d">>),
    [{"answered at a server's host",
      ?_assertEqual({host, host, decided}, Col(undecided, h2, [{{ok, h1}, {ok, p1}}, {{ok, h2}, {ok, p2}}]))},
     {"answered at a server's public IP",
      ?_assertEqual({public_ip, public_ip, decided},
                    Col(undecided, p2, [{{ok, h1}, {ok, p1}}, {{ok, h2}, {ok, p2}}]))},
     {"a server whose host and public IP are one address means host",
      ?_assertEqual({host, host, decided}, Col(undecided, lb, [{{ok, h1}, {ok, h1}}]))},
     {"the first server that settles it decides, in yb_servers() order",
      [?_assertEqual({host, host, decided}, Col(undecided, p2, [{{ok, h1}, {ok, h1}}, {{ok, h2}, {ok, p2}}])),
       ?_assertEqual({public_ip, public_ip, decided},
                     Col(undecided, p2, [{{ok, h2}, {ok, p2}}, {{ok, h1}, {ok, h1}}]))]},
     {"a decision stands, whatever answers",
      [?_assertEqual({public_ip, public_ip, decided}, Col(public_ip, h1, [{{ok, h1}, {ok, p1}}])),
       ?_assertEqual({host, host, decided}, Col(host, p1, [{{ok, h1}, {ok, p1}}])),
       ?_assertEqual({host, host, decided}, Col(host, lb, []))]},
     {"names that do not resolve match nothing, not even each other",
      [?_assertEqual({undecided, host, unknown}, Col(undecided, lb, [{error, none}, {{ok, h2}, none}])),
       ?_assertEqual({undecided, host, unresolved_public}, Col(undecided, lb, [{error, error}]))]},
     {"undecided, every server with a public IP that resolves: public IPs, a guess",
      ?_assertEqual({undecided, public_ip, all_public},
                    Col(undecided, lb, [{{ok, h1}, {ok, p1}}, {error, {ok, p2}}]))},
     {"undecided, a server without one: hosts",
      ?_assertEqual({undecided, host, unknown}, Col(undecided, lb, [{{ok, h1}, {ok, p1}}, {{ok, h2}, none}]))},
     {"undecided, a public IP that does not resolve: hosts",
      ?_assertEqual({undecided, host, unresolved_public},
                    Col(undecided, lb, [{{ok, h1}, {ok, p1}}, {{ok, h2}, error}]))},
     {"public IPs replace hosts where servers have one",
      ?_assertEqual([A#{host := <<"34.0.0.1">>}, B, C], eysql_topology:use_public_ip([A, B, C]))}
    ].

%% The least loaded wins, and a tie goes to one of the tied at random, as
%% the JDBC driver's getLeastLoadedServer draws from its
%% minConnectionsHostList. Over many picks every tied server comes up, and
%% never a busier one, whatever the order of the candidates.
choose_test_() ->
    A = s(<<"a">>, <<"z">>),
    B = s(<<"b">>, <<"z">>),
    C = s(<<"c">>, <<"z">>),
    D = s(<<"d">>, <<"z">>),
    Loads = #{<<"a">> => 2, <<"b">> => 1, <<"c">> => 1, <<"d">> => 1},
    Load = fun(#{host := H}) -> maps:get(H, Loads) end,
    Chosen = fun(Candidates) ->
                     lists:usort([begin {ok, S} = eysql_topology:choose(Candidates, Load), S end
                                  || _ <- lists:seq(1, 300)])
             end,
    [{"least loaded", ?_assertEqual({ok, B}, eysql_topology:choose([A, B], Load))},
     {"a tie goes to each of the tied, and to no other", ?_assertEqual([B, C, D], Chosen([A, B, C, D]))},
     {"whatever their order", ?_assertEqual([B, C, D], Chosen([D, A, C, B]))},
     ?_assertEqual({error, no_server_available}, eysql_topology:choose([], fun none/1))
    ].

backoff_test_() ->
    [{"fixed without a cap", ?_assertEqual(5000, eysql_topology:backoff(1, 5000, undefined))},
     {"fixed however often it fails", ?_assertEqual(5000, eysql_topology:backoff(7, 5000, undefined))},
     ?_assertEqual(5000, eysql_topology:backoff(1, 5000, 60000)),
     ?_assertEqual(10000, eysql_topology:backoff(2, 5000, 60000)),
     ?_assertEqual(40000, eysql_topology:backoff(4, 5000, 60000)),
     ?_assertEqual(60000, eysql_topology:backoff(5, 5000, 60000)),
     ?_assertEqual(60000, eysql_topology:backoff(50, 5000, 60000)),
     {"0 stays 0: due at the next refresh", ?_assertEqual(0, eysql_topology:backoff(3, 0, undefined))},
     %% A host that has failed some million times would have `bsl' build an
     %% integer too big to hold, and crash the cluster with system_limit.
     {"a host that has failed for weeks", ?_assertEqual(60000, eysql_topology:backoff(5000000, 5000, 60000))},
     {"and for years, quickly",
      ?_assertEqual(3600000, eysql_topology:backoff(1 bsl 40, 1000, 3600000))}
    ].
