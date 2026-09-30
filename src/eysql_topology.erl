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

%% @doc Host selection, with no processes and no I/O.
%%
%% This is the part of a YugabyteDB smart driver that decides where the next
%% connection goes: filter servers by node type and by topology preference,
%% then take the least-loaded one. It also parses topology keys, computes
%% the back-off for hosts that failed, and decides whether servers are
%% reached at their host or their public IP.
%%
%% Placement names match without regard to case, as the JDBC driver's
%% CloudPlacement compares them with equalsIgnoreCase. They are case-folded
%% once, when keys are parsed and when servers are discovered
%% ({@link casefold_placement/1}), so that a comparison is a plain match.
-module(eysql_topology).

-export([parse_keys/1,
         matches/2,
         candidates/5,
         allowed/5,
         choose/2,
         backoff/3,
         key/1,
         seed/2,
         casefold_placement/1,
         address_column/3,
         use_public_ip/1
        ]).

-export_type([server/0,
              key/0,
              topology_key/0,
              load_balance/0,
              node_type/0,
              column/0,
              decision/0,
              address/0,
              resolved/0,
              address_basis/0
             ]).

-type node_type() :: primary | read_replica.

%% A second doubled 32 times is over a century, past any cap; stopping there
%% keeps the delay a small integer.
-define(BACKOFF_MAX_SHIFT, 32).

%% A server as `yb_servers()' reports it. Seeds, which have not been
%% discovered yet, have empty placement fields. `public_ip' is empty or
%% absent when the server has none.
-type server() :: #{host := binary(),
                    port := inet:port_number(),
                    node_type := node_type(),
                    cloud := binary(),
                    region := binary(),
                    zone := binary(),
                    public_ip => binary()
                   }.

-type key() :: {Host :: binary(), Port :: inet:port_number()}.

%% Which of a discovered server's addresses connections go to.
-type column() :: host | public_ip.

%% The column, once decided for good, or `undecided'.
-type decision() :: column() | undecided.

%% What a name resolves to: an IP address, or whatever a test's resolver
%% stands in for one. Only compared.
-type address() :: inet:ip_address() | term().

%% A name's address, or `error' when it did not resolve.
-type resolved() :: {ok, address()} | error.

%% Why address_column/3 chose the column it did.
-type address_basis() :: decided | all_public | unresolved_public | unknown.

%% `{Cloud, Region, Zone, Preference}', the names case-folded. Zone `*'
%% matches every zone of the region. Preference 1 is tried first.
-type topology_key() :: {binary(), binary(), binary() | '*', 1..10}.

-type load_balance() :: false | true | any | only_primary | only_rr
                      | prefer_primary | prefer_rr.

%% The servers new connections come from: a node type, or `any', and a
%% topology preference level, or `all' for the whole cluster, or `none' when
%% no key matches a server and only the keys' servers may be used.
-type level() :: {node_type() | any, 1..10 | all | none}.

%% @doc Parse topology keys in the smart drivers' syntax, for example
%% `"gcp.us-east1.us-east1-b:1,gcp.us-east1.*:2"'. A key without `:N' has
%% preference 1.
%%
%% Keys can also be given parsed, as `{Cloud, Region, Zone, Preference}'
%% tuples. The names may be atoms or non-empty text; zone `*', in any of
%% those forms, matches every zone; the preference is 1 to 10.
%%
%% Text is Unicode, by the rule every text option follows: a string, a UTF-8
%% binary, or a mix of both. Anything else, such as an atom or a binary that
%% is not UTF-8, is `{invalid_topology_key, Keys}'.
%%
%% The names come back case-folded with `string:casefold/1', so that
%% `"GCP.US-East1.*"' matches the servers of `gcp.us-east1', as in the JDBC
%% driver.
-spec parse_keys(term()) ->
          {ok, [topology_key()]} | {error, {invalid_topology_key, term()}}.
parse_keys([Key | _] = Keys) when is_tuple(Key) ->
    parse_tuples(Keys, []);
parse_keys(Keys) ->
    case eysql_util:text(Keys) of
        {ok, Text} ->
            Parts = [string:trim(Part) || Part <- binary:split(Text, <<",">>, [global])],
            parse_parts([Part || Part <- Parts, Part =/= <<>>], []);
        error ->
            {error, {invalid_topology_key, Keys}}
    end.

parse_parts([], Acc) ->
    {ok, lists:reverse(Acc)};
parse_parts([Part | Rest], Acc) ->
    {Placement, Preference} =
        case binary:split(Part, <<":">>) of
            [Place] -> {Place, <<"1">>};
            [Place, Pref] -> {Place, Pref}
        end,
    case {binary:split(Placement, <<".">>, [global]), string:to_integer(Preference)} of
        {[Cloud, Region, Zone], {N, <<>>}}
          when N >= 1, N =< 10, Cloud =/= <<>>, Region =/= <<>>, Zone =/= <<>> ->
            parse_parts(Rest, [{fold(Cloud), fold(Region), zone(Zone), N} | Acc]);
        _ ->
            {error, {invalid_topology_key, Part}}
    end.

%% Names that stay charlists would never equal the binaries `yb_servers()'
%% returns, and silently turn topology preference off; convert or refuse.
%% Names are text by the same rule as keys given as text, so that a binary
%% that is not UTF-8, say, is refused in either form.
parse_tuples([], Acc) ->
    {ok, lists:reverse(Acc)};
parse_tuples([{Cloud, Region, Zone, Pref} = Key | Rest], Acc)
  when is_integer(Pref), Pref >= 1, Pref =< 10 ->
    case {name(Cloud), name(Region), name(Zone)} of
        {C, R, Z} when is_binary(C), is_binary(R), is_binary(Z) ->
            parse_tuples(Rest, [{fold(C), fold(R), zone(Z), Pref} | Acc]);
        _ ->
            {error, {invalid_topology_key, Key}}
    end;
parse_tuples([Key | _], _Acc) ->
    {error, {invalid_topology_key, Key}}.

name(Name) when is_atom(Name) ->
    name(atom_to_binary(Name, utf8));
name(Name) ->
    case eysql_util:text(Name) of
        {ok, Binary} when Binary =/= <<>> -> Binary;
        _ -> error
    end.

zone(<<"*">>) -> '*';
zone(Zone) -> fold(Zone).

%% A Unicode case fold, which also takes in what a plain lowercasing would
%% miss, such as `ß' against `SS'. string:casefold/1 returns chardata.
fold(Name) ->
    unicode:characters_to_binary(string:casefold(Name)).

%% @doc The servers with their cloud, region and zone case-folded, as
%% {@link parse_keys/1} folds the keys' names. {@link eysql_cluster} stores
%% discovered servers this way, so {@link matches/2} compares them as they
%% are.
-spec casefold_placement([server()]) -> [server()].
casefold_placement(Servers) ->
    [Server#{cloud := fold(Cloud), region := fold(Region), zone := fold(Zone)}
     || #{cloud := Cloud, region := Region, zone := Zone} = Server <- Servers].

%% @doc Whether a server is in the placement a topology key names. Both are
%% compared as they are: the server's placement must be case-folded, as
%% {@link casefold_placement/1} leaves it, for the match to ignore case.
-spec matches(topology_key(), server()) -> boolean().
matches({Cloud, Region, '*', _}, #{cloud := Cloud, region := Region}) -> true;
matches({Cloud, Region, Zone, _}, #{cloud := Cloud, region := Region, zone := Zone}) -> true;
matches(_, _) -> false.

%% @doc The servers a new connection may go to.
%%
%% `Available' excludes hosts that failed and have not been found back
%% since; {@link eysql_cluster} lets every host through only for its last
%% resort, the seeds. Node type is applied first, then topology: the lowest
%% preference level with any server wins. With no match at any level, all
%% typed servers are candidates, unless `FallbackOnly' is set, in which case
%% there are none.
%%
%% `prefer_primary' and `prefer_rr' ignore `FallbackOnly', as the smart
%% drivers do. They apply the topology levels to the preferred node type, then
%% take that type anywhere in the cluster, and only while no server of that
%% type is available do they take the other type, anywhere in the cluster.
-spec candidates([server()], fun((server()) -> boolean()), load_balance(),
                 [topology_key()], boolean()) -> [server()].
candidates(Servers, Available, LoadBalance, Keys, FallbackOnly) ->
    Up = [Server || Server <- Servers, Available(Server)],
    in_level(level(Up, LoadBalance, Keys, FallbackOnly), Keys, Up).

%% @doc The servers a pool may keep connections on.
%%
%% `Available' is the servers that are up. {@link eysql_cluster} passes
%% every server that has not failed since a connection to it last opened,
%% whatever its back-off, so a failure alone never moves a connection. A
%% server is allowed while it is in either of two levels (node type, then
%% topology preference), each taken whole, failed servers included:
%%
%% <ul>
%% <li>the level new connections to the servers that are up would use, the
%%     one {@link candidates/5} takes its candidates from;</li>
%% <li>the level they would use if no server had failed.</li>
%% </ul>
%%
%% So while a preferred server is down, it keeps its connections and the
%% fallback level keeps those opened there meanwhile. Once it is up again
%% the two levels are the same, and connections on the fallback level are no
%% longer allowed. With no server up, only the second level counts.
-spec allowed([server()], fun((server()) -> boolean()), load_balance(),
              [topology_key()], boolean()) -> [server()].
allowed(Servers, Available, LoadBalance, Keys, FallbackOnly) ->
    Up = [Server || Server <- Servers, Available(Server)],
    Now = level(Up, LoadBalance, Keys, FallbackOnly),
    Healthy = level(Servers, LoadBalance, Keys, FallbackOnly),
    Levels = case in_level(Now, Keys, Up) of
                 [] -> [Healthy];
                 _ -> [Now, Healthy]
             end,
    [Server || Server <- Servers,
               lists:any(fun(Level) -> member(Level, Keys, Server) end, Levels)].

%% The level new connections to `Servers' come from.
-spec level([server()], load_balance(), [topology_key()], boolean()) -> level().
level(Servers, LoadBalance, Keys, _FallbackOnly)
  when LoadBalance =:= prefer_primary; LoadBalance =:= prefer_rr ->
    {Preferred, Other} = preference(LoadBalance),
    case of_type(Preferred, Servers) of
        [] -> {Other, all};
        Typed -> {Preferred, topology_level(Typed, Keys, false)}
    end;
level(Servers, LoadBalance, Keys, FallbackOnly) ->
    Type = type_level(LoadBalance),
    {Type, topology_level(of_type(Type, Servers), Keys, FallbackOnly)}.

type_level(only_primary) -> primary;
type_level(only_rr) -> read_replica;
type_level(_Any) -> any.

preference(prefer_primary) -> {primary, read_replica};
preference(prefer_rr) -> {read_replica, primary}.

of_type(any, Servers) ->
    Servers;
of_type(Type, Servers) ->
    [Server || #{node_type := T} = Server <- Servers, T =:= Type].

topology_level(_Servers, [], _FallbackOnly) ->
    all;
topology_level(Servers, Keys, FallbackOnly) ->
    Matched = [Level || Level <- lists:usort([Pref || {_, _, _, Pref} <- Keys]),
                        lists:any(fun(Server) -> level_matches(Level, Keys, Server) end, Servers)],
    case Matched of
        [Level | _] -> Level;
        [] when FallbackOnly -> none;
        [] -> all
    end.

in_level(Level, Keys, Servers) ->
    [Server || Server <- Servers, member(Level, Keys, Server)].

member({Type, Topology}, Keys, #{node_type := NodeType} = Server) ->
    (Type =:= any orelse Type =:= NodeType) andalso in_topology(Topology, Keys, Server).

in_topology(all, _Keys, _Server) -> true;
in_topology(none, _Keys, _Server) -> false;
in_topology(Level, Keys, Server) -> level_matches(Level, Keys, Server).

level_matches(Level, Keys, Server) ->
    lists:any(fun({_, _, _, Pref} = Key) -> Pref =:= Level andalso matches(Key, Server) end,
              Keys).

%% @doc Take the least-loaded candidate. `Load' counts this client's
%% connections to a server. A tie goes to one of the tied servers at random,
%% as the JDBC driver's getLeastLoadedServer draws one from its
%% minConnectionsHostList.
-spec choose([server()], fun((server()) -> non_neg_integer())) ->
          {ok, server()} | {error, no_server_available}.
choose([], _Load) ->
    {error, no_server_available};
choose(Candidates, Load) ->
    Loads = [{Load(Server), Server} || Server <- Candidates],
    Min = lists:min([L || {L, _} <- Loads]),
    Tied = [Server || {L, Server} <- Loads, L =:= Min],
    {ok, lists:nth(rand:uniform(length(Tied)), Tied)}.

%% @doc Milliseconds a host stays out after `Failures' consecutive failures.
%% With `Max' undefined, `Base' every time, as the smart drivers do; else
%% `Base' doubled per failure, capped at `Max'. A `Base' of 0 stays 0: the
%% host is due at the next refresh, however often it fails.
%%
%% The doubling stops after 32 failures, by when it is past any cap: `bsl'
%% would otherwise build an integer as many bits long as the failure count,
%% and for a host that has failed some million times, raise system_limit.
-spec backoff(pos_integer(), non_neg_integer(), pos_integer() | undefined) -> non_neg_integer().
backoff(_Failures, Base, undefined) ->
    Base;
backoff(Failures, Base, Max) ->
    min(Max, Base bsl min(max(Failures, 1) - 1, ?BACKOFF_MAX_SHIFT)).

-spec key(server()) -> key().
key(#{host := Host, port := Port}) -> {Host, Port}.

%% @doc A server for a configured seed host, before discovery. Its node type
%% and empty placement only fill the fields: {@link eysql_cluster} takes the
%% seeds whole, as the smart drivers make a plain connection to the hosts in
%% their URL, and never filters them by type or zone.
-spec seed(binary(), inet:port_number()) -> server().
seed(Host, Port) ->
    #{host => Host,
      port => Port,
      node_type => primary,
      cloud => <<>>,
      region => <<>>,
      zone => <<>>
     }.

%% @doc Which of the servers' two addresses to connect to, decided as the JDBC
%% driver's LoadBalanceService.refresh decides it, from addresses resolved
%% beforehand: `Answered', the address of the name the discovery dialled
%% (the driver's getConnectedInetAddress), and for each server, in the
%% order `yb_servers()' gave them, its host's address and its public IP's
%% (`none' for a server with no public IP). A server's name that did not
%% resolve is `error', as the driver takes an UnknownHostException for null
%% there.
%%
%% Returns the decision to keep, the column to use now, and why:
%%
%% <ul>
%% <li>With a decision already made, it stands (`decided'): the driver keeps
%%     its useHostColumn for the cluster once it is set, whatever later
%%     refreshes find.</li>
%% <li>Undecided, the first server that settles it decides, for good: one
%%     whose host is the address that answered means `host'; one whose
%%     public IP is means `public_ip', since a client outside the cluster's
%%     network reaches it at a public IP; and one whose host and public IP
%%     are the same address means `host'.</li>
%% <li>With none that settles it, the decision stays open and the driver
%%     guesses: `public_ip' when every server has a public IP and every one
%%     resolves (`all_public'), `host' when every server has one but some
%%     do not resolve (`unresolved_public'), and `host' otherwise
%%     (`unknown').</li>
%% </ul>
-spec address_column(decision(), address(), [{resolved(), resolved() | none}]) ->
          {decision(), column(), address_basis()}.
address_column(undecided, Answered, Servers) ->
    case decide(Answered, Servers) of
        undecided -> guess(Servers);
        Column -> {Column, Column, decided}
    end;
address_column(Column, _Answered, _Servers) ->
    {Column, Column, decided}.

decide(_Answered, []) ->
    undecided;
decide(Answered, [{Host, Public} | Rest]) ->
    if
        Host =:= {ok, Answered} -> host;
        Public =:= {ok, Answered} -> public_ip;
        %% "Both host and public_ip are same"
        Host =/= error, Host =:= Public -> host;
        true -> decide(Answered, Rest)
    end.

guess([]) ->
    {undecided, host, unknown};
guess(Servers) ->
    Public = [P || {_Host, P} <- Servers],
    case {lists:member(none, Public), lists:member(error, Public)} of
        {false, false} -> {undecided, public_ip, all_public};
        {false, true} -> {undecided, host, unresolved_public};
        {true, _} -> {undecided, host, unknown}
    end.

%% @doc The servers with their public IP as the host, where they have one.
-spec use_public_ip([server()]) -> [server()].
use_public_ip(Servers) ->
    [case public_ip(Server) of
         <<>> -> Server;
         Ip -> Server#{host := Ip}
     end
     || Server <- Servers].

public_ip(Server) -> maps:get(public_ip, Server, <<>>).
