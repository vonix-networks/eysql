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

%% A driver with no database behind it. Connections are processes; what
%% discovery returns and how long it takes, which hosts refuse connections,
%% are unreachable, never answer a connect, answer it with an error such as
%% a failed login, crash it or are standbys, and what each statement
%% answers are set from the test. A connection also answers the calls
%% epgsql makes on it, as eysql_conn:equery/3 does, with one reply the test
%% sets. It counts discoveries and connects per host.
-module(eysql_fake_driver).

-behaviour(eysql_driver).

-include_lib("epgsql/include/epgsql.hrl").

-export([open/3, close/1, squery/2, discover/1, is_primary/1, committed/1,
         transaction_status/1]).

-export([start/0,
         stop/0,
         set_servers/1,
         set_discover_error/1,
         set_discover_delay/1,
         down/1,
         up/1,
         reject/2,
         unreachable/1,
         open_hangs/1,
         hung/1,
         opens/1,
         crash_open/1,
         set_standby/1,
         set_primary/1,
         script/2,
         set_epgsql_reply/1,
         set_committed/1,
         set_transaction_status/1,
         conns/0,
         conns_to/1,
         kill_conn/1,
         discoveries/0,
         server/4,
         server/5,
         server/6,
         pg_error/1
        ]).

-define(TABLE, eysql_fake).

%%%=============================================================================
%%% Control
%%%=============================================================================

start() ->
    Parent = self(),
    Owner = spawn(fun() ->
                          ?TABLE = ets:new(?TABLE, [named_table, public, set]),
                          Parent ! {table_ready, self()},
                          receive stop -> ok end
                  end),
    receive {table_ready, Owner} -> ok end,
    ets:insert(?TABLE, {owner, Owner}),
    ok.

stop() ->
    lists:foreach(fun(Pid) -> exit(Pid, kill) end, conns() ++ hung()),
    [{owner, Owner}] = ets:lookup(?TABLE, owner),
    Ref = erlang:monitor(process, Owner),
    Owner ! stop,
    receive {'DOWN', Ref, process, Owner, _} -> ok end.

set_servers(Servers) -> ets:insert(?TABLE, {discover, {ok, Servers}}).
set_discover_error(Error) -> ets:insert(?TABLE, {discover, {error, Error}}).
%% discover/1 answers after this many ms.
set_discover_delay(Ms) -> ets:insert(?TABLE, {discover_delay, Ms}).
down(Key) -> ets:insert(?TABLE, {{down, Key}, true}).
%% Connects to this host succeed again, from now on: one under way goes on
%% as it began.
up(Key) ->
    [ets:delete(?TABLE, {How, Key}) || How <- [down, unreachable, hang, reject]],
    ok.
%% The host answers, and open/3 fails with `Reason', as a failed login
%% (`invalid_password') or a server error (pg_error/1) would.
reject(Key, Reason) -> ets:insert(?TABLE, {{reject, Key}, Reason}).
%% An address that drops packets: open/3 waits out `connect_timeout', as
%% epgsql would, then fails with `timeout'.
unreachable(Key) -> ets:insert(?TABLE, {{unreachable, Key}, true}).
%% open/3 never answers for this host, as a driver that ignores its timeout
%% would: it gives up after a minute, by when its caller has been killed.
open_hangs(Key) -> ets:insert(?TABLE, {{hang, Key}, true}).

%% The processes hanging in open/3 for this host.
hung(Key) ->
    [Pid || {{hung, Pid}, K} <- ets:tab2list(?TABLE), K =:= Key, is_process_alive(Pid)].

hung() ->
    [Pid || {{hung, Pid}, _Key} <- ets:tab2list(?TABLE), is_process_alive(Pid)].

%% How many times open/3 has answered for this host, or started to wait.
opens(Key) ->
    case ets:lookup(?TABLE, {opens, Key}) of
        [{_, N}] -> N;
        [] -> 0
    end.

%% open/3 raises for this host, as a driver bug would.
crash_open(Key) -> ets:insert(?TABLE, {{crash, Key}, true}).
set_standby(Key) -> ets:insert(?TABLE, {{standby, Key}, true}).
%% A standby is promoted.
set_primary(Key) -> ets:delete(?TABLE, {standby, Key}).
set_committed(Bool) -> ets:insert(?TABLE, {committed, Bool}).
%% What every connection reports from transaction_status/1; idle by default.
set_transaction_status(Status) -> ets:insert(?TABLE, {transaction_status, Status}).

%% Answers for a statement, used in order; the last one repeats.
script(Sql, Answers) -> ets:insert(?TABLE, {{script, iolist_to_binary(Sql)}, Answers}).

%% What every connection answers to a call epgsql makes on it, such as the
%% parse that epgsql:equery/3 starts with. `no_reply' answers nothing, as a
%% server that has died without closing its sockets.
set_epgsql_reply(Reply) -> ets:insert(?TABLE, {epgsql_reply, Reply}).

conns() ->
    [Pid || {{conn, Pid}, _Key} <- ets:tab2list(?TABLE), is_process_alive(Pid)].

conns_to(Key) ->
    [Pid || {{conn, Pid}, K} <- ets:tab2list(?TABLE), K =:= Key, is_process_alive(Pid)].

kill_conn(Pid) -> exit(Pid, kill).

%% How many times discover/1 has run.
discoveries() ->
    case ets:lookup(?TABLE, discoveries) of
        [{discoveries, N}] -> N;
        [] -> 0
    end.

server(Host, Cloud, Region, Zone) -> server(Host, Cloud, Region, Zone, primary).

server(Host, Cloud, Region, Zone, Type) ->
    #{host => Host, port => 5433, node_type => Type, cloud => Cloud, region => Region, zone => Zone}.

%% A server with a public IP, as yb_servers() reports one.
server(Host, Cloud, Region, Zone, Type, PublicIp) ->
    maps:put(public_ip, PublicIp, server(Host, Cloud, Region, Zone, Type)).

pg_error(Code) ->
    #error{severity = error, code = Code, codename = undefined, message = <<"fake">>, extra = []}.

%%%=============================================================================
%%% eysql_driver
%%%=============================================================================

open(Host, Port, Settings) ->
    Key = {Host, Port},
    case ets:lookup(?TABLE, {crash, Key}) of
        [_] -> erlang:error({fake_crash, Key});
        [] -> ok
    end,
    case [How || How <- [hang, unreachable, down, reject], ets:member(?TABLE, {How, Key})] of
        [hang | _] ->
            counted(Key),
            ets:insert(?TABLE, {{hung, self()}, Key}),
            timer:sleep(60000),
            {error, timeout};
        [unreachable | _] ->
            counted(Key),
            timer:sleep(maps:get(connect_timeout, Settings)),
            {error, timeout};
        [down | _] ->
            counted(Key),
            {error, econnrefused};
        [reject] ->
            counted(Key),
            [{_, Reason}] = ets:lookup(?TABLE, {reject, Key}),
            {error, Reason};
        [] ->
            Pid = spawn_link(fun conn_loop/0),
            ets:insert(?TABLE, {{conn, Pid}, Key}),
            counted(Key),
            {ok, Pid}
    end.

counted(Key) ->
    _ = ets:update_counter(?TABLE, {opens, Key}, 1, {{opens, Key}, 0}),
    ok.

close(Pid) ->
    Pid ! stop,
    ok.

squery(Pid, Sql) ->
    case is_process_alive(Pid) of
        false -> {error, {connection_lost, noproc}};
        true -> scripted(iolist_to_binary(Sql))
    end.

discover(_Pid) ->
    _ = ets:update_counter(?TABLE, discoveries, 1, {discoveries, 0}),
    Result = case ets:lookup(?TABLE, discover) of
                 [{discover, Found}] -> Found;
                 [] -> {error, pg_error(<<"42883">>)}
             end,
    case ets:lookup(?TABLE, discover_delay) of
        [{discover_delay, Ms}] -> timer:sleep(Ms);
        [] -> ok
    end,
    Result.

is_primary(Pid) ->
    case ets:lookup(?TABLE, {standby, key_of(Pid)}) of
        [_] -> {ok, false};
        [] -> {ok, true}
    end.

committed(_Pid) ->
    case ets:lookup(?TABLE, committed) of
        [{committed, Bool}] -> Bool;
        [] -> true
    end.

%% A connection that is gone reads as `unknown', as through eysql_conn.
transaction_status(Pid) ->
    case {is_process_alive(Pid), ets:lookup(?TABLE, transaction_status)} of
        {false, _} -> unknown;
        {true, [{transaction_status, Status}]} -> Status;
        {true, []} -> idle
    end.

%%%=============================================================================
%%% Internals
%%%=============================================================================

conn_loop() ->
    receive
        stop ->
            ok;
        {'$gen_call', From, {command, epgsql_cmd_sync, _Args}} ->
            %% epgsql syncs after an extended query fails.
            gen_server:reply(From, ok),
            conn_loop();
        {'$gen_call', From, {command, _Command, _Args}} ->
            case epgsql_reply() of
                no_reply -> ok;
                Reply -> gen_server:reply(From, Reply)
            end,
            conn_loop();
        _ ->
            conn_loop()
    end.

epgsql_reply() ->
    case ets:lookup(?TABLE, epgsql_reply) of
        [{epgsql_reply, Reply}] -> Reply;
        [] -> {error, not_scripted}
    end.

key_of(Pid) ->
    case ets:lookup(?TABLE, {conn, Pid}) of
        [{_, Key}] -> Key;
        [] -> undefined
    end.

scripted(Statement) ->
    case ets:lookup(?TABLE, {script, Statement}) of
        [{_, [Answer]}] ->
            Answer;
        [{Key, [Answer | Rest]}] ->
            ets:insert(?TABLE, {Key, Rest}),
            Answer;
        _ ->
            {ok, [], []}
    end.
