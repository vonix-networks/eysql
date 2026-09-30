%% A driver whose connections are epgsql connection processes that never
%% reach a server, so eysql_conn:transaction_status/1 reads a real epgsql
%% state. A connection opens with the status a handshake leaves, idle; tests
%% change it with epgsql's own setters. Every statement succeeds.
-module(eysql_sock_driver).

-behaviour(eysql_driver).

-include_lib("epgsql/include/epgsql.hrl").

-export([open/3, close/1, squery/2, discover/1, is_primary/1, committed/1,
         transaction_status/1]).

-export([set_txstatus/2, set_sync_required/1, set_copy_mode/1, set_codec/2]).

%% The byte a ReadyForQuery carries: $I, $T or $E.
set_txstatus(Conn, Byte) ->
    change(Conn, fun(State) -> epgsql_sock:set_attr(txstatus, Byte, State) end).

%% As after an extended-query error, until epgsql:sync/1.
set_sync_required(Conn) ->
    change(Conn, fun(State) -> epgsql_sock:set_attr(sync_required, true, State) end).

%% As during COPY FROM STDIN.
set_copy_mode(Conn) ->
    change(Conn, fun(State) -> epgsql_sock:set_packet_handler(on_copy_from_stdin, State) end).

%% In place of the type codecs a connected epgsql process holds, most of its
%% state: about 9 KB against PostgreSQL 17.
set_codec(Conn, Codec) ->
    change(Conn, fun(State) -> epgsql_sock:set_attr(codec, Codec, State) end).

change(Conn, Fun) ->
    _ = sys:replace_state(Conn, Fun),
    ok.

open(_Host, _Port, _Settings) ->
    {ok, Conn} = epgsql_sock:start_link(),
    ok = set_txstatus(Conn, $I),
    {ok, Conn}.

close(Conn) ->
    try gen_server:stop(Conn)
    catch exit:_ -> ok
    end,
    ok.

squery(Conn, _Sql) ->
    case is_process_alive(Conn) of
        true -> {ok, [], [{<<"1">>}]};
        false -> {error, {connection_lost, noproc}}
    end.

discover(_Conn) ->
    {error, #error{severity = error, code = <<"42883">>, codename = undefined,
                   message = <<"no yb_servers()">>, extra = []}}.

is_primary(_Conn) -> {ok, true}.

committed(_Conn) -> true.

transaction_status(Conn) -> eysql_conn:transaction_status(Conn).
