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

%% @doc The default driver: epgsql, with the sharp edges filed off.
%%
%% `epgsql:connect/1' links the socket process to the caller before
%% connecting, and that process exits abnormally when the connect fails, so a
%% failed connect can kill the caller. {@link open/3} starts the socket
%% process unlinked and links it only once the connection is up.
%%
%% An epgsql call on a connection whose process has died exits the caller.
%% {@link squery/2} and {@link equery/3} turn that into
%% `{error, {connection_lost, Reason}}'.
%%
%% epgsql waits for the server for as long as it takes, and a server that
%% has died silently or hangs never answers. On a connection checked out of
%% a pool with a `socket_timeout', in the process that checked it out,
%% {@link squery/2} and {@link equery/3} wait that long at most: the pool
%% then kills the connection, and they return
%% `{error, {connection_lost, socket_timeout}}' (see
%% {@link eysql_pool:watch/1}). So code inside {@link eysql:with_connection/3}
%% or {@link eysql:transaction/3} gets bounded queries by calling them in
%% place of `epgsql:squery/2' and `epgsql:equery/3'. Anywhere else they
%% behave as those do, with no bound.
%%
%% epgsql has no call that tells whether a connection is inside a
%% transaction. {@link transaction_status/1} reads it inside the connection
%% process.
-module(eysql_conn).

-behaviour(eysql_driver).

-export([open/3,
         close/1,
         squery/2,
         equery/3,
         discover/1,
         is_primary/1,
         committed/1,
         transaction_status/1
        ]).

-export_type([transaction_status/0]).

-type transaction_status() :: idle | in_transaction | failed | unknown.

-define(YB_SERVERS,
        "SELECT host, port, node_type, cloud, region, zone, public_ip FROM yb_servers()").

%% A connection answers at once when no command is running on it. One that
%% takes longer than this is busy, and so not idle.
-define(STATUS_TIMEOUT, 1000).

%% @doc Connect to one host. The connection is linked to the caller, as with
%% `epgsql:connect/1'.
-spec open(binary(), inet:port_number(), map()) -> {ok, pid()} | {error, term()}.
open(Host, Port, Settings) ->
    #{username := User, password := Password} = Settings,
    Opts = connect_opts(Host, Port, Settings),
    {ok, Conn} = epgsql_sock:start_link(),
    true = unlink(Conn),
    Monitor = erlang:monitor(process, Conn),
    Result = try epgsql:connect(Conn, unicode:characters_to_list(Host), User, Password, Opts)
             catch Class:Reason -> {error, {Class, Reason}}
             end,
    erlang:demonitor(Monitor, [flush]),
    case Result of
        {ok, Conn} ->
            case set_statement_timeout(Conn, maps:get(statement_timeout, Settings, undefined)) of
                ok ->
                    link_caller(Conn);
                {error, _} = Error ->
                    close(Conn),
                    Error
            end;
        {error, _} = Error ->
            exit(Conn, kill),
            Error
    end.

%% The connection can die after its last reply and before the link, for
%% example when the server closes it right after the handshake. link/1 then
%% raises noproc, which would kill the caller; return an error instead.
%%
%% One window remains: if the process starts to exit after link/1 has found
%% it alive, the link is refused and the caller gets an exit signal with
%% reason noproc. A non-trapping caller dies of it, as it would have died of
%% the connection's own exit signal had the connection died just after the
%% link. Closing that window would take trapping exits around the link.
link_caller(Conn) ->
    try link(Conn) of
        true -> {ok, Conn}
    catch
        error:noproc -> {error, closed}
    end.

connect_opts(Host, Port, Settings) ->
    Passed = maps:with([database, ssl, ssl_opts, tcp_opts, application_name], Settings),
    Extra = maps:get(epgsql_opts, Settings, #{}),
    Opts = maps:merge(Extra, Passed),
    with_server_name(Host, Opts#{port => Port,
                                 timeout => maps:get(connect_timeout, Settings, 10000)
                                }).

%% epgsql starts TLS on a TCP connection it has already opened, so OTP knows
%% the server only by the address it is connected to, and checks the
%% certificate against that address. A server dialled by name is checked
%% against the name instead, as libpq's and pgjdbc's `sslmode=verify-full'
%% check the host they connect to: the name goes in as the connection's
%% `server_name_indication', which OTP both sends and checks. A server
%% dialled by IP address is still checked against the address, and a
%% `server_name_indication' already in `ssl_opts', a name or `disable',
%% applies to every server as before.
with_server_name(Host, #{ssl := Ssl} = Opts) when Ssl =/= false ->
    SslOpts = maps:get(ssl_opts, Opts, []),
    Name = unicode:characters_to_list(Host),
    case proplists:is_defined(server_name_indication, SslOpts) orelse is_address(Name) of
        true -> Opts;
        false -> Opts#{ssl_opts => [{server_name_indication, Name} | SslOpts]}
    end;
with_server_name(_Host, Opts) ->
    Opts.

is_address(Name) ->
    case inet:parse_address(Name) of
        {ok, _} -> true;
        {error, _} -> false
    end.

set_statement_timeout(_Conn, undefined) ->
    ok;
set_statement_timeout(Conn, Ms) when is_integer(Ms), Ms >= 0 ->
    case squery(Conn, ["SET statement_timeout = ", integer_to_list(Ms)]) of
        {error, _} = Error -> Error;
        _ -> ok
    end.

-spec close(pid()) -> ok.
close(Conn) ->
    try epgsql:close(Conn)
    catch _:_ -> ok
    end,
    ok.

%% @doc `epgsql:squery/2', returning an error rather than exiting when the
%% connection has died, and bounded by the pool's `socket_timeout' when this
%% process checked `Conn' out of a pool.
-spec squery(pid(), iodata()) -> term().
squery(Conn, Sql) ->
    call(Conn, fun() -> epgsql:squery(Conn, Sql) end).

%% @doc `epgsql:equery/3', as {@link squery/2} is `epgsql:squery/2'.
-spec equery(pid(), iodata(), [term()]) -> term().
equery(Conn, Sql, Params) ->
    call(Conn, fun() -> epgsql:equery(Conn, Sql, Params) end).

%% The clock runs across the whole call, epgsql:equery/3's parse and execute
%% together, not per read from the socket as pgjdbc's socketTimeout does:
%% epgsql reads its socket in its own process, where nothing can time a
%% read.
call(Conn, Call) ->
    Watch = eysql_pool:watch(Conn),
    try guard(Call) of
        Result -> stop_clock(Watch, Conn, Result)
    catch
        Class:Reason:Stack ->
            _ = stop_clock(Watch, Conn, raised),
            erlang:raise(Class, Reason, Stack)
    end.

%% A call that answers after the clock ran out is lost all the same: the
%% pool is killing its connection, and it cannot be reused. What the server
%% did with it is unknown, as with any connection lost under a call.
stop_clock(Watch, Conn, Result) ->
    case eysql_pool:unwatch(Watch, Conn) of
        ok -> Result;
        expired -> {error, {connection_lost, socket_timeout}}
    end.

%% A call on a dead connection process exits the caller. When the socket
%% closes while epgsql handles a statement error, epgsql itself fails a match
%% on `{error, sock_closed}' while syncing. Both mean the connection is gone.
guard(Call) ->
    try Call()
    catch
        exit:Reason ->
            {error, {connection_lost, Reason}};
        error:{badmatch, {error, Reason}} = Exception:Stack ->
            case eysql_error:is_connection_lost(Reason) of
                true -> {error, {connection_lost, Reason}};
                false -> erlang:raise(error, Exception, Stack)
            end
    end.

%% @doc Read `yb_servers()'. On PostgreSQL this fails with SQLSTATE 42883
%% (undefined function), which the cluster takes to mean "not YugabyteDB".
%% `public_ip' is empty for a server that has none.
-spec discover(pid()) -> {ok, [eysql_topology:server()]} | {error, term()}.
discover(Conn) ->
    case equery(Conn, ?YB_SERVERS, []) of
        {ok, _Columns, Rows} -> {ok, [server(Row) || Row <- Rows]};
        {error, _} = Error -> Error
    end.

server({Host, Port, NodeType, Cloud, Region, Zone, PublicIp}) ->
    #{host => Host,
      port => Port,
      node_type => node_type(NodeType),
      cloud => text(Cloud),
      region => text(Region),
      zone => text(Zone),
      public_ip => text(PublicIp)
     }.

node_type(<<"read_replica">>) -> read_replica;
node_type(_) -> primary.

text(null) -> <<>>;
text(Value) when is_binary(Value) -> Value.

-spec is_primary(pid()) -> {ok, boolean()} | {error, term()}.
is_primary(Conn) ->
    case equery(Conn, "SELECT pg_is_in_recovery()", []) of
        {ok, _Columns, [{InRecovery}]} -> {ok, not InRecovery};
        {error, _} = Error -> Error
    end.

-spec committed(pid()) -> boolean().
committed(Conn) ->
    try epgsql:get_cmd_status(Conn) of
        {ok, commit} -> true;
        _ -> false
    catch exit:_ -> false
    end.

%% @doc Whether the server has a transaction open on the connection, from the
%% status byte of its last ReadyForQuery: `idle' (I), `in_transaction' (T) or
%% `failed' (E, a statement failed and only ROLLBACK is accepted). No round
%% trip to the server.
%%
%% `unknown' when the byte does not tell: the process is gone or took longer
%% than a second to answer, a command is still running, epgsql waits for a
%% sync after an extended-query error, the connection is in COPY mode, or the
%% state is not one this function knows.
%%
%% After `epgsql:parse', `epgsql:bind' or `epgsql:execute' with no
%% `epgsql:sync/1' after them, the byte is stale: the server sends
%% ReadyForQuery only on sync.
%%
%% The connection process reads the byte itself and sends back only the
%% answer, so the call does not copy the connection's state.
-spec transaction_status(pid()) -> transaction_status().
transaction_status(Conn) ->
    %% sys:get_state/2 would copy the whole state here, about 11 KB, mostly
    %% type codecs, on every checkin. Instead read_status/1 runs inside the
    %% connection process and throws the status. OTP documents what follows:
    %% a StateFun that raises leaves a gen_server's state as it was, and
    %% sys:replace_state/3 raises `{callback_failed, _, {throw, Thrown}}'
    %% here, which carries only the status. The fun never returns, so the
    %% state is never replaced.
    try sys:replace_state(Conn, fun read_status/1, ?STATUS_TIMEOUT) of
        %% The fun never ran, as in a gen_event with no handlers: not an
        %% epgsql connection.
        _ -> unknown
    catch
        error:{callback_failed, _, {throw, {?MODULE, transaction_status, Status}}} -> Status;
        _:_ -> unknown
    end.

%% Runs inside the connection process.
-spec read_status(term()) -> no_return().
read_status(State) ->
    throw({?MODULE, transaction_status, transaction_status_of(State)}).

%% epgsql 4.8 has no call for the status. epgsql_sock keeps it in the
%% `txstatus' field of its private #state{} record, whose layout is the same
%% from 4.7.0 to 4.8.0, so this matches the record's shape. Any other shape
%% reads as `unknown', and the connection is not reused: safe, if slow. The
%% tests set the status with epgsql's own epgsql_sock:set_attr/3 and read it
%% back here, so an epgsql whose layout moves the field fails them.
transaction_status_of({state, _Mod, _Sock, _Data, _Backend, on_message, _Codec, _Queue,
                       undefined, _CmdState, _CmdTransport, _Async, _Parameters, _Rows, _Results,
                       SyncRequired, TxStatus, _CompleteStatus, _SubprotoState, _ConnectOpts})
  when SyncRequired =/= true ->
    case TxStatus of
        $I -> idle;
        $T -> in_transaction;
        $E -> failed;
        _ -> unknown
    end;
transaction_status_of(_) ->
    unknown.
