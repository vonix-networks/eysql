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

%% @doc Sort epgsql errors into what a caller can do about them.
%%
%% `retryable': the statement or transaction failed and nothing was
%% committed; run it again (serialization failure 40001, deadlock 40P01).
%% YugabyteDB reports its transaction conflicts and read restarts as 40001.
%%
%% `connection_lost': the connection is gone (server shutdown 57P0x, any
%% class 08 connection exception, or a closed socket). Work in an open
%% transaction was not committed; a COMMIT in flight has an unknown outcome.
%%
%% `other': anything else, such as a constraint violation or bad SQL.
%%
%% {@link connect_failure/1} sorts the errors of a failed connect instead:
%% whether the host could not be reached at all, which leaves it out of new
%% connections, or answered and turned the connection away.
-module(eysql_error).

-include_lib("epgsql/include/epgsql.hrl").

-export([classify/1,
         is_retryable/1,
         is_connection_lost/1,
         connect_failure/1,
         code/1
        ]).

-export_type([class/0, connect_failure/0]).

-type class() :: retryable | connection_lost | other.

-type connect_failure() :: unreachable | rejected.

-spec classify(term()) -> class().
classify({error, Reason}) -> classify(Reason);
classify(#error{code = Code}) -> classify_code(Code);
classify({connection_lost, _}) -> connection_lost;
classify(closed) -> connection_lost;
classify(sock_closed) -> connection_lost;
classify(sock_error) -> connection_lost;
classify(noproc) -> connection_lost;
classify({noproc, _}) -> connection_lost;
classify(_) -> other.

classify_code(<<"40001">>) -> retryable;
classify_code(<<"40P01">>) -> retryable;
classify_code(<<"57P01">>) -> connection_lost;
classify_code(<<"57P02">>) -> connection_lost;
classify_code(<<"57P03">>) -> connection_lost;
classify_code(<<"08", _/binary>>) -> connection_lost;
classify_code(_) -> other.

-spec is_retryable(term()) -> boolean().
is_retryable(Error) -> classify(Error) =:= retryable.

-spec is_connection_lost(term()) -> boolean().
is_connection_lost(Error) -> classify(Error) =:= connection_lost.

%% @doc Why a connect failed, as the JDBC smart driver sorts it.
%%
%% `unreachable': no connection could be made to the host. These are the
%% failures pgjdbc reports as SQLSTATE 08001, CONNECTION_UNABLE_TO_CONNECT,
%% the only ones after which the smart driver marks a host down
%% (LoadBalanceService.getConnection). ConnectionFactoryImpl.openConnectionImpl
%% gives that state to every I/O failure while connecting: a refused or
%% timed-out TCP connect, a name that does not resolve, no route, and a
%% connection that drops before the session is ready. In epgsql and OTP terms:
%%
%% <ul>
%% <li>`econnrefused', `timeout', `etimedout', `nxdomain', `ehostunreach',
%%     `ehostdown', `enetunreach', `enetdown', `econnreset', `econnaborted',
%%     `epipe', `enotconn' and `eaddrnotavail' from the TCP connect, or from
%%     reading the server's answer to the TLS request;</li>
%% <li>`closed', `sock_closed' and `{sock_error, _}': the server closed the
%%     socket, or it failed, during startup or authentication;</li>
%% <li>`{connection_lost, _}': {@link eysql_conn} lost the connection while
%%     setting `statement_timeout' or checking `target_session_attrs';</li>
%% <li>`probe_timeout' and `{probe_exit, _}': an {@link eysql_cluster} probe
%%     that overran its time, or died with its connection;</li>
%% <li>a server error whose SQLSTATE is itself 08001.</li>
%% </ul>
%%
%% `rejected': anything else. The server answered, and the connection
%% failed all the same: a failed login (`invalid_password', 28P01, or
%% `invalid_authorization_specification', 28000), a missing database
%% (3D000), too many connections (53300), a server starting up or shutting
%% down (57P03), any other server error, an authentication method epgsql
%% does not support, a server without TLS under `ssl => required'
%% (`ssl_not_available', 08004 in pgjdbc), and a failed TLS handshake
%% (`{ssl_negotiation_failed, _}', which pgjdbc's MakeSSL.convert reports as
%% 08006, CONNECTION_FAILURE, whatever the cause). pgjdbc skips such a host
%% for the rest of that one connection and does not mark it down.
-spec connect_failure(term()) -> connect_failure().
connect_failure({error, Reason}) -> connect_failure(Reason);
connect_failure(#error{code = <<"08001">>}) -> unreachable;
connect_failure({sock_error, _}) -> unreachable;
connect_failure({connection_lost, _}) -> unreachable;
connect_failure({probe_exit, _}) -> unreachable;
connect_failure(Reason) when is_atom(Reason) ->
    case lists:member(Reason, unreachable()) of
        true -> unreachable;
        false -> rejected
    end;
connect_failure(_Reason) ->
    rejected.

%% The atoms a connect that reached no server returns: inet's POSIX errors
%% for a connect or a read that failed, gen_tcp's `timeout' and `closed',
%% epgsql's `sock_closed', and the cluster's own `probe_timeout'.
unreachable() ->
    [econnrefused, timeout, etimedout, nxdomain, ehostunreach, ehostdown, enetunreach,
     enetdown, econnreset, econnaborted, epipe, enotconn, eaddrnotavail, closed, sock_closed,
     probe_timeout].

%% @doc The SQLSTATE of a server error, or `undefined'.
-spec code(term()) -> binary() | undefined.
code({error, Reason}) -> code(Reason);
code(#error{code = Code}) -> Code;
code(_) -> undefined.
