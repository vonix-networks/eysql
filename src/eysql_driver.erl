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

%% @doc The connection operations eysql needs, as a behaviour.
%%
%% {@link eysql_conn} implements it with epgsql and is the default. The
%% `driver' option replaces it, which is how the test suite runs the cluster
%% and pool logic without a database.
-module(eysql_driver).

%% Open a connection linked to the calling process.
-callback open(Host :: binary(), Port :: inet:port_number(), Settings :: map()) ->
    {ok, pid()} | {error, term()}.

-callback close(Conn :: pid()) -> ok.

%% A simple-protocol query; used for BEGIN, COMMIT and ROLLBACK. The pool's
%% `socket_timeout' travels with the checkout, not as an argument: a driver
%% bounds the call by running it between eysql_pool:watch/1 and
%% eysql_pool:unwatch/2, as eysql_conn does, and returns
%% `{error, {connection_lost, socket_timeout}}' when the clock ran out.
-callback squery(Conn :: pid(), Sql :: iodata()) -> term().

%% Read the cluster's servers with `yb_servers()'.
-callback discover(Conn :: pid()) -> {ok, [eysql_topology:server()]} | {error, term()}.

%% Whether the server accepts writes (not a hot standby).
-callback is_primary(Conn :: pid()) -> {ok, boolean()} | {error, term()}.

%% Whether the last COMMIT committed or, because the transaction had already
%% failed, rolled back.
-callback committed(Conn :: pid()) -> boolean().

%% Whether the connection is inside a transaction, as the server last said,
%% without a round trip. The pool takes a connection back only when `idle';
%% anything else, `unknown' included, is closed instead. It runs each time
%% eysql:with_connection/3 or eysql:transaction/3 gives a connection back, so
%% it should be cheap, and it should answer `unknown' rather than raise.
-callback transaction_status(Conn :: pid()) -> idle | in_transaction | failed | unknown.
