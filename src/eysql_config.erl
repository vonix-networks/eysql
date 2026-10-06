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

%% @doc Options, their defaults, and validation.
%%
%% The load-balancing options keep the YugabyteDB smart drivers' names and
%% units (seconds), so anyone who has configured one of those recognises
%% them. Every other duration is in milliseconds.
%%
%% An option with a counterpart in the JDBC smart driver has that driver's
%% default: `load_balance' is off, `connect_timeout' is pgjdbc's 10 s, and
%% `database' is named after the user. `username' and `application_name'
%% name who connects, so they keep eysql's own values.
-module(eysql_config).

-export([normalize/1, defaults/0]).

-export_type([options/0, config/0]).

-type options() :: #{atom() => term()}.
-type config() :: #{atom() => term()}.

%% @doc The default for every option.
-spec defaults() -> options().
defaults() ->
    #{%% Where to connect first. A host is a name or `{Name, Port}'. Prefer
      %% DNS names that resolve to several servers, such as a headless
      %% Kubernetes service, to fixed addresses.
      hosts => [<<"localhost">>],
      port => 5433,
      %% username, database and application_name are text: a string or a
      %% UTF-8 binary.
      username => <<"yugabyte">>,
      %% A binary, a string, or a zero-arity fun that returns one.
      password => <<>>,
      %% undefined is the username, as in pgjdbc and libpq.
      database => undefined,
      %% Per connection attempt: the TCP connect and the TLS handshake, as
      %% pgjdbc's connectTimeout, whose default of 10 s this is.
      connect_timeout => 10000,
      application_name => <<"eysql">>,
      %% Milliseconds, set on each connection with SET statement_timeout.
      statement_timeout => undefined,
      %% Milliseconds, or infinity: how long a call on a pooled connection
      %% may wait for the server before the connection is closed and the
      %% call returns {error, {connection_lost, socket_timeout}}. The server
      %% enforces statement_timeout, and cannot when it has died silently or
      %% hangs; this bounds the call on the client. Off by default, as
      %% pgjdbc's socketTimeout is. See eysql_pool:watch/1 for what it
      %% bounds.
      socket_timeout => infinity,
      %% `true' uses TLS if the server offers it, `required' fails if not.
      %% ssl_opts go to epgsql exactly as given. The ssl application verifies
      %% the server's certificate by default since OTP 26, so TLS needs a CA
      %% in ssl_opts; only the caller's own `{verify, verify_none}' turns the
      %% check off.
      ssl => false,
      ssl_opts => [],
      %% Anything else epgsql:connect/1 accepts, passed through.
      epgsql_opts => #{},

      %% Smart-driver options. `load_balance' also takes the drivers'
      %% spellings, such as "prefer-primary", as a string or binary. Off by
      %% default, as in the drivers: the configured hosts only, with no
      %% discovery.
      load_balance => false,
      topology_keys => [],
      fallback_to_topology_keys_only => false,
      %% Seconds between refreshes, 0 to 600. A refresh reads yb_servers()
      %% again, except on PostgreSQL, and probes the hosts that are down, and
      %% the standbys. 0 refreshes whenever a connection is opened, instead
      %% of on a timer.
      yb_servers_refresh_interval => 300,
      %% How long a host that could not be connected to is left out at
      %% least, 0 to 60 seconds, as the JDBC driver accepts. The same every
      %% time, unless failed_host_max_delay_secs is set: then it doubles on
      %% each consecutive failure, up to that. The first refresh after it
      %% ends probes the host, and a probe that succeeds brings it back. 0
      %% leaves the host out until the next refresh, which the failure
      %% itself brings forward. A delay of 0 does not double, so it takes
      %% no failed_host_max_delay_secs.
      failed_host_reconnect_delay_secs => 5,
      failed_host_max_delay_secs => undefined,

      %% `read_write' makes every connection check pg_is_in_recovery() and
      %% move on from standbys, as libpq's target_session_attrs does. For
      %% PostgreSQL with several hosts; YugabyteDB servers all accept writes.
      target_session_attrs => any,

      %% Pool options. There are no health checks: like the smart drivers,
      %% the pool sends no query of its own. An idle connection past its
      %% lifetime closes at the next rebalance tick.
      pool_size => 10,
      max_lifetime => 1800000,
      lifetime_jitter => 300000,
      rebalance_interval => 30000,
      rebalance_batch => 2,
      %% Run on each connection the pool opens, after it opens and before
      %% the pool hands it to anyone, to prepare it: `undefined', a fun of
      %% one argument, the connection, or `{Module, Function, Args}', called
      %% as apply(Module, Function, [Conn | Args]). `ok', or a tuple whose
      %% first element is `ok', lets the connection into the pool; anything
      %% else closes it, and the pool opens another. Not run on the
      %% connections discovery and probes open. See eysql_pool.
      after_connect => undefined,
      %% Milliseconds, or infinity: how long after_connect may run before
      %% the connection is closed as failed.
      after_connect_timeout => 60000,

      %% Testing seam; see eysql_driver.
      driver => eysql_conn
     }.

%% @doc Merge options over the defaults, validate them and derive the
%% internal settings.
-spec normalize(options()) -> {ok, config()} | {error, term()}.
normalize(Options) when is_map(Options) ->
    Defaults = defaults(),
    case maps:keys(maps:without(maps:keys(Defaults), Options)) of
        [] -> build(maps:merge(Defaults, Options));
        Unknown -> {error, {unknown_options, lists:sort(Unknown)}}
    end;
normalize(Options) ->
    {error, {invalid_options, Options}}.

build(Options) ->
    try
        Port = pos_int(port, Options),
        {ok, Keys} = keys(maps:get(topology_keys, Options)),
        Config = #{seeds => seeds(maps:get(hosts, Options), Port),
                   settings => settings(Options),
                   driver => atom(driver, Options),
                   load_balance => load_balance(maps:get(load_balance, Options)),
                   topology_keys => Keys,
                   fallback_to_topology_keys_only => bool(fallback_to_topology_keys_only, Options),
                   refresh_interval => refresh_interval(Options) * 1000,
                   failed_host_delay => failed_host_delay(Options) * 1000,
                   failed_host_max_delay => max_delay(Options),
                   target_session_attrs => session_attrs(maps:get(target_session_attrs, Options)),
                   socket_timeout => socket_timeout(maps:get(socket_timeout, Options)),
                   pool_size => pos_int(pool_size, Options),
                   max_lifetime => pos_int(max_lifetime, Options),
                   lifetime_jitter => non_neg_int(lifetime_jitter, Options),
                   rebalance_interval => pos_int(rebalance_interval, Options),
                   rebalance_batch => pos_int(rebalance_batch, Options),
                   after_connect => after_connect(maps:get(after_connect, Options)),
                   after_connect_timeout => after_connect_timeout(maps:get(after_connect_timeout, Options))
                  },
        {ok, Config}
    catch
        throw:{invalid, Name, Value} -> {error, {invalid_option, Name, Value}};
        error:{badmatch, {error, Reason}} -> {error, Reason}
    end.

settings(Options) ->
    Username = text(username, Options),
    #{username => Username,
      password => password(maps:get(password, Options)),
      database => database(Username, Options),
      connect_timeout => pos_int(connect_timeout, Options),
      application_name => text(application_name, Options),
      statement_timeout => statement_timeout(maps:get(statement_timeout, Options)),
      ssl => ssl(maps:get(ssl, Options)),
      ssl_opts => secret_list(ssl_opts, Options),
      epgsql_opts => secret_map(epgsql_opts, Options)
     }.

seeds([], _Port) ->
    throw({invalid, hosts, []});
seeds(Hosts, Port) when is_list(Hosts) ->
    case io_lib:printable_unicode_list(Hosts) of
        true -> [seed(Hosts, Port)];
        false -> [seed(Host, Port) || Host <- Hosts]
    end;
seeds(Host, Port) when is_binary(Host) ->
    [seed(Host, Port)];
seeds(Other, _Port) ->
    throw({invalid, hosts, Other}).

seed({Host, Port}, _Default) when is_integer(Port), Port > 0, Port < 65536 ->
    eysql_topology:seed(host(Host), Port);
seed(Host, Default) ->
    eysql_topology:seed(host(Host), Default).

host(Host) when is_atom(Host) -> atom_to_binary(Host, utf8);
host(Host) ->
    case eysql_util:text(Host) of
        {ok, Binary} when Binary =/= <<>> -> Binary;
        _ -> throw({invalid, hosts, Host})
    end.

%% With no database given, the one named after the user, as pgjdbc's
%% Driver.parseURL sets PGDBNAME from the user and libpq does the same.
database(Username, Options) ->
    case maps:get(database, Options) of
        undefined -> Username;
        _ -> text(database, Options)
    end.

%% eysql_topology:parse_keys/1 returns an error for text that is not keys. A
%% value it raises on instead, such as an atom, is refused the same way, so
%% that it cannot crash the caller of normalize/1.
keys(Keys) ->
    try eysql_topology:parse_keys(Keys) of
        {ok, _} = Ok -> Ok;
        {error, _} = Error -> Error
    catch
        error:_ -> {error, {invalid_topology_key, Keys}}
    end.

load_balance(Value) when Value =:= false; Value =:= true; Value =:= any;
                         Value =:= only_primary; Value =:= only_rr;
                         Value =:= prefer_primary; Value =:= prefer_rr ->
    Value;
load_balance(Value) when is_binary(Value); is_list(Value) ->
    case load_balance_spelling(Value) of
        {ok, Mode} -> Mode;
        error -> throw({invalid, load_balance, Value})
    end;
load_balance(Value) -> throw({invalid, load_balance, Value}).

%% The smart drivers' values, as the JDBC driver reads them: any case.
load_balance_spelling(Value) ->
    try
        {ok, Text} = eysql_util:text(Value),
        string:lowercase(Text)
    of
        <<"true">> -> {ok, true};
        <<"false">> -> {ok, false};
        <<"any">> -> {ok, any};
        <<"only-primary">> -> {ok, only_primary};
        <<"only-rr">> -> {ok, only_rr};
        <<"prefer-primary">> -> {ok, prefer_primary};
        <<"prefer-rr">> -> {ok, prefer_rr};
        _ -> error
    catch
        error:_ -> error
    end.

%% Seconds, 0 to 600, as the JDBC driver accepts.
refresh_interval(Options) ->
    case maps:get(yb_servers_refresh_interval, Options) of
        Value when is_integer(Value), Value >= 0, Value =< 600 -> Value;
        Value -> throw({invalid, yb_servers_refresh_interval, Value})
    end.

%% Seconds, 0 to 60, as the JDBC driver's LoadBalanceProperties accepts
%% failed-host-reconnect-delay-secs.
failed_host_delay(Options) ->
    case maps:get(failed_host_reconnect_delay_secs, Options) of
        Value when is_integer(Value), Value >= 0, Value =< 60 -> Value;
        Value -> throw({invalid, failed_host_reconnect_delay_secs, Value})
    end.

%% Milliseconds, or undefined for a fixed delay. A cap below the first delay
%% would shorten it rather than cap its growth. A first delay of 0 never
%% grows, since 0 doubled is 0: a cap on it would look like a doubling
%% delay and change nothing, so it is refused.
max_delay(Options) ->
    Base = failed_host_delay(Options),
    case maps:get(failed_host_max_delay_secs, Options) of
        undefined -> undefined;
        Value when is_integer(Value), Base > 0, Value >= Base -> Value * 1000;
        Value -> throw({invalid, failed_host_max_delay_secs, Value})
    end.

session_attrs(any) -> any;
session_attrs(read_write) -> read_write;
session_attrs(Value) -> throw({invalid, target_session_attrs, Value}).

ssl(Value) when Value =:= false; Value =:= true; Value =:= required -> Value;
ssl(Value) -> throw({invalid, ssl, Value}).

statement_timeout(undefined) -> undefined;
statement_timeout(Ms) when is_integer(Ms), Ms >= 0 -> Ms;
statement_timeout(Value) -> throw({invalid, statement_timeout, Value}).

%% A bound of 0 would fail every call, so off is `infinity', not pgjdbc's 0.
socket_timeout(infinity) -> infinity;
socket_timeout(Ms) when is_integer(Ms), Ms > 0 -> Ms;
socket_timeout(Value) -> throw({invalid, socket_timeout, Value}).

%% The hook is kept as given, as the password fun is. Whether `Function' is
%% exported is not checked here: its module may not be loaded yet. A call
%% that fails, `undef' included, fails the connection it prepares.
after_connect(undefined) ->
    undefined;
after_connect(Fun) when is_function(Fun, 1) ->
    Fun;
after_connect({Module, Function, Args} = Mfa) when is_atom(Module), is_atom(Function) ->
    case proper_list(Args) of
        true -> Mfa;
        false -> throw({invalid, after_connect, Mfa})
    end;
after_connect(Value) ->
    throw({invalid, after_connect, Value}).

%% A bound of 0 would fail every connection, so off is `infinity', as for
%% socket_timeout. The opener waits with `receive ... after', which takes
%% at most 2^32 - 1 ms, about 49 days.
after_connect_timeout(infinity) -> infinity;
after_connect_timeout(Ms) when is_integer(Ms), Ms > 0, Ms =< 16#FFFFFFFF -> Ms;
after_connect_timeout(Value) -> throw({invalid, after_connect_timeout, Value}).

proper_list([]) -> true;
proper_list([_ | Tail]) -> proper_list(Tail);
proper_list(_) -> false.

%% The password as epgsql takes it: a binary, a string as UTF-8, or the
%% caller's own zero-arity fun, kept as it is. Unlike a text option, a binary
%% is not checked for UTF-8: a password need not be text. It is not wrapped
%% in a fun made here: such a fun refers to this module's code, which is
%% purged once the module has been reloaded twice, and calling it then raises
%% badfun on every connect. The pool and cluster processes redact it when
%% they print their state. An invalid password is not echoed in the error.
password(Fun) when is_function(Fun, 0) ->
    Fun;
password(Binary) when is_binary(Binary) ->
    Binary;
password(Value) ->
    case eysql_util:text(Value) of
        {ok, Binary} -> Binary;
        error -> throw({invalid, password, redacted})
    end.

%% Text by the one rule eysql_util:text/1 sets, so that a binary with invalid
%% UTF-8 stops the pool from starting rather than reach the server.
text(Name, Options) ->
    Value = maps:get(Name, Options),
    case eysql_util:text(Value) of
        {ok, Binary} -> Binary;
        error -> throw({invalid, Name, Value})
    end.

pos_int(Name, Options) ->
    case maps:get(Name, Options) of
        Value when is_integer(Value), Value > 0 -> Value;
        Value -> throw({invalid, Name, Value})
    end.

non_neg_int(Name, Options) ->
    case maps:get(Name, Options) of
        Value when is_integer(Value), Value >= 0 -> Value;
        Value -> throw({invalid, Name, Value})
    end.

bool(Name, Options) ->
    case maps:get(Name, Options) of
        Value when is_boolean(Value) -> Value;
        Value -> throw({invalid, Name, Value})
    end.

atom(Name, Options) ->
    case maps:get(Name, Options) of
        Value when is_atom(Value) -> Value;
        Value -> throw({invalid, Name, Value})
    end.

%% `ssl_opts' and `epgsql_opts' can hold keys and passwords, so an invalid
%% one is not echoed in the error, as with `password'.
secret_list(Name, Options) ->
    case maps:get(Name, Options) of
        Value when is_list(Value) -> Value;
        _ -> throw({invalid, Name, redacted})
    end.

secret_map(Name, Options) ->
    case maps:get(Name, Options) of
        Value when is_map(Value) -> Value;
        _ -> throw({invalid, Name, redacted})
    end.
