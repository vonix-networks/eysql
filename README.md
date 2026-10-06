# eysql

Topology-aware load balancing and failover for [epgsql](https://github.com/epgsql/epgsql), for YugabyteDB and PostgreSQL.

YugabyteDB ships "smart drivers" for Java, Go, Python, Node.js, C#, Rust and Ruby. They discover the cluster's servers, spread connections across them, prefer servers in your zone, and skip servers that fail. Nothing like that existed for Erlang. eysql adds it on top of epgsql, without replacing epgsql: your connections are ordinary epgsql connections.

- **Discovery.** With `load_balance` on, reads `yb_servers()` at start, on a timer and after any connection failure.
- **Load balancing.** Opens each connection to the server where this client holds the fewest connections.
- **Topology preference.** Prefers servers in the zones or regions you list, falling back level by level.
- **Failover.** Leaves a server it cannot connect to out until a refresh finds it back, and replaces a dropped connection on another server.
- **A pool.** Keeps a fixed number of connections open. It recycles them after a maximum lifetime and rebalances them when servers join or leave. Like the drivers, it sends no queries of its own.
- **Transactions.** Retries serialization failures and deadlocks, and reports an unknown COMMIT outcome instead of guessing.
- **PostgreSQL.** Works with the configured hosts, and can pick the writable primary among several.

It is written in plain Erlang with epgsql as its only dependency, so Erlang, Elixir and Gleam projects can all use it.

## Install

rebar3:

```erlang
{deps, [{eysql, "~> 0.1"}]}.
```

Mix:

```elixir
{:eysql, "~> 0.1"}
```

Gleam:

```sh
gleam add eysql
```

## Use

```erlang
{ok, Pool} = eysql:start_link(#{hosts => ["yb-tservers.db.svc.cluster.local"],
                               username => <<"app">>,
                               password => <<"secret">>,
                               database => <<"app">>,
                               load_balance => true,
                               topology_keys => "gcp.us-east1.us-east1-b:1,gcp.us-east1.*:2"}),

%% One statement.
{ok, _Columns, Rows} = eysql:equery(Pool, "SELECT id, name FROM accounts WHERE realm = $1", [Realm]),

%% Several statements on one connection. eysql_conn:equery/3 and squery/2
%% are epgsql's, bounded by socket_timeout; epgsql's own functions work on
%% the connection too, without the bound.
eysql:with_connection(Pool, fun(Conn) ->
    {ok, _, _} = eysql_conn:equery(Conn, "SELECT ...", []),
    eysql_conn:equery(Conn, "SELECT ...", [])
end),

%% A transaction, retried on serialization failure.
{ok, Id} = eysql:transaction(Pool, fun(Conn) ->
    case eysql_conn:equery(Conn, "INSERT INTO t (name) VALUES ($1) RETURNING id", [Name]) of
        {ok, 1, _, [{Id}]} -> {ok, Id};
        {error, _} = Error -> Error
    end
end).
```

Under a supervisor, `eysql:child_spec(my_db, Options)` gives a pool registered as `my_db`.

Elixir:

```elixir
{:ok, pool} = :eysql.start_link(%{hosts: [~c"yb-tservers"], username: "app", password: "secret", database: "app",
                                  load_balance: true})
{:ok, _cols, rows} = :eysql.equery(pool, "SELECT 1", [])
```

## Options

The load-balancing options keep the smart drivers' names and units (seconds). Every other duration is in milliseconds. An option with a counterpart in the YugabyteDB JDBC driver defaults as it does there, so load balancing is off until you set `load_balance`.

Text, such as `username`, a host or `topology_keys`, is a string or a UTF-8 binary. A binary that is not UTF-8 is an invalid option.

| Option | Default | Meaning |
|---|---|---|
| `hosts` | `["localhost"]` | Seed hosts: names, or `{Name, Port}`. A DNS name that resolves to several servers, such as a headless Kubernetes service, is a good seed. |
| `port` | `5433` | Port for hosts given without one. PostgreSQL uses 5432. |
| `username`, `password`, `database` | `yugabyte`, empty, the username | As for epgsql. `password` may be any binary, or a zero-arity fun that returns it; `fun Module:Function/0` keeps it out of the start arguments a supervisor prints. `database` defaults to the username, as in pgjdbc and libpq. |
| `ssl`, `ssl_opts` | `false`, `[]` | Passed to epgsql, with the host each connection dialled as its `server_name_indication` unless `ssl_opts` sets one. `true` uses TLS when the server offers it, and `required` insists on it. OTP 26 and later verify the server's certificate by default, so `ssl_opts` need a CA. See [TLS](#tls). |
| `connect_timeout` | `10000` | Per connection attempt, for the TCP connect and the TLS handshake. pgjdbc's `connectTimeout` defaults to the same 10 seconds. |
| `statement_timeout` | none | Set on each connection with `SET statement_timeout`. |
| `socket_timeout` | `infinity` | How long a call on a pooled connection waits for the server, in milliseconds. When it runs out, eysql closes the connection and the call returns `{error, {connection_lost, socket_timeout}}`. It covers `equery/3`, `squery/2`, the BEGIN, COMMIT and ROLLBACK of `transaction/2,3`, and `eysql_conn:equery/3` and `squery/2` called inside `with_connection` or `transaction`, or on a connection you checked out. epgsql's own functions, called on the connection directly, wait for as long as the server takes. Off by default, as pgjdbc's `socketTimeout` is. |
| `application_name` | `eysql` | Shown in `pg_stat_activity`. |
| `epgsql_opts` | `#{}` | Anything else `epgsql:connect/1` accepts, such as `tcp_opts` with TCP keepalive settings. |
| `load_balance` | `false` | `false` uses the configured hosts only, in the order given, with no discovery, as the drivers do by default: every connection goes to the first host that works. `true`/`any`: all servers. `only_primary`, `only_rr`: primary or read-replica nodes only. `prefer_primary`, `prefer_rr`: that type first, the other if none is up. The drivers' spellings work too, as strings or binaries in any case: `"true"`, `"false"`, `"any"`, `"only-primary"`, `"only-rr"`, `"prefer-primary"`, `"prefer-rr"`. |
| `topology_keys` | none | `"cloud.region.zone:N,…"`. Zone `*` means any zone in the region, and preference `N` (1–10, default 1) orders the levels. Names match in any case, as in the JDBC driver: `GCP.US-East1.*` matches the servers `yb_servers()` places in `gcp.us-east1`. |
| `fallback_to_topology_keys_only` | `false` | With no server up in any listed placement, fail instead of using other servers or the seeds. It needs `topology_keys`, and `prefer_primary` and `prefer_rr` ignore it, as the drivers do. |
| `yb_servers_refresh_interval` | `300` | Seconds between refreshes, 0 to 600. A refresh reads `yb_servers()` again, except on PostgreSQL, and probes the servers that have failed, so this also bounds how long a failed server that is back waits to be used. A connection failure brings the next refresh forward. With 0 there is no timer: eysql refreshes each time it opens a connection, in the background, so the connection does not wait for it. |
| `failed_host_reconnect_delay_secs` | `5` | How long a server that failed a connection is left out at least, 0 to 60 seconds, as in the JDBC driver. The same every time, as in the drivers. The first refresh after it ends probes the server, and a probe that succeeds brings it back. With 0 the server is due at the next refresh, which its failure brings forward. |
| `failed_host_max_delay_secs` | none | Set it to double the delay on each consecutive failure, up to this many seconds. It must be at least `failed_host_reconnect_delay_secs`, and a delay of 0 takes none, since 0 doubled stays 0. |
| `target_session_attrs` | `any` | `read_write` checks `pg_is_in_recovery()` and moves on from standbys, as libpq's `target_session_attrs` and pgjdbc's `targetServerType=primary` do. |
| `pool_size` | `10` | Connections the pool keeps open. |
| `max_lifetime` | `1800000` | A connection is replaced after this… |
| `lifetime_jitter` | `300000` | …plus a random share of this, so they do not all reconnect together. |
| `rebalance_interval` | `30000` | How often the pool moves connections, and closes idle ones past their lifetime… |
| `rebalance_batch` | `2` | …and at most how many it moves at a time. |
| `after_connect` | none | Prepares each connection the pool opens before anyone gets it: a fun of one argument, the connection, or `{Module, Function, Args}`, called as `apply(Module, Function, [Conn \| Args])`. See [Preparing connections](#preparing-connections). |
| `after_connect_timeout` | `60000` | How long `after_connect` may run on one connection, in milliseconds, or `infinity`. A hook still running then fails the connection. |

There are no health check options. `health_check_interval` and `health_check_timeout` are unknown options, and naming one is an error. Set `socket_timeout` so a query on a server that has died silently fails instead of hanging.

## TLS

`ssl => required` encrypts every connection and fails if the server does not offer TLS. `ssl => true` goes on unencrypted when the server does not offer TLS, as libpq's `sslmode=prefer` does. When the server does offer TLS, a failed certificate check fails the connection with either setting.

eysql passes `ssl_opts` to epgsql, adding only the name to check, and never turns certificate checks off. The ssl application in OTP 26 and later verifies the server's certificate by default, so TLS needs a CA to check it against:

```erlang
ssl => required,
ssl_opts => [{cacertfile, "/etc/ssl/certs/yugabyte-ca.crt"}]
```

For the operating system's trust store, use `{cacerts, public_key:cacerts_get()}` instead. Without a CA, OTP refuses to connect, and every connection fails with:

```erlang
{error, {ssl_negotiation_failed, {options, incompatible, [{verify, verify_peer}, {cacerts, undefined}]}}}
```

With a CA, OTP checks the certificate chain, then the server's name, as libpq's and pgjdbc's `sslmode=verify-full` do: the certificate must list the host each connection dialled as a subject alternative name. That is the seed host for the first connection, and each server's `host` or `public_ip` from `yb_servers()` after that, so on YugabyteDB every server's certificate lists its own name and the seed's. A certificate can list several names; the check passes when the one dialled is among them. A host given as an IP address is checked against the address, so the certificate must list that address.

epgsql starts TLS on a TCP connection it has already opened, which leaves OTP knowing the server only by its address. eysql therefore passes the host it dialled, when that is a name, as the connection's `server_name_indication`, which OTP both sends and checks. A certificate that fails the check fails the connection with a TLS alert that includes `hostname_check_failed`.

To check something else, set `server_name_indication` in `ssl_opts` yourself. It then applies to every server:

- `{server_name_indication, "yb.example.com"}` checks that one name on every server, so it suits a certificate the servers share.
- `{server_name_indication, disable}` checks the chain only, as libpq's `sslmode=verify-ca` does.

To encrypt without verifying, as libpq's `sslmode=require` does, set `{verify, verify_none}` yourself. The connection then accepts any certificate, so nothing stops a man in the middle.

## PostgreSQL

With PostgreSQL, `yb_servers()` does not exist. With `load_balance` off, the default, eysql never asks for it. With it on, eysql notices on the first discovery and uses the configured hosts from then on, in the order given, whatever the mode. With several hosts and `target_session_attrs => read_write`, it connects to the first that accepts writes.

## Preparing connections

`after_connect` runs on each connection the pool opens, before the pool hands it to anyone. On YugabyteDB a new connection is a new backend, and its first statements wait while it loads the catalog entries they need from the masters. A hook that runs the application's hot statements once keeps that wait off the first caller.

```erlang
{ok, Pool} = eysql:start_link(#{hosts => ["yb-tservers"],
                               load_balance => true,
                               after_connect => {my_db, warm, []}}).

%% In my_db. Each statement runs once, with parameters that match no row,
%% so the backend parses and plans it: it loads what the planner reads, such
%% as the table's indexes and statistics, as well as the names the parser
%% looks up. epgsql:parse/2 alone would warm only the parser's lookups.
warm(Conn) ->
    lists:foreach(fun({Sql, Params}) -> {ok, _, _} = eysql_conn:equery(Conn, Sql, Params) end,
                  [{"SELECT id, doc FROM accounts WHERE id = $1", [0]},
                   {"SELECT id FROM devices WHERE account_id = $1 AND name = $2", [0, <<>>]}]).
```

- It runs once on every connection the pool opens: the first ones, and those that replace a connection that died, reached `max_lifetime` or was moved by a rebalance. It does not run on the connections that discovery and probes open, nor on those `eysql_cluster:connect/1` opens.
- It runs in a process of its own, once the connection has opened and `statement_timeout` is set, so `statement_timeout` applies to its statements. `socket_timeout` does not, since nothing has checked the connection out. `after_connect_timeout` bounds the whole hook instead. Its default of a minute leaves room for tens of statements that each wait on the masters, and frees the connection's place in the pool within a minute when a server hangs during the hook.
- Until it returns, `stats/1` counts the connection as `opening`, and checkouts wait for it.
- `ok`, or a tuple whose first element is `ok`, such as `{ok, _}` or epgsql's `{ok, Columns, Rows}`, lets the connection into the pool. Anything else fails it: `{error, Reason}`, an exception, another value, or still running after `after_connect_timeout`. So does a connection the hook leaves closed or inside a transaction. A multi-statement `squery` returns a list of results, so a hook that ends with one fails; end it with `ok`.
- On a failure eysql closes the connection. The failure's reason, `Reason` below, is the reason the hook returned, `{Class, Reason}` for an exception, `{bad_return, Value}`, `{after_connect_timeout, Ms}`, `{connection_lost, _}` or `{transaction_status, Status}`.
- A failing hook marks no server: the failure is the application's, not the server's, and leaving servers out for it would let one bad statement take every server out of rotation. The pool steers its own connections instead, but only between the servers it would choose among anyway. A hook failure never changes the topology level, node type or host order a connection goes to.
- For `failed_host_reconnect_delay_secs` after a hook fails on a server, at least a second, the pool's new connections go to the other servers of that same choice, and the failed connection is replaced at once on one of them. Rebalancing moves no connection towards the server meanwhile. After the delay, new connections and rebalancing try it again: a rebalance moves up to `rebalance_batch` connections towards it, which open elsewhere if the hook still fails there and stay once it passes. A hook that fails on one of several servers to choose among thus costs, after each delay, a few connects there, and closes up to `rebalance_batch` idle connections elsewhere to make them, while no checkout fails for it. With the defaults that is about four connects a minute for as long as the hook keeps failing there.
- With a single server to choose, as with `load_balance` off, where every connection goes to the first host that works, or with one server in the preferred topology level, there is nothing to steer to, and the pool keeps retrying that server. The first failure of a run is replaced at once; from the second on, each failure is a failed open: the pool opens another a second later, as after a failed connect, and a checkout with no other connection to wait for fails with `{error, {after_connect, Reason}}`.
- The pool logs the first failure on a server as a warning with the reason, and for an exception the hook's own stack frames, without their arguments. It logs no further failures there until a connection to the server has passed the hook and a minute has gone by since the last failure, and then logs the end of the run at info.
- In a pool that lives across code upgrades, give `{Module, Function, Args}` or `fun Module:Function/1`. An anonymous fun fails with `badfun` once the module that made it has been upgraded twice, and from then on every new connection fails its hook. The pool prints such a hook as `{Module, Function, redacted}` in its status and crash reports, so a credential in `Args` stays out of them.

## Without a pool

`eysql_cluster` does placement and failover for code that manages its own connections. `connect/1` waits for the first discovery, then tries hosts as above, and returns the last connect error, or `{error, {no_node_available, Type}}` in the modes that refuse:

```erlang
{ok, Config} = eysql_config:normalize(Options),
{ok, Cluster} = eysql_cluster:start_link(Config),
{ok, Conn} = eysql_cluster:connect(Cluster).   % linked to the caller, like epgsql:connect/1
```

## Tests

```sh
rebar3 eunit                 # logic, against a fake driver
integration/run.sh           # PostgreSQL and a three-zone YugabyteDB cluster in Docker
```

The integration suite includes stopping a YugabyteDB node under load and starting it again. On both engines it also checks that a connection left inside a transaction, or a failed one, is closed rather than handed to the next holder, that a query in flight when its pool stops returns an error, that `socket_timeout` cuts off a `pg_sleep` and the pool recovers, and that every pooled connection, replacements included, runs `after_connect` before it serves a query. `eysql_driver` is the behaviour the fake driver implements; the `driver` option swaps it in.

## Compatibility

- OTP 26 to 29.
- epgsql 4.7 and later.
- YugabyteDB: any release with `yb_servers()`, the function the smart drivers use. Tested against 2026.1.
- PostgreSQL: tested against 17.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
