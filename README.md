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
| `username`, `password`, `database` | `yugabyte`, empty, the username | As for epgsql. `password` may be any binary, or a zero-arity fun that returns it. See [Passwords in reports](#passwords-in-reports). `database` defaults to the username, as in pgjdbc and libpq. |
| `ssl`, `ssl_opts` | `false`, `[]` | Passed to epgsql as given. `true` uses TLS when the server offers it, and `required` insists on it. OTP 26 and later verify the server's certificate by default, so `ssl_opts` need a CA. See [TLS](#tls). |
| `connect_timeout` | `10000` | Per connection attempt, for the TCP connect and the TLS handshake. pgjdbc's `connectTimeout` defaults to the same 10 seconds. |
| `statement_timeout` | none | Set on each connection with `SET statement_timeout`. |
| `socket_timeout` | `infinity` | How long a call on a pooled connection waits for the server, in milliseconds. When it runs out, eysql closes the connection and the call returns `{error, {connection_lost, socket_timeout}}`. It covers `equery/3`, `squery/2`, the BEGIN, COMMIT and ROLLBACK of `transaction/2,3`, and `eysql_conn:equery/3` and `squery/2` called inside `with_connection` or `transaction`, or on a connection you checked out. epgsql's own functions, called on the connection directly, wait for as long as the server takes. Off by default, as pgjdbc's `socketTimeout` is. See [Dead connections](#dead-connections). |
| `application_name` | `eysql` | Shown in `pg_stat_activity`. |
| `epgsql_opts` | `#{}` | Anything else `epgsql:connect/1` accepts, such as `tcp_opts`. See [Dead connections](#dead-connections) for TCP keepalive. |
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

There are no health check options. `health_check_interval` and `health_check_timeout` are unknown options, and naming one is an error. Set `socket_timeout` so a query on a server that has died silently fails instead of hanging; see [Dead connections](#dead-connections).

## TLS

`ssl => required` encrypts every connection and fails if the server does not offer TLS. `ssl => true` goes on unencrypted when the server does not offer TLS, as libpq's `sslmode=prefer` does. When the server does offer TLS, a failed certificate check fails the connection with either setting.

eysql passes `ssl_opts` to epgsql exactly as you give them, and never turns certificate checks off. The ssl application in OTP 26 and later verifies the server's certificate by default, so TLS needs a CA to check it against:

```erlang
ssl => required,
ssl_opts => [{cacertfile, "/etc/ssl/certs/yugabyte-ca.crt"}]
```

For the operating system's trust store, use `{cacerts, public_key:cacerts_get()}` instead. Without a CA, OTP refuses to connect, and every connection fails with:

```erlang
{error, {ssl_negotiation_failed, {options, incompatible, [{verify, verify_peer}, {cacerts, undefined}]}}}
```

With a CA, OTP checks the certificate chain, then the server's address. epgsql starts TLS on a TCP connection it has already opened, so OTP checks the certificate against the IP address it is connected to, not against the host name. A certificate that names the server only by DNS name fails that check, with a TLS alert that includes `hostname_check_failed`. To pass it, do one of these:

- Give each server a certificate that lists its IP address as a subject alternative name. OTP then checks each connection against the address of the server it went to.
- Add `{server_name_indication, "yb.example.com"}` to `ssl_opts`. OTP then checks that one name on every server, so it suits a certificate the servers share.
- Add `{server_name_indication, disable}` to check the chain only, as libpq's `sslmode=verify-ca` does.

To encrypt without verifying, as libpq's `sslmode=require` does, set `{verify, verify_none}` yourself. The connection then accepts any certificate, so nothing stops a man in the middle.

Hostname checks by DNS name across load-balanced hosts, as `sslmode=verify-full` does them, are not supported yet. eysql passes the same `ssl_opts` for every server, so a `server_name_indication` there cannot follow the server picked.

## Passwords in reports

eysql keeps the password as you give it. A string becomes a binary, any binary stays as it is, and a fun stays a fun, which epgsql calls each time a connection authenticates.

Crash reports and `sys:get_status/1` show the pool's and the cluster's state with `redacted` in place of the password, and in place of `ssl_opts` and `epgsql_opts` when they are set.

The options map has no such protection. A supervisor prints each child's start arguments when the child fails to start and when it exits. Those arguments hold the map you gave `child_spec/2`, or `start_link` in a child spec of your own, password included. To keep the password out, pass it as a zero-arity fun from your own module:

```erlang
eysql:child_spec(my_db, #{hosts => ["yb-tservers"],
                          load_balance => true,
                          password => fun my_app_secrets:db_password/0})
```

Use the `fun Module:Function/0` form. An anonymous `fun() -> ... end` fails with `badfun` once the module that made it has been upgraded twice, and from then on no new connection can log in.

## How a connection is placed

With `load_balance` off, the default, eysql uses the configured hosts only, and never reads `yb_servers()`. It takes them in the order given, as pgjdbc takes the hosts in its URL with `loadBalanceHosts` off, its default: every connection goes to the first host that works, and the next host takes connections only when that one fails. With `target_session_attrs => read_write`, that is the first host that accepts writes. With `load_balance` on:

1. Drop servers that have failed: those that are down, standbys, and those that turned a connection away (rejected), until a refresh finds them back.
2. Keep the node types `load_balance` allows.
3. With `topology_keys`, keep the lowest preference level that has a server. If no level has one, keep everything, or nothing with `fallback_to_topology_keys_only`.
4. Take the server where this pool holds the fewest connections. A tie goes to one of the tied servers at random, as in the JDBC driver.

`prefer_primary` takes primaries by preference level, then primaries anywhere in the cluster, and only then read replicas anywhere in the cluster. `prefer_rr` does the same with the types swapped. `fallback_to_topology_keys_only` does not apply to either. This is the order the smart drivers document.

A connection tries servers until one opens, each at most once. When no discovered server that steps 2 and 3 accept is left, eysql tries the seed hosts, the ones you configured, in the order given, as the drivers fall back to a plain connection to the hosts in their URL. It starts with the first seed that has not failed, since a client that cannot reach the addresses the servers advertise may still reach a seed, such as a load balancer's name. If every seed has failed too, it tries the seeds regardless of their failures. That keeps work going after a cluster-wide blip, and lets a lone PostgreSQL server that comes back, or a standby promoted when the primary fails, take connections before a refresh has probed it. Each seed is tried once as well. When every host it tried has failed, the connection returns the last error.

`only_primary`, `only_rr`, and `fallback_to_topology_keys_only` with `topology_keys` under `true` or `any`, leave the seeds out once servers are discovered. In those modes the JDBC driver refuses rather than fall back, and a seed behind a load balancer could lead to a server of any type in any zone. A connection that finds no eligible server fails with `{error, {no_node_available, Type}}`, where `Type` is `primary`, `read_replica` or `cluster`, as the driver throws "No node available in the given placements for the primary, read-replica or entire cluster". It does so whether or not it tried some servers first. It does not bring a refresh forward: failed servers come back at the next refresh on the timer, or one a connection failure brings forward.

`yb_servers()` gives each server two addresses: `host`, on the cluster's network, and `public_ip`. eysql chooses between them as the JDBC driver does. After a discovery it resolves the name it connected to, and each server's `host` and `public_ip`, and compares the addresses, server by server in the order `yb_servers()` lists them. The first server whose `host` is the address that answered means `host`; one whose `public_ip` is means `public_ip`, since the client is outside the cluster's network; and one whose `host` and `public_ip` are the same address means `host`. That decision is final for the pool. Until a discovery makes it, eysql guesses as the driver does: each server's `public_ip` when every server has one and every one resolves, and `host` otherwise, looking again at each discovery. A server that has no `public_ip` is reached at its `host` either way.

A name that does not resolve matches nothing. If the name the discovery connected to does not resolve, the driver fails that refresh, and eysql tries the next host instead. The lookups run in the discovery's own process, all at once, and one still running after `connect_timeout` counts as a name that does not resolve.

A discovery asks the seed hosts first, each once, in the order given, and then the servers it knows that are not down, as the JDBC driver's refresh does. It never dials a server that is down.

Node types and topology keys apply only once a discovery has succeeded, because seed hosts carry neither. `eysql_cluster:connect/1` waits for the first discovery, as the JDBC driver runs its first refresh before it picks a server, so the pool's first connections do not all land on the seed name. Until a discovery has succeeded, and on PostgreSQL, the seeds are all there is, and every mode takes them, as the driver makes a plain connection to the hosts in its URL when its refresh fails.

### When a server is left out

A failure to connect marks a server down: the failures pgjdbc reports as SQLSTATE 08001, `CONNECTION_UNABLE_TO_CONNECT`. In epgsql and OTP terms those are `econnrefused`, `timeout`, `etimedout`, `nxdomain`, `ehostunreach`, `ehostdown`, `enetunreach`, `enetdown`, `econnreset`, `econnaborted`, `epipe`, `enotconn` and `eaddrnotavail`; the socket closing or failing before the session is ready (`closed`, `sock_closed`, `{sock_error, _}`, and `{connection_lost, _}` while eysql sets `statement_timeout` or checks `target_session_attrs`); and a server error whose SQLSTATE is 08001. `eysql_error:connect_failure/1` holds the list.

A server that answers and fails the connection otherwise is not down, and `cluster_info` lists it under `rejected`. That covers a failed login (28P01, 28000), a missing database (3D000), too many connections (53300), a server starting up or shutting down (57P03), any other server error, `ssl_not_available`, and a failed TLS handshake (`{ssl_negotiation_failed, _}`, which pgjdbc reports as 08006). The connection moves on to another server. The server is then left out of new connections as one that is down is, and probed in the same way, until it takes connections again. The JDBC driver skips such a server only for that one connection; see [Differences](#differences-from-the-yugabytedb-smart-drivers). A server restarted gracefully shows why: while it shuts down it answers new connections with 57P03, and once it has stopped it refuses them. It is out of new connections throughout, first rejected and then down, and it takes connections again from the first refresh after it is back. YugabyteDB drops a server from `yb_servers()` once it has been unreachable for a minute, and one that comes back after that starts afresh.

Nothing that happens on an open connection leaves its server out. A query that fails, one that `statement_timeout` cancels (57014), one that `socket_timeout` cuts off, and a connection that dies all leave the server where it was. One slow or cancelled query never takes a server out of rotation. A connection that dies is replaced, and its server is left out only if the replacement's connect fails as above.

A server that has failed is left out of new connections until a refresh finds it back, as the drivers leave out a server that is down: they mark it down, and un-mark it at the first refresh after `failed_host_reconnect_delay_secs`. Refreshes come every `yb_servers_refresh_interval`, early after a connection fails, and on PostgreSQL too, where there is nothing to discover. At each refresh eysql probes every failed server, whether down, a standby or rejected, whose delay has passed: it opens a connection in the background, checks it as `target_session_attrs` asks, and closes it. A probe that succeeds, or any connection to the server that opens, ends the failure. A probe that fails, or runs longer than twice `connect_timeout`, starts another delay, and the server waits for the first refresh after that. What the probe's error is decides what the server counts as: a server that was down and now answers with "starting up" is rejected, and one that answered "shutting down" and now refuses connections is down. Either way it stays out. There is at most one probe per server at a time, and its connection is not counted. So a server that stays failed is tried once per refresh, not once per delay, and an application connection tries it only if it is a seed, as the last resort above. A server that is back takes connections from the first refresh after its delay, up to `yb_servers_refresh_interval` later; a shorter interval brings it back sooner. Until then, `cluster_info` lists it under `failed`, `rejected` or `read_only`.

A discovery that fails leaves the refresh due, as the JDBC driver's failed refresh does: the next connection eysql opens starts another, though not within a second of the last refresh, and the timer comes back after the full interval, not sooner. Each of those refreshes probes the failed servers that are due, and a discovery that succeeds probes the due ones at once. During an outage, then, discovery is retried at most once a second while connections are being opened, and once per interval while none are. Servers take connections again within seconds of the cluster coming back.

With `target_session_attrs => read_write`, a PostgreSQL standby is reachable but turns the connection away. eysql leaves it out and probes it in the same way, so a promotion is found at the next refresh, or at once through the seeds when the primary fails. `cluster_info` lists it under `read_only`, not `failed`.

A server that fails is logged once, as a warning that gives the reason, and a standby once, at info. Further failures are not logged until a connection to the server, a probe's included, has opened since its last failure and a minute has passed since that failure. That is logged at info, and a failure after it is logged again. A server that turns from down to standby or rejected, or back, is logged again, once, in words that say which: a failed login is never reported as a server that is down. Discovery forgets the failures of servers it no longer lists, so a server that leaves and comes back starts afresh. Failed discoveries follow the same rules: one warning for a run of them, and one line at info once a discovery has succeeded and a minute has passed since the last failure.

## The pool

The pool keeps `pool_size` connections open, spread as above. It opens each one in a short-lived process, so a slow connect never blocks a checkout. It starts all of them at once; before the first discovery has finished they wait for it in `eysql_cluster:open/1`, and checkouts wait for them.

- **A connection dies.** It is replaced straight away.
- **A caller dies holding a connection.** The pool closes that connection, since its state is unknown, and opens another.
- **Returning a connection.** Only the process that checked a connection out can check it in or discard it. The pool ignores `checkin/2` and `discard/2` from any other process, including a late one from an earlier holder.
- **Recycling.** Every connection is replaced after `max_lifetime`. An idle connection past it closes at the next rebalance tick, or when a checkout reaches it first, and a leased one when it comes back. The smart drivers only balance new connections, so without this a server that comes back would stay empty.
- **Rebalancing.** Every `rebalance_interval`, the pool moves up to `rebalance_batch` connections, busy ones included. A rebalance asks the cluster process where connections should be; it sends nothing to any server.
  - First it moves connections off servers it no longer keeps. An idle one closes at once. A leased one is marked to close when it comes back, and the mark lapses if the server is kept again first.
  - A server is kept while it is in the level new connections would use, or in the level they would use if no server had failed. For this, a failed server counts as up again only once a connection to it has opened, as a rule a probe's at a refresh. The end of its delay is not enough. So a failure moves nothing, and neither does a server that is still down when its delay ends. While a preferred server is down, it keeps its connections, and those opened on the fallback level meanwhile stay too, however long it stays down. Once it answers again, the first refresh after its delay finds it, and the fallback's connections move back to it from the next rebalance, `rebalance_batch` at a time.
  - The seeds, in the order given, are kept while no discovered server is up: the first seed, and the first seed that is up. Seeds behind a load balancer therefore keep their connections while the addresses the servers advertise are unreachable, and after a cluster-wide blip the servers keep theirs. Once an earlier seed is up again, connections on a later one move back to it. While no server or seed is up, nothing moves. A server that discovery drops is not kept, and nor is a seed in the modes that leave the seeds out once servers are discovered (see above). With `load_balance` off the hosts are the seeds, so connections gather on the first host that works, and move back to it once a refresh's probe finds it back.
  - With nothing to move off, the pool moves connections from the busiest server new connections may go to, when it holds two or more above the quietest. Failed servers, standbys and rejected servers take no part in that comparison: they hold few connections or none, and moving connections towards them would only reopen them where they were. So while new connections go to the seeds regardless of their failures, or with `load_balance` off, nothing moves this way.
- **Open transactions.** `with_connection/2,3`, `equery/3`, `squery/2` and `transaction/2,3` check the connection's transaction status when they give it back. The status is what epgsql last heard from the server, read inside the connection process, so the check costs no round trip and copies none of the connection's state. A connection left inside a transaction, such as BEGIN without COMMIT or ROLLBACK or a failed transaction, is closed instead of returned, and so is one whose status cannot be read. Closing it rolls the transaction back, and the pool opens another. `checkin/2` returns a connection as it is: code that checks connections out itself must COMMIT or ROLLBACK first, or call `discard/2`. `eysql_conn:transaction_status/1` tells whether a transaction is open.
- **The pool stops.** If the pool stops while `with_connection/2,3`, `equery/3`, `squery/2` or `transaction/2,3` holds one of its connections, for example because its cluster process died, the call still returns. The pool closes its connections as it stops, so the result is usually a lost-connection error. A transaction does not retry on a stopped pool; it returns the error that made it want to retry.

`eysql:stats(Pool)` counts the connections: `size` (the target), `idle`, `leased`, `opening`, and `waiting` checkouts. `draining` counts the leased connections marked to close when they come back, because their server is no longer kept. `by_host` gives the open connections per server. `eysql:cluster_info(Pool)` shows the servers, `placement` (where new connections go now), `allowed` (the servers the pool keeps connections on), the connection count per server (`counts`), the servers that are down, having failed to connect and not accepted a connection since (`failed`), the standbys that `target_session_attrs => read_write` turned away (`read_only`), and the servers that answered but failed a connection otherwise (`rejected`).

YugabyteDB's built-in Connection Manager works with this. eysql picks the server, and Connection Manager multiplexes connections onto backends inside it.

### Dead connections

eysql does not ping servers. Neither do the smart drivers: the JDBC driver and the Go driver have no health check, and periodic checks would add traffic that grows with every client and every server. The pool finds a dead connection in two ways, as a driver's pool does:

- epgsql's connection process exits when its socket closes or fails, as it does when a server shuts down or restarts. The pool is linked to it and opens another at once.
- A query on a connection that is gone fails with a lost-connection error, such as `{error, {connection_lost, _}}` or `{error, closed}`. `with_connection/2,3`, `equery/3`, `squery/2` and `transaction/2,3` then read the connection's status as `unknown` and discard it, and the pool opens another.

A server that dies without closing its sockets, such as one whose machine loses power or whose network is cut, leaves idle connections that look alive until they are used, and a query on one waits until TCP gives up, about 15 minutes on Linux. A server that accepts connections but hangs never answers either. `statement_timeout` does not help there: the server enforces it, so it ends a query on a server that is slow but still running, and not one on a server that has died or is stuck too hard to cancel its own queries. epgsql has no timeout of its own on the client side.

Set `socket_timeout` so a query on a server that has died silently fails instead of hanging. When a call has waited that long, eysql kills the connection's process, which closes its socket, and the call returns `{error, {connection_lost, socket_timeout}}`, which `eysql_error:classify/1` sorts as `connection_lost`. The pool then opens another connection. Nothing is sent to the server for this: each call starts a timer and cancels it when the answer comes, which costs well under a microsecond. Set it above your longest query, and above `statement_timeout`, so that the server cancels a slow query on a live server first.

None of this leaves the server out: only a failed connect does that. A timeout on an open connection starts no refresh either.

epgsql 4.8 turns TCP keepalive on when `epgsql_opts` has no `tcp_opts`, and eysql adds no TCP options of its own. The operating system's keepalive timers then apply, which on Linux wait two hours before the first probe. To find a peer that has gone silent sooner, pass `tcp_opts` in `epgsql_opts`, with `{keepalive, true}` and raw options for the timers. `tcp_opts` replaces epgsql's defaults, so include `{keepalive, true}` yourself. Keepalive finds a machine or network that has gone; it does not find a server process that hangs while its machine still answers.

## Transactions

`eysql:transaction(Pool, Fun)` runs `Fun(Conn)` between `BEGIN` and `COMMIT`. `Fun` returns one of:

- `{ok, Value}` to commit;
- `{rollback, Reason}` to roll back;
- `{error, Reason}` to roll back, usually passing on an epgsql error.

The whole transaction runs again, on a fresh connection, after a serialization failure (40001, which YugabyteDB also uses for its conflicts and read restarts), a deadlock (40P01), or a lost connection before `COMMIT`. Nothing was committed in any of those cases. `Fun` must not have side effects outside the database. Before each retry eysql waits a random time: up to 20 ms before the second attempt, twice that before the third, and so on, but never more than `max_backoff` milliseconds.

If a retry cannot get a connection within `checkout_timeout`, the transaction stops and returns the error that caused the retry, such as the serialization failure. `{error, checkout_timeout}` therefore means the transaction never ran.

If the connection is lost during `COMMIT`, the result is `{error, commit_outcome_unknown}`. The transaction may or may not have committed; check, for example by reading back a row it wrote. A call that `socket_timeout` cuts off counts as a lost connection: before `COMMIT`, the transaction runs again on a fresh connection, and during it, the outcome is unknown. The bound covers `BEGIN`, `COMMIT`, `ROLLBACK`, and the queries `Fun` makes with `eysql_conn:equery/3` and `squery/2`. Options: `attempts` (3), `checkout_timeout` (5000), `max_backoff` (500) and `begin` (for example `"BEGIN ISOLATION LEVEL REPEATABLE READ"`).

`eysql_error:classify/1` sorts any epgsql error into `retryable`, `connection_lost` or `other`, for code that manages its own transactions.

## Differences from the YugabyteDB smart drivers

eysql follows the JDBC smart driver, `com.yugabyte.ysql` in [yugabyte/pgjdbc](https://github.com/yugabyte/pgjdbc), where the drivers differ.

What matches:

- The option names, units, meanings and defaults of `load_balance` (off), `topology_keys`, `fallback_to_topology_keys_only`, `yb_servers_refresh_interval` (300 seconds, 0 to 600) and `failed_host_reconnect_delay_secs` (a fixed 5 seconds, 0 to 60), including the drivers' `load_balance` spellings and refresh interval 0. Topology keys match without regard to case. `connect_timeout` defaults to pgjdbc's 10 seconds, `target_session_attrs` to `any`, as `targetServerType` does, and `database` to the username.
- Discovery with `yb_servers()`, asking the configured hosts first and then the servers not marked down. The first discovery runs before the first connection is placed.
- Choosing each server's `host` or `public_ip` by resolved address, once per cluster, and guessing `public_ip` until then when every server has one that resolves (`LoadBalanceService.refresh`).
- The order of node selection, `prefer_primary` and `prefer_rr` included, and least-loaded placement with counts kept per client, a tie going to one server at random.
- A connection tries every eligible server, each once, and then the hosts it was given, in order, as the driver falls back to a plain connection to the hosts in its URL. Under `only_primary`, `only_rr`, and `fallback_to_topology_keys_only` with topology keys, it fails with `no_node_available` instead once servers are discovered, as the driver throws.
- With `load_balance` off, the hosts are tried in the order given, as pgjdbc does with `loadBalanceHosts` off.
- Only a failure to connect (SQLSTATE 08001) marks a server down. Nothing that happens on an open connection leaves a server out.
- Servers that are down are left out until the first refresh after their delay. A connection failure brings the next refresh forward, and a connection that finds no server does not. A failed discovery leaves the refresh due, so the next connection opened tries it again.
- No health checks. Neither the JDBC driver nor the Go driver pings servers.
- `socket_timeout`, off by default as pgjdbc's `socketTimeout` is, closes the connection when it runs out, as pgjdbc does.

What eysql adds:

- A pool. The drivers only place new connections and leave pooling to libraries such as HikariCP. eysql's pool recycles connections after `max_lifetime` and rebalances them across servers.
- Transaction retries on serialization failures and deadlocks, with `commit_outcome_unknown` when a COMMIT's outcome is lost.
- A doubling failed-host delay, if you set `failed_host_max_delay_secs`.
- Probes of failed servers at refreshes, so the pool moves connections back to a server once it takes connections again, and never towards one that has not.
- `target_session_attrs => read_write` for PostgreSQL with several hosts, with standbys listed apart from servers that are down.
- One warning per run of failures, for each server and for discovery, where the JDBC driver logs each failure.

Other differences, each decided on purpose:

- **Retries during an outage.** While discovery keeps failing, eysql retries it when connections are opened, and probes the servers left out that are due at each of those refreshes; a discovery that succeeds probes the due ones at once. The JDBC driver un-marks a down server only inside a refresh that succeeded, and dials only the hosts in its URL and the servers it has not marked down. After a full outage, connections should come back within seconds, not after the 300-second refresh interval.
- **A second between refresh retries.** eysql retries a failed discovery at most once a second, however many connections are being opened. The JDBC driver retries on every connection request. A busy pool must not hammer a dead cluster.
- **`username` defaults to `yugabyte`.** pgjdbc takes the operating-system user, which Erlang cannot read reliably, least of all in a container.
- **TLS is off by default.** pgjdbc defaults to `sslmode=prefer`, which tries TLS without verifying the certificate. eysql never ships a default less secure than upstream's secure behaviour: OTP 26 and later verify the certificate, which needs a CA, so TLS is something you turn on with one. See [TLS](#tls).
- **A probe tests a failed server.** At the first refresh after its delay, eysql connects to a failed server, whether down, a standby or rejected, to find out whether it is back. The JDBC driver un-marks a server that is down there instead, and lets the next application connection find out. The probe lets the pool move connections back only towards a server that has taken a connection, never towards one that is still failing.
- **A server that turns connections away stays out.** A server that answers but fails the connection otherwise, with a failed login, too many connections (53300), or "starting up" or "shutting down" (57P03), is left out of new connections as one that is down is, and probed at each refresh past its delay until it takes connections again. The JDBC driver skips such a server only for the one connection request, and its next request tries it again. A server that turns connections away, for example while it restarts, thus stays out of the serving pool until a probe finds it accepting connections again, and comes back within one refresh rather than when a pooled connection next reaches `max_lifetime`. After a password rotation, the probes add one failing login per server per refresh.
- **`socket_timeout` bounds a whole call, not each read.** pgjdbc's `socketTimeout` bounds each read from the socket, so a result that keeps arriving, however slowly, never trips it. eysql's bounds the call from start to finish, `epgsql:equery/3`'s parse and execute together, as the Go driver's context deadline does. epgsql reads the socket in its own process and has no hook to time one read, so matching pgjdbc would mean changing epgsql. It bounds calls on open connections only: pgjdbc sets `socketTimeout` before the startup message, so it also bounds the login, where eysql has `connect_timeout` for the TCP connect and the TLS handshake. eysql's option is in milliseconds, as its other durations are, and off is `infinity`, not 0.

Differences not yet settled:

- Invalid values stop the pool from starting. The JDBC driver logs them and uses the default.
- Discovery runs in the background, and after the first one no connection waits for it. The JDBC driver refreshes inside `getConnection`, one refresh at a time, and when a refresh that is due fails, it makes that connection a plain one to the hosts in its URL, in every mode. eysql goes on placing connections among the servers it knows.
- Each discovery opens a connection of its own and closes it. The JDBC driver keeps one control connection per cluster for its refreshes, and marks a server down when `yb_servers()` fails on it. eysql's discovery marks nothing.
- With `load_balance` off, a server left out stays out until a refresh probes it. pgjdbc's plain connection skips any host that failed, for any reason, in the last `hostRecheckSeconds` (10 seconds).
- `application_name` defaults to `eysql`, the library's own name, as pgjdbc's defaults to its own. eysql does not read `.pgpass`.

## PostgreSQL

With PostgreSQL, `yb_servers()` does not exist. With `load_balance` off, the default, eysql never asks for it. With it on, eysql notices on the first discovery and uses the configured hosts from then on, in the order given, whatever the mode. With several hosts and `target_session_attrs => read_write`, it connects to the first that accepts writes.

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

The integration suite includes stopping a YugabyteDB node under load and starting it again. On both engines it also checks that a connection left inside a transaction, or a failed one, is closed rather than handed to the next holder, that a query in flight when its pool stops returns an error, and that `socket_timeout` cuts off a `pg_sleep` and the pool recovers. `eysql_driver` is the behaviour the fake driver implements; the `driver` option swaps it in.

## Compatibility

- OTP 26 to 29.
- epgsql 4.7 and later.
- YugabyteDB: any release with `yb_servers()`, the function the smart drivers use. Tested against 2026.1.
- PostgreSQL: tested against 17.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
