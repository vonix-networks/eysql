-module(eysql_test_util).

-export([config/1, three/0, key/1, wait_until/2, wait_until/3, refreshing_until/3,
         refreshing_until/4, fail/2, fail/3, reserve/2, resolve_as_is/1]).

%% A normalized config for the fake driver with short intervals, load
%% balancing on, as it is not by default. Names resolve to themselves, so
%% that no test asks DNS, and the address that answered discovery is some
%% server's host or public IP only when its name is.
config(Overrides) ->
    Base = #{hosts => [{<<"a">>, 5433}, {<<"b">>, 5433}, {<<"c">>, 5433}],
             driver => eysql_fake_driver,
             load_balance => true,
             yb_servers_refresh_interval => 300,
             failed_host_reconnect_delay_secs => 1,
             rebalance_interval => 100,
             max_lifetime => 600000,
             lifetime_jitter => 0
            },
    {ok, Config} = eysql_config:normalize(maps:merge(Base, Overrides)),
    Config#{resolve => fun ?MODULE:resolve_as_is/1}.

resolve_as_is(Name) -> {ok, Name}.

three() ->
    [eysql_fake_driver:server(<<"a">>, <<"gcp">>, <<"us-east1">>, <<"us-east1-b">>),
     eysql_fake_driver:server(<<"b">>, <<"gcp">>, <<"us-east1">>, <<"us-east1-c">>),
     eysql_fake_driver:server(<<"c">>, <<"gcp">>, <<"us-east1">>, <<"us-east1-d">>)].

key(Host) -> {Host, 5433}.

wait_until(Fun, What) -> wait_until(Fun, What, 5000).

wait_until(Fun, What, Timeout) when Timeout =< 0 ->
    erlang:error({timeout_waiting_for, What, Fun()});
wait_until(Fun, What, Timeout) ->
    case Fun() of
        true -> ok;
        _ -> timer:sleep(20), wait_until(Fun, What, Timeout - 20)
    end.

%% Wait until `Fun' holds, asking `Cluster' for a refresh before each look,
%% so that a failed host is probed as soon as its delay allows.
refreshing_until(Cluster, Fun, What) -> refreshing_until(Cluster, Fun, What, 5000).

refreshing_until(Cluster, Fun, What, Timeout) ->
    wait_until(fun() -> eysql_cluster:refresh(Cluster), Fun() end, What, Timeout).

%% Fail `Key' as a connect to it that could not be made fails it: reserve it
%% and report the reservation failed with `econnrefused', which marks it
%% down. As fail/3.
fail(Cluster, Key) -> fail(Cluster, Key, econnrefused).

%% As fail/2, the connect failing with `Reason'. Only a connect marks a
%% host, so this is how a test fails one it names.
fail(Cluster, Key, Reason) ->
    eysql_cluster:open_failed(Cluster, reserve(Cluster, Key), Reason).

%% A reservation of `Key': pick until the cluster offers it, giving back
%% every other host it offers first. The host must be one a pick can offer,
%% such as a working server, or a seed once the servers are all tried.
reserve(Cluster, Key) -> reserve(Cluster, Key, []).

reserve(Cluster, Key, Tried) ->
    case eysql_cluster:pick(Cluster, Tried) of
        {ok, _Server, _Spec, {_Ref, Key} = Reservation} ->
            Reservation;
        {ok, _Server, _Spec, {_Ref, Other} = Reservation} ->
            eysql_cluster:cancel(Cluster, Reservation),
            reserve(Cluster, Key, [Other | Tried]);
        {error, _} = Error ->
            erlang:error({not_offered, Key, Error})
    end.
