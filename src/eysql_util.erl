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

%% @private
%% @doc Helpers the other modules share: the clock the cluster and the pool
%% both read, counters kept in maps, hiding secrets from status and crash
%% reports, and the one rule for what counts as text in the options.
-module(eysql_util).

-export([now_ms/0,
         incr/2,
         decr/2,
         redact_config/1,
         redact/1,
         redact_reason/1,
         text/1
        ]).

%% @doc Monotonic time in milliseconds, for deadlines and intervals.
-spec now_ms() -> integer().
now_ms() -> erlang:monotonic_time(millisecond).

%% @doc Add one to `Key''s count, starting it at 1.
-spec incr(Key, #{Key => pos_integer()}) -> #{Key => pos_integer()}.
incr(Key, Map) -> maps:update_with(Key, fun(N) -> N + 1 end, 1, Map).

%% @doc Take one from `Key''s count. A count that reaches 0 is removed, and a
%% missing key is left missing.
-spec decr(Key, #{Key => pos_integer()}) -> #{Key => pos_integer()}.
decr(Key, Map) ->
    case maps:find(Key, Map) of
        {ok, N} when N > 1 -> maps:put(Key, N - 1, Map);
        {ok, _} -> maps:remove(Key, Map);
        error -> Map
    end.

%% @doc The config with its secrets replaced by `redacted', for printing.
%% In its `settings', the password always goes, and `ssl_opts' and
%% `epgsql_opts' go whole unless they are empty: both can hold key
%% passwords, keys and other credentials. An `after_connect' given as
%% `{Module, Function, Args}' loses its `Args', which the application may
%% have put a credential in; a fun prints without the values it captured,
%% and stays as it is.
-spec redact_config(map()) -> map().
redact_config(#{settings := Settings} = Config) when is_map(Settings) ->
    redact_hook(Config#{settings := maps:map(fun redact_setting/2, Settings)});
redact_config(Config) ->
    Config.

redact_hook(#{after_connect := {Module, Function, _Args}} = Config) ->
    Config#{after_connect := {Module, Function, redacted}};
redact_hook(Config) ->
    Config.

redact_setting(password, _Password) -> redacted;
redact_setting(ssl_opts, []) -> [];
redact_setting(ssl_opts, _Options) -> redacted;
redact_setting(epgsql_opts, Options) when map_size(Options) =:= 0 -> Options;
redact_setting(epgsql_opts, _Options) -> redacted;
redact_setting(_Name, Value) -> Value.

%% @doc `Term' with every config in it redacted, wherever it sits: in a
%% process's state, a message, or a crash reason and its stack trace. Any map
%% with a `settings' map counts, which takes in the connect specs the cluster
%% hands out as well as configs.
-spec redact(term()) -> term().
redact(#{settings := Settings} = Config) when is_map(Settings) ->
    redact_config(Config);
redact(Map) when is_map(Map) ->
    maps:map(fun(_Key, Value) -> redact(Value) end, Map);
redact([Head | Tail]) ->
    [redact(Head) | redact(Tail)];
redact(Tuple) when is_tuple(Tuple) ->
    list_to_tuple(redact(tuple_to_list(Tuple)));
redact(Term) ->
    Term.

%% @doc A failure's reason as it may be logged: {@link redact/1}, and the
%% password hidden wherever it sits in a proplist, as `{password, _}', and in
%% a map, such as epgsql's connect options, under a `password' key, atom or
%% binary. A connect error can echo options, as an ssl option error does the
%% option it rejects.
-spec redact_reason(term()) -> term().
redact_reason(Reason) ->
    hide(redact(Reason)).

hide({password, _}) -> {password, redacted};
hide([Head | Tail]) -> [hide(Head) | hide(Tail)];
hide(Tuple) when is_tuple(Tuple) -> list_to_tuple(hide(tuple_to_list(Tuple)));
hide(Map) when is_map(Map) -> maps:map(fun hide/2, Map);
hide(Term) -> Term.

hide(Key, _Value) when Key =:= password; Key =:= <<"password">> -> redacted;
hide(_Key, Value) -> hide(Value).

%% @doc `Value' as a UTF-8 binary, if it is Unicode text: a string, a UTF-8
%% binary, or a list mixing the two, as `unicode:characters_to_binary/1'
%% takes them. Anything else is `error': an atom or a number, a binary that
%% is not UTF-8, such as Latin-1 bytes above 127, or a list holding a
%% surrogate, a code point beyond Unicode, or a term that is neither a code
%% point nor a binary.
%%
%% Every option that is text, the topology keys and their names follow this
%% rule, so a value is accepted or refused the same way wherever it is given.
%% The password is the exception: it need not be text, so eysql_config keeps
%% a binary password as it is and uses this only for a string.
-spec text(term()) -> {ok, binary()} | error.
text(Value) when is_binary(Value); is_list(Value) ->
    %% characters_to_binary/1 raises badarg for what is not chardata at all,
    %% such as an improper list, and returns a tuple for invalid code points
    %% or UTF-8.
    try unicode:characters_to_binary(Value) of
        Binary when is_binary(Binary) -> {ok, Binary};
        _ -> error
    catch
        error:badarg -> error
    end;
text(_Value) ->
    error.
