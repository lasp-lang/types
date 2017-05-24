%%
%% Copyright (c) 2015-2016 Christopher Meiklejohn.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Single-assignment variable.
%%      Write once register.

-module(state_ivar).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-behaviour(type).
-behaviour(state_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_bottom/1,
         is_inflation/2, is_strict_inflation/2,
         irreducible_is_strict_inflation/2]).
-export([join_decomposition/1, delta/2, digest/1]).
-export([encode/2, decode/2]).

-export_type([state_ivar/0, state_ivar_op/0]).

-opaque state_ivar() :: {?TYPE, payload()}.
-type payload() :: term().
-type state_ivar_op() :: {set, term()}.

%% @doc Create a new `state_ivar()'
-spec new() -> state_ivar().
new() ->
    {?TYPE, undefined}.

%% @doc Create a new, empty `state_ivar()'
-spec new([term()]) -> state_ivar().
new([]) ->
    new().

%% @doc Mutate a `state_ivar()'.
-spec mutate(state_ivar_op(), type:id(), state_ivar()) ->
    {ok, state_ivar()}.
mutate({set, Value}, _Actor, {?TYPE, undefined}) ->
    {ok, {?TYPE, Value}}.

%% @doc Delta-mutate a `state_ivar()'.
-spec delta_mutate(state_ivar_op(), type:id(), state_ivar()) ->
    {ok, state_ivar()}.
delta_mutate(Op, Actor, {?TYPE, _}=Var) ->
    {ok, {?TYPE, Value}} = mutate(Op, Actor, Var),
    {ok, {?TYPE, Value}}.

%% @doc Returns the value of the `state_ivar()'.
-spec query(state_ivar()) -> term().
query({?TYPE, Value}) ->
    Value.

%% @doc Merge two `state_ivar()'.
-spec merge(state_ivar(), state_ivar()) -> state_ivar().
merge({?TYPE, undefined}, {?TYPE, undefined}) ->
    {?TYPE, undefined};
merge({?TYPE, Value}, {?TYPE, undefined}) ->
    {?TYPE, Value};
merge({?TYPE, undefined}, {?TYPE, Value}) ->
    {?TYPE, Value};
merge({?TYPE, Value}, {?TYPE, Value}) ->
    {?TYPE, Value}.

%% @doc Equality for `state_ivar()'.
-spec equal(state_ivar(), state_ivar()) -> boolean().
equal({?TYPE, Value1}, {?TYPE, Value2}) ->
    Value1 == Value2.

%% @doc Check if an IVar is bottom.
-spec is_bottom(state_ivar()) -> boolean().
is_bottom({?TYPE, Value}) ->
    Value == undefined.

%% @doc Given two `state_ivar()', check if the second is and inflation
%%      of the first.
%%      The second `state_ivar()' is and inflation if the first set is
%%      a subset of the second.
-spec is_inflation(state_ivar(), state_ivar()) -> boolean().
is_inflation({?TYPE, undefined}, {?TYPE, undefined}) ->
    true;
is_inflation({?TYPE, undefined}, {?TYPE, _Value}) ->
    true;
is_inflation({?TYPE, _Value}, {?TYPE, undefined}) ->
    false;
is_inflation({?TYPE, _}=Var1, {?TYPE, _}=Var2) ->
    equal(Var1, Var2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_ivar(), state_ivar()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_ivar(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=A, B) ->
    state_type:irreducible_is_strict_inflation(A, B).

-spec digest(state_ivar()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_ivar()'.
-spec join_decomposition(state_ivar()) -> [state_ivar()].
join_decomposition({?TYPE, _}=Var) ->
    [Var].

%% @doc Delta calculation for `state_ivar()'.
-spec delta(state_ivar(), state_type:digest()) -> state_ivar().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_ivar()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_ivar().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, undefined}, new()).

query_test() ->
    Var0 = new(),
    Var1 = {?TYPE, <<"a">>},
    ?assertEqual(undefined, query(Var0)),
    ?assertEqual(<<"a">>, query(Var1)).

set_test() ->
    Actor = 1,
    Var0 = new(),
    {ok, Var1} = mutate({set, <<"a">>}, Actor, Var0),
    ?assertEqual({?TYPE, <<"a">>}, Var1).

merge_test() ->
    Var0 = new(),
    Var1 = {?TYPE, <<"a">>},
    Var2 = merge(Var0, Var0),
    Var3 = merge(Var0, Var1),
    Var4 = merge(Var1, Var0),
    Var5 = merge(Var1, Var1),
    ?assertEqual({?TYPE, undefined}, Var2),
    ?assertEqual({?TYPE, <<"a">>}, Var3),
    ?assertEqual({?TYPE, <<"a">>}, Var4),
    ?assertEqual({?TYPE, <<"a">>}, Var5).

equal_test() ->
    Var0 = new(),
    Var1 = {?TYPE, [<<"a">>]},
    Var2 = {?TYPE, [<<"b">>]},
    ?assert(equal(Var0, Var0)),
    ?assertNot(equal(Var0, Var1)),
    ?assert(equal(Var1, Var1)),
    ?assertNot(equal(Var1, Var2)).

is_bottom_test() ->
    Var0 = new(),
    Var1 = {?TYPE, [<<"a">>]},
    ?assert(is_bottom(Var0)),
    ?assertNot(is_bottom(Var1)).

is_inflation_test() ->
    Var0 = new(),
    Var1 = {?TYPE, [<<"a">>]},
    Var2 = {?TYPE, [<<"b">>]},
    ?assert(is_inflation(Var0, Var0)),
    ?assert(is_inflation(Var0, Var1)),
    ?assert(is_inflation(Var1, Var1)),
    ?assertNot(is_inflation(Var1, Var2)),
    ?assertNot(is_inflation(Var2, Var1)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Var0, Var0)),
    ?assert(state_type:is_inflation(Var0, Var1)),
    ?assert(state_type:is_inflation(Var1, Var1)).

is_strict_inflation_test() ->
    Var0 = new(),
    Var1 = {?TYPE, [<<"a">>]},
    Var2 = {?TYPE, [<<"b">>]},
    ?assertNot(is_strict_inflation(Var0, Var0)),
    ?assert(is_strict_inflation(Var0, Var1)),
    ?assertNot(is_strict_inflation(Var1, Var1)),
    ?assertNot(is_strict_inflation(Var1, Var2)),
    ?assertNot(is_strict_inflation(Var2, Var1)).

join_decomposition_test() ->
    Var0 = new(),
    Var1 = {?TYPE, <<"a">>},
    Decomp0 = join_decomposition(Var0),
    Decomp1 = join_decomposition(Var1),
    ?assertEqual([{?TYPE, undefined}], Decomp0),
    ?assertEqual([{?TYPE, <<"a">>}], Decomp1).

encode_decode_test() ->
    Var = {?TYPE, [<<"a">>]},
    Binary = encode(erlang, Var),
    EVar = decode(erlang, Binary),
    ?assertEqual(Var, EVar).

-endif.
