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

%% @doc Max Int CRDT.
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, Alcino Cunha and Carla Ferreira
%%      Composition of State-based CRDTs (2015)
%%      [http://haslab.uminho.pt/cbm/files/crdtcompositionreport.pdf]

-module(state_max_int).
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

-export_type([state_max_int/0, state_max_int_op/0]).

-opaque state_max_int() :: {?TYPE, payload()}.
-type payload() :: non_neg_integer().
-type state_max_int_op() :: increment.

%% @doc Create a new `state_max_int()'
-spec new() -> state_max_int().
new() ->
    {?TYPE, 0}.

%% @doc Create a new `state_max_int()'
-spec new([term()]) -> state_max_int().
new([]) ->
    new().

%% @doc Mutate a `state_max_int()'.
-spec mutate(state_max_int_op(), type:id(), state_max_int()) ->
    {ok, state_max_int()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_max_int()'.
%%      The first argument can only be `increment'.
%%      Returns a `state_max_int()' delta which is a new `state_max_int()'
%%      with the value incremented by one.
-spec delta_mutate(state_max_int_op(), type:id(), state_max_int()) ->
    {ok, state_max_int()}.
delta_mutate(increment, _Actor, {?TYPE, Value}) ->
    {ok, {?TYPE, Value + 1}}.

%% @doc Returns the value of the `state_max_int()'.
-spec query(state_max_int()) -> non_neg_integer().
query({?TYPE, Value}) ->
    Value.

%% @doc Merge two `state_max_int()'.
%%      Join is the max function.
-spec merge(state_max_int(), state_max_int()) -> state_max_int().
merge({?TYPE, Value1}, {?TYPE, Value2}) ->
    {?TYPE, max(Value1, Value2)}.

%% @doc Equality for `state_max_int()'.
-spec equal(state_max_int(), state_max_int()) -> boolean().
equal({?TYPE, Value1}, {?TYPE, Value2}) ->
    Value1 == Value2.

%% @doc Check if a Max Int is bottom.
-spec is_bottom(state_max_int()) -> boolean().
is_bottom({?TYPE, Value}) ->
    Value == 0.

%% @doc Given two `state_max_int()', check if the second is an inflation
%%      of the first.
%%      The second is an inflation if its value is greater or equal
%%      to the value of the first.
-spec is_inflation(state_max_int(), state_max_int()) -> boolean().
is_inflation({?TYPE, Value1}, {?TYPE, Value2}) ->
    Value1 =< Value2.

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_max_int(), state_max_int()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_max_int(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=A, B) ->
    state_type:irreducible_is_strict_inflation(A, B).

-spec digest(state_max_int()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_max_int()'.
-spec join_decomposition(state_max_int()) -> [state_max_int()].
join_decomposition({?TYPE, _}=MaxInt) ->
    [MaxInt].

%% @doc Delta calculation for `state_max_int()'.
-spec delta(state_max_int(), state_type:digest()) -> state_max_int().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_max_int()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_max_int().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, 0}, new()).

query_test() ->
    MaxInt0 = new(),
    MaxInt1 = {?TYPE, 17},
    ?assertEqual(0, query(MaxInt0)),
    ?assertEqual(17, query(MaxInt1)).

delta_increment_test() ->
    Actor = 1,
    MaxInt0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate(increment, Actor, MaxInt0),
    MaxInt1 = merge({?TYPE, Delta1}, MaxInt0),
    {ok, {?TYPE, Delta2}} = delta_mutate(increment, Actor, MaxInt1),
    MaxInt2 = merge({?TYPE, Delta2}, MaxInt1),
    ?assertEqual({?TYPE, 1}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, 1}, MaxInt1),
    ?assertEqual({?TYPE, 2}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, 2}, MaxInt2).

increment_test() ->
    Actor = 1,
    MaxInt0 = {?TYPE, 15},
    {ok, MaxInt1} = mutate(increment, Actor, MaxInt0),
    {ok, MaxInt2} = mutate(increment, Actor, MaxInt1),
    ?assertEqual({?TYPE, 16}, MaxInt1),
    ?assertEqual({?TYPE, 17}, MaxInt2).

merge_idempotent_test() ->
    MaxInt1 = {?TYPE, 1},
    MaxInt2 = {?TYPE, 17},
    MaxInt3 = merge(MaxInt1, MaxInt1),
    MaxInt4 = merge(MaxInt2, MaxInt2),
    ?assertEqual(MaxInt1, MaxInt3),
    ?assertEqual(MaxInt2, MaxInt4).

merge_commutative_test() ->
    MaxInt1 = {?TYPE, 1},
    MaxInt2 = {?TYPE, 17},
    MaxInt3 = merge(MaxInt1, MaxInt2),
    MaxInt4 = merge(MaxInt2, MaxInt1),
    ?assertEqual({?TYPE, 17}, MaxInt3),
    ?assertEqual({?TYPE, 17}, MaxInt4).

merge_deltas_test() ->
    MaxInt1 = {?TYPE, 1},
    Delta1 = {?TYPE, 17},
    Delta2 = {?TYPE, 23},
    MaxInt2 = merge(Delta1, MaxInt1),
    MaxInt3 = merge(MaxInt1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, 17}, MaxInt2),
    ?assertEqual({?TYPE, 17}, MaxInt3),
    ?assertEqual({?TYPE, 23}, DeltaGroup).

equal_test() ->
    MaxInt1 = {?TYPE, 17},
    MaxInt2 = {?TYPE, 23},
    ?assert(equal(MaxInt1, MaxInt1)),
    ?assertNot(equal(MaxInt1, MaxInt2)).

is_bottom_test() ->
    MaxInt0 = new(),
    MaxInt1 = {?TYPE, 17},
    ?assert(is_bottom(MaxInt0)),
    ?assertNot(is_bottom(MaxInt1)).

is_inflation_test() ->
    MaxInt1 = {?TYPE, 23},
    MaxInt2 = {?TYPE, 42},
    ?assert(is_inflation(MaxInt1, MaxInt1)),
    ?assert(is_inflation(MaxInt1, MaxInt2)),
    ?assertNot(is_inflation(MaxInt2, MaxInt1)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(MaxInt1, MaxInt1)),
    ?assert(state_type:is_inflation(MaxInt1, MaxInt2)),
    ?assertNot(state_type:is_inflation(MaxInt2, MaxInt1)).

is_strict_inflation_test() ->
    MaxInt1 = {?TYPE, 23},
    MaxInt2 = {?TYPE, 42},
    ?assertNot(is_strict_inflation(MaxInt1, MaxInt1)),
    ?assert(is_strict_inflation(MaxInt1, MaxInt2)),
    ?assertNot(is_strict_inflation(MaxInt2, MaxInt1)).

join_decomposition_test() ->
    MaxInt1 = {?TYPE, 17},
    Decomp1 = join_decomposition(MaxInt1),
    ?assertEqual([{?TYPE, 17}], Decomp1).

encode_decode_test() ->
    MaxInt = {?TYPE, 17},
    Binary = encode(erlang, MaxInt),
    EMaxInt = decode(erlang, Binary),
    ?assertEqual(MaxInt, EMaxInt).

-endif.
