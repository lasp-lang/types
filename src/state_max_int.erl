%%
%% Copyright (c) 2015-2016 Christopher Meiklejohn.  All Rights Reserved.
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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
-export([query/1, equal/2, is_inflation/2, is_strict_inflation/2]).
-export([join_decomposition/1]).

-export_type([state_max_int/0, delta_state_max_int/0, state_max_int_op/0]).

-opaque state_max_int() :: {?TYPE, payload()}.
-opaque delta_state_max_int() :: {?TYPE, {delta, payload()}}.
-type delta_or_state() :: state_max_int() | delta_state_max_int().
-type payload() :: non_neg_integer().
-type state_max_int_op() :: increment.

%% @doc Create a new, empty `state_max_int()'
-spec new() -> state_max_int().
new() ->
    {?TYPE, 0}.

%% @doc Create a new, empty `state_max_int()'
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
    {ok, delta_state_max_int()}.
delta_mutate(increment, _Actor, {?TYPE, Value}) ->
    {ok, {?TYPE, {delta, Value + 1}}}.

%% @doc Returns the value of the `state_max_int()'.
-spec query(state_max_int()) -> non_neg_integer().
query({?TYPE, Value}) ->
    Value.

%% @doc Merge two `state_max_int()'.
%%      Uses max function.
-spec merge(delta_or_state(), delta_or_state()) -> delta_or_state().
merge({?TYPE, {delta, Delta1}}, {?TYPE, {delta, Delta2}}) ->
    {?TYPE, DeltaGroup} = ?TYPE:merge({?TYPE, Delta1}, {?TYPE, Delta2}),
    {?TYPE, {delta, DeltaGroup}};
merge({?TYPE, {delta, Delta}}, {?TYPE, CRDT}) ->
    merge({?TYPE, Delta}, {?TYPE, CRDT});
merge({?TYPE, CRDT}, {?TYPE, {delta, Delta}}) ->
    merge({?TYPE, Delta}, {?TYPE, CRDT});
merge({?TYPE, Value1}, {?TYPE, Value2}) ->
    {?TYPE, max(Value1, Value2)}.

%% @doc Equality for `state_max_int()'.
-spec equal(state_max_int(), state_max_int()) -> boolean().
equal({?TYPE, Value1}, {?TYPE, Value2}) ->
    Value1 == Value2.

%% @doc Given two `state_max_int()', check if the second is an inflation
%%      of the first.
%%      The second is an inflation if its value is greater or equal
%%      to the value of the first.
-spec is_inflation(delta_or_state(), state_max_int()) -> boolean().
is_inflation({?TYPE, {delta, Value1}}, {?TYPE, Value2}) ->
    is_inflation({?TYPE, Value1}, {?TYPE, Value2});
is_inflation({?TYPE, Value1}, {?TYPE, Value2}) ->
    Value1 =< Value2.

%% @doc Check for strict inflation.
-spec is_strict_inflation(delta_or_state(), state_max_int()) -> boolean().
is_strict_inflation({?TYPE, {delta, Value1}}, {?TYPE, Value2}) ->
    is_strict_inflation({?TYPE, Value1}, {?TYPE, Value2});
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Join decomposition for `state_max_int()'.
-spec join_decomposition(state_max_int()) -> [state_max_int()].
join_decomposition({?TYPE, _}=MaxInt) ->
    [MaxInt].

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
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate(increment, Actor, MaxInt0),
    MaxInt1 = merge({?TYPE, Delta1}, MaxInt0),
    {ok, {?TYPE, {delta, Delta2}}} = delta_mutate(increment, Actor, MaxInt1),
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

merge_idempontent_test() ->
    MaxInt1 = {?TYPE, 1},
    MaxInt2 = {?TYPE, 17},
    MaxInt3 = merge(MaxInt1, MaxInt1),
    MaxInt4 = merge(MaxInt2, MaxInt2),
    ?assertEqual({?TYPE, 1}, MaxInt3),
    ?assertEqual({?TYPE, 17}, MaxInt4).

merge_commutative_test() ->
    MaxInt1 = {?TYPE, 1},
    MaxInt2 = {?TYPE, 17},
    MaxInt3 = merge(MaxInt1, MaxInt2),
    MaxInt4 = merge(MaxInt2, MaxInt1),
    ?assertEqual({?TYPE, 17}, MaxInt3),
    ?assertEqual({?TYPE, 17}, MaxInt4).

merge_deltas_test() ->
    MaxInt1 = {?TYPE, 1},
    Delta1 = {?TYPE, {delta, 17}},
    Delta2 = {?TYPE, {delta, 23}},
    MaxInt2 = merge(Delta1, MaxInt1),
    MaxInt3 = merge(MaxInt1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, 17}, MaxInt2),
    ?assertEqual({?TYPE, 17}, MaxInt3),
    ?assertEqual({?TYPE, {delta, 23}}, DeltaGroup).

equal_test() ->
    MaxInt1 = {?TYPE, 17},
    MaxInt2 = {?TYPE, 23},
    ?assert(equal(MaxInt1, MaxInt1)),
    ?assertNot(equal(MaxInt1, MaxInt2)).

is_inflation_test() ->
    MaxInt1 = {?TYPE, 23},
    DeltaMaxInt1 = {?TYPE, {delta, 23}},
    MaxInt2 = {?TYPE, 42},
    ?assert(is_inflation(MaxInt1, MaxInt1)),
    ?assert(is_inflation(MaxInt1, MaxInt2)),
    ?assert(is_inflation(DeltaMaxInt1, MaxInt1)),
    ?assert(is_inflation(DeltaMaxInt1, MaxInt2)),
    ?assertNot(is_inflation(MaxInt2, MaxInt1)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(MaxInt1, MaxInt1)),
    ?assert(state_type:is_inflation(MaxInt1, MaxInt2)),
    ?assertNot(state_type:is_inflation(MaxInt2, MaxInt1)).

is_strict_inflation_test() ->
    MaxInt1 = {?TYPE, 23},
    DeltaMaxInt1 = {?TYPE, {delta, 23}},
    MaxInt2 = {?TYPE, 42},
    ?assertNot(is_strict_inflation(MaxInt1, MaxInt1)),
    ?assert(is_strict_inflation(MaxInt1, MaxInt2)),
    ?assertNot(is_strict_inflation(DeltaMaxInt1, MaxInt1)),
    ?assert(is_strict_inflation(DeltaMaxInt1, MaxInt2)),
    ?assertNot(is_strict_inflation(MaxInt2, MaxInt1)).

join_decomposition_test() ->
    MaxInt1 = {?TYPE, 17},
    Decomp1 = join_decomposition(MaxInt1),
    ?assertEqual([{?TYPE, 17}], Decomp1).

-endif.
