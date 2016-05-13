% -------------------------------------------------------------------
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

%% @doc GCounter CRDT: grow only counter.
%%      Modeled as a dictionary where keys are replicas ids and
%%      values are the correspondent count.
%%      An actor may only update its own entry in the dictionary.
%%      The value of the counter is the sum all values in the dictionary.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(gcounter).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-behaviour(type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_inflation/2, is_strict_inflation/2]).
-export([join_decomposition/1]).

-export_type([gcounter/0, delta_gcounter/0, gcounter_op/0]).

-opaque gcounter() :: {?TYPE, payload()}.
-opaque delta_gcounter() :: {?TYPE, {delta, payload()}}.
-type payload() :: orddict:orddict().
-type gcounter_op() :: increment.

%% @doc Create a new, empty `gcounter()'
-spec new() -> gcounter().
new() ->
    {?TYPE, orddict:new()}.

%% @doc Create a new, empty `gcounter()'
-spec new([term()]) -> gcounter().
new([]) ->
    new().

%% @doc Mutate a `gcounter()'.
-spec mutate(gcounter_op(), type:actor(), gcounter()) ->
    {ok, gcounter()}.
mutate(Op, Actor, {?TYPE, _GCounter}=CRDT) ->
    type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `gcounter()'.
%%      The first argument can only be `increment'.
%%      The second argument is the replica id.
%%      The third argument is the `gcounter()' to be inflated.
%%      Returns a `gcounter()' delta where the only entry in the
%%      dictionary maps the replica id to the last value plus 1.
-spec delta_mutate(gcounter_op(), type:actor(), gcounter()) ->
    {ok, delta_gcounter()}.
delta_mutate(increment, Actor, {?TYPE, GCounter}) ->
    Count = case orddict:find(Actor, GCounter) of
        {ok, Value} ->
            Value;
        error ->
            0
    end,
    Delta = orddict:store(Actor, Count + 1, orddict:new()),
    {ok, {?TYPE, {delta, Delta}}}.

%% @doc Returns the value of the `gcounter()'.
%%      This value is the sum of all values in the `gcounter()'.
-spec query(gcounter()) -> non_neg_integer().
query({?TYPE, GCounter}) ->
    lists:sum([ Value || {_Actor, Value} <- GCounter ]).

%% @doc Merge two `gcounter()'.
%%      The keys of the resulting `gcounter()' are the union of the
%%      keys of both `gcounter()' passed as input.
%%      If a key is only present on one of the `gcounter()',
%%      its correspondent value is preserved.
%%      If a key is present in both `gcounter()', the new value
%%      will be the max of both values.
%%      Return the join of the two `gcounter()'.
-spec merge(gcounter(), gcounter()) -> gcounter().
merge({?TYPE, GCounter1}, {?TYPE, GCounter2}) ->
    GCounter = orddict:merge(
        fun(_, Value1, Value2) ->
            max(Value1, Value2)
        end,
        GCounter1,
        GCounter2
    ),
    {?TYPE, GCounter}.

%% @doc Are two `gcounter()'s structurally equal?
%%      This is not `query/1' equality.
%%      Two counters might represent the total `42', and not be `equal/2'.
%%      Equality here is that both counters contain the same replica ids
%%      and those replicas have the same count.
-spec equal(gcounter(), gcounter()) -> boolean().
equal({?TYPE, GCounter1}, {?TYPE, GCounter2}) ->
    Fun = fun(Value1, Value2) -> Value1 == Value2 end,
    orddict_ext:equal(GCounter1, GCounter2, Fun).

%% @doc Given two `gcounter()', check if the second is and inflation
%%      of the first.
%%      Two conditions should be met:
%%          - each replica id in the first `gcounter()' is also in
%%          the second `gcounter()'
%%          - the value for each replica in the first `gcounter()'
%%          should be less or equal than the value for the same
%%          replica in the second `gcounter()'
-spec is_inflation(gcounter(), gcounter()) -> boolean().
is_inflation({?TYPE, GCounter1}, {?TYPE, GCounter2}) ->
    orddict:fold(
        fun(Key, Value1, Acc) ->
            case orddict:find(Key, GCounter2) of
                {ok, Value2} ->
                    Acc andalso Value1 =< Value2;
                error ->
                    Acc andalso false
            end
        end,
        true,
        GCounter1
     ).

%% @doc Check for strict inflation.
-spec is_strict_inflation(gcounter(), gcounter()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Join decomposition for `gcounter()'.
%%      A `gcounter()' is a set of entries.
%%      The result of the join decomposition is a list of `gcounter()'
%%      where each of the `gcounter()' only has one entry.
%%      This join decomposition is a set partition where each set in
%%      the partition has exactly the size of one.
-spec join_decomposition(gcounter()) -> [gcounter()].
join_decomposition({?TYPE, GCounter}) ->
    lists:foldl(
        fun(Entry, Acc) ->
            [{?TYPE, [Entry]} | Acc]
        end,
        [],
        GCounter
     ).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, []}, new()).

query_test() ->
    Counter0 = new(),
    Counter1 = {?TYPE, [{1, 1}, {2, 13}, {3, 1}]},
    ?assertEqual(0, query(Counter0)),
    ?assertEqual(15, query(Counter1)).

delta_increment_test() ->
    Counter0 = new(),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate(increment, 1, Counter0),
    Counter1 = merge({?TYPE, Delta1}, Counter0),
    {ok, {?TYPE, {delta, Delta2}}} = delta_mutate(increment, 2, Counter1),
    Counter2 = merge({?TYPE, Delta2}, Counter1),
    {ok, {?TYPE, {delta, Delta3}}} = delta_mutate(increment, 1, Counter2),
    Counter3 = merge({?TYPE, Delta3}, Counter2),
    ?assertEqual({?TYPE, [{1, 1}]}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, [{1, 1}]}, Counter1),
    ?assertEqual({?TYPE, [{2, 1}]}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, [{1, 1}, {2, 1}]}, Counter2),
    ?assertEqual({?TYPE, [{1, 2}]}, {?TYPE, Delta3}),
    ?assertEqual({?TYPE, [{1, 2}, {2, 1}]}, Counter3).

increment_test() ->
    Counter0 = new(),
    {ok, Counter1} = mutate(increment, 1, Counter0),
    {ok, Counter2} = mutate(increment, 2, Counter1),
    {ok, Counter3} = mutate(increment, 1, Counter2),
    ?assertEqual({?TYPE, [{1, 1}]}, Counter1),
    ?assertEqual({?TYPE, [{1, 1}, {2, 1}]}, Counter2),
    ?assertEqual({?TYPE, [{1, 2}, {2, 1}]}, Counter3).

merge_idempontent_test() ->
    Counter1 = {?TYPE, [{<<"5">>, 5}]},
    Counter2 = {?TYPE, [{<<"6">>, 6}, {<<"7">>, 7}]},
    Counter3 = merge(Counter1, Counter1),
    Counter4 = merge(Counter2, Counter2),
    ?assertEqual({?TYPE, [{<<"5">>, 5}]}, Counter3),
    ?assertEqual({?TYPE, [{<<"6">>, 6}, {<<"7">>, 7}]}, Counter4).

merge_commutative_test() ->
    Counter1 = {?TYPE, [{<<"5">>, 5}]},
    Counter2 = {?TYPE, [{<<"6">>, 6}, {<<"7">>, 7}]},
    Counter3 = merge(Counter1, Counter2),
    Counter4 = merge(Counter2, Counter1),
    ?assertEqual({?TYPE, [{<<"5">>, 5}, {<<"6">>, 6}, {<<"7">>, 7}]}, Counter3),
    ?assertEqual({?TYPE, [{<<"5">>, 5}, {<<"6">>, 6}, {<<"7">>, 7}]}, Counter4).

merge_same_id_test() ->
    Counter1 = {?TYPE, [{<<"1">>, 2}, {<<"2">>, 5}]},
    Counter2 = {?TYPE, [{<<"1">>, 3}, {<<"2">>, 4}]},
    Counter3 = merge(Counter1, Counter2),
    ?assertEqual({?TYPE, [{<<"1">>, 3}, {<<"2">>, 5}]}, Counter3).

equal_test() ->
    Counter1 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}]},
    Counter2 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}, {5, 6}]},
    Counter3 = {?TYPE, [{1, 2}, {2, 2}, {4, 1}]},
    Counter4 = {?TYPE, [{1, 2}, {2, 1}]},
    ?assert(equal(Counter1, Counter1)),
    ?assertNot(equal(Counter1, Counter2)),
    ?assertNot(equal(Counter1, Counter3)),
    ?assertNot(equal(Counter1, Counter4)).

is_inflation_test() ->
    Counter1 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}]},
    Counter2 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}, {5, 6}]},
    Counter3 = {?TYPE, [{1, 2}, {2, 2}, {4, 1}]},
    Counter4 = {?TYPE, [{1, 2}, {2, 1}]},
    ?assert(is_inflation(Counter1, Counter1)),
    ?assert(is_inflation(Counter1, Counter2)),
    ?assert(is_inflation(Counter1, Counter3)),
    ?assertNot(is_inflation(Counter1, Counter4)).

is_strict_inflation_test() ->
    Counter1 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}]},
    Counter2 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}, {5, 6}]},
    Counter3 = {?TYPE, [{1, 2}, {2, 2}, {4, 1}]},
    Counter4 = {?TYPE, [{1, 2}, {2, 1}]},
    ?assertNot(is_strict_inflation(Counter1, Counter1)),
    ?assert(is_strict_inflation(Counter1, Counter2)),
    ?assert(is_strict_inflation(Counter1, Counter3)),
    ?assertNot(is_strict_inflation(Counter1, Counter4)).

join_decomposition_test() ->
    Counter0 = new(),
    Counter1 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}]},
    Decomp0 = join_decomposition(Counter0),
    Decomp1 = join_decomposition(Counter1),
    ?assertEqual([], Decomp0),
    ?assertEqual(lists:sort([{?TYPE, [{1, 2}]}, {?TYPE, [{2, 1}]}, {?TYPE, [{4, 1}]}]), lists:sort(Decomp1)).

-endif.
