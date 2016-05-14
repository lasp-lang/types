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

%% @doc PNCounter CRDT: counter that allows both increments and decrements.
%%      Modeled as a dictionary where keys are replicas ids and
%%      values are pairs where the first component is the number of
%%      increments and the second component is the number of
%%      decrements.
%%      An actor may only update its own entry in the dictionary.
%%      The value of the counter is the sum of all first components minus the sum of all second components.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(pncounter).
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

-export_type([pncounter/0, delta_pncounter/0, pncounter_op/0]).

-opaque pncounter() :: {?TYPE, payload()}.
-opaque delta_pncounter() :: {?TYPE, {delta, payload()}}.
-type payload() :: orddict:orddict().
-type pncounter_op() :: increment | decrement.

%% @doc Create a new, empty `pncounter()'
-spec new() -> pncounter().
new() ->
    {?TYPE, orddict:new()}.

%% @doc Create a new, empty `pncounter()'
-spec new([term()]) -> pncounter().
new([]) ->
    new().

%% @doc Mutate a `pncounter()'.
-spec mutate(pncounter_op(), type:actor(), pncounter()) ->
    {ok, pncounter()}.
mutate(Op, Actor, {?TYPE, _PNCounter}=CRDT) ->
    type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `pncounter()'.
%%      The first argument can be `increment' or `decrement'.
%%      The second argument is the replica id.
%%      The third argument is the `pncounter()' to be inflated.
%%      Returns a `pncounter()' delta where the only entry in the
%%      dictionary maps the replica id to the last value plus 1:
%%          - if it is a `increment' the replica id will map to a pair
%%          where the first component will be the last value for
%%          increments plus 1 and the second component will be zero
%%          - vice versa for `decrement'
-spec delta_mutate(pncounter_op(), type:actor(), pncounter()) ->
    {ok, delta_pncounter()}.
delta_mutate(increment, Actor, {?TYPE, PNCounter}) ->
    Value = case orddict:find(Actor, PNCounter) of
        {ok, {Inc, _Dec}} ->
            Inc;
        error ->
            0
    end,
    Delta = orddict:store(Actor, {Value + 1, 0}, orddict:new()),
    {ok, {?TYPE, {delta, Delta}}};

delta_mutate(decrement, Actor, {?TYPE, PNCounter}) ->
    Value = case orddict:find(Actor, PNCounter) of
        {ok, {_Inc, Dec}} ->
            Dec;
        error ->
            0
    end,
    Delta = orddict:store(Actor, {0, Value + 1}, orddict:new()),
    {ok, {?TYPE, {delta, Delta}}}.


%% @doc Returns the value of the `pncounter()'.
%%      This value is the sum of all increments minus the sum of all
%%      decrements.
-spec query(pncounter()) -> non_neg_integer().
query({?TYPE, PNCounter}) ->
    lists:sum([ Inc - Dec || {_Actor, {Inc, Dec}} <- PNCounter ]).

%% @doc Merge two `pncounter()'.
%%      The keys of the resulting `pncounter()' are the union of the
%%      keys of both `pncounter()' passed as input.
%%      If a key is only present on one of the `pncounter()',
%%      its correspondent value is preserved.
%%      If a key is present in both `pncounter()', the new value
%%      will be the componenet wise max of both values.
%%      Return the join of the two `pncounter()'.
-spec merge(pncounter(), pncounter()) -> pncounter().
merge({?TYPE, PNCounter1}, {?TYPE, PNCounter2}) ->
    PNCounter = orddict:merge(
        fun(_, {Inc1, Dec1}, {Inc2, Dec2}) ->
            {max(Inc1, Inc2), max(Dec1, Dec2)}
        end,
        PNCounter1,
        PNCounter2
    ),
    {?TYPE, PNCounter}.

%% @doc Are two `pncounter()'s structurally equal?
%%      This is not `query/1' equality.
%%      Two counters might represent the total `42', and not be `equal/2'.
%%      Equality here is that both counters contain the same replica ids
%%      and those replicas have the same pair representing the number of
%%      increments and decrements.
-spec equal(pncounter(), pncounter()) -> boolean().
equal({?TYPE, PNCounter1}, {?TYPE, PNCounter2}) ->
    Fun = fun({Inc1, Dec1}, {Inc2, Dec2}) -> Inc1 == Inc2 andalso Dec1 == Dec2 end,
    orddict_ext:equal(PNCounter1, PNCounter2, Fun).

%% @doc Given two `pncounter()', check if the second is and inflation
%%      of the first.
%%      Two conditions should be met:
%%          - each replica id in the first `pncounter()' is also in
%%          the second `pncounter()'
%%          - component wise the value for each replica in the first
%%          `pncounter()' should be less or equal than the value
%%          for the same replica in the second `pncounter()'
-spec is_inflation(pncounter(), pncounter()) -> boolean().
is_inflation({?TYPE, PNCounter1}, {?TYPE, PNCounter2}) ->
    orddict:fold(
        fun(Key, {Inc1, Dec1}, Acc) ->
            case orddict:find(Key, PNCounter2) of
                {ok, {Inc2, Dec2}} ->
                    Acc andalso Inc1 =< Inc2 andalso Dec1 =< Dec2;
                error ->
                    Acc andalso false
            end
        end,
        true,
        PNCounter1
     ).

%% @doc Check for strict inflation.
-spec is_strict_inflation(pncounter(), pncounter()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Join decomposition for `pncounter()'.
%%      A `pncounter()' is a set of entries.
%%      The result of the join decomposition is a list of `pncounter()'
%%      where each of the `pncounter()' only has one entry.
%%      Also, increments and decrements should be separated.
%%      This means that for each replica id in the `pncounter()'
%%      passed as a input we'll have 2 `pncounter()' in the
%%      resulting join decomposition.
-spec join_decomposition(pncounter()) -> [pncounter()].
join_decomposition({?TYPE, PNCounter}) ->
    lists:foldl(
        fun({Actor, {Inc, Dec}}, Acc) ->
            [{?TYPE, [{Actor, {Inc, 0}}]} | [{?TYPE, [{Actor, {0, Dec}}]} | Acc]]
        end,
        [],
        PNCounter
     ).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, []}, new()).

query_test() ->
    Counter0 = new(),
    Counter1 = {?TYPE, [{1, {1, 2}}, {2, {13, 1}}, {3, {1, 0}}]},
    ?assertEqual(0, query(Counter0)),
    ?assertEqual(12, query(Counter1)).

delta_increment_test() ->
    Counter0 = new(),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate(increment, 1, Counter0),
    Counter1 = merge({?TYPE, Delta1}, Counter0),
    {ok, {?TYPE, {delta, Delta2}}} = delta_mutate(decrement, 2, Counter1),
    Counter2 = merge({?TYPE, Delta2}, Counter1),
    {ok, {?TYPE, {delta, Delta3}}} = delta_mutate(increment, 1, Counter2),
    Counter3 = merge({?TYPE, Delta3}, Counter2),
    ?assertEqual({?TYPE, [{1, {1, 0}}]}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, [{1, {1, 0}}]}, Counter1),
    ?assertEqual({?TYPE, [{2, {0, 1}}]}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, [{1, {1, 0}}, {2, {0, 1}}]}, Counter2),
    ?assertEqual({?TYPE, [{1, {2, 0}}]}, {?TYPE, Delta3}),
    ?assertEqual({?TYPE, [{1, {2, 0}}, {2, {0, 1}}]}, Counter3).

increment_test() ->
    Counter0 = new(),
    {ok, Counter1} = mutate(increment, 1, Counter0),
    {ok, Counter2} = mutate(decrement, 2, Counter1),
    {ok, Counter3} = mutate(increment, 1, Counter2),
    ?assertEqual({?TYPE, [{1, {1, 0}}]}, Counter1),
    ?assertEqual({?TYPE, [{1, {1, 0}}, {2, {0, 1}}]}, Counter2),
    ?assertEqual({?TYPE, [{1, {2, 0}}, {2, {0, 1}}]}, Counter3).

merge_idempontent_test() ->
    Counter1 = {?TYPE, [{<<"5">>, {5, 2}}]},
    Counter2 = {?TYPE, [{<<"6">>, {6, 3}}, {<<"7">>, {7, 4}}]},
    Counter3 = merge(Counter1, Counter1),
    Counter4 = merge(Counter2, Counter2),
    ?assertEqual({?TYPE, [{<<"5">>, {5, 2}}]}, Counter3),
    ?assertEqual({?TYPE, [{<<"6">>, {6, 3}}, {<<"7">>, {7, 4}}]}, Counter4).

merge_commutative_test() ->
    Counter1 = {?TYPE, [{<<"5">>, {5, 2}}]},
    Counter2 = {?TYPE, [{<<"6">>, {6, 3}}, {<<"7">>, {7, 4}}]},
    Counter3 = merge(Counter1, Counter2),
    Counter4 = merge(Counter2, Counter1),
    ?assertEqual({?TYPE, [{<<"5">>, {5, 2}}, {<<"6">>, {6, 3}}, {<<"7">>, {7, 4}}]}, Counter3),
    ?assertEqual({?TYPE, [{<<"5">>, {5, 2}}, {<<"6">>, {6, 3}}, {<<"7">>, {7, 4}}]}, Counter4).

merge_same_id_test() ->
    Counter1 = {?TYPE, [{<<"1">>, {2, 3}}, {<<"2">>, {5, 2}}]},
    Counter2 = {?TYPE, [{<<"1">>, {3, 2}}, {<<"2">>, {4, 9}}]},
    Counter3 = merge(Counter1, Counter2),
    ?assertEqual({?TYPE, [{<<"1">>, {3, 3}}, {<<"2">>, {5, 9}}]}, Counter3).

equal_test() ->
    Counter1 = {?TYPE, [{1, {2, 0}}, {2, {1, 2}}, {4, {1, 2}}]},
    Counter2 = {?TYPE, [{1, {2, 0}}, {2, {1, 2}}, {4, {1, 2}}, {5, {6, 3}}]},
    Counter3 = {?TYPE, [{1, {2, 0}}, {2, {2, 2}}, {4, {1, 2}}]},
    Counter4 = {?TYPE, [{1, {2, 1}}, {2, {1, 2}}, {4, {1, 2}}]},
    ?assert(equal(Counter1, Counter1)),
    ?assertNot(equal(Counter1, Counter2)),
    ?assertNot(equal(Counter1, Counter3)),
    ?assertNot(equal(Counter1, Counter4)).

is_inflation_test() ->
    Counter1 = {?TYPE, [{1, {2, 0}}, {2, {1, 2}}, {4, {1, 2}}]},
    Counter2 = {?TYPE, [{1, {2, 0}}, {2, {1, 2}}, {4, {1, 2}}, {5, {6, 3}}]},
    Counter3 = {?TYPE, [{1, {2, 0}}, {2, {2, 2}}, {4, {1, 2}}]},
    Counter4 = {?TYPE, [{1, {2, 1}}, {2, {1, 1}}, {4, {1, 2}}]},
    ?assert(is_inflation(Counter1, Counter1)),
    ?assert(is_inflation(Counter1, Counter2)),
    ?assert(is_inflation(Counter1, Counter3)),
    ?assertNot(is_inflation(Counter1, Counter4)).

is_strict_inflation_test() ->
    Counter1 = {?TYPE, [{1, {2, 0}}, {2, {1, 2}}, {4, {1, 2}}]},
    Counter2 = {?TYPE, [{1, {2, 0}}, {2, {1, 2}}, {4, {1, 2}}, {5, {6, 3}}]},
    Counter3 = {?TYPE, [{1, {2, 0}}, {2, {2, 2}}, {4, {1, 2}}]},
    Counter4 = {?TYPE, [{1, {2, 1}}, {2, {1, 1}}, {4, {1, 2}}]},
    ?assertNot(is_strict_inflation(Counter1, Counter1)),
    ?assert(is_strict_inflation(Counter1, Counter2)),
    ?assert(is_strict_inflation(Counter1, Counter3)),
    ?assertNot(is_strict_inflation(Counter1, Counter4)).

join_decomposition_test() ->
    Counter0 = new(),
    Counter1 = {?TYPE, [{1, {2, 1}}, {2, {1, 0}}, {4, {1, 2}}]},
    Decomp0 = join_decomposition(Counter0),
    Decomp1 = join_decomposition(Counter1),
    ?assertEqual([], Decomp0),
    List = [{?TYPE, [{1, {2, 0}}]}, {?TYPE, [{2, {1, 0}}]}, {?TYPE, [{4, {1, 0}}]},
            {?TYPE, [{1, {0, 1}}]}, {?TYPE, [{2, {0, 0}}]}, {?TYPE, [{4, {0, 2}}]}],
    ?assertEqual(lists:sort(List), lists:sort(Decomp1)).

-endif.
