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
%%
%% @reference Carlos Baquero
%%      delta-enabled-crdts C++ library
%%      [https://github.com/CBaquero/delta-enabled-crdts]

-module(state_pncounter).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-behaviour(type).
-behaviour(state_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_bottom/1, is_inflation/2,
         is_strict_inflation/2,
         irreducible_is_strict_inflation/2]).
-export([join_decomposition/1, delta/2, digest/1]).
-export([encode/2, decode/2]).

-export_type([state_pncounter/0, state_pncounter_op/0]).

-opaque state_pncounter() :: {?TYPE, payload()}.
-type payload() :: orddict:orddict().
-type state_pncounter_op() :: increment | decrement.

%% @doc Create a new, empty `state_pncounter()'
-spec new() -> state_pncounter().
new() ->
    {?TYPE, orddict:new()}.

%% @doc Create a new, empty `state_pncounter()'
-spec new([term()]) -> state_pncounter().
new([]) ->
    new().

-spec mutate(state_pncounter_op(), type:id(), state_pncounter()) ->
    {ok, state_pncounter()}.
mutate(Op, Actor, {?TYPE, _PNCounter}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_pncounter()'.
%%      The first argument can be `increment' or `decrement'.
%%      The second argument is the replica id.
%%      The third argument is the `state_pncounter()' to be inflated.
%%      Returns a `state_pncounter()' delta where the only entry in the
%%      dictionary maps the replica id to the last value plus 1:
%%          - if it is a `increment' the replica id will map to a pair
%%          where the first component will be the last value for
%%          increments plus 1 and the second component will be zero
%%          - vice versa for `decrement'
-spec delta_mutate(state_pncounter_op(), type:id(), state_pncounter()) ->
    {ok, state_pncounter()}.
delta_mutate(increment, Actor, {?TYPE, PNCounter}) ->
    {Value, _} = orddict_ext:fetch(Actor, PNCounter, {0, 0}),
    Delta = orddict:store(Actor, {Value + 1, 0}, orddict:new()),
    {ok, {?TYPE, Delta}};

delta_mutate(decrement, Actor, {?TYPE, PNCounter}) ->
    {_, Value} = orddict_ext:fetch(Actor, PNCounter, {0, 0}),
    Delta = orddict:store(Actor, {0, Value + 1}, orddict:new()),
    {ok, {?TYPE, Delta}}.

%% @doc Returns the value of the `state_pncounter()'.
%%      This value is the sum of all increments minus the sum of all
%%      decrements.
-spec query(state_pncounter()) -> integer().
query({?TYPE, PNCounter}) ->
    lists:sum([ Inc - Dec || {_Actor, {Inc, Dec}} <- PNCounter ]).

%% @doc Merge two `state_pncounter()'.
%%      The keys of the resulting `state_pncounter()' are the union of the
%%      keys of both `state_pncounter()' passed as input.
%%      If a key is only present on one of the `state_pncounter()',
%%      its correspondent value is preserved.
%%      If a key is present in both `state_pncounter()', the new value
%%      will be the componenet wise max of both values.
%%      Return the join of the two `state_pncounter()'.
-spec merge(state_pncounter(), state_pncounter()) -> state_pncounter().
merge({?TYPE, PNCounter1}, {?TYPE, PNCounter2}) ->
    PNCounter = orddict:merge(
        fun(_, {Inc1, Dec1}, {Inc2, Dec2}) ->
            {max(Inc1, Inc2), max(Dec1, Dec2)}
        end,
        PNCounter1,
        PNCounter2
    ),
    {?TYPE, PNCounter}.

%% @doc Are two `state_pncounter()'s structurally equal?
%%      This is not `query/1' equality.
%%      Two counters might represent the total `42', and not be `equal/2'.
%%      Equality here is that both counters contain the same replica ids
%%      and those replicas have the same pair representing the number of
%%      increments and decrements.
-spec equal(state_pncounter(), state_pncounter()) -> boolean().
equal({?TYPE, PNCounter1}, {?TYPE, PNCounter2}) ->
    Fun = fun({Inc1, Dec1}, {Inc2, Dec2}) -> Inc1 == Inc2 andalso Dec1 == Dec2 end,
    orddict_ext:equal(PNCounter1, PNCounter2, Fun).

%% @doc Check if a PNCounter is bottom.
-spec is_bottom(state_pncounter()) -> boolean().
is_bottom({?TYPE, PNCounter}) ->
    orddict:is_empty(PNCounter).

%% @doc Given two `state_pncounter()', check if the second is and inflation
%%      of the first.
%%      Two conditions should be met:
%%          - each replica id in the first `state_pncounter()' is also in
%%          the second `state_pncounter()'
%%          - component wise the value for each replica in the first
%%          `state_pncounter()' should be less or equal than the value
%%          for the same replica in the second `state_pncounter()'
-spec is_inflation(state_pncounter(), state_pncounter()) -> boolean().
is_inflation({?TYPE, PNCounter1}, {?TYPE, PNCounter2}) ->
    lists_ext:iterate_until(
        fun({Key, {Inc1, Dec1}}) ->
            case orddict:find(Key, PNCounter2) of
                {ok, {Inc2, Dec2}} ->
                    Inc1 =< Inc2 andalso Dec1 =< Dec2;
                error ->
                    false
            end
        end,
        PNCounter1
     ).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_pncounter(), state_pncounter()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_pncounter(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=A, B) ->
    state_type:irreducible_is_strict_inflation(A, B).

-spec digest(state_pncounter()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
        {state, CRDT}.

%% @doc Join decomposition for `state_pncounter()'.
%%      A `state_pncounter()' is a set of entries.
%%      The result of the join decomposition is a list of `state_pncounter()'
%%      where each of the `state_pncounter()' only has one entry.
%%      Also, increments and decrements should be separated.
%%      This means that for each replica id in the `state_pncounter()'
%%      passed as a input we'll have 2 `state_pncounter()' in the
%%      resulting join decomposition.
-spec join_decomposition(state_pncounter()) -> [state_pncounter()].
join_decomposition({?TYPE, PNCounter}) ->
    lists:foldl(
        fun({Actor, {Inc, Dec}}, Acc0) ->
            Acc1 = case Inc > 0 of
                true ->
                    [{?TYPE, [{Actor, {Inc, 0}}]} | Acc0];
                false ->
                    Acc0
            end,
            case Dec > 0 of
                true ->
                    [{?TYPE, [{Actor, {0, Dec}}]} | Acc1];
                false ->
                    Acc1
            end
        end,
        [],
        PNCounter
    ).

%% @doc Delta calculation for `state_pncounter()'.
-spec delta(state_pncounter(), state_type:digest()) -> state_pncounter().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_pncounter()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_pncounter().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


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
    {ok, {?TYPE, Delta1}} = delta_mutate(increment, 1, Counter0),
    Counter1 = merge({?TYPE, Delta1}, Counter0),
    {ok, {?TYPE, Delta2}} = delta_mutate(decrement, 2, Counter1),
    Counter2 = merge({?TYPE, Delta2}, Counter1),
    {ok, {?TYPE, Delta3}} = delta_mutate(increment, 1, Counter2),
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

merge_idempotent_test() ->
    Counter1 = {?TYPE, [{<<"5">>, {5, 2}}]},
    Counter2 = {?TYPE, [{<<"6">>, {6, 3}}, {<<"7">>, {7, 4}}]},
    Counter3 = merge(Counter1, Counter1),
    Counter4 = merge(Counter2, Counter2),
    ?assertEqual(Counter1, Counter3),
    ?assertEqual(Counter2, Counter4).

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

merge_delta_test() ->
    Counter1 = {?TYPE, [{<<"1">>, {2, 3}}, {<<"2">>, {5, 2}}]},
    Delta1 = {?TYPE, [{<<"1">>, {3, 2}}]},
    Delta2 = {?TYPE, [{<<"3">>, {1, 2}}]},
    Counter2 = merge(Delta1, Counter1),
    Counter3 = merge(Counter1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, [{<<"1">>, {3, 3}}, {<<"2">>, {5, 2}}]}, Counter2),
    ?assertEqual({?TYPE, [{<<"1">>, {3, 3}}, {<<"2">>, {5, 2}}]}, Counter3),
    ?assertEqual({?TYPE, [{<<"1">>, {3, 2}}, {<<"3">>, {1, 2}}]}, DeltaGroup).

equal_test() ->
    Counter1 = {?TYPE, [{1, {2, 0}}, {2, {1, 2}}, {4, {1, 2}}]},
    Counter2 = {?TYPE, [{1, {2, 0}}, {2, {1, 2}}, {4, {1, 2}}, {5, {6, 3}}]},
    Counter3 = {?TYPE, [{1, {2, 0}}, {2, {2, 2}}, {4, {1, 2}}]},
    Counter4 = {?TYPE, [{1, {2, 1}}, {2, {1, 2}}, {4, {1, 2}}]},
    ?assert(equal(Counter1, Counter1)),
    ?assertNot(equal(Counter1, Counter2)),
    ?assertNot(equal(Counter1, Counter3)),
    ?assertNot(equal(Counter1, Counter4)).

is_bottom_test() ->
    Counter0 = new(),
    Counter1 = {?TYPE, [{1, {2, 0}}, {2, {1, 2}}, {4, {1, 2}}]},
    ?assert(is_bottom(Counter0)),
    ?assertNot(is_bottom(Counter1)).

is_inflation_test() ->
    Counter1 = {?TYPE, [{1, {2, 0}}, {2, {1, 2}}, {4, {1, 2}}]},
    Counter2 = {?TYPE, [{1, {2, 0}}, {2, {1, 2}}, {4, {1, 2}}, {5, {6, 3}}]},
    Counter3 = {?TYPE, [{1, {2, 0}}, {2, {2, 2}}, {4, {1, 2}}]},
    Counter4 = {?TYPE, [{1, {2, 1}}, {2, {1, 1}}, {4, {1, 2}}]},
    ?assert(is_inflation(Counter1, Counter1)),
    ?assert(is_inflation(Counter1, Counter2)),
    ?assert(is_inflation(Counter1, Counter3)),
    ?assertNot(is_inflation(Counter1, Counter4)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Counter1, Counter1)),
    ?assert(state_type:is_inflation(Counter1, Counter2)),
    ?assert(state_type:is_inflation(Counter1, Counter3)),
    ?assertNot(state_type:is_inflation(Counter1, Counter4)).

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
    List = [{?TYPE, [{1, {2, 0}}]},
            {?TYPE, [{1, {0, 1}}]},
            {?TYPE, [{2, {1, 0}}]},
            {?TYPE, [{4, {1, 0}}]},
            {?TYPE, [{4, {0, 2}}]}],
    ?assertEqual(lists:sort(List), lists:sort(Decomp1)).

encode_decode_test() ->
    Counter = {?TYPE, [{1, {2, 1}}, {2, {1, 0}}, {4, {1, 2}}]},
    Binary = encode(erlang, Counter),
    ECounter = decode(erlang, Binary),
    ?assertEqual(Counter, ECounter).

-endif.
