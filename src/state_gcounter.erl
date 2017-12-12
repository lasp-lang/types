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

%% @doc GCounter CRDT: grow only counter.
%%      Modeled as a dictionary where keys are replicas ids and
%%      values are the correspondent count.
%%      An actor may only update its own entry in the dictionary.
%%      The value of the counter is the sum all values in the dictionary.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]
%%
%% @reference Carlos Baquero
%%      delta-enabled-crdts C++ library
%%      [https://github.com/CBaquero/delta-enabled-crdts]

-module(state_gcounter).
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

-export_type([state_gcounter/0, state_gcounter_op/0]).

-opaque state_gcounter() :: {?TYPE, payload()}.
-type payload() :: orddict:orddict().
-type state_gcounter_op() :: increment.

%% @doc Create a new, empty `state_gcounter()'
-spec new() -> state_gcounter().
new() ->
    {?TYPE, orddict:new()}.

%% @doc Create a new, empty `state_gcounter()'
-spec new([term()]) -> state_gcounter().
new([]) ->
    new().

%% @doc Mutate a `state_gcounter()'.
-spec mutate(state_gcounter_op(), type:id(), state_gcounter()) ->
    {ok, state_gcounter()}.
mutate(Op, Actor, {?TYPE, _GCounter}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_gcounter()'.
%%      The first argument can only be `increment'.
%%      The second argument is the replica id.
%%      The third argument is the `state_gcounter()' to be inflated.
%%      Returns a `state_gcounter()' delta where the only entry in the
%%      dictionary maps the replica id to the last value plus 1.
-spec delta_mutate(state_gcounter_op(), type:id(), state_gcounter()) ->
    {ok, state_gcounter()}.
delta_mutate(increment, Actor, {?TYPE, GCounter}) ->
    Count = orddict_ext:fetch(Actor, GCounter, 0),
    Delta = orddict:store(Actor, Count + 1, orddict:new()),
    {ok, {?TYPE, Delta}}.

%% @doc Returns the value of the `state_gcounter()'.
%%      This value is the sum of all values in the `state_gcounter()'.
-spec query(state_gcounter()) -> non_neg_integer().
query({?TYPE, GCounter}) ->
    lists:sum([ Value || {_Actor, Value} <- GCounter ]).

%% @doc Merge two `state_gcounter()'.
%%      The keys of the resulting `state_gcounter()' are the union of the
%%      keys of both `state_gcounter()' passed as input.
%%      If a key is only present on one of the `state_gcounter()',
%%      its correspondent value is preserved.
%%      If a key is present in both `state_gcounter()', the new value
%%      will be the max of both values.
%%      Return the join of the two `state_gcounter()'.
-spec merge(state_gcounter(), state_gcounter()) -> state_gcounter().
merge({?TYPE, GCounter1}, {?TYPE, GCounter2}) ->
    GCounter = orddict:merge(
        fun(_, Value1, Value2) ->
            max(Value1, Value2)
        end,
        GCounter1,
        GCounter2
    ),
    {?TYPE, GCounter}.

%% @doc Are two `state_gcounter()'s structurally equal?
%%      This is not `query/1' equality.
%%      Two counters might represent the total `42', and not be `equal/2'.
%%      Equality here is that both counters contain the same replica ids
%%      and those replicas have the same count.
-spec equal(state_gcounter(), state_gcounter()) -> boolean().
equal({?TYPE, GCounter1}, {?TYPE, GCounter2}) ->
    Fun = fun(Value1, Value2) -> Value1 == Value2 end,
    orddict_ext:equal(GCounter1, GCounter2, Fun).

%% @doc Check if a GCounter is bottom.
-spec is_bottom(state_gcounter()) -> boolean().
is_bottom({?TYPE, GCounter}) ->
    orddict:is_empty(GCounter).

%% @doc Given two `state_gcounter()', check if the second is an inflation
%%      of the first.
%%      Two conditions should be met:
%%          - each replica id in the first `state_gcounter()' is also in
%%          the second `state_gcounter()'
%%          - the value for each replica in the first `state_gcounter()'
%%          should be less or equal than the value for the same
%%          replica in the second `state_gcounter()'
-spec is_inflation(state_gcounter(), state_gcounter()) -> boolean().
is_inflation({?TYPE, GCounter1}, {?TYPE, GCounter2}) ->
    lists_ext:iterate_until(
        fun({Key, Value1}) ->
            case orddict:find(Key, GCounter2) of
                {ok, Value2} ->
                    Value1 =< Value2;
                error ->
                    false
            end
        end,
        GCounter1
     );

%% @todo get back here later
%% Just trying to fix https://travis-ci.org/lasp-lang/lasp/builds/131343590
is_inflation({value, Value1}, {?TYPE, _}=GCounter) ->
    Value2 = query(GCounter),
    Value2 >= Value1.

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_gcounter(), state_gcounter()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2);

%% @todo get back here later
%% Just trying to fix https://travis-ci.org/lasp-lang/lasp/builds/131343590
is_strict_inflation({value, Value1}, {?TYPE, _}=GCounter) ->
    Value2 = query(GCounter),
    Value2 > Value1.

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_gcounter(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, [{Actor, Value}]},
                                {state, {?TYPE, GCounter}}) ->
    case orddict:find(Actor, GCounter) of
        {ok, Current} ->
            Current < Value;
        error ->
            true
    end.

-spec digest(state_gcounter()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_gcounter()'.
%%      A `state_gcounter()' is a set of entries.
%%      The result of the join decomposition is a list of `state_gcounter()'
%%      where each of the `state_gcounter()' only has one entry.
%%      This join decomposition is a set partition where each set in
%%      the partition has exactly the size of one.
-spec join_decomposition(state_gcounter()) -> [state_gcounter()].
join_decomposition({?TYPE, GCounter}) ->
    lists:foldl(
        fun(Entry, Acc) ->
            [{?TYPE, [Entry]} | Acc]
        end,
        [],
        GCounter
    ).

%% @doc Delta calculation for `state_gcounter()'.
-spec delta(state_gcounter(),
            state_type:digest()) -> state_gcounter().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_gcounter()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_gcounter().
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
    Counter1 = {?TYPE, [{1, 1}, {2, 13}, {3, 1}]},
    ?assertEqual(0, query(Counter0)),
    ?assertEqual(15, query(Counter1)).

delta_increment_test() ->
    Counter0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate(increment, 1, Counter0),
    Counter1 = merge({?TYPE, Delta1}, Counter0),
    {ok, {?TYPE, Delta2}} = delta_mutate(increment, 2, Counter1),
    Counter2 = merge({?TYPE, Delta2}, Counter1),
    {ok, {?TYPE, Delta3}} = delta_mutate(increment, 1, Counter2),
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

merge_idempotent_test() ->
    Counter1 = {?TYPE, [{<<"5">>, 5}]},
    Counter2 = {?TYPE, [{<<"6">>, 6}, {<<"7">>, 7}]},
    Counter3 = merge(Counter1, Counter1),
    Counter4 = merge(Counter2, Counter2),
    ?assertEqual(Counter1, Counter3),
    ?assertEqual(Counter2, Counter4).

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

merge_deltas_test() ->
    Counter1 = {?TYPE, [{<<"1">>, 2}, {<<"2">>, 5}]},
    Delta1 = {?TYPE, [{<<"1">>, 3}, {<<"2">>, 4}]},
    Delta2 = {?TYPE, [{<<"1">>, 5}, {<<"2">>, 2}]},
    Counter2 = merge(Delta1, Counter1),
    Counter3 = merge(Counter1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, [{<<"1">>, 3}, {<<"2">>, 5}]}, Counter2),
    ?assertEqual({?TYPE, [{<<"1">>, 3}, {<<"2">>, 5}]}, Counter3),
    ?assertEqual({?TYPE, [{<<"1">>, 5}, {<<"2">>, 4}]}, DeltaGroup).

equal_test() ->
    Counter1 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}]},
    Counter2 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}, {5, 6}]},
    Counter3 = {?TYPE, [{1, 2}, {2, 2}, {4, 1}]},
    Counter4 = {?TYPE, [{1, 2}, {2, 1}]},
    ?assert(equal(Counter1, Counter1)),
    ?assertNot(equal(Counter1, Counter2)),
    ?assertNot(equal(Counter1, Counter3)),
    ?assertNot(equal(Counter1, Counter4)).

is_bottom_test() ->
    Counter0 = new(),
    Counter1 = {?TYPE, [{1, 2}]},
    ?assert(is_bottom(Counter0)),
    ?assertNot(is_bottom(Counter1)).

is_inflation_test() ->
    Counter1 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}]},
    Counter2 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}, {5, 6}]},
    Counter3 = {?TYPE, [{1, 2}, {2, 2}, {4, 1}]},
    Counter4 = {?TYPE, [{1, 2}, {2, 1}]},
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
    Counter1 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}]},
    Counter2 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}, {5, 6}]},
    Counter3 = {?TYPE, [{1, 2}, {2, 2}, {4, 1}]},
    Counter4 = {?TYPE, [{1, 2}, {2, 1}]},
    ?assertNot(is_strict_inflation(Counter1, Counter1)),
    ?assert(is_strict_inflation(Counter1, Counter2)),
    ?assert(is_strict_inflation(Counter1, Counter3)),
    ?assertNot(is_strict_inflation(Counter1, Counter4)).

irreducible_is_strict_inflation_test() ->
    Counter1 = {?TYPE, [{a, 2}, {b, 1}]},
    Digest = digest(Counter1),
    Irreducible1 = {?TYPE, [{a, 2}]},
    Irreducible2 = {?TYPE, [{a, 3}]},
    Irreducible3 = {?TYPE, [{c, 2}]},
    ?assertNot(irreducible_is_strict_inflation(Irreducible1, Digest)),
    ?assert(irreducible_is_strict_inflation(Irreducible2, Digest)),
    ?assert(irreducible_is_strict_inflation(Irreducible3, Digest)).

join_decomposition_test() ->
    Counter0 = new(),
    Counter1 = {?TYPE, [{1, 2}, {2, 1}, {4, 1}]},
    Decomp0 = join_decomposition(Counter0),
    Decomp1 = join_decomposition(Counter1),
    ?assertEqual([], Decomp0),
    ?assertEqual(lists:sort([{?TYPE, [{1, 2}]}, {?TYPE, [{2, 1}]}, {?TYPE, [{4, 1}]}]), lists:sort(Decomp1)).

encode_decode_test() ->
    Counter = {?TYPE, [{1, 2}, {2, 1}, {4, 1}, {5, 6}]},
    Binary = encode(erlang, Counter),
    ECounter = decode(erlang, Binary),
    ?assertEqual(Counter, ECounter).

-endif.
