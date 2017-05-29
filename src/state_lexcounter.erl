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

%% @doc Lexicographic Counter.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]
%%
%% @reference Carlos Baquero
%%      delta-enabled-crdts C++ library
%%      [https://github.com/CBaquero/delta-enabled-crdts]

-module(state_lexcounter).
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

-export_type([state_lexcounter/0, state_lexcounter_op/0]).

-opaque state_lexcounter() :: {?TYPE, payload()}.
-type payload() :: orddict:orddict().
-type state_lexcounter_op() :: increment | decrement.

%% @doc Create a new, empty `state_lexcounter()'
-spec new() -> state_lexcounter().
new() ->
    {?TYPE, orddict:new()}.

%% @doc Create a new, empty `state_lexcounter()'
-spec new([term()]) -> state_lexcounter().
new([]) ->
    new().

%% @doc Mutate a `state_lexcounter()'.
-spec mutate(state_lexcounter_op(), type:id(), state_lexcounter()) ->
    {ok, state_lexcounter()}.
mutate(Op, Actor, {?TYPE, _LexCounter}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_lexcounter()'.
%%      The first argument can be `increment' or `decrement'.
%%      The second argument is the replica id.
%%      The third argument is the `state_lexcounter()' to be inflated.
-spec delta_mutate(state_lexcounter_op(), type:id(), state_lexcounter()) ->
    {ok, state_lexcounter()}.
delta_mutate(increment, Actor, {?TYPE, LexCounter}) ->
    {Left, Right} = orddict_ext:fetch(Actor, LexCounter, {0, 0}),
    Delta = orddict:store(Actor, {Left, Right + 1}, orddict:new()),
    {ok, {?TYPE, Delta}};

delta_mutate(decrement, Actor, {?TYPE, LexCounter}) ->
    {Left, Right} = orddict_ext:fetch(Actor, LexCounter, {0, 0}),
    Delta = orddict:store(Actor, {Left + 1, Right - 1}, orddict:new()),
    {ok, {?TYPE, Delta}}.

%% @doc Returns the value of the `state_lexcounter()'.
%%      A `state_lexcounter()' is a dictionary where the values are
%%      pairs. The value of the `state_lexcounter()' is the sum of
%%      the second components of these pairs.
-spec query(state_lexcounter()) -> non_neg_integer().
query({?TYPE, LexCounter}) ->
    lists:sum([ Right || {_Actor, {_Left, Right}} <- LexCounter ]).

%% @doc Merge two `state_lexcounter()'.
%%      The keys of the resulting `state_lexcounter()' are the union of the
%%      keys of both `state_lexcounter()' passed as input.
%%      If a key is only present on one of the `state_lexcounter()',
%%      its correspondent lexicographic pair is preserved.
%%      If a key is present in both `state_lexcounter()', the new value
%%      will be the join of the lexicographic pairs.
-spec merge(state_lexcounter(), state_lexcounter()) -> state_lexcounter().
merge({?TYPE, LexCounter1}, {?TYPE, LexCounter2}) ->
    LexCounter = orddict:merge(
        fun(_, Value1, Value2) ->
            join(Value1, Value2)
        end,
        LexCounter1,
        LexCounter2
    ),
    {?TYPE, LexCounter}.

join({Left1, Right1}, {Left2, _Right2}) when Left1 > Left2 ->
    {Left1, Right1};
join({Left1, _Right1}, {Left2, Right2}) when Left2 > Left1 ->
    {Left2, Right2};
join({Left1, Right1}, {Left2, Right2}) when Left1 == Left2 ->
    {Left1, max(Right1, Right2)};
join({Left1, _Right1}, {Left2, _Right2}) ->
    {max(Left1, Left2), 0}.

%% @doc Equality for `state_lexcounter()'.
-spec equal(state_lexcounter(), state_lexcounter()) -> boolean().
equal({?TYPE, LexCounter1}, {?TYPE, LexCounter2}) ->
    Fun = fun({Left1, Right1}, {Left2, Right2}) -> Left1 == Left2 andalso Right1 == Right2 end,
    orddict_ext:equal(LexCounter1, LexCounter2, Fun).

%% @doc Check if a LexCounter is bottom.
-spec is_bottom(state_lexcounter()) -> boolean().
is_bottom({?TYPE, LexCounter}) ->
    orddict:is_empty(LexCounter).

%% @doc Given two `state_lexcounter()', check if the second is and
%%      inflation of the first.
%%      We have an inflation if, for every key present in the first
%%      `state_lexcounter()', that key is also in the second and,
%%      the correspondent lexicographic pair in the second
%%      `state_lexcounter()' is an inflation of the lexicographic pair
%%      associated to the same key in the first `state_lexcounter()'.
%%      A lexicographic pair P1 is an inflation of a lexicographic
%%      pair P2 if one of the following:
%%          - the first component of P2 is an inflation of the first
%%          component of P1
%%          - their first components are equal and the second component
%%          of P2 is and inflation of the second component of P1
-spec is_inflation(state_lexcounter(), state_lexcounter()) -> boolean().
is_inflation({?TYPE, LexCounter1}, {?TYPE, LexCounter2}) ->
    LexPairInflation = fun({Left1, Right1}, {Left2, Right2}) ->
        (Left2 > Left1)
        orelse
        (Left1 == Left2 andalso Right2 >= Right1)
    end,
    lists_ext:iterate_until(
        fun({Key, Value1}) ->
            case orddict:find(Key, LexCounter2) of
                {ok, Value2} ->
                    LexPairInflation(Value1, Value2);
                error ->
                    false
            end
        end,
        LexCounter1
    ).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_lexcounter(), state_lexcounter()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_lexcounter(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=A, B) ->
    state_type:irreducible_is_strict_inflation(A, B).

-spec digest(state_lexcounter()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_lexcounter()'.
%% @todo
-spec join_decomposition(state_lexcounter()) -> [state_lexcounter()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

%% @doc Delta calculation for `state_lexcounter()'.
-spec delta(state_lexcounter(), state_type:digest()) -> state_lexcounter().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_lexcounter()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_lexcounter().
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
    Counter1 = {?TYPE, [{1, {1, 2}}, {2, {1, 13}}, {3, {1, 2}}]},
    ?assertEqual(0, query(Counter0)),
    ?assertEqual(17, query(Counter1)).

delta_test() ->
    Counter0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate(increment, 1, Counter0),
    Counter1 = merge({?TYPE, Delta1}, Counter0),
    {ok, {?TYPE, Delta2}} = delta_mutate(decrement, 2, Counter1),
    Counter2 = merge({?TYPE, Delta2}, Counter1),
    {ok, {?TYPE, Delta3}} = delta_mutate(increment, 1, Counter2),
    Counter3 = merge({?TYPE, Delta3}, Counter2),
    ?assertEqual({?TYPE, [{1, {0, 1}}]}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, [{1, {0, 1}}]}, Counter1),
    ?assertEqual({?TYPE, [{2, {1, -1}}]}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, [{1, {0, 1}}, {2, {1, -1}}]}, Counter2),
    ?assertEqual({?TYPE, [{1, {0, 2}}]}, {?TYPE, Delta3}),
    ?assertEqual({?TYPE, [{1, {0, 2}}, {2, {1, -1}}]}, Counter3).

increment_test() ->
    Counter0 = new(),
    {ok, Counter1} = mutate(increment, 1, Counter0),
    {ok, Counter2} = mutate(decrement, 2, Counter1),
    {ok, Counter3} = mutate(increment, 1, Counter2),
    ?assertEqual({?TYPE, [{1, {0, 1}}]}, Counter1),
    ?assertEqual({?TYPE, [{1, {0, 1}}, {2, {1, -1}}]}, Counter2),
    ?assertEqual({?TYPE, [{1, {0, 2}}, {2, {1, -1}}]}, Counter3).

merge_test() ->
    Counter1 = {?TYPE, [{<<"5">>, {6, 2}}]},
    Counter2 = {?TYPE, [{<<"5">>, {5, 3}}]},
    Counter3 = {?TYPE, [{<<"5">>, {5, 10}}]},
    Counter4 = merge(Counter1, Counter1),
    Counter5 = merge(Counter1, Counter2),
    Counter6 = merge(Counter2, Counter1),
    Counter7 = merge(Counter2, Counter3),
    ?assertEqual({?TYPE, [{<<"5">>, {6, 2}}]}, Counter4),
    ?assertEqual({?TYPE, [{<<"5">>, {6, 2}}]}, Counter5),
    ?assertEqual({?TYPE, [{<<"5">>, {6, 2}}]}, Counter6),
    ?assertEqual({?TYPE, [{<<"5">>, {5, 10}}]}, Counter7).

merge_delta_test() ->
    Counter1 = {?TYPE, [{<<"1">>, {2, 3}}, {<<"2">>, {5, 2}}]},
    Delta1 = {?TYPE, [{<<"1">>, {2, 4}}]},
    Delta2 = {?TYPE, [{<<"3">>, {1, 2}}]},
    Counter2 = merge(Delta1, Counter1),
    Counter3 = merge(Counter1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, [{<<"1">>, {2, 4}}, {<<"2">>, {5, 2}}]}, Counter2),
    ?assertEqual({?TYPE, [{<<"1">>, {2, 4}}, {<<"2">>, {5, 2}}]}, Counter3),
    ?assertEqual({?TYPE, [{<<"1">>, {2, 4}}, {<<"3">>, {1, 2}}]}, DeltaGroup).

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
    Counter1 = {?TYPE, [{<<"1">>, {2, 0}}]},
    Counter2 = {?TYPE, [{<<"1">>, {2, 0}}, {<<"2">>, {1, -1}}]},
    Counter3 = {?TYPE, [{<<"1">>, {2, 1}}]},
    Counter4 = {?TYPE, [{<<"1">>, {3, -2}}]},
    Counter5 = {?TYPE, [{<<"1">>, {2, -1}}]},
    ?assert(is_inflation(Counter1, Counter1)),
    ?assert(is_inflation(Counter1, Counter2)),
    ?assertNot(is_inflation(Counter2, Counter1)),
    ?assert(is_inflation(Counter1, Counter3)),
    ?assert(is_inflation(Counter1, Counter4)),
    ?assertNot(is_inflation(Counter1, Counter5)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Counter1, Counter1)),
    ?assert(state_type:is_inflation(Counter1, Counter2)),
    ?assertNot(state_type:is_inflation(Counter2, Counter1)),
    ?assert(state_type:is_inflation(Counter1, Counter3)),
    ?assert(state_type:is_inflation(Counter1, Counter4)),
    ?assertNot(state_type:is_inflation(Counter1, Counter5)).

is_strict_inflation_test() ->
    Counter1 = {?TYPE, [{<<"1">>, {2, 0}}]},
    Counter2 = {?TYPE, [{<<"1">>, {2, 0}}, {<<"2">>, {1, -1}}]},
    Counter3 = {?TYPE, [{<<"1">>, {2, 1}}]},
    Counter4 = {?TYPE, [{<<"1">>, {3, -2}}]},
    Counter5 = {?TYPE, [{<<"1">>, {2, -1}}]},
    ?assertNot(is_strict_inflation(Counter1, Counter1)),
    ?assert(is_strict_inflation(Counter1, Counter2)),
    ?assertNot(is_strict_inflation(Counter2, Counter1)),
    ?assert(is_strict_inflation(Counter1, Counter3)),
    ?assert(is_strict_inflation(Counter1, Counter4)),
    ?assertNot(is_strict_inflation(Counter1, Counter5)).

join_decomposition_test() ->
    %% @todo
    ok.

encode_decode_test() ->
    Counter = {?TYPE, [{<<"1">>, {2, 0}}, {<<"2">>, {1, -1}}]},
    Binary = encode(erlang, Counter),
    ECounter = decode(erlang, Binary),
    ?assertEqual(Counter, ECounter).

-endif.
