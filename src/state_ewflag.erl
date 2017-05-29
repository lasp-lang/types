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

%% @doc Enable-Wins Flag CRDT.
%%      Starts disabled.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(state_ewflag).
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

-export_type([state_ewflag/0, state_ewflag_op/0]).

-opaque state_ewflag() :: {?TYPE, payload()}.
-type payload() :: state_causal_type:causal_crdt().
-type state_ewflag_op() :: enable | disable.

%% @doc Create a new, empty `state_ewflag()'
-spec new() -> state_ewflag().
new() ->
    {?TYPE, state_causal_type:new(dot_set)}.

%% @doc Create a new, empty `state_ewflag()'
-spec new([term()]) -> state_ewflag().
new([]) ->
    new().

%% @doc Mutate a `state_ewflag()'.
-spec mutate(state_ewflag_op(), type:id(), state_ewflag()) ->
    {ok, state_ewflag()}.
mutate(Op, Actor, {?TYPE, _Flag}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_ewflag()'.
%%      The first argument can be:
%%          - `enable'
%%          - `disable'
%%      The second argument is the replica id.
%%      The third argument is the `state_ewflag()' to be inflated.
-spec delta_mutate(state_ewflag_op(), type:id(), state_ewflag()) ->
    {ok, state_ewflag()}.

%% @doc Enables `state_ewflag()'.
delta_mutate(enable, Actor, {?TYPE, {DotStore, CausalContext}}) ->
    NextDot = causal_context:next_dot(Actor, CausalContext),

    DeltaDotStore = dot_set:add_dot(NextDot, dot_set:new()),
    DeltaCausalContext = causal_context:from_dot_set(
        dot_set:union(DotStore, DeltaDotStore)
    ),

    Delta = {DeltaDotStore, DeltaCausalContext},
    {ok, {?TYPE, Delta}};

%% @doc Disables `state_ewflag()'.
delta_mutate(disable, _Actor, {?TYPE, {DotStore, _CausalContext}}) ->
    DeltaDotStore = dot_set:new(),
    DeltaCausalContext = causal_context:from_dot_set(DotStore),

    Delta = {DeltaDotStore, DeltaCausalContext},
    {ok, {?TYPE, Delta}}.

%% @doc Returns the value of the `state_ewflag()'.
-spec query(state_ewflag()) -> boolean().
query({?TYPE, {DotStore, _CausalContext}}) ->
    not dot_set:is_empty(DotStore).

%% @doc Merge two `state_ewflag()'.
%%      Merging is handled by the `merge' function in
%%      `state_causal_type' common library.
-spec merge(state_ewflag(), state_ewflag()) -> state_ewflag().
merge({?TYPE, Flag1}, {?TYPE, Flag2}) ->
    Flag = state_causal_type:merge(dot_set, Flag1, Flag2),
    {?TYPE, Flag}.

%% @doc Equality for `state_ewflag()'.
%%      Since everything is ordered, == should work.
-spec equal(state_ewflag(), state_ewflag()) -> boolean().
equal({?TYPE, Flag1}, {?TYPE, Flag2}) ->
    Flag1 == Flag2.

%% @doc Check if an `state_ewflag()' is bottom.
-spec is_bottom(state_ewflag()) -> boolean().
is_bottom({?TYPE, _}=CRDT) ->
    CRDT == new().

%% @doc Given two `state_ewflag()', check if the second is and inflation of the first.
%% @todo
-spec is_inflation(state_ewflag(), state_ewflag()) -> boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_ewflag(), state_ewflag()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_ewflag(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=A, B) ->
    state_type:irreducible_is_strict_inflation(A, B).

-spec digest(state_ewflag()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_ewflag()'.
%% @todo
-spec join_decomposition(state_ewflag()) -> [state_ewflag()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

%% @doc Delta calculation for `state_ewflag()'.
-spec delta(state_ewflag(), state_type:digest()) -> state_ewflag().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_ewflag()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_ewflag().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {[], causal_context:new()}},
                 new()).

query_test() ->
    Flag0 = new(),
    Flag1 = {?TYPE,
        {
            [{a, 2}],
            {[{a, 2}], []}
        }
    },
    ?assertEqual(false, query(Flag0)),
    ?assertEqual(true, query(Flag1)).

delta_mutate_test() ->
    ActorOne = 1,
    ActorTwo = 2,
    Flag0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate(enable, ActorOne, Flag0),
    Flag1 = merge({?TYPE, Delta1}, Flag0),
    {ok, {?TYPE, Delta2}} = delta_mutate(enable, ActorTwo, Flag1),
    Flag2 = merge({?TYPE, Delta2}, Flag1),
    {ok, {?TYPE, Delta3}} = delta_mutate(disable, ActorTwo, Flag2),
    Flag3 = merge({?TYPE, Delta3}, Flag2),
    {ok, {?TYPE, Delta4}} = delta_mutate(enable, ActorTwo, Flag3),
    Flag4 = merge({?TYPE, Delta4}, Flag3),

    ?assertEqual({?TYPE,
        {
            [{ActorOne, 1}],
            {[{ActorOne, 1}], []}
        }},
        {?TYPE, Delta1}
    ),
    ?assertEqual({?TYPE,
        {
            [{ActorOne, 1}],
            {[{ActorOne, 1}], []}
        }},
        Flag1
    ),
    ?assertEqual({?TYPE,
        {
            [{ActorTwo, 1}],
            {[{ActorOne, 1}, {ActorTwo, 1}], []}
        }},
        {?TYPE, Delta2}
    ),
    ?assertEqual({?TYPE,
        {
            [{ActorTwo, 1}],
            {[{ActorOne, 1}, {ActorTwo, 1}], []}
        }},
        Flag2
    ),
    ?assertEqual({?TYPE,
        {
            [],
            {[{ActorTwo, 1}], []}
        }},
        {?TYPE, Delta3}
    ),
    ?assertEqual({?TYPE,
        {
            [],
            {[{ActorOne, 1}, {ActorTwo, 1}], []}
        }},
        Flag3
    ),
    ?assertEqual({?TYPE,
        {
            [{ActorTwo, 2}],
            {[], [{ActorTwo, 2}]}
        }},
        {?TYPE, Delta4}
    ),
    ?assertEqual({?TYPE,
        {
            [{ActorTwo, 2}],
            {[{ActorOne, 1}, {ActorTwo, 2}], []}
        }},
        Flag4
    ).

mutate_test() ->
    ActorOne = 1,
    ActorTwo = 2,
    Flag0 = new(),
    {ok, Flag1} = mutate(enable, ActorOne, Flag0),
    {ok, Flag2} = mutate(enable, ActorTwo, Flag1),
    {ok, Flag3} = mutate(disable, ActorTwo, Flag2),
    {ok, Flag4} = mutate(enable, ActorTwo, Flag3),

    ?assertEqual({?TYPE,
        {
            [{ActorOne, 1}],
            {[{ActorOne, 1}], []}
        }},
        Flag1
    ),
    ?assertEqual({?TYPE,
        {
            [{ActorTwo, 1}],
            {[{ActorOne, 1}, {ActorTwo, 1}], []}
        }},
        Flag2
    ),
    ?assertEqual({?TYPE,
        {
            [],
            {[{ActorOne, 1}, {ActorTwo, 1}], []}
        }},
        Flag3
    ),
    ?assertEqual({?TYPE,
        {
            [{ActorTwo, 2}],
            {[{ActorOne, 1}, {ActorTwo, 2}], []}
        }},
        Flag4
    ).

merge_idempontent_test() ->
    Flag1 = {?TYPE, {[], {[{1, 1}], []}}},
    Flag2 = {?TYPE, {[{2, 3}], {[{1, 1}, {2, 3}], []}}},
    Flag3 = {?TYPE, {[{1, 1}, {2, 3}], {[{1, 1}, {2, 3}], []}}},
    Flag4 = merge(Flag1, Flag1),
    Flag5 = merge(Flag2, Flag2),
    Flag6 = merge(Flag3, Flag3),
    ?assertEqual(Flag1, Flag4),
    ?assertEqual(Flag2, Flag5),
    ?assertEqual(Flag3, Flag6).

merge_commutative_test() ->
    Flag1 = {?TYPE, {[], {[{1, 1}], []}}},
    Flag2 = {?TYPE, {[{2, 3}], {[{1, 1}, {2, 3}], []}}},
    Flag3 = {?TYPE, {[{1, 1}, {2, 3}], {[{1, 1}, {2, 3}], []}}},
    Flag4 = merge(Flag1, Flag2),
    Flag5 = merge(Flag2, Flag1),
    Flag6 = merge(Flag1, Flag3),
    Flag7 = merge(Flag3, Flag1),
    Flag8 = merge(Flag2, Flag3),
    Flag9 = merge(Flag3, Flag2),
    Flag10 = merge(Flag1, merge(Flag2, Flag3)),
    Flag1_2 = Flag2,
    Flag1_3 = Flag2,
    Flag2_3 = Flag2,
    ?assertEqual(Flag1_2, Flag4),
    ?assertEqual(Flag1_2, Flag5),
    ?assertEqual(Flag1_3, Flag6),
    ?assertEqual(Flag1_3, Flag7),
    ?assertEqual(Flag2_3, Flag8),
    ?assertEqual(Flag2_3, Flag9),
    ?assertEqual(Flag1_3, Flag10).

merge_delta_test() ->
    Flag1 = {?TYPE, {[{1, 1}, {2, 3}], {[{1, 1}, {2, 3}], []}}},
    Delta1 = {?TYPE, {[], {[{1, 1}], []}}},
    Delta2 = {?TYPE, {[], {[{2, 3}], []}}},
    Flag2 = merge(Delta1, Flag1),
    Flag3 = merge(Flag1, Delta2),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, {[{2, 3}], {[{1, 1}, {2, 3}], []}}}, Flag2),
    ?assertEqual({?TYPE, {[{1, 1}], {[{1, 1}, {2, 3}], []}}}, Flag3),
    ?assertEqual({?TYPE, {[], {[{1, 1}, {2, 3}], []}}}, DeltaGroup).

equal_test() ->
    Flag1 = {?TYPE, {[{1, 1}, {2, 3}], {[{1, 1}, {2, 3}], []}}},
    Flag2 = {?TYPE, {[{1, 1}], {[{1, 1}, {2, 3}], []}}},
    Flag3 = {?TYPE, {[{1, 1}], {[{1, 1}, {2, 2}], []}}},
    ?assert(equal(Flag1, Flag1)),
    ?assert(equal(Flag2, Flag2)),
    ?assert(equal(Flag3, Flag3)),
    ?assertNot(equal(Flag1, Flag2)),
    ?assertNot(equal(Flag1, Flag3)),
    ?assertNot(equal(Flag2, Flag3)).

is_bottom_test() ->
    Flag0 = new(),
    Flag1 = {?TYPE, {[], {[{2, 3}], []}}},
    ?assert(is_bottom(Flag0)),
    ?assertNot(is_bottom(Flag1)).

is_inflation_test() ->
    Flag1 = {?TYPE, {[{1, 1}, {2, 3}], {[{1, 1}, {2, 3}], []}}},
    Flag2 = {?TYPE, {[{2, 3}], {[{1, 1}, {2, 3}], []}}},
    Flag3 = {?TYPE, {[{1, 1}], {[{1, 1}, {2, 3}], []}}},
    ?assert(is_inflation(Flag1, Flag1)),
    ?assert(is_inflation(Flag1, Flag2)),
    ?assertNot(is_inflation(Flag2, Flag1)),
    ?assert(is_inflation(Flag1, Flag3)),
    ?assertNot(is_inflation(Flag2, Flag3)),
    ?assertNot(is_inflation(Flag3, Flag2)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Flag1, Flag1)),
    ?assert(state_type:is_inflation(Flag1, Flag2)),
    ?assertNot(state_type:is_inflation(Flag2, Flag1)),
    ?assert(state_type:is_inflation(Flag1, Flag3)),
    ?assertNot(state_type:is_inflation(Flag2, Flag3)),
    ?assertNot(state_type:is_inflation(Flag3, Flag2)).

is_strict_inflation_test() ->
    Flag1 = {?TYPE, {[{1, 1}, {2, 3}], {[{1, 1}, {2, 3}], []}}},
    Flag2 = {?TYPE, {[{2, 3}], {[{1, 1}, {2, 3}], []}}},
    Flag3 = {?TYPE, {[{1, 1}], {[{1, 1}, {2, 3}], []}}},
    ?assertNot(is_strict_inflation(Flag1, Flag1)),
    ?assert(is_strict_inflation(Flag1, Flag2)),
    ?assertNot(is_strict_inflation(Flag2, Flag1)),
    ?assert(is_strict_inflation(Flag1, Flag3)),
    ?assertNot(is_strict_inflation(Flag2, Flag3)),
    ?assertNot(is_strict_inflation(Flag3, Flag2)).

join_decomposition_test() ->
    %% @todo
    ok.

encode_decode_test() ->
    Flag = {?TYPE, {[{1, 1}, {2, 3}], {[{1, 1}, {2, 3}], []}}},
    Binary = encode(erlang, Flag),
    EFlag = decode(erlang, Binary),
    ?assertEqual(Flag, EFlag).

-endif.
