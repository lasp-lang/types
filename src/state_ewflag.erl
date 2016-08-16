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

-export([new/0, new/1, new_delta/0, new_delta/1, is_delta/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_bottom/1, is_inflation/2, is_strict_inflation/2]).
-export([join_decomposition/1]).
-export([encode/2, decode/2]).

-export_type([state_ewflag/0, delta_state_ewflag/0, state_ewflag_op/0]).

-opaque state_ewflag() :: {?TYPE, payload()}.
-opaque delta_state_ewflag() :: {?TYPE, {delta, payload()}}.
-type delta_or_state() :: state_ewflag() | delta_state_ewflag().
-type payload() :: state_causal_type:causal_crdt().
-type element() :: term().
-type state_ewflag_op() :: enable | disable.

%% @doc Create a new, empty `state_ewflag()'
%%      DotMap<Elem, DotFlag>
-spec new() -> state_ewflag().
new() ->
    {?TYPE, state_causal_type:new(dot_set)}.

%% @doc Create a new, empty `state_ewflag()'
-spec new([term()]) -> state_ewflag().
new([]) ->
    new().

-spec new_delta() -> delta_state_ewflag().
new_delta() ->
    state_type:new_delta(?TYPE).

-spec new_delta([term()]) -> delta_state_ewflag().
new_delta([]) ->
    new_delta().

-spec is_delta(delta_or_state()) -> boolean().
is_delta({?TYPE, _}=CRDT) ->
    state_type:is_delta(CRDT).

%% @doc Mutate a `state_ewflag()'.
-spec mutate(state_ewflag_op(), type:id(), state_ewflag()) ->
    {ok, state_ewflag()} | {error, {precondition, {not_present, [element()]}}}.
mutate(Op, Actor, {?TYPE, _Flag}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_ewflag()'.
%%      The first argument can be:
%%          - `enable'
%%          - `disable'
%%      The second argument is the replica id.
%%      The third argument is the `state_ewflag()' to be inflated.
-spec delta_mutate(state_ewflag_op(), type:id(), state_ewflag()) ->
    {ok, delta_state_ewflag()}.

%% @doc Enables `state_ewflag()'.
delta_mutate(enable, Actor, {?TYPE, {DotStore, CausalContext}}) ->
    NextDot = causal_context:next_dot(Actor, CausalContext),

    EmptyDotFlag = dot_set:new(),
    DeltaDotStore = dot_set:add_element(NextDot, EmptyDotFlag),
    DeltaCausalContext = dot_set:to_causal_context(
        dot_set:union(DotStore, DeltaDotStore)
    ),

    Delta = {DeltaDotStore, DeltaCausalContext},
    {ok, {?TYPE, {delta, Delta}}};

%% @doc Disables `state_ewflag()'.
delta_mutate(disable, _Actor, {?TYPE, {DotStore, _CausalContext}}) ->
    DeltaDotStore = dot_set:new(),
    DeltaCausalContext = dot_set:to_causal_context(DotStore),

    Delta = {DeltaDotStore, DeltaCausalContext},
    {ok, {?TYPE, {delta, Delta}}}.

%% @doc Returns the value of the `state_ewflag()'.
-spec query(state_ewflag()) -> boolean().
query({?TYPE, {DotStore, _CausalContext}}) ->
    not dot_set:is_empty(DotStore).

%% @doc Merge two `state_ewflag()'.
%%      Merging is handled by the `merge' function in
%%      `state_causal_type' common library.
-spec merge(delta_or_state(), delta_or_state()) -> delta_or_state().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun({?TYPE, Flag1}, {?TYPE, Flag2}) ->
        Flag = state_causal_type:merge(Flag1, Flag2),
        {?TYPE, Flag}
    end,
    state_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Equality for `state_ewflag()'.
%%      Since everything is ordered, == should work.
-spec equal(state_ewflag(), state_ewflag()) -> boolean().
equal({?TYPE, Flag1}, {?TYPE, Flag2}) ->
    Flag1 == Flag2.

%% @doc Check if an `state_ewflag()' is bottom.
-spec is_bottom(delta_or_state()) -> boolean().
is_bottom({?TYPE, {delta, Flag}}) ->
    is_bottom({?TYPE, Flag});
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

%% @doc Join decomposition for `state_ewflag()'.
%% @todo
-spec join_decomposition(state_ewflag()) -> [state_ewflag()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

-spec encode(state_type:format(), delta_or_state()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> delta_or_state().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {{dot_set, ordsets:new()}, ordsets:new()}},
                 new()),
    ?assertEqual({?TYPE, {delta, {{dot_set, ordsets:new()}, ordsets:new()}}},
                 new_delta()).

query_test() ->
    Flag0 = new(),
    Flag1 = {?TYPE,
        {
            {dot_set, [{a, 2}]},
            [{a, 1}, {a, 2}]
        }
    },
    ?assertEqual(false, query(Flag0)),
    ?assertEqual(true, query(Flag1)).

delta_mutate_test() ->
    ActorOne = 1,
    ActorTwo = 2,
    Flag0 = new(),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate(enable, ActorOne, Flag0),
    Flag1 = merge({?TYPE, Delta1}, Flag0),
    {ok, {?TYPE, {delta, Delta2}}} = delta_mutate(enable, ActorTwo, Flag1),
    Flag2 = merge({?TYPE, Delta2}, Flag1),
    {ok, {?TYPE, {delta, Delta3}}} = delta_mutate(disable, ActorTwo, Flag2),
    Flag3 = merge({?TYPE, Delta3}, Flag2),
    {ok, {?TYPE, {delta, Delta4}}} = delta_mutate(enable, ActorTwo, Flag3),
    Flag4 = merge({?TYPE, Delta4}, Flag3),

    ?assertEqual({?TYPE,
        {
            {dot_set, [{ActorOne, 1}]},
            [{ActorOne, 1}]
        }},
        {?TYPE, Delta1}
    ),
    ?assertEqual({?TYPE,
        {
            {dot_set, [{ActorOne, 1}]},
            [{ActorOne, 1}]
        }},
        Flag1
    ),
    ?assertEqual({?TYPE,
        {
            {dot_set, [{ActorTwo, 1}]},
            [{ActorOne, 1}, {ActorTwo, 1}]
        }},
        {?TYPE, Delta2}
    ),
    ?assertEqual({?TYPE,
        {
            {dot_set, [{ActorTwo, 1}]},
            [{ActorOne, 1}, {ActorTwo, 1}]
        }},
        Flag2
    ),
    ?assertEqual({?TYPE,
        {
            {dot_set, []},
            [{ActorTwo, 1}]
        }},
        {?TYPE, Delta3}
    ),
    ?assertEqual({?TYPE,
        {
            {dot_set, []},
            [{ActorOne, 1}, {ActorTwo, 1}]
        }},
        Flag3
    ),
    ?assertEqual({?TYPE,
        {
            {dot_set, [{ActorTwo, 2}]},
            [{ActorTwo, 2}]
        }},
        {?TYPE, Delta4}
    ),
    ?assertEqual({?TYPE,
        {
            {dot_set, [{ActorTwo, 2}]},
            [{ActorOne, 1}, {ActorTwo, 1}, {ActorTwo, 2}]
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
            {dot_set, [{ActorOne, 1}]},
            [{ActorOne, 1}]
        }},
        Flag1
    ),
    ?assertEqual({?TYPE,
        {
            {dot_set, [{ActorTwo, 1}]},
            [{ActorOne, 1}, {ActorTwo, 1}]
        }},
        Flag2
    ),
    ?assertEqual({?TYPE,
        {
            {dot_set, []},
            [{ActorOne, 1}, {ActorTwo, 1}]
        }},
        Flag3
    ),
    ?assertEqual({?TYPE,
        {
            {dot_set, [{ActorTwo, 2}]},
            [{ActorOne, 1}, {ActorTwo, 1}, {ActorTwo, 2}]
        }},
        Flag4
    ).

merge_idempontent_test() ->
    Flag1 = {?TYPE, {{dot_set, []}, [{1, 1}]}},
    Flag2 = {?TYPE, {{dot_set, [{2, 3}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
    Flag3 = {?TYPE, {{dot_set, [{1, 1}, {2, 3}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
    Flag4 = merge(Flag1, Flag1),
    Flag5 = merge(Flag2, Flag2),
    Flag6 = merge(Flag3, Flag3),
    ?assertEqual(Flag1, Flag4),
    ?assertEqual(Flag2, Flag5),
    ?assertEqual(Flag3, Flag6).

merge_commutative_test() ->
    Flag1 = {?TYPE, {{dot_set, []}, [{1, 1}]}},
    Flag2 = {?TYPE, {{dot_set, [{2, 3}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
    Flag3 = {?TYPE, {{dot_set, [{1, 1}, {2, 3}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
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
    Flag1 = {?TYPE, {{dot_set, [{1, 1}, {2, 3}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
    Delta1 = {?TYPE, {delta, {{dot_set, []}, [{1, 1}]}}},
    Delta2 = {?TYPE, {delta, {{dot_set, []}, [{2, 3}]}}},
    Flag2 = merge(Delta1, Flag1),
    Flag3 = merge(Flag1, Delta2),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, {{dot_set, [{2, 3}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}}, Flag2),
    ?assertEqual({?TYPE, {{dot_set, [{1, 1}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}}, Flag3),
    ?assertEqual({?TYPE, {delta, {{dot_set, []}, [{1, 1}, {2, 3}]}}}, DeltaGroup).

equal_test() ->
    Flag1 = {?TYPE, {{dot_set, [{1, 1}, {2, 3}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
    Flag2 = {?TYPE, {{dot_set, [{1, 1}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
    Flag3 = {?TYPE, {{dot_set, [{1, 1}]}, [{1, 1}, {2, 1}, {2, 2}]}},
    ?assert(equal(Flag1, Flag1)),
    ?assert(equal(Flag2, Flag2)),
    ?assert(equal(Flag3, Flag3)),
    ?assertNot(equal(Flag1, Flag2)),
    ?assertNot(equal(Flag1, Flag3)),
    ?assertNot(equal(Flag2, Flag3)).

is_bottom_test() ->
    Flag0 = new(),
    Flag1 = {?TYPE, {{dot_set, []}, [{2, 3}]}},
    ?assert(is_bottom(Flag0)),
    ?assertNot(is_bottom(Flag1)).

is_inflation_test() ->
    Flag1 = {?TYPE, {{dot_set, [{1, 1}, {2, 3}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
    Flag2 = {?TYPE, {{dot_set, [{2, 3}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
    Flag3 = {?TYPE, {{dot_set, [{1, 1}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
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
    Flag1 = {?TYPE, {{dot_set, [{1, 1}, {2, 3}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
    Flag2 = {?TYPE, {{dot_set, [{2, 3}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
    Flag3 = {?TYPE, {{dot_set, [{1, 1}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
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
    Flag = {?TYPE, {{dot_set, [{1, 1}, {2, 3}]}, [{1, 1}, {2, 1}, {2, 2}, {2, 3}]}},
    Binary = encode(erlang, Flag),
    EFlag = decode(erlang, Binary),
    ?assertEqual(Flag, EFlag).

-endif.
