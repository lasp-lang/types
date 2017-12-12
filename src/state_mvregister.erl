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

%% @doc Multi-Value Register CRDT.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(state_mvregister).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-include("state_type.hrl").

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

-export_type([state_mvregister/0, state_mvregister_op/0]).

-opaque state_mvregister() :: {?TYPE, payload()}.
-type payload() :: state_causal_type:causal_crdt().
-type timestamp() :: non_neg_integer().
-type value() :: term().
-type state_mvregister_op() :: {set, timestamp(), value()}.

%% @doc Create a new, empty `state_mvregister()'
-spec new() -> state_mvregister().
new() ->
    {?TYPE, state_causal_type:new(dot_fun)}.

%% @doc Create a new, empty `state_mvregister()'
-spec new([term()]) -> state_mvregister().
new([]) ->
    new().

%% @doc Mutate a `state_mvregister()'.
-spec mutate(state_mvregister_op(), type:id(), state_mvregister()) ->
    {ok, state_mvregister()}.
mutate(Op, Actor, {?TYPE, _Register}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_mvregister()'.
%%      The first argument is:
%%          - `{set, timestamp(), value()}'.
%%          - the second component in this triple will not be used.
%%          - in order to have an unified API for all registers
%%          (since LWWRegister needs to receive a timestamp),
%%          the timestamp is also supplied here
%%      The second argument is the replica id.
%%      The third argument is the `state_mvregister()' to be inflated.
-spec delta_mutate(state_mvregister_op(), type:id(),
                   state_mvregister()) -> {ok, state_mvregister()}.

%% @doc Sets `state_mvregister()' value.
delta_mutate({set, _Timestamp, Value}, Actor,
             {?TYPE, {DotStore, CausalContext}}) ->
    NextDot = causal_context:next_dot(Actor, CausalContext),

    DeltaDotStore = dot_fun:store(NextDot, Value, dot_fun:new()),

    DotSet0 = state_causal_type:dots({dot_fun, ?IVAR_TYPE}, DotStore),
    DotSet = dot_set:add_dot(NextDot, DotSet0),
    DeltaCausalContext = causal_context:from_dot_set(DotSet),

    Delta = {DeltaDotStore, DeltaCausalContext},
    {ok, {?TYPE, Delta}}.

%% @doc Returns the value of the `state_mvregister()'.
-spec query(state_mvregister()) -> sets:set(value()).
query({?TYPE, {DotStore, _}}) ->
    sets:from_list([Value || {_, Value} <- dot_fun:to_list(DotStore)]).

%% @doc Merge two `state_mvregister()'.
-spec merge(state_mvregister(), state_mvregister()) -> state_mvregister().
merge({?TYPE, Register1}, {?TYPE, Register2}) ->
    Register = state_causal_type:merge({dot_fun, ?IVAR_TYPE},
                                       Register1,
                                       Register2),
    {?TYPE, Register}.

%% @doc Equality for `state_mvregister()'.
%%      Since everything is ordered, == should work.
-spec equal(state_mvregister(), state_mvregister()) -> boolean().
equal({?TYPE, Register1}, {?TYPE, Register2}) ->
    Register1 == Register2.

%% @doc Check if an `state_mvregister()' is bottom.
-spec is_bottom(state_mvregister()) -> boolean().
is_bottom({?TYPE, _}=CRDT) ->
    CRDT == new().

%% @doc Given two `state_mvregister()', check if the second is and inflation of the first.
%% @todo
-spec is_inflation(state_mvregister(), state_mvregister()) ->
    boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_mvregister(), state_mvregister()) ->
    boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_mvregister(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=A, B) ->
    state_type:irreducible_is_strict_inflation(A, B).

-spec digest(state_mvregister()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_mvregister()'.
%% @todo
-spec join_decomposition(state_mvregister()) -> [state_mvregister()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

%% @doc Delta calculation for `state_mvregister()'.
-spec delta(state_mvregister(), state_type:digest()) -> state_mvregister().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_mvregister()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_mvregister().
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
    Register0 = new(),
    Register1 = {?TYPE,
        {
            [{{a, 2}, value}],
            {[{a, 2}], []}
        }
    },
    ?assertEqual(sets:from_list([]), query(Register0)),
    ?assertEqual(sets:from_list([value]), query(Register1)).

delta_mutate_test() ->
    Timestamp = 0, %% won't be used
    Value1 = "17",
    Value2 = "23",
    ActorOne = 1,
    ActorTwo = 2,
    Register0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate({set, Timestamp, Value1}, ActorOne, Register0),
    Register1 = merge({?TYPE, Delta1}, Register0),
    {ok, {?TYPE, Delta2}} = delta_mutate({set, Timestamp, Value2}, ActorTwo, Register1),
    Register2 = merge({?TYPE, Delta2}, Register1),
    {ok, {?TYPE, Delta3}} = delta_mutate({set, Timestamp, Value1}, ActorTwo, Register2),
    Register3 = merge({?TYPE, Delta3}, Register2),

    ?assertEqual({?TYPE,
        {
            [{{ActorOne, 1}, Value1}],
            {[{ActorOne, 1}], []}
        }},
        {?TYPE, Delta1}
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorOne, 1}, Value1}],
            {[{ActorOne, 1}], []}
        }},
        Register1
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorTwo, 1}, Value2}],
            {[{ActorOne, 1}, {ActorTwo, 1}], []}
        }},
        {?TYPE, Delta2}
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorTwo, 1}, Value2}],
            {[{ActorOne, 1}, {ActorTwo, 1}], []}
        }},
        Register2
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorTwo, 2}, Value1}],
            {[{ActorTwo, 2}], []}
        }},
        {?TYPE, Delta3}
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorTwo, 2}, Value1}],
            {[{ActorOne, 1}, {ActorTwo, 2}], []}
        }},
        Register3
    ).

mutate_test() ->
    Timestamp = 0, %% won't be used
    Value1 = "17",
    Value2 = "23",
    ActorOne = 1,
    ActorTwo = 2,
    Register0 = new(),
    {ok, Register1} = mutate({set, Timestamp, Value1}, ActorOne, Register0),
    {ok, Register2} = mutate({set, Timestamp, Value2}, ActorTwo, Register1),
    {ok, Register3} = mutate({set, Timestamp, Value1}, ActorTwo, Register2),

    ?assertEqual({?TYPE,
        {
            [{{ActorOne, 1}, Value1}],
            {[{ActorOne, 1}], []}
        }},
        Register1
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorTwo, 1}, Value2}],
            {[{ActorOne, 1}, {ActorTwo, 1}], []}
        }},
        Register2
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorTwo, 2}, Value1}],
            {[{ActorOne, 1}, {ActorTwo, 2}], []}
        }},
        Register3
    ).

merge_idempontent_test() ->
    Value1 = "17",
    Value2 = "23",
    ActorOne = 1,
    ActorTwo = 2,
    Register1 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}],
                    {[{ActorOne, 1}], []}
                }},
    Register2 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}, {{ActorTwo, 1}, Value2}],
                    {[{ActorOne, 1}, {ActorTwo, 1}], []}
                }},
    Register3 = {?TYPE, {
                    [{{ActorTwo, 1}, Value2}],
                    {[{ActorTwo, 1}], []}
                }},
    Register4 = merge(Register1, Register1),
    Register5 = merge(Register2, Register2),
    Register6 = merge(Register3, Register3),
    ?assertEqual(Register1, Register4),
    ?assertEqual(Register2, Register5),
    ?assertEqual(Register3, Register6).

merge_commutative_test() ->
    Value1 = "17",
    Value2 = "23",
    ActorOne = 1,
    ActorTwo = 2,
    Register1 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}],
                    {[{ActorOne, 1}], []}
                }},
    Register2 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}, {{ActorTwo, 1}, Value2}],
                    {[{ActorOne, 1}, {ActorTwo, 1}], []}
                }},
    Register3 = {?TYPE, {
                    [{{ActorTwo, 1}, Value2}],
                    {[{ActorTwo, 1}], []}
                }},
    Register4 = merge(Register1, Register2),
    Register5 = merge(Register2, Register1),
    Register6 = merge(Register1, Register3),
    Register7 = merge(Register3, Register1),
    Register8 = merge(Register2, Register3),
    Register9 = merge(Register3, Register2),
    Register10 = merge(Register1, merge(Register2, Register3)),
    Register1_2 = Register2,
    Register1_3 = Register2,
    Register2_3 = Register2,
    ?assertEqual(Register1_2, Register4),
    ?assertEqual(Register1_2, Register5),
    ?assertEqual(Register1_3, Register6),
    ?assertEqual(Register1_3, Register7),
    ?assertEqual(Register2_3, Register8),
    ?assertEqual(Register2_3, Register9),
    ?assertEqual(Register1_3, Register10).

equal_test() ->
    Value1 = "17",
    Value2 = "23",
    ActorOne = 1,
    ActorTwo = 2,
    Register1 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}],
                    {[{ActorOne, 1}], []}
                }},
    Register2 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}, {{ActorTwo, 1}, Value2}],
                    {[{ActorOne, 1}, {ActorTwo, 1}], []}
                }},
    Register3 = {?TYPE, {
                    [{{ActorTwo, 1}, Value2}],
                    {[{ActorTwo, 1}], []}
                }},
    ?assert(equal(Register1, Register1)),
    ?assert(equal(Register2, Register2)),
    ?assert(equal(Register3, Register3)),
    ?assertNot(equal(Register1, Register2)),
    ?assertNot(equal(Register1, Register3)),
    ?assertNot(equal(Register2, Register3)).

is_bottom_test() ->
    Register0 = new(),
    Register1 = {?TYPE, {
                    [{{1, 1}, "17"}],
                    {[{1, 1}], []}
                }},
    ?assert(is_bottom(Register0)),
    ?assertNot(is_bottom(Register1)).

is_inflation_test() ->
    Value1 = "17",
    Value2 = "23",
    ActorOne = 1,
    ActorTwo = 2,
    Register1 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}],
                    {[{ActorOne, 1}], []}
                }},
    Register2 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}, {{ActorTwo, 1}, Value2}],
                    {[{ActorOne, 1}, {ActorTwo, 1}], []}
                }},
    Register3 = {?TYPE, {
                    [{{ActorTwo, 1}, Value2}],
                    {[{ActorTwo, 1}], []}
                }},
    ?assert(is_inflation(Register1, Register1)),
    ?assert(is_inflation(Register1, Register2)),
    ?assertNot(is_inflation(Register2, Register1)),
    ?assertNot(is_inflation(Register1, Register3)),
    ?assertNot(is_inflation(Register2, Register3)),
    ?assert(is_inflation(Register3, Register2)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Register1, Register1)),
    ?assert(state_type:is_inflation(Register1, Register2)),
    ?assertNot(state_type:is_inflation(Register2, Register1)),
    ?assertNot(state_type:is_inflation(Register1, Register3)),
    ?assertNot(state_type:is_inflation(Register2, Register3)),
    ?assert(state_type:is_inflation(Register3, Register2)).

is_strict_inflation_test() ->
    Value1 = "17",
    Value2 = "23",
    ActorOne = 1,
    ActorTwo = 2,
    Register1 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}],
                    {[{ActorOne, 1}], []}
                }},
    Register2 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}, {{ActorTwo, 1}, Value2}],
                    {[{ActorOne, 1}, {ActorTwo, 1}], []}
                }},
    Register3 = {?TYPE, {
                    [{{ActorTwo, 1}, Value2}],
                    {[{ActorTwo, 1}], []}
                }},
    ?assertNot(is_strict_inflation(Register1, Register1)),
    ?assert(is_strict_inflation(Register1, Register2)),
    ?assertNot(is_strict_inflation(Register2, Register1)),
    ?assertNot(is_strict_inflation(Register1, Register3)),
    ?assertNot(is_strict_inflation(Register2, Register3)),
    ?assert(is_strict_inflation(Register3, Register2)).

join_decomposition_test() ->
    %% @todo
    ok.

encode_decode_test() ->
    Register = {?TYPE, {
                    [{{1, 1}, "17"}],
                    {[{1, 1}], []}
                }},
    Binary = encode(erlang, Register),
    ERegister = decode(erlang, Binary),
    ?assertEqual(Register, ERegister).

-endif.
