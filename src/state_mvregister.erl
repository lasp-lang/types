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

-export([new/0, new/1, new_delta/0, new_delta/1, is_delta/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_bottom/1, is_inflation/2, is_strict_inflation/2]).
-export([join_decomposition/1]).
-export([encode/2, decode/2]).

-export_type([state_mvregister/0, delta_state_mvregister/0, state_mvregister_op/0]).

-opaque state_mvregister() :: {?TYPE, payload()}.
-opaque delta_state_mvregister() :: {?TYPE, {delta, payload()}}.
-type delta_or_state() :: state_mvregister() | delta_state_mvregister().
-type payload() :: {orddict:orddict(dot_store:dot(), term()), causal_context:causal_context()}.
-type timestamp() :: non_neg_integer().
-type value() :: term().
-type state_mvregister_op() :: {set, timestamp(), value()}.

%% @doc Create a new, empty `state_mvregister()'
-spec new() -> state_mvregister().
new() ->
    {?TYPE, {orddict:new(), causal_context:new()}}.

%% @doc Create a new, empty `state_mvregister()'
-spec new([term()]) -> state_mvregister().
new([]) ->
    new().

-spec new_delta() -> delta_state_mvregister().
new_delta() ->
    state_type:new_delta(?TYPE).

-spec new_delta([term()]) -> delta_state_mvregister().
new_delta([]) ->
    new_delta().

-spec is_delta(delta_or_state()) -> boolean().
is_delta({?TYPE, _}=CRDT) ->
    state_type:is_delta(CRDT).

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
-spec delta_mutate(state_mvregister_op(), type:id(), state_mvregister()) ->
    {ok, delta_state_mvregister()}.

%% @doc Sets `state_mvregister()' value.
delta_mutate({set, _Timestamp, Value}, Actor, {?TYPE, {DotStore, CausalContext}}) ->
    NextDot = causal_context:next_dot(Actor, CausalContext),

    DeltaDotStore = orddict:store(NextDot, Value, orddict:new()),
    DeltaCausalContext0 = to_causal_context(DotStore),
    DeltaCausalContext1 = causal_context:add_dot(NextDot, DeltaCausalContext0),

    Delta = {DeltaDotStore, DeltaCausalContext1},
    {ok, {?TYPE, {delta, Delta}}}.

%% @doc Returns the value of the `state_mvregister()'.
-spec query(state_mvregister()) -> sets:set(value()).
query({?TYPE, {DotStore, _CausalContext}}) ->
    sets:from_list([Value || {_Dot, Value} <- orddict:to_list(DotStore)]).

%% @doc Merge two `state_mvregister()'.
-spec merge(delta_or_state(), delta_or_state()) -> delta_or_state().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun({?TYPE, {DotStoreA, CausalContextA}},
                   {?TYPE, {DotStoreB, CausalContextB}}) ->
        %% This is basically a copy of state_causal_type:merge
        %% for DotFun's that does not call merge on the values.
        %% Since associated to some dot, will always be the same
        %% value, no need to call merge.
        %% This also means we support any value to be stored in
        %% the register (as expected), otherwise, only CRDTs
        %% could be stored.
        DotSetA = causal_context:to_dot_set(
            to_causal_context(DotStoreA)
        ),
        DotSetB = causal_context:to_dot_set(
            to_causal_context(DotStoreB)
        ),
        CCDotSetA = causal_context:to_dot_set(CausalContextA),
        CCDotSetB = causal_context:to_dot_set(CausalContextB),

        CommonDotSet = dot_set:intersection(DotSetA, DotSetB),

        DotStore0 = lists:foldl(
            fun(Dot, DotStore) ->
                %% using the same variable in the next two fetches
                Value = orddict:fetch(Dot, DotStoreA),
                Value = orddict:fetch(Dot, DotStoreB),
                orddict:store(Dot, Value, DotStore)
            end,
            orddict:new(),
            dot_set:to_list(CommonDotSet)
        ),

        DotStore1 = lists:foldl(
            fun(Dot, DotStore) ->
                case dot_set:is_element(Dot, CCDotSetB) of
                    true ->
                        DotStore;
                    false ->
                        Value = orddict:fetch(Dot, DotStoreA),
                        orddict:store(Dot, Value, DotStore)
                end
            end,
            DotStore0,
            orddict:fetch_keys(DotStoreA)
        ),

        DotStore2 = lists:foldl(
            fun(Dot, DotStore) ->
                case dot_set:is_element(Dot, CCDotSetA) of
                    true ->
                        DotStore;
                    false ->
                        Value = orddict:fetch(Dot, DotStoreB),
                        orddict:store(Dot, Value, DotStore)
                end
            end,
            DotStore1,
            orddict:fetch_keys(DotStoreB)
        ),

        CausalContext = causal_context:merge(CausalContextA, CausalContextB),

        {?TYPE, {DotStore2, CausalContext}}
    end,
    state_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Equality for `state_mvregister()'.
%%      Since everything is ordered, == should work.
-spec equal(state_mvregister(), state_mvregister()) -> boolean().
equal({?TYPE, Register1}, {?TYPE, Register2}) ->
    Register1 == Register2.

%% @doc Check if an `state_mvregister()' is bottom.
-spec is_bottom(delta_or_state()) -> boolean().
is_bottom({?TYPE, {delta, Register}}) ->
    is_bottom({?TYPE, Register});
is_bottom({?TYPE, _}=CRDT) ->
    CRDT == new().

%% @doc Given two `state_mvregister()', check if the second is and inflation of the first.
%% @todo
-spec is_inflation(delta_or_state(), state_mvregister()) -> boolean().
is_inflation({?TYPE, {delta, Register1}}, {?TYPE, Register2}) ->
    is_inflation({?TYPE, Register1}, {?TYPE, Register2});
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(delta_or_state(), state_mvregister()) -> boolean().
is_strict_inflation({?TYPE, {delta, Register1}}, {?TYPE, Register2}) ->
    is_strict_inflation({?TYPE, Register1}, {?TYPE, Register2});
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Join decomposition for `state_mvregister()'.
%% @todo
-spec join_decomposition(state_mvregister()) -> [state_mvregister()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

-spec encode(state_type:format(), delta_or_state()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> delta_or_state().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.

%% @private
%% Extract Causal Context from an `orddict:orddict(dot_store:dot(), term())'
to_causal_context(Dict) ->
    lists:foldl(
        fun(Dot, Acc) ->
            causal_context:add_dot(Dot, Acc)
        end,
        causal_context:new(),
        orddict:fetch_keys(Dict)
    ).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {orddict:new(), ordsets:new()}},
                 new()),
    ?assertEqual({?TYPE, {delta, {orddict:new(), ordsets:new()}}},
                 new_delta()).

query_test() ->
    Register0 = new(),
    Register1 = {?TYPE,
        {
            [{{a, 2}, {?MAX_INT_TYPE, 17}}],
            [{a, 1}, {a, 2}]
        }
    },
    ?assertEqual(sets:from_list([]), query(Register0)),
    ?assertEqual(sets:from_list([{?MAX_INT_TYPE, 17}]), query(Register1)).

delta_mutate_test() ->
    Timestamp = 0, %% won't be used
    Value1 = {?MAX_INT_TYPE, 17},
    Value2 = {?MAX_INT_TYPE, 23},
    ActorOne = 1,
    ActorTwo = 2,
    Register0 = new(),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate({set, Timestamp, Value1}, ActorOne, Register0),
    Register1 = merge({?TYPE, Delta1}, Register0),
    {ok, {?TYPE, {delta, Delta2}}} = delta_mutate({set, Timestamp, Value2}, ActorTwo, Register1),
    Register2 = merge({?TYPE, Delta2}, Register1),
    {ok, {?TYPE, {delta, Delta3}}} = delta_mutate({set, Timestamp, Value1}, ActorTwo, Register2),
    Register3 = merge({?TYPE, Delta3}, Register2),

    ?assertEqual({?TYPE,
        {
            [{{ActorOne, 1}, Value1}],
            [{ActorOne, 1}]
        }},
        {?TYPE, Delta1}
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorOne, 1}, Value1}],
            [{ActorOne, 1}]
        }},
        Register1
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorTwo, 1}, Value2}],
            [{ActorOne, 1}, {ActorTwo, 1}]
        }},
        {?TYPE, Delta2}
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorTwo, 1}, Value2}],
            [{ActorOne, 1}, {ActorTwo, 1}]
        }},
        Register2
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorTwo, 2}, Value1}],
            [{ActorTwo, 1}, {ActorTwo, 2}]
        }},
        {?TYPE, Delta3}
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorTwo, 2}, Value1}],
            [{ActorOne, 1}, {ActorTwo, 1}, {ActorTwo, 2}]
        }},
        Register3
    ).

mutate_test() ->
    Timestamp = 0, %% won't be used
    Value1 = {?MAX_INT_TYPE, 17},
    Value2 = {?MAX_INT_TYPE, 23},
    ActorOne = 1,
    ActorTwo = 2,
    Register0 = new(),
    {ok, Register1} = mutate({set, Timestamp, Value1}, ActorOne, Register0),
    {ok, Register2} = mutate({set, Timestamp, Value2}, ActorTwo, Register1),
    {ok, Register3} = mutate({set, Timestamp, Value1}, ActorTwo, Register2),

    ?assertEqual({?TYPE,
        {
            [{{ActorOne, 1}, Value1}],
            [{ActorOne, 1}]
        }},
        Register1
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorTwo, 1}, Value2}],
            [{ActorOne, 1}, {ActorTwo, 1}]
        }},
        Register2
    ),
    ?assertEqual({?TYPE,
        {
            [{{ActorTwo, 2}, Value1}],
            [{ActorOne, 1}, {ActorTwo, 1}, {ActorTwo, 2}]
        }},
        Register3
    ).

merge_idempontent_test() ->
    Value1 = {?MAX_INT_TYPE, 17},
    Value2 = {?MAX_INT_TYPE, 23},
    ActorOne = 1,
    ActorTwo = 2,
    Register1 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}],
                    [{ActorOne, 1}]
                }},
    Register2 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}, {{ActorTwo, 1}, Value2}],
                    [{ActorOne, 1}, {ActorTwo, 1}]
                }},
    Register3 = {?TYPE, {
                    [{{ActorTwo, 1}, Value2}],
                    [{ActorTwo, 1}]
                }},
    Register4 = merge(Register1, Register1),
    Register5 = merge(Register2, Register2),
    Register6 = merge(Register3, Register3),
    ?assertEqual(Register1, Register4),
    ?assertEqual(Register2, Register5),
    ?assertEqual(Register3, Register6).

merge_commutative_test() ->
    Value1 = {?MAX_INT_TYPE, 17},
    Value2 = {?MAX_INT_TYPE, 23},
    ActorOne = 1,
    ActorTwo = 2,
    Register1 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}],
                    [{ActorOne, 1}]
                }},
    Register2 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}, {{ActorTwo, 1}, Value2}],
                    [{ActorOne, 1}, {ActorTwo, 1}]
                }},
    Register3 = {?TYPE, {
                    [{{ActorTwo, 1}, Value2}],
                    [{ActorTwo, 1}]
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
    Value1 = {?MAX_INT_TYPE, 17},
    Value2 = {?MAX_INT_TYPE, 23},
    ActorOne = 1,
    ActorTwo = 2,
    Register1 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}],
                    [{ActorOne, 1}]
                }},
    Register2 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}, {{ActorTwo, 1}, Value2}],
                    [{ActorOne, 1}, {ActorTwo, 1}]
                }},
    Register3 = {?TYPE, {
                    [{{ActorTwo, 1}, Value2}],
                    [{ActorTwo, 1}]
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
                    [{{1, 1}, {?MAX_INT_TYPE, 17}}],
                    [{1, 1}]
                }},
    ?assert(is_bottom(Register0)),
    ?assertNot(is_bottom(Register1)).

is_inflation_test() ->
    Value1 = {?MAX_INT_TYPE, 17},
    Value2 = {?MAX_INT_TYPE, 23},
    ActorOne = 1,
    ActorTwo = 2,
    Register1 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}],
                    [{ActorOne, 1}]
                }},
    Register2 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}, {{ActorTwo, 1}, Value2}],
                    [{ActorOne, 1}, {ActorTwo, 1}]
                }},
    Register3 = {?TYPE, {
                    [{{ActorTwo, 1}, Value2}],
                    [{ActorTwo, 1}]
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
    Value1 = {?MAX_INT_TYPE, 17},
    Value2 = {?MAX_INT_TYPE, 23},
    ActorOne = 1,
    ActorTwo = 2,
    Register1 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}],
                    [{ActorOne, 1}]
                }},
    Register2 = {?TYPE, {
                    [{{ActorOne, 1}, Value1}, {{ActorTwo, 1}, Value2}],
                    [{ActorOne, 1}, {ActorTwo, 1}]
                }},
    Register3 = {?TYPE, {
                    [{{ActorTwo, 1}, Value2}],
                    [{ActorTwo, 1}]
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
                    [{{1, 1}, {?MAX_INT_TYPE, 17}}],
                    [{1, 1}]
                }},
    Binary = encode(erlang, Register),
    ERegister = decode(erlang, Binary),
    ?assertEqual(Register, ERegister).

-endif.
