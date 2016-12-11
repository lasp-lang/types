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

%% @doc Bounded Counter CRDT.
%%      Modeled as a pair where the first component is a
%%      PNCounter and the second component is a GMap.
%%
%% @reference Valter Balegas et al.
%%      Extending Eventually Consistent Cloud Databases for
%%      Enforcing Numeric Invariants (2015)
%%      [http://arxiv.org/abs/1503.09052]
%%
%% @reference Carlos Baquero
%%      delta-enabled-crdts C++ library
%%      [https://github.com/CBaquero/delta-enabled-crdts]

-module(state_bcounter).
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
-export([join_decomposition/1, delta/3]).
-export([encode/2, decode/2]).

-export_type([state_bcounter/0, delta_state_bcounter/0, state_bcounter_op/0]).

-opaque state_bcounter() :: {?TYPE, payload()}.
-opaque delta_state_bcounter() :: {?TYPE, {delta, payload()}}.
-type delta_or_state() :: state_bcounter() | delta_state_bcounter().
-type payload() :: {?PNCOUNTER_TYPE:state_pncounter(), ?GMAP_TYPE:state_gmap()}.
-type state_bcounter_op() :: {move, pos_integer(), term()} |
                             increment |
                             decrement.

%% @doc Create a new, empty `state_bcounter()'
-spec new() -> state_bcounter().
new() ->
    {?TYPE, {?PNCOUNTER_TYPE:new(), ?GMAP_TYPE:new([?MAX_INT_TYPE])}}.

%% @doc Create a new, empty `state_bcounter()'
-spec new([term()]) -> state_bcounter().
new([]) ->
    new().

-spec new_delta() -> delta_state_bcounter().
new_delta() ->
    state_type:new_delta(?TYPE).

-spec new_delta([term()]) -> delta_state_bcounter().
new_delta([]) ->
    new_delta().

-spec is_delta(delta_or_state()) -> boolean().
is_delta({?TYPE, _}=CRDT) ->
    state_type:is_delta(CRDT).

%% @doc Mutate a `state_bcounter()'.
-spec mutate(state_bcounter_op(), type:id(), state_bcounter()) ->
    {ok, state_bcounter()} | {error, {precondition, non_enough_permissions}}.
mutate(Op, Actor, {?TYPE, _BCounter}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_bcounter()'.
%%      The first argument can be:
%%          - `{move, term()}', that moves permissions to
%%          decrement to another replica (if it has enough permissions)
%%          - `increment' which can always happen
%%          - `decrement' which can happen when the replica has enough
%%          local increments, or has permissions received from
%%          other replicas
-spec delta_mutate(state_bcounter_op(), type:id(), state_bcounter()) ->
    {ok, delta_state_bcounter()} | {error, {precondition, non_enough_permissions}}.
delta_mutate({move, Count, To}, Actor, {?TYPE, {PNCounter, GMap}}=BCounter) ->
    {?GMAP_TYPE, {?MAX_INT_TYPE, Map0}} = GMap,
    case Count =< permissions(BCounter, Actor) of
        true ->
            Current = case orddict:find({Actor, To}, Map0) of
                {ok, Value} ->
                    Value;
                error ->
                    0
            end,
            Map1 = orddict:store({Actor, To}, Current + Count, orddict:new()),
            Delta = {state_type:new(PNCounter), {?GMAP_TYPE, {?MAX_INT_TYPE, Map1}}},
            {ok, {?TYPE, {delta, Delta}}};
        false ->
            {error, {precondition, non_enough_permissions}}
    end;

delta_mutate(increment, Actor, {?TYPE, {PNCounter, GMap}}) ->
    {ok, DM} = ?PNCOUNTER_TYPE:delta_mutate(increment, Actor, PNCounter),
    IncDelta = ?PNCOUNTER_TYPE:extract_delta(DM),
    Delta = {{?PNCOUNTER_TYPE, IncDelta}, state_type:new(GMap)},
    {ok, {?TYPE, {delta, Delta}}};

delta_mutate(decrement, Actor, {?TYPE, {PNCounter, GMap}}=BCounter) ->
    case 0 < permissions(BCounter, Actor) of
        true ->
            {ok, DM} = ?PNCOUNTER_TYPE:delta_mutate(decrement, Actor, PNCounter),
            DecDelta = ?PNCOUNTER_TYPE:extract_delta(DM),
            Delta = {{?PNCOUNTER_TYPE, DecDelta}, state_type:new(GMap)},
            {ok, {?TYPE, {delta, Delta}}};
        false ->
            {error, {precondition, non_enough_permissions}}
    end.

%% @doc Returns the number of permissions a given replica has.
%%      This is calculated as:
%%          - the number of increments minus the number of decrements
%%          - plus permissions received
%%          - minus permissions given
permissions({?TYPE, {{?PNCOUNTER_TYPE, PNCounter},
                     {?GMAP_TYPE, {?MAX_INT_TYPE, GMap}}}}, Actor) ->
    Local = case orddict:find(Actor, PNCounter) of
        {ok, {Inc, Dec}} ->
            Inc - Dec;
        error ->
            0
    end,
    {Incoming, Outgoing} = orddict:fold(
        fun({From, To}, Value, {In0, Out0}) ->
            In1 = case To == Actor of
                true ->
                    In0 + Value;
                false ->
                    In0
            end,
            Out1 = case From == Actor of
                true ->
                    Out0 + Value;
                false ->
                    Out0
            end,
            {In1, Out1}
        end,
        {0, 0},
        GMap
    ),
    Local + Incoming - Outgoing.

%% @doc Returns the value of the `state_bcounter()'.
%%      The value of the `state_bcounter()' is the
%%      value of the first component, the `state_pncounter()'.
-spec query(state_bcounter()) -> non_neg_integer().
query({?TYPE, {PNCounter, _GMap}}) ->
    ?PNCOUNTER_TYPE:query(PNCounter).

%% @doc Merge two `state_bcounter()'.
%%      The result is the merge of both `state_pncounter()'
%%      in the first component, and the merge of both
%%      `state_gmap()' in the second component.
-spec merge(delta_or_state(), delta_or_state()) -> delta_or_state().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun({?TYPE, {PNCounter1, GMap1}}, {?TYPE, {PNCounter2, GMap2}}) ->
        PNCounter = ?PNCOUNTER_TYPE:merge(PNCounter1, PNCounter2),
        GMap = ?GMAP_TYPE:merge(GMap1, GMap2),
        {?TYPE, {PNCounter, GMap}}
    end,
    state_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Equality for `state_bcounter()'.
%%      Two `state_bcounter()' are equal if each
%%      component is `equal/2'.
-spec equal(state_bcounter(), state_bcounter()) -> boolean().
equal({?TYPE, {PNCounter1, GMap1}}, {?TYPE, {PNCounter2, GMap2}}) ->
    ?PNCOUNTER_TYPE:equal(PNCounter1, PNCounter2) andalso
    ?GMAP_TYPE:equal(GMap1, GMap2).

%% @doc Some BCounter state is bottom is both components
%%      of the pair (the PNCounter and the GMap)
%%      are bottom.
-spec is_bottom(delta_or_state()) -> boolean().
is_bottom({?TYPE, {delta, BCounter}}) ->
    is_bottom({?TYPE, BCounter});
is_bottom({?TYPE, {PNCounter, GMap}}) ->
    ?PNCOUNTER_TYPE:is_bottom(PNCounter) andalso
    ?GMAP_TYPE:is_bottom(GMap).

%% @doc Given two `state_bcounter()', check if the second is an
%%      inflation of the first.
%%      We have and inflation if we have an inflation component wise.
-spec is_inflation(delta_or_state(), state_bcounter()) -> boolean().
is_inflation({?TYPE, {delta, BCounter1}}, {?TYPE, BCounter2}) ->
    is_inflation({?TYPE, BCounter1}, {?TYPE, BCounter2});
is_inflation({?TYPE, {PNCounter1, GMap1}}, {?TYPE, {PNCounter2, GMap2}}) ->
    ?PNCOUNTER_TYPE:is_inflation(PNCounter1, PNCounter2) andalso
    ?GMAP_TYPE:is_inflation(GMap1, GMap2).

%% @doc Check for strict inflation.
%%      In pairs we have strict inflations if we have component wise
%%      inflations and at least one strict inflation in the composition.
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, Alcino Cunha and Carla Ferreira
%%      Composition of State-based CRDTs (2015)
%%      [http://haslab.uminho.pt/cbm/files/crdtcompositionreport.pdf]
%%
-spec is_strict_inflation(delta_or_state(), state_bcounter()) -> boolean().
is_strict_inflation({?TYPE, {delta, BCounter1}}, {?TYPE, BCounter2}) ->
    is_strict_inflation({?TYPE, BCounter1}, {?TYPE, BCounter2});
is_strict_inflation({?TYPE, {PNCounter1, GMap1}}, {?TYPE, {PNCounter2, GMap2}}) ->
    (?PNCOUNTER_TYPE:is_strict_inflation(PNCounter1, PNCounter2)
        andalso
    ?GMAP_TYPE:is_inflation(GMap1, GMap2))
    orelse
    (?PNCOUNTER_TYPE:is_inflation(PNCounter1, PNCounter2)
        andalso
    ?GMAP_TYPE:is_strict_inflation(GMap1, GMap2)).

%% @doc Join decomposition for `state_bcounter()'.
%% @todo
-spec join_decomposition(state_bcounter()) -> [state_bcounter()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

%% @doc Delta calculation for `state_bcounter()'.
-spec delta(state_type:delta_method(), state_bcounter(), state_bcounter()) ->
    state_bcounter().
delta(Method, {?TYPE, _}=A, {?TYPE, _}=B) ->
    state_type:delta(Method, A, B).

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
    ?assertEqual({?TYPE, {?PNCOUNTER_TYPE:new(), ?GMAP_TYPE:new([?MAX_INT_TYPE])}}, new()),
    ?assertEqual({?TYPE, {delta, {?PNCOUNTER_TYPE:new(), ?GMAP_TYPE:new([?MAX_INT_TYPE])}}}, new_delta()).

query_test() ->
    BCounter0 = new(),
    BCounter1 = {?TYPE, {{?PNCOUNTER_TYPE, [{1, {2, 0}}, {2, {5, 0}}, {3, {10, 0}}]}, {?GMAP_TYPE, {?MAX_INT_TYPE, []}}}},
    ?assertEqual(0, query(BCounter0)),
    ?assertEqual(17, query(BCounter1)).

delta_increment_test() ->
    BCounter0 = new(),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate(increment, 1, BCounter0),
    BCounter1 = merge({?TYPE, Delta1}, BCounter0),
    {ok, {?TYPE, {delta, Delta2}}} = delta_mutate(increment, 1, BCounter1),
    BCounter2 = merge({?TYPE, Delta2}, BCounter1),
    {ok, {?TYPE, {delta, Delta3}}} = delta_mutate(increment, 2, BCounter2),
    BCounter3 = merge({?TYPE, Delta3}, BCounter2),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{1, {1, 0}}]}, {?GMAP_TYPE, {?MAX_INT_TYPE, []}}}}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{1, {1, 0}}]}, {?GMAP_TYPE, {?MAX_INT_TYPE, []}}}}, BCounter1),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{1, {2, 0}}]}, {?GMAP_TYPE, {?MAX_INT_TYPE, []}}}}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{1, {2, 0}}]}, {?GMAP_TYPE, {?MAX_INT_TYPE, []}}}}, BCounter2),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{2, {1, 0}}]}, {?GMAP_TYPE, {?MAX_INT_TYPE, []}}}}, {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{1, {2, 0}}, {2, {1, 0}}]}, {?GMAP_TYPE, {?MAX_INT_TYPE, []}}}}, BCounter3).

add_test() ->
    BCounter0 = new(),
    {ok, BCounter1} = mutate(increment, 1, BCounter0),
    {ok, BCounter2} = mutate(increment, 1, BCounter1),
    {ok, BCounter3} = mutate(increment, 2, BCounter2),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{1, {1, 0}}]}, {?GMAP_TYPE, {?MAX_INT_TYPE, []}}}}, BCounter1),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{1, {2, 0}}]}, {?GMAP_TYPE, {?MAX_INT_TYPE, []}}}}, BCounter2),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{1, {2, 0}}, {2, {1, 0}}]}, {?GMAP_TYPE, {?MAX_INT_TYPE, []}}}}, BCounter3).

delta_decrement_test() ->
    Actor = 1,
    BCounter0 = new(),
    {error, _} = delta_mutate(decrement, Actor, BCounter0),
    {ok, BCounter1} = mutate(increment, Actor, BCounter0),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate(decrement, Actor, BCounter1),
    BCounter2 = merge({?TYPE, Delta1}, BCounter1),
    {error, _} = delta_mutate(decrement, Actor, BCounter2),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{Actor, {1, 1}}]}, {?GMAP_TYPE, {?MAX_INT_TYPE, []}}}}, BCounter2).

delta_move_test() ->
    From = 1,
    To = 2,
    BCounter0 = new(),
    {error, _} = delta_mutate({move, 1, To}, From, BCounter0),
    {ok, BCounter1} = mutate(increment, From, BCounter0),
    {ok, BCounter2} = mutate(increment, From, BCounter1),
    {error, _} = delta_mutate({move, 3, To}, From, BCounter0),
    {error, _} = delta_mutate(decrement, To, BCounter2),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate({move, 2, To}, From, BCounter2),
    BCounter3 = merge({?TYPE, Delta1}, BCounter2),
    {error, _} = delta_mutate({move, 1, To}, From, BCounter3),
    {ok, {?TYPE, {delta, Delta2}}} = delta_mutate(decrement, To, BCounter3),
    BCounter4 = merge({?TYPE, Delta2}, BCounter3),
    {error, _} = delta_mutate(decrement, From, BCounter4),
    {error, _} = delta_mutate({move, 2, From}, To, BCounter4),
    {ok, {?TYPE, {delta, Delta3}}} = delta_mutate({move, 1, From}, To, BCounter4),
    BCounter5 = merge({?TYPE, Delta3}, BCounter4),
    {ok, {?TYPE, {delta, Delta4}}} = delta_mutate(decrement, From, BCounter5),
    BCounter6 = merge({?TYPE, Delta4}, BCounter5),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{From, {2, 0}}]},
                          {?GMAP_TYPE, {?MAX_INT_TYPE, [{{From, To}, 2}]}}}}, BCounter3),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{From, {2, 1}}, {To, {0, 1}}]},
                          {?GMAP_TYPE, {?MAX_INT_TYPE, [{{From, To}, 2}, {{To, From}, 1}]}}}}, BCounter6).

merge_deltas_test() ->
    GMap = {?GMAP_TYPE, {?MAX_INT_TYPE, []}},
    BCounter1 = {?TYPE, {{?PNCOUNTER_TYPE, [{1, {2, 0}}, {2, {1, 0}}]}, GMap}},
    Delta1 = {?TYPE, {delta, {{?PNCOUNTER_TYPE, [{1, {4, 0}}]}, GMap}}},
    Delta2 = {?TYPE, {delta, {{?PNCOUNTER_TYPE, [{2, {1, 17}}]}, GMap}}},
    BCounter2 = merge(Delta1, BCounter1),
    BCounter3 = merge(BCounter1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{1, {4, 0}}, {2, {1, 0}}]}, GMap}}, BCounter2),
    ?assertEqual({?TYPE, {{?PNCOUNTER_TYPE, [{1, {4, 0}}, {2, {1, 0}}]}, GMap}}, BCounter3),
    ?assertEqual({?TYPE, {delta, {{?PNCOUNTER_TYPE, [{1, {4, 0}}, {2, {1, 17}}]}, GMap}}}, DeltaGroup).

join_decomposition_test() ->
    %% @todo
    ok.

encode_decode_test() ->
    GMap = {?GMAP_TYPE, {?MAX_INT_TYPE, []}},
    Counter = {?TYPE, {{?PNCOUNTER_TYPE, [{1, {4, 0}}, {2, {1, 0}}]}, GMap}},
    Binary = encode(erlang, Counter),
    ECounter = decode(erlang, Binary),
    ?assertEqual(Counter, ECounter).

-endif.
