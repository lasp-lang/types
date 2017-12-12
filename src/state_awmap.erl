
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

%% @doc AWMap CRDT.
%%      Modeled as a dictionary where keys can be anything and the
%%      values are causal-CRDTs.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(state_awmap).
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

-export_type([state_awmap/0, state_awmap_op/0]).

-opaque state_awmap() :: {?TYPE, payload()}.
-type payload() :: {state_type:state_type(),
                    state_causal_type:causal_crdt()}.
-type key() :: term().
-type key_op() :: term().
-type state_awmap_op() :: {apply, key(), key_op()} |
                          {rmv, key()}.

%% @doc Create a new, empty `state_awmap()'.
%%      By default the values are a AWSet Causal CRDT.
-spec new() -> state_awmap().
new() ->
    new([?AWSET_TYPE]).

%% @doc Create a new, empty `state_awmap()'
-spec new([term()]) -> state_awmap().
new([CType]) ->
    {?TYPE, {CType, state_causal_type:new(dot_map)}}.

%% @doc Mutate a `state_awmap()'.
-spec mutate(state_awmap_op(), type:id(), state_awmap()) ->
    {ok, state_awmap()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_awmap()'.
%%      The first argument can be:
%%          - `{apply, Key, Op}'
%%          - `{rmv, Key}'
%%      `apply' also receives an operation that will be applied to the
%%      key.
%%      This operation has to be a valid operation in the CausalCRDT
%%      choosed to be in the values (by defaul an AWSet).
%%      The second argument is the replica id.
%%      The third argument is the `state_awmap()' to be inflated.
-spec delta_mutate(state_awmap_op(), type:id(), state_awmap()) ->
    {ok, state_awmap()}.
delta_mutate({apply, Key, Op}, Actor,
             {?TYPE, {CType, {DotMap, CC}}}) ->
    {Type, Args} = state_type:extract_args(CType),
    Default = state_causal_type:ds_bottom(CType),
    SubDS = dot_map:fetch(Key, DotMap, Default),

    CRDT = ccrdt(Type, Args, SubDS, CC),
    {ok, {Type, SubDelta}} = Type:delta_mutate(Op, Actor, CRDT),

    {DeltaSubDS, DeltaCC} = dcrdt(Type, SubDelta),

    DeltaDS = case Default == DeltaSubDS of
        true ->
                %% if the resulting sub ds is empty
                dot_map:new();
        false ->
                dot_map:store(Key, DeltaSubDS, dot_map:new())
    end,

    Delta = {CType, {DeltaDS, DeltaCC}},
    {ok, {?TYPE, Delta}};

delta_mutate({rmv, Key}, _Actor, {?TYPE, {CType, {DotMap, _CC}}}) ->
    Default = state_causal_type:ds_bottom(CType),
    SubDS = dot_map:fetch(Key, DotMap, Default),
    DotSet = state_causal_type:dots(CType, SubDS),
    DeltaCC = causal_context:from_dot_set(DotSet),
    DeltaDS = dot_map:new(),

    Delta = {CType, {DeltaDS, DeltaCC}},
    {ok, {?TYPE, Delta}}.

%% @doc Returns the value of the `state_awmap()'.
%%      This value is a dictionary where each key maps to the
%%      result of `query/1' over the current value.
-spec query(state_awmap()) -> term().
query({?TYPE, {CType, {DotMap, CC}}}) ->
    {Type, Args} = state_type:extract_args(CType),
    lists:foldl(
        fun(Key, Result) ->
            Value = dot_map:fetch(Key, DotMap, undefined),
            CRDT = ccrdt(Type, Args, Value, CC),
            Query = Type:query(CRDT),
            orddict:store(Key, Query, Result)
        end,
        orddict:new(),
        dot_map:fetch_keys(DotMap)
    ).

%% @doc Merge two `state_awmap()'.
%%      Merging is handled by the `merge' function in
%%      `state_causal_type' common library.
-spec merge(state_awmap(), state_awmap()) -> state_awmap().
merge({?TYPE, {CType, AWMap1}}, {?TYPE, {CType, AWMap2}}) ->
    Map = state_causal_type:merge({dot_map, CType},
                                  AWMap1, AWMap2),
    {?TYPE, {CType, Map}}.

%% @doc Equality for `state_awmap()'.
%%      Since everything is ordered, == should work.
-spec equal(state_awmap(), state_awmap()) -> boolean().
equal({?TYPE, AWMap1}, {?TYPE, AWMap2}) ->
    AWMap1 == AWMap2.

%% @doc Check if a `state_awmap()' is bottom
-spec is_bottom(state_awmap()) -> boolean().
is_bottom({?TYPE, _}=CRDT) ->
    CRDT == new().

%% @doc Given two `state_awmap()', check if the second is an inflation
%%      of the first.
%% @todo
-spec is_inflation(state_awmap(), state_awmap()) -> boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_awmap(), state_awmap()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_awmap(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=A, B) ->
    state_type:irreducible_is_strict_inflation(A, B).

-spec digest(state_awmap()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_awmap()'.
%% @todo
-spec join_decomposition(state_awmap()) -> [state_awmap()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

%% @doc Delta calculation for `state_awmap()'.
-spec delta(state_awmap(), state_type:digest()) -> state_awmap().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_awmap()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_awmap().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.

%% @private construct CRDT.
ccrdt(Type, Args, DS, CC) ->
    case Type of
        ?TYPE ->
            [SubType] = Args,
            {?TYPE, {SubType, {DS, CC}}};
        _ ->
            {Type, {DS, CC}}
    end.

%% @private deconstruct CRDT.
dcrdt(Type, Delta) ->
    case Type of
        ?TYPE ->
            {_, {DS, CC}} = Delta,
            {DS, CC};
        _ ->
            Delta
    end.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {?AWSET_TYPE, {[], causal_context:new()}}},
                 new()).

query_test() ->
    Actor = 1,
    Map0 = new([?AWSET_TYPE]),
    Map1 = {?TYPE, {?AWSET_TYPE,
                    {[{"a", [{17, [{Actor, 2}]}]},
                      {"b", [{10, [{Actor, 1}]},
                             {13, [{Actor, 3}]}]}],
                     {[{Actor, 3}], []}}}},
    ?assertEqual([], query(Map0)),
    ?assertEqual([{"a", sets:from_list([17])},
                  {"b", sets:from_list([10, 13])}], query(Map1)).

delta_apply_test() ->
    Actor = 1,
    Map0 = new([?AWSET_TYPE]),
    {ok, {?TYPE, Delta1}} = delta_mutate({apply, "b", {add, 3}},
                                         Actor, Map0),
    Map1 = merge({?TYPE, Delta1}, Map0),
    {ok, {?TYPE, Delta2}} = delta_mutate({apply, "a", {add, 17}},
                                         Actor, Map1),
    Map2 = merge({?TYPE, Delta2}, Map1),
    {ok, {?TYPE, Delta3}} = delta_mutate({apply, "b", {add, 13}},
                                         Actor, Map2),
    Map3 = merge({?TYPE, Delta3}, Map2),
    {ok, {?TYPE, Delta4}} = delta_mutate({apply, "b", {rmv, 3}},
                                         Actor, Map3),
    Map4 = merge({?TYPE, Delta4}, Map3),

    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"b", [{3, [{Actor, 1}]}]}],
                           {[{Actor, 1}], []}}}},
                 {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"b", [{3, [{Actor, 1}]}]}],
                           {[{Actor, 1}], []}}}},
                 Map1),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"a", [{17, [{Actor, 2}]}]}],
                           {[], [{Actor, 2}]}}}},
                 {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"a", [{17, [{Actor, 2}]}]},
                            {"b", [{3, [{Actor, 1}]}]}],
                           {[{Actor, 2}], []}}}},
                 Map2),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"b", [{13, [{Actor, 3}]}]}],
                           {[], [{Actor, 3}]}}}},
                 {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"a", [{17, [{Actor, 2}]}]},
                            {"b", [{3, [{Actor, 1}]},
                                   {13, [{Actor, 3}]}]}],
                           {[{Actor, 3}], []}}}},
                 Map3),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[],
                           {[{Actor, 1}], []}}}},
                 {?TYPE, Delta4}),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"a", [{17, [{Actor, 2}]}]},
                            {"b", [{13, [{Actor, 3}]}]}],
                           {[{Actor, 3}], []}}}},
                 Map4).

apply_test() ->
    Actor = 1,
    Map0 = new([?AWSET_TYPE]),
    {ok, Map1} = mutate({apply, "b", {add, 3}}, Actor, Map0),
    {ok, Map2} = mutate({apply, "a", {add, 17}}, Actor, Map1),
    {ok, Map3} = mutate({apply, "b", {add, 13}}, Actor, Map2),
    {ok, Map4} = mutate({apply, "b", {rmv, 3}}, Actor, Map3),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"b", [{3, [{Actor, 1}]}]}],
                           {[{Actor, 1}], []}}}},
                 Map1),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"a", [{17, [{Actor, 2}]}]},
                            {"b", [{3, [{Actor, 1}]}]}],
                           {[{Actor, 2}], []}}}},
                 Map2),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"a", [{17, [{Actor, 2}]}]},
                            {"b", [{3, [{Actor, 1}]},
                                   {13, [{Actor, 3}]}]}],
                           {[{Actor, 3}], []}}}},
                 Map3),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"a", [{17, [{Actor, 2}]}]},
                            {"b", [{13, [{Actor, 3}]}]}],
                           {[{Actor, 3}], []}}}},
                 Map4).

rmv_test() ->
    Actor = 1,
    Map0 = {?TYPE, {?AWSET_TYPE,
                    {[{"a", [{17, [{Actor, 2}]}]},
                      {"b", [{3, [{Actor, 1}]},
                             {13, [{Actor, 3}]}]}],
                     {[{Actor, 3}], []}}}},
    {ok, Map1} = mutate({rmv, "a"}, Actor, Map0),
    {ok, Map2} = mutate({apply, "a", {add, 17}}, Actor, Map1),
    {ok, Map3} = mutate({rmv, "b"}, Actor, Map2),
    {ok, Map4} = mutate({rmv, "a"}, Actor, Map3),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"b", [{3, [{Actor, 1}]},
                                   {13, [{Actor, 3}]}]}],
                           {[{Actor, 3}], []}}}},
                 Map1),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"a", [{17, [{Actor, 4}]}]},
                            {"b", [{3, [{Actor, 1}]},
                                   {13, [{Actor, 3}]}]}],
                           {[{Actor, 4}], []}}}},
                 Map2),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[{"a", [{17, [{Actor, 4}]}]}],
                           {[{Actor, 4}], []}}}},
                 Map3),
    ?assertEqual({?TYPE, {?AWSET_TYPE,
                          {[],
                           {[{Actor, 4}], []}}}},
                 Map4).


equal_test() ->
    Actor = "one",
    Map1 = {?TYPE, {?AWSET_TYPE,
                    {[{"b", [{3, [{Actor, 1}]}]}],
                     {[{Actor, 1}], []}}}},
    Map2 = {?TYPE, {?AWSET_TYPE,
                    {[{"a", [{17, [{Actor, 2}]}]}],
                     {[], [{Actor, 2}]}}}},
    Map3 = {?TYPE, {?AWSET_TYPE,
                    {[{"a", [{17, [{Actor, 2}]}]},
                      {"b", [{3, [{Actor, 1}]}]}],
                     {[{Actor, 2}], []}}}},
    ?assert(equal(Map1, Map1)),
    ?assertNot(equal(Map1, Map2)),
    ?assertNot(equal(Map1, Map3)).

is_bottom_test() ->
    Actor = "one",
    Map0 = new(),
    Map1 = {?TYPE, {?AWSET_TYPE,
                    {[{"a", [{17, [{Actor, 2}]}]}],
                     {[], [{Actor, 2}]}}}},
    ?assert(is_bottom(Map0)),
    ?assertNot(is_bottom(Map1)).

is_inflation_test() ->
    Actor = "1",
    Map1 = {?TYPE, {?AWSET_TYPE,
                    {[{"b", [{3, [{Actor, 1}]}]}],
                     {[{Actor, 1}], []}}}},
    Map2 = {?TYPE, {?AWSET_TYPE,
                    {[{"a", [{17, [{Actor, 2}]}]}],
                     {[], [{Actor, 2}]}}}},
    Map3 = {?TYPE, {?AWSET_TYPE,
                    {[{"a", [{17, [{Actor, 2}]}]},
                      {"b", [{3, [{Actor, 1}]}]}],
                     {[{Actor, 2}], []}}}},
    ?assert(is_inflation(Map1, Map1)),
    ?assertNot(is_inflation(Map1, Map2)),
    ?assertNot(is_inflation(Map2, Map1)),
    ?assert(is_inflation(Map1, Map3)),
    ?assert(is_inflation(Map2, Map3)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Map1, Map1)),
    ?assertNot(state_type:is_inflation(Map1, Map2)),
    ?assertNot(state_type:is_inflation(Map2, Map1)),
    ?assert(state_type:is_inflation(Map1, Map3)),
    ?assert(state_type:is_inflation(Map2, Map3)).

is_strict_inflation_test() ->
    Actor = "1",
    Map1 = {?TYPE, {?AWSET_TYPE,
                    {[{"b", [{3, [{Actor, 1}]}]}],
                     {[{Actor, 1}], []}}}},
    Map2 = {?TYPE, {?AWSET_TYPE,
                    {[{"a", [{17, [{Actor, 2}]}]}],
                     {[], [{Actor, 2}]}}}},
    Map3 = {?TYPE, {?AWSET_TYPE,
                    {[{"a", [{17, [{Actor, 2}]}]},
                      {"b", [{3, [{Actor, 1}]}]}],
                     {[{Actor, 2}], []}}}},
    ?assertNot(is_strict_inflation(Map1, Map1)),
    ?assertNot(is_strict_inflation(Map1, Map2)),
    ?assertNot(is_strict_inflation(Map2, Map1)),
    ?assert(is_strict_inflation(Map1, Map3)),
    ?assert(is_strict_inflation(Map2, Map3)).

join_decomposition_test() ->
    %% @todo
    ok.

encode_decode_test() ->
    Actor = "hey",
    Map = {?TYPE, {?AWSET_TYPE,
                   {[{"b", [{3, [{Actor, 1}]}]}],
                    {[{Actor, 1}], []}}}},
    Binary = encode(erlang, Map),
    EMap = decode(erlang, Binary),
    ?assertEqual(Map, EMap).

-endif.
