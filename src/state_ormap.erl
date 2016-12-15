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

%% @doc ORMap CRDT.
%%      Modeled as a dictionary where keys can be anything and the
%%      values are causal-CRDTs.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(state_ormap).
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
-export([query/1, equal/2, is_bottom/1, is_inflation/2, is_strict_inflation/2, irreducible_is_strict_inflation/2]).
-export([join_decomposition/1, delta/3]).
-export([encode/2, decode/2]).

-export_type([state_ormap/0, delta_state_ormap/0, state_ormap_op/0]).

-opaque state_ormap() :: {?TYPE, payload()}.
-opaque delta_state_ormap() :: {?TYPE, {delta, payload()}}.
-type delta_or_state() :: state_ormap() | delta_state_ormap().
-type payload() :: state_causal_type:causal_crdt().
-type key() :: term().
-type key_op() :: term().
-type state_ormap_op() :: {apply, key(), key_op()} |
                          {rmv, key()}.

%% @doc Create a new, empty `state_ormap()'.
%%      By default the values are a AWSet Causal CRDT.
-spec new() -> state_ormap().
new() ->
    new([?AWSET_TYPE]).

%% @doc Create a new, empty `state_ormap()'
-spec new([term()]) -> state_ormap().
new([CType]) ->
    {?TYPE, state_causal_type:new({dot_map, CType})}.

-spec new_delta() -> delta_state_ormap().
new_delta() ->
    new_delta([?AWSET_TYPE]).

-spec new_delta([term()]) -> delta_state_ormap().
new_delta(Args) ->
    state_type:new_delta(?TYPE, Args).

-spec is_delta(delta_or_state()) -> boolean().
is_delta({?TYPE, _}=CRDT) ->
    state_type:is_delta(CRDT).

%% @doc Mutate a `state_ormap()'.
-spec mutate(state_ormap_op(), type:id(), state_ormap()) ->
    {ok, state_ormap()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_ormap()'.
%%      The first argument can be:
%%          - `{apply, Key, Op}'
%%          - `{rmv, Key}'
%%      `apply' also receives an operation that will be applied to the
%%      key.
%%      This operation has to be a valid operation in the CausalCRDT
%%      choosed to be in the values (by defaul an AWSet).
%%      The second argument is the replica id.
%%      The third argument is the `state_ormap()' to be inflated.
-spec delta_mutate(state_ormap_op(), type:id(), state_ormap()) ->
    {ok, delta_state_ormap()}.
delta_mutate({apply, Key, Op}, Actor, {?TYPE, {{{dot_map, CType}, _}=DotMap, CausalContext}}) ->
    {Type, _} = state_type:extract_args(CType),
    SubDotStore = dot_map:fetch(Key, DotMap),
    {ok, {Type, {delta, {DeltaSubDotStore, DeltaCausalContext}}}} = Type:delta_mutate(Op, Actor, {Type, {SubDotStore, CausalContext}}),
    EmptyDotMap = dot_map:new(CType),
    DeltaDotStore = dot_map:store(Key, DeltaSubDotStore, EmptyDotMap),

    Delta = {DeltaDotStore, DeltaCausalContext},
    {ok, {?TYPE, {delta, Delta}}};

delta_mutate({rmv, Key}, _Actor, {?TYPE, {{{dot_map, CType}, _}=DotMap, _CausalContext}}) ->
    SubDotStore = dot_map:fetch(Key, DotMap),
    DeltaCausalContext = causal_context:to_causal_context(SubDotStore),
    DeltaDotStore = dot_map:new(CType),

    Delta = {DeltaDotStore, DeltaCausalContext},
    {ok, {?TYPE, {delta, Delta}}}.

%% @doc Returns the value of the `state_ormap()'.
%%      This value is a dictionary where each key maps to the
%%      result of `query/1' over the current value.
-spec query(state_ormap()) -> term().
query({?TYPE, {{{dot_map, CType}, _}=DotMap, CausalContext}}) ->
    {Type, _Args} = state_type:extract_args(CType),

    lists:foldl(
        fun(Key, Result) ->
            Value = dot_map:fetch(Key, DotMap),
            orddict:store(Key, Type:query({Type, {Value, CausalContext}}), Result)
        end,
        orddict:new(),
        dot_map:fetch_keys(DotMap)
    ).

%% @doc Merge two `state_ormap()'.
%%      Merging is handled by the `merge' function in
%%      `state_causal_type' common library.
-spec merge(delta_or_state(), delta_or_state()) -> delta_or_state().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun({?TYPE, ORMap1}, {?TYPE, ORMap2}) ->
        Map = state_causal_type:merge(ORMap1, ORMap2),
        {?TYPE, Map}
    end,
    state_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Equality for `state_ormap()'.
%%      Since everything is ordered, == should work.
-spec equal(state_ormap(), state_ormap()) -> boolean().
equal({?TYPE, ORMap1}, {?TYPE, ORMap2}) ->
    ORMap1 == ORMap2.

%% @doc Check if a `state_ormap()' is bottom
-spec is_bottom(delta_or_state()) -> boolean().
is_bottom({?TYPE, {delta, Map}}) ->
    is_bottom({?TYPE, Map});
is_bottom({?TYPE, _}=CRDT) ->
    CRDT == new().

%% @doc Given two `state_ormap()', check if the second is an inflation
%%      of the first.
%% @todo
-spec is_inflation(delta_or_state(), state_ormap()) -> boolean().
is_inflation({?TYPE, {delta, Map1}}, {?TYPE, Map2}) ->
    is_inflation({?TYPE, Map1}, {?TYPE, Map2});
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(delta_or_state(), state_ormap()) -> boolean().
is_strict_inflation({?TYPE, {delta, Map1}}, {?TYPE, Map2}) ->
    is_strict_inflation({?TYPE, Map1}, {?TYPE, Map2});
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_ormap(), state_ormap()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=Irreducible, {?TYPE, _}=CRDT) ->
    state_type:irreducible_is_strict_inflation(Irreducible, CRDT).

%% @doc Join decomposition for `state_ormap()'.
%% @todo
-spec join_decomposition(delta_or_state()) -> [state_ormap()].
join_decomposition({?TYPE, {delta, Payload}}) ->
    join_decomposition({?TYPE, Payload});
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

%% @doc Delta calculation for `state_ormap()'.
-spec delta(state_type:delta_method(), delta_or_state(), delta_or_state()) ->
    state_ormap().
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
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE}, []}, []}}, new()),
    ?assertEqual({?TYPE, {delta, {{{dot_map, ?AWSET_TYPE}, []}, []}}}, new_delta()).

query_test() ->
    Actor = 1,
    Map0 = new([?AWSET_TYPE]),
    Map1 = {?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17, {dot_set, [{Actor, 2}]}}]}},
                      {"b", {{dot_map, dot_set}, [{3, {dot_set, [{Actor, 1}]}},
                                                  {13, {dot_set, [{Actor, 3}]}}]}}]},
                   [{Actor, 1}, {Actor, 2}, {Actor, 3}]}},
    ?assertEqual([], query(Map0)),
    ?assertEqual([{"a", sets:from_list([17])}, {"b", sets:from_list([3, 13])}], query(Map1)).

delta_apply_test() ->
    Actor = 1,
    Map0 = new([?AWSET_TYPE]),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate({apply, "b", {add, 3}}, Actor, Map0),
    Map1 = merge({?TYPE, Delta1}, Map0),
    {ok, {?TYPE, {delta, Delta2}}} = delta_mutate({apply, "a", {add, 17}}, Actor, Map1),
    Map2 = merge({?TYPE, Delta2}, Map1),
    {ok, {?TYPE, {delta, Delta3}}} = delta_mutate({apply, "b", {add, 13}}, Actor, Map2),
    Map3 = merge({?TYPE, Delta3}, Map2),
    {ok, {?TYPE, {delta, Delta4}}} = delta_mutate({apply, "b", {rmv, 3}}, Actor, Map3),
    Map4 = merge({?TYPE, Delta4}, Map3),
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"b", {{dot_map, dot_set}, [{3,  {dot_set, [{Actor, 1}]}}]}}]},
                   [{Actor, 1}]}},
                 {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"b", {{dot_map, dot_set}, [{3,  {dot_set, [{Actor, 1}]}}]}}]},
                   [{Actor, 1}]}},
                 Map1),
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17,  {dot_set, [{Actor, 2}]}}]}}]},
                   [{Actor, 2}]}},
                 {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17, {dot_set, [{Actor, 2}]}}]}},
                      {"b", {{dot_map, dot_set}, [{3, {dot_set, [{Actor, 1}]}}]}}]},
                   [{Actor, 1}, {Actor, 2}]}},
                 Map2),
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"b", {{dot_map, dot_set}, [{13,  {dot_set, [{Actor, 3}]}}]}}]},
                   [{Actor, 3}]}},
                 {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17, {dot_set, [{Actor, 2}]}}]}},
                      {"b", {{dot_map, dot_set}, [{3, {dot_set, [{Actor, 1}]}},
                                                  {13, {dot_set, [{Actor, 3}]}}]}}]},
                   [{Actor, 1}, {Actor, 2}, {Actor, 3}]}},
                 Map3),
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"b", {{dot_map, dot_set}, []}}]},
                   [{Actor, 1}]}},
                 {?TYPE, Delta4}),
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17, {dot_set, [{Actor, 2}]}}]}},
                      {"b", {{dot_map, dot_set}, [{13, {dot_set, [{Actor, 3}]}}]}}]},
                   [{Actor, 1}, {Actor, 2}, {Actor, 3}]}},
                 Map4).

apply_test() ->
    Actor = 1,
    Map0 = new([?AWSET_TYPE]),
    {ok, Map1} = mutate({apply, "b", {add, 3}}, Actor, Map0),
    {ok, Map2} = mutate({apply, "a", {add, 17}}, Actor, Map1),
    {ok, Map3} = mutate({apply, "b", {add, 13}}, Actor, Map2),
    {ok, Map4} = mutate({apply, "b", {rmv, 3}}, Actor, Map3),
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"b", {{dot_map, dot_set}, [{3,  {dot_set, [{Actor, 1}]}}]}}]},
                   [{Actor, 1}]}},
                 Map1),
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17, {dot_set, [{Actor, 2}]}}]}},
                      {"b", {{dot_map, dot_set}, [{3, {dot_set, [{Actor, 1}]}}]}}]},
                   [{Actor, 1}, {Actor, 2}]}},
                 Map2),
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17, {dot_set, [{Actor, 2}]}}]}},
                      {"b", {{dot_map, dot_set}, [{3, {dot_set, [{Actor, 1}]}},
                                                  {13, {dot_set, [{Actor, 3}]}}]}}]},
                   [{Actor, 1}, {Actor, 2}, {Actor, 3}]}},
                 Map3),
    ?assertEqual({?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17, {dot_set, [{Actor, 2}]}}]}},
                      {"b", {{dot_map, dot_set}, [{13, {dot_set, [{Actor, 3}]}}]}}]},
                   [{Actor, 1}, {Actor, 2}, {Actor, 3}]}},
                 Map4).

equal_test() ->
    Actor = "one",
    Map1 = {?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"b", {{dot_map, dot_set}, [{3,  {dot_set, [{Actor, 1}]}}]}}]},
                   [{Actor, 1}]}},
    Map2 = {?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17,  {dot_set, [{Actor, 2}]}}]}}]},
                   [{Actor, 2}]}},
    Map3 = {?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17, {dot_set, [{Actor, 2}]}}]}},
                      {"b", {{dot_map, dot_set}, [{3, {dot_set, [{Actor, 1}]}}]}}]},
                   [{Actor, 1}, {Actor, 2}]}},
    ?assert(equal(Map1, Map1)),
    ?assertNot(equal(Map1, Map2)),
    ?assertNot(equal(Map1, Map3)).

is_bottom_test() ->
    Actor = "one",
    Map0 = new(),
    Map1 = {?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"b", {{dot_map, dot_set}, [{3,  {dot_set, [{Actor, 1}]}}]}}]},
                   [{Actor, 1}]}},
    ?assert(is_bottom(Map0)),
    ?assertNot(is_bottom(Map1)).

is_inflation_test() ->
    Actor = "1",
    Map1 = {?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"b", {{dot_map, dot_set}, [{3,  {dot_set, [{Actor, 1}]}}]}}]},
                   [{Actor, 1}]}},
    Map2 = {?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17,  {dot_set, [{Actor, 2}]}}]}}]},
                   [{Actor, 2}]}},
    Map3 = {?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17, {dot_set, [{Actor, 2}]}}]}},
                      {"b", {{dot_map, dot_set}, [{3, {dot_set, [{Actor, 1}]}}]}}]},
                   [{Actor, 1}, {Actor, 2}]}},
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
    Map1 = {?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"b", {{dot_map, dot_set}, [{3,  {dot_set, [{Actor, 1}]}}]}}]},
                   [{Actor, 1}]}},
    Map2 = {?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17,  {dot_set, [{Actor, 2}]}}]}}]},
                   [{Actor, 2}]}},
    Map3 = {?TYPE, {{{dot_map, ?AWSET_TYPE},
                     [{"a", {{dot_map, dot_set}, [{17, {dot_set, [{Actor, 2}]}}]}},
                      {"b", {{dot_map, dot_set}, [{3, {dot_set, [{Actor, 1}]}}]}}]},
                   [{Actor, 1}, {Actor, 2}]}},
    ?assertNot(is_strict_inflation(Map1, Map1)),
    ?assertNot(is_strict_inflation(Map1, Map2)),
    ?assertNot(is_strict_inflation(Map2, Map1)),
    ?assert(is_strict_inflation(Map1, Map3)),
    ?assert(is_strict_inflation(Map2, Map3)).

join_decomposition_test() ->
    %% @todo
    ok.

encode_decode_test() ->
    Map = {?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 2}]}}]}},
    Binary = encode(erlang, Map),
    EMap = decode(erlang, Binary),
    ?assertEqual(Map, EMap).

-endif.
