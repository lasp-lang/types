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

%% @doc Multi-Value Map CRDT.
%%      MVMap = ORMap<MVRegister<V>>
%%            = DotMap<K, DotFun<V>>

-module(state_mvmap).
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

-export_type([state_mvmap/0, delta_state_mvmap/0, state_mvmap_op/0]).

-opaque state_mvmap() :: {?TYPE, payload()}.
-opaque delta_state_mvmap() :: {?TYPE, {delta, payload()}}.
-type delta_or_state() :: state_mvmap() | delta_state_mvmap().
-type payload() :: state_causal_type:causal_crdt().
-type key() :: term().
-type value() :: term().
-type state_mvmap_op() :: {set, key(), value()}.

%% @doc Create a new, empty `state_mvmap()'.
-spec new() -> state_mvmap().
new() ->
    {?TYPE, state_causal_type:new({dot_map, ?MVREGISTER_TYPE})}.

%% @doc Create a new, empty `state_mvmap()'
-spec new([term()]) -> state_mvmap().
new([]) ->
    new().

-spec new_delta() -> delta_state_mvmap().
new_delta() ->
    state_type:new_delta(?TYPE).

-spec new_delta([term()]) -> delta_state_mvmap().
new_delta([]) ->
    new_delta().

-spec is_delta(delta_or_state()) -> boolean().
is_delta({?TYPE, _}=CRDT) ->
    state_type:is_delta(CRDT).

%% @doc Mutate a `state_mvmap()'.
-spec mutate(state_mvmap_op(), type:id(), state_mvmap()) ->
    {ok, state_mvmap()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_mvmap()'.
%%      The first argument can be:
%%          - `{set, Key, Value}'
%%      The second argument is the replica id.
%%      The third argument is the `state_mvmap()' to be inflated.
-spec delta_mutate(state_mvmap_op(), type:id(), state_mvmap()) ->
    {ok, delta_state_mvmap()}.
delta_mutate({set, Key, Value}, Actor, {?TYPE, {{{dot_map, RegisterType}, _}=DotMap, CausalContext}}) ->
    SubDotStore = dot_map:fetch(Key, DotMap),
    {ok, {Type, {delta, {DeltaSubDotStore, DeltaCausalContext}}}} =
        RegisterType:delta_mutate({set, 0, Value},
                                  Actor,
                                  {RegisterType, {SubDotStore, CausalContext}}),
    EmptyDotMap = dot_map:new(Type),
    DeltaDotStore = dot_map:store(Key, DeltaSubDotStore, EmptyDotMap),

    Delta = {DeltaDotStore, DeltaCausalContext},
    {ok, {?TYPE, {delta, Delta}}}.

%% @doc Returns the value of the `state_mvmap()'.
%%      This value is a dictionary where each key maps to the
%%      result of `query/1' over the current value.
-spec query(state_mvmap()) -> term().
query({?TYPE, {{{dot_map, Type}, _}=DotMap, CausalContext}}) ->
    lists:foldl(
        fun(Key, Result) ->
            Value = dot_map:fetch(Key, DotMap),
            orddict:store(Key, Type:query({Type, {Value, CausalContext}}), Result)
        end,
        orddict:new(),
        dot_map:fetch_keys(DotMap)
    ).

%% @doc Merge two `state_mvmap()'.
%%      Merging is handled by the `merge' function in
%%      `state_causal_type' common library.
-spec merge(delta_or_state(), delta_or_state()) -> delta_or_state().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun({?TYPE, MVMap1}, {?TYPE, MVMap2}) ->
        Map = dot_map_merge(MVMap1, MVMap2),
        {?TYPE, Map}
    end,
    state_type:merge(CRDT1, CRDT2, MergeFun).

%% @private Copied from state_causal_type.
%%          This dot_map_merge does not call the merge recursively on the values.
%%          Instead, it uses the merge of the mvregister.
dot_map_merge({{{dot_map, RegisterType}, _}=DotMapA, CausalContextA},
      {{{dot_map, RegisterType}, _}=DotMapB, CausalContextB}) ->

    KeysA = dot_map:fetch_keys(DotMapA),
    KeysB = dot_map:fetch_keys(DotMapB),
    Keys = ordsets:union(
        ordsets:from_list(KeysA),
        ordsets:from_list(KeysB)
    ),

    DotStore = ordsets:fold(
        fun(Key, DotMap) ->
            KeyDotStoreA = dot_map:fetch(Key, DotMapA),
            KeyDotStoreB = dot_map:fetch(Key, DotMapB),

            {RegisterType, {VK, _}} = RegisterType:merge(
                {RegisterType, {KeyDotStoreA, CausalContextA}},
                {RegisterType, {KeyDotStoreB, CausalContextB}}
            ),

            case orddict:is_empty(VK) of
                true ->
                    DotMap;
                false ->
                    dot_map:store(Key, VK, DotMap)
            end
        end,
        dot_map:new(RegisterType),
        Keys
    ),
    CausalContext = causal_context:merge(CausalContextA, CausalContextB),

    {DotStore, CausalContext}.


%% @doc Equality for `state_mvmap()'.
%%      Since everything is ordered, == should work.
-spec equal(state_mvmap(), state_mvmap()) -> boolean().
equal({?TYPE, MVMap1}, {?TYPE, MVMap2}) ->
    MVMap1 == MVMap2.

%% @doc Check if a `state_mvmap()' is bottom
-spec is_bottom(delta_or_state()) -> boolean().
is_bottom({?TYPE, {delta, Map}}) ->
    is_bottom({?TYPE, Map});
is_bottom({?TYPE, _}=CRDT) ->
    CRDT == new().

%% @doc Given two `state_mvmap()', check if the second is an inflation
%%      of the first.
%% @todo
-spec is_inflation(delta_or_state(), state_mvmap()) -> boolean().
is_inflation({?TYPE, {delta, Map1}}, {?TYPE, Map2}) ->
    is_inflation({?TYPE, Map1}, {?TYPE, Map2});
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(delta_or_state(), state_mvmap()) -> boolean().
is_strict_inflation({?TYPE, {delta, Map1}}, {?TYPE, Map2}) ->
    is_strict_inflation({?TYPE, Map1}, {?TYPE, Map2});
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Join decomposition for `state_mvmap()'.
%% @todo
-spec join_decomposition(state_mvmap()) -> [state_mvmap()].
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

set_test() ->
    Actor = 1,
    Map0 = new(),
    {ok, Map1} = mutate({set, "a", "a_value"}, Actor, Map0),
    ?debugVal(Map0),
    ?debugVal(Map1),
    {ok, Map2} = mutate({set, "b", "b_value"}, Actor, Map1),
    {ok, Map3} = mutate({set, "c", "c_value"}, Actor, Map2),
    ?assertEqual([{"a", sets:from_list(["a_value"])}], query(Map1)),
    ?assertEqual([{"a", sets:from_list(["a_value"])}, {"b", sets:from_list(["b_value"])}], query(Map2)),
    ?assertEqual([{"a", sets:from_list(["a_value"])}, {"b", sets:from_list(["b_value"])}, {"c", sets:from_list(["c_value"])}], query(Map3)).

-endif.
