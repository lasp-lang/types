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
%%      MVMap = AWMap<MVRegister<V>>
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

-export([new/0, new/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_bottom/1,
         is_inflation/2, is_strict_inflation/2,
         irreducible_is_strict_inflation/2]).
-export([join_decomposition/1, delta/2, digest/1]).
-export([encode/2, decode/2]).

-export_type([state_mvmap/0, state_mvmap_op/0]).

-opaque state_mvmap() :: {?TYPE, payload()}.
-type payload() :: state_awmap:state_awmap().
-type key() :: term().
-type value() :: term().
-type state_mvmap_op() :: {set, key(), value()}.

%% @doc Create a new, empty `state_mvmap()'.
-spec new() -> state_mvmap().
new() ->
    {?TYPE, ?AWMAP_TYPE:new([?MVREGISTER_TYPE])}.

%% @doc Create a new, empty `state_mvmap()'
-spec new([term()]) -> state_mvmap().
new([]) ->
    new().

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
    {ok, state_mvmap()}.
delta_mutate({set, Key, Value}, Actor, {?TYPE, AWMap}) ->
    {ok, Delta} = ?AWMAP_TYPE:delta_mutate(
        {apply, Key, {set, undefined, Value}},
        Actor,
        AWMap
    ),
    {ok, {?TYPE, Delta}}.

%% @doc Returns the value of the `state_mvmap()'.
%%      This value is a dictionary where each key maps to the
%%      result of `query/1' over the current value.
-spec query(state_mvmap()) -> term().
query({?TYPE, AWMap}) ->
    ?AWMAP_TYPE:query(AWMap).

%% @doc Merge two `state_mvmap()'.
-spec merge(state_mvmap(), state_mvmap()) -> state_mvmap().
merge({?TYPE, AWMap1}, {?TYPE, AWMap2}) ->
    Map = ?AWMAP_TYPE:merge(AWMap1, AWMap2),
    {?TYPE, Map}.

%% @doc Equality for `state_mvmap()'.
%%      Since everything is ordered, == should work.
-spec equal(state_mvmap(), state_mvmap()) -> boolean().
equal({?TYPE, AWMap1}, {?TYPE, AWMap2}) ->
    ?AWMAP_TYPE:equal(AWMap1, AWMap2).

%% @doc Check if a `state_mvmap()' is bottom
-spec is_bottom(state_mvmap()) -> boolean().
is_bottom({?TYPE, _}=CRDT) ->
    CRDT == new().

%% @doc Given two `state_mvmap()', check if the second is an inflation
%%      of the first.
%% @todo
-spec is_inflation(state_mvmap(), state_mvmap()) -> boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_mvmap(), state_mvmap()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_mvmap(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=A, B) ->
    state_type:irreducible_is_strict_inflation(A, B).

-spec digest(state_mvmap()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_mvmap()'.
%% @todo
-spec join_decomposition(state_mvmap()) -> [state_mvmap()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

%% @doc Delta calculation for `state_mvmap()'.
-spec delta(state_mvmap(), state_type:digest()) -> state_mvmap().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_mvmap()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_mvmap().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

set_test() ->
    ActorOne = 1,
    ActorTwo = 2,
    Map0 = new(),
    {ok, Map1} = mutate({set, "a", "a_value"}, ActorOne, Map0),
    {ok, Map2} = mutate({set, "b", "b_value"}, ActorOne, Map1),
    {ok, Map3} = mutate({set, "c", "c1_value"}, ActorOne, Map2),
    {ok, Map4} = mutate({set, "c", "c2_value"}, ActorTwo, Map2),
    Map5 = merge(Map3, Map4),
    {ok, Map6} = mutate({set, "c", "c_value"}, ActorOne, Map5),
    ?assertEqual([{"a", sets:from_list(["a_value"])}], query(Map1)),
    ?assertEqual([{"a", sets:from_list(["a_value"])}, {"b", sets:from_list(["b_value"])}], query(Map2)),
    ?assertEqual([{"a", sets:from_list(["a_value"])}, {"b", sets:from_list(["b_value"])}, {"c", sets:from_list(["c1_value"])}], query(Map3)),
    ?assertEqual([{"a", sets:from_list(["a_value"])}, {"b", sets:from_list(["b_value"])}, {"c", sets:from_list(["c2_value"])}], query(Map4)),
    ?assertEqual([{"a", sets:from_list(["a_value"])}, {"b", sets:from_list(["b_value"])}, {"c", sets:from_list(["c1_value", "c2_value"])}], query(Map5)),
    ?assertEqual([{"a", sets:from_list(["a_value"])}, {"b", sets:from_list(["b_value"])}, {"c", sets:from_list(["c_value"])}], query(Map6)).

-endif.
