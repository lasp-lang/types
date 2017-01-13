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

%% @doc GMap CRDT: grow only map.
%%      Modeled as a dictionary where keys can be anything and the
%%      values are join-semilattices.
%%
%% @reference Carlos Baquero
%%      delta-enabled-crdts C++ library
%%      [https://github.com/CBaquero/delta-enabled-crdts]

-module(state_gmap).
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
-export([query/1, equal/2, is_bottom/1, is_inflation/2, is_strict_inflation/2, irreducible_is_strict_inflation/2]).
-export([join_decomposition/1, delta/3]).
-export([encode/2, decode/2]).

-export_type([state_gmap/0, state_gmap_op/0]).

-opaque state_gmap() :: {?TYPE, payload()}.
-type ctype() :: state_type:state_type() | {state_type:state_type(), [term()]}.
-type payload() :: {ctype(), orddict:orddict()}.
-type key() :: term().
-type key_op() :: term().
-type state_gmap_op() :: {apply, key(), key_op()}.

%% @doc Create a new, empty `state_gmap()'.
%%      By default the values are a MaxInt CRDT.
-spec new() -> state_gmap().
new() ->
    new([?MAX_INT_TYPE]).

%% @doc Create a new, empty `state_gmap()'
-spec new([term()]) -> state_gmap().
new([CType]) ->
    {?TYPE, {CType, orddict:new()}}.

%% @doc Mutate a `state_gmap()'.
-spec mutate(state_gmap_op(), type:id(), state_gmap()) ->
    {ok, state_gmap()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_gmap()'.
%%      The first argument can only be a triple where the first
%%      component `apply`, the second is a key, and the third is the
%%      operation to be performed on the correspondent value of that
%%      key.
-spec delta_mutate(state_gmap_op(), type:id(), state_gmap()) ->
    {ok, state_gmap()}.
delta_mutate({apply, Key, Op}, Actor, {?TYPE, {CType, GMap}}) ->
    {Type, Args} = state_type:extract_args(CType),

    Current = case orddict:find(Key, GMap) of
        {ok, Value} ->
            Value;
        error ->
            Type:new(Args)
    end,
    {ok, {Type, KeyDelta}} = Type:delta_mutate(Op, Actor, Current),
    Delta = orddict:store(Key, {Type, KeyDelta}, orddict:new()),
    {ok, {?TYPE, {CType, Delta}}}.

%% @doc Returns the value of the `state_gmap()'.
%%      This value is a dictionary where each key maps to the
%%      result of `query/1' over the current value.
-spec query(state_gmap()) -> term().
query({?TYPE, {CType, GMap}}) ->
    {Type, _Args} = state_type:extract_args(CType),
    lists:map(
        fun({Key, Value}) ->
            {Key, Type:query(Value)}
        end,
        GMap
    ).

%% @doc Merge two `state_gmap()'.
%%      The keys of the resulting `state_gmap()' are the union of the
%%      keys of both `state_gmap()' passed as input.
%%      If a key is only present on one of the `state_gmap()',
%%      its correspondent value is preserved.
%%      If a key is present in both `state_gmap()', the new value
%%      will be the `merge/2' of both values.
-spec merge(state_gmap(), state_gmap()) -> state_gmap().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun({?TYPE, {CType, GMap1}}, {?TYPE, {CType, GMap2}}) ->
        {Type, _Args} = state_type:extract_args(CType),
        GMap = orddict:merge(
            fun(_, Value1, Value2) ->
                Type:merge(Value1, Value2)
            end,
            GMap1,
            GMap2
        ),
        {?TYPE, {CType, GMap}}
    end,
    state_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Equality for `state_gmap()'.
%%      Two `state_gmap()' are equal if they have the same keys
%%      and for each key, their values are also `equal/2'.
-spec equal(state_gmap(), state_gmap()) -> boolean().
equal({?TYPE, {CType, GMap1}}, {?TYPE, {CType, GMap2}}) ->
    {Type, _Args} = state_type:extract_args(CType),
    Fun = fun(Value1, Value2) ->
        Type:equal(Value1, Value2)
    end,
    orddict_ext:equal(GMap1, GMap2, Fun).

%% @doc Check if a `state_gmap()' is bottom
-spec is_bottom(state_gmap()) -> boolean().
is_bottom({?TYPE, {_CType, GMap}}) ->
    orddict:is_empty(GMap).

%% @doc Given two `state_gmap()', check if the second is an inflation
%%      of the first.
%%      Two conditions should be met:
%%          - each key in the first `state_gmap()' is also in
%%          the second `state_gmap()'
%%          - for each key in the first `state_gmap()',
%%          the correspondent value in the second `state_gmap()'
%%          should be an inflation of the value in the first.
-spec is_inflation(state_gmap(), state_gmap()) -> boolean().
is_inflation({?TYPE, {CType, GMap1}}, {?TYPE, {CType, GMap2}}) ->
    {Type, _Args} = state_type:extract_args(CType),
    lists_ext:iterate_until(
        fun({Key, Value1}) ->
            case orddict:find(Key, GMap2) of
                {ok, Value2} ->
                    Type:is_inflation(Value1, Value2);
                error ->
                    false
            end
        end,
        GMap1
     ).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_gmap(), state_gmap()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_gmap(), state_gmap()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=Irreducible, {?TYPE, _}=CRDT) ->
    state_type:irreducible_is_strict_inflation(Irreducible, CRDT).

%% @doc Join decomposition for `state_gmap()'.
%% @todo
-spec join_decomposition(state_gmap()) -> [state_gmap()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

%% @doc Delta calculation for `state_gmap()'.
-spec delta(state_type:delta_method(), state_gmap(), state_gmap()) ->
    state_gmap().
delta(Method, {?TYPE, _}=A, {?TYPE, _}=B) ->
    state_type:delta(Method, A, B).

-spec encode(state_type:format(), state_gmap()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_gmap().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {?MAX_INT_TYPE, []}}, new()),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, []}}, new([?GCOUNTER_TYPE])).

query_test() ->
    Counter1 = {?GCOUNTER_TYPE, [{1, 1}, {2, 13}, {3, 1}]},
    Counter2 = {?GCOUNTER_TYPE, [{2, 2}, {3, 13}, {5, 2}]},
    Map0 = new([?GCOUNTER_TYPE]),
    Map1 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, Counter1}, {<<"key2">>, Counter2}]}},
    ?assertEqual([], query(Map0)),
    ?assertEqual([{<<"key1">>, 15}, {<<"key2">>, 17}], query(Map1)).

delta_apply_test() ->
    Map0 = new([?GCOUNTER_TYPE]),
    {ok, {?TYPE, Delta1}} = delta_mutate({apply, <<"key1">>, increment}, 1, Map0),
    Map1 = merge({?TYPE, Delta1}, Map0),
    {ok, {?TYPE, Delta2}} = delta_mutate({apply, <<"key1">>, increment}, 2, Map1),
    Map2 = merge({?TYPE, Delta2}, Map1),
    {ok, {?TYPE, Delta3}} = delta_mutate({apply, <<"key2">>, increment}, 1, Map2),
    Map3 = merge({?TYPE, Delta3}, Map2),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}}, Map1),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{2, 1}]}}]}}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 1}, {2, 1}]}}]}}, Map2),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, [{<<"key2">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}}, {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 1}, {2, 1}]}},
                                           {<<"key2">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}}, Map3).

apply_test() ->
    Map0 = new([?GCOUNTER_TYPE]),
    {ok, Map1} = mutate({apply, <<"key1">>, increment}, 1, Map0),
    {ok, Map2} = mutate({apply, <<"key1">>, increment}, 2, Map1),
    {ok, Map3} = mutate({apply, <<"key2">>, increment}, 1, Map2),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}}, Map1),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 1}, {2, 1}]}}]}}, Map2),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 1}, {2, 1}]}},
                                           {<<"key2">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}}, Map3).

merge_deltas_test() ->
    Map1 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}},
    Delta1 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 17}]}}]}},
    Delta2 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key2">>, {?GCOUNTER_TYPE, [{1, 17}]}}]}},
    Map2 = merge(Delta1, Map1),
    Map3 = merge(Map1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 17}]}}]}}, Map2),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 17}]}}]}}, Map3),
    ?assertEqual({?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 17}]}},
                                                   {<<"key2">>, {?GCOUNTER_TYPE, [{1, 17}]}}]}}, DeltaGroup).

equal_test() ->
    Map1 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}},
    Map2 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 2}]}}]}},
    Map3 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key2">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}},
    ?assert(equal(Map1, Map1)),
    ?assertNot(equal(Map1, Map2)),
    ?assertNot(equal(Map1, Map3)).

is_bottom_test() ->
    Map0 = new(),
    Map1 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}},
    ?assert(is_bottom(Map0)),
    ?assertNot(is_bottom(Map1)).

is_inflation_test() ->
    Map1 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}},
    Map2 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 2}]}}]}},
    Map3 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key2">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}},
    ?assert(is_inflation(Map1, Map1)),
    ?assert(is_inflation(Map1, Map2)),
    ?assertNot(is_inflation(Map1, Map3)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Map1, Map1)),
    ?assert(state_type:is_inflation(Map1, Map2)),
    ?assertNot(state_type:is_inflation(Map1, Map3)).

is_strict_inflation_test() ->
    Map1 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}},
    Map2 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 2}]}}]}},
    Map3 = {?TYPE, {?GCOUNTER_TYPE, [{<<"key2">>, {?GCOUNTER_TYPE, [{1, 1}]}}]}},
    ?assertNot(is_strict_inflation(Map1, Map1)),
    ?assert(is_strict_inflation(Map1, Map2)),
    ?assertNot(is_strict_inflation(Map1, Map3)).

join_decomposition_test() ->
    %% @todo
    ok.

encode_decode_test() ->
    Map = {?TYPE, {?GCOUNTER_TYPE, [{<<"key1">>, {?GCOUNTER_TYPE, [{1, 2}]}}]}},
    Binary = encode(erlang, Map),
    EMap = decode(erlang, Binary),
    ?assertEqual(Map, EMap).

equivalent_with_gcounter_test() ->
    Actor1 = 1,
    Actor2 = 2,
    Map0 = new([?MAX_INT_TYPE]),
    {ok, Map1} = mutate({apply, Actor1, increment}, undefined, Map0),
    {ok, Map2} = mutate({apply, Actor1, increment}, undefined, Map1),
    {ok, Map3} = mutate({apply, Actor2, increment}, undefined, Map2),
    [{Actor1, Value1}, {Actor2, Value2}] = query(Map3),
    GCounter0 = ?GCOUNTER_TYPE:new(),
    {ok, GCounter1} = ?GCOUNTER_TYPE:mutate(increment, Actor1, GCounter0),
    {ok, GCounter2} = ?GCOUNTER_TYPE:mutate(increment, Actor1, GCounter1),
    {ok, GCounter3} = ?GCOUNTER_TYPE:mutate(increment, Actor2, GCounter2),
    ?assertEqual(Value1 + Value2, ?GCOUNTER_TYPE:query(GCounter3)).

-endif.
