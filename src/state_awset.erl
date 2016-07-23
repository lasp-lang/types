%%
%% Copyright (c) 2015-2016 Christopher Meiklejohn.  All Rights Reserved.
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Add-Wins ORSet CRDT: observed-remove set without tombstones.
%%      This type is an example of the causal CRDTs using the common library.
%%      Currently, the set-theoretic functions (product(), union(), intersection()) and
%%      the functional programming functions (map(), filter(), fold()) are not supported.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(state_awset).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-behaviour(type).
-behaviour(state_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_bottom/1, is_inflation/2, is_strict_inflation/2]).
-export([join_decomposition/1]).
-export([encode/2, decode/2]).

-export_type([state_awset/0, delta_state_awset/0, state_awset_op/0]).

-opaque state_awset() :: {?TYPE, payload()}.
-opaque delta_state_awset() :: {?TYPE, {delta, payload()}}.
-type delta_or_state() :: state_awset() | delta_state_awset().
-type payload() :: state_causal_type:causal_crdt().
-type element() :: term().
-type state_awset_op() :: {add, element()}
                        | {add_all, [element()]}
                        | {rmv, element()}
                        | {rmv_all, [element()]}.

%% @doc Create a new, empty `state_awset()'
%% DotMap<Elem, DotSet>
-spec new() -> state_awset().
new() ->
    {?TYPE, state_causal_type:new_causal_crdt({dot_map, dot_set})}.

%% @doc Create a new, empty `state_awset()'
-spec new([term()]) -> state_awset().
new([]) ->
    new().

%% @doc Mutate a `state_awset()'.
-spec mutate(state_awset_op(), type:id(), state_awset()) ->
    {ok, state_awset()} | {error, {precondition, {not_present, [element()]}}}.
mutate(Op, Actor, {?TYPE, _AWSet}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_awset()'.
%% The first argument can be:
%%     - `{add, element()}'
%%     - `{rmv, element()}'
%%     - `{add_all, [element()]}'
%%     - `{rmv_all, [element()]}'
%% The second argument is the replica id.
%% The third argument is the `state_awset()' to be inflated.
-spec delta_mutate(state_awset_op(), type:id(), state_awset()) ->
    {ok, delta_state_awset()} | {error, {precondition, {not_present, element()}}}.
%% @doc Adds a single elemenet to `state_awset()'.
delta_mutate({add, Elem}, Actor, {?TYPE, AWSet}) ->
    {ok, _AWSet1, Delta} =
        add_elem_delta(Elem,
                       Actor,
                       AWSet,
                       {state_causal_type:new_data_store({dot_map, dot_set}),
                        ordsets:new()}),
    {ok, {?TYPE, {delta, Delta}}};

%% @doc Adds a list of elemenets to `state_awset()'.
delta_mutate({add_all, Elems}, Actor, {?TYPE, AWSet}) ->
    {ok, _AWSet1, Delta} =
        lists:foldl(
          fun(Elem, {ok, AWSet0, Delta0}) ->
                  add_elem_delta(Elem, Actor, AWSet0, Delta0)
          end,
          {ok,
           AWSet,
           {state_causal_type:new_data_store({dot_map, dot_set}), ordsets:new()}},
          Elems),
    {ok, {?TYPE, {delta, Delta}}};

%% @doc Removes a single element in `state_awset()'.
%% An empty data store and observed dots for the element.
delta_mutate({rmv, Elem}, _Actor, {?TYPE, {DataStore0, _DotCloud}}) ->
    case remove_elem_delta(Elem,
                           DataStore0,
                           {state_causal_type:new_data_store({dot_map, dot_set}),
                            ordsets:new()}) of
        {ok, _DataStore, Delta} -> {ok, {?TYPE, {delta, Delta}}};
        Error -> Error
    end;

%% @doc Removes a list of elemenets in `state_awset()'.
%% An empty data store and observed dots for all elements in the list.
delta_mutate({rmv_all, Elems}, _Actor, {?TYPE, {DataStore0, _DotCloud}}) ->
    case remove_elems_delta(Elems,
                            DataStore0,
                            {state_causal_type:new_data_store({dot_map, dot_set}),
                             ordsets:new()}) of
        {ok, _DataStore, Delta} -> {ok, {?TYPE, {delta, Delta}}};
        Error -> Error
    end.

%% @doc Returns the value of the `state_awset()'.
%% This value is a set with all the keys (elements) in the data store.
-spec query(state_awset()) -> sets:set(element()).
query({?TYPE, {DataStore, _DotCloud}=_AWSet}) ->
    Result = state_causal_type:get_all_objects(DataStore),
    sets:from_list(Result).

%% @doc Merge two `state_awset()'.
%% Merging will be handled by the causal_join() in the common library.
-spec merge(delta_or_state(), delta_or_state()) -> delta_or_state().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun({?TYPE, AWSet1}, {?TYPE, AWSet2}) ->
        AWSet = state_causal_type:causal_join(AWSet1, AWSet2),
        {?TYPE, AWSet}
    end,
    state_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Equality for `state_awset()'.
%% Since everything is ordered, == should work.
-spec equal(state_awset(), state_awset()) -> boolean().
equal({?TYPE, AWSet1}, {?TYPE, AWSet2}) ->
    AWSet1 == AWSet2.

%% @doc Check if an AWSet is bottom.
-spec is_bottom(delta_or_state()) -> boolean().
is_bottom({?TYPE, {delta, AWSet}}) ->
    is_bottom({?TYPE, AWSet});
is_bottom({?TYPE, _}=CRDT) ->
    CRDT == new().

%% @doc Given two `state_awset()', check if the second is and inflation of the first.
%% The inflation will be checked by the is_lattice_inflation() in the common library.
-spec is_inflation(state_awset(), state_awset()) -> boolean().
is_inflation({?TYPE, AWSet1}, {?TYPE, AWSet2}) ->
    state_causal_type:is_lattice_inflation(AWSet1, AWSet2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_awset(), state_awset()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Join decomposition for `state_awset()'.
-spec join_decomposition(state_awset()) -> [state_awset()].
join_decomposition({?TYPE, {{{dot_map, dot_set}, DataStoreDict}, DotCloud0}}) ->
    {DecompList, AccDotCloud} =
        orddict:fold(
          fun(Elem, SubDataStore, {DecompList0, AccDotCloud0}) ->
                  NewDotCloud = state_causal_type:get_dot_cloud(SubDataStore),
                  NewDataStore =
                      state_causal_type:insert_object(
                        Elem,
                        SubDataStore,
                        state_causal_type:new_data_store({dot_map, dot_set})),
                  NewAWSet = [{?TYPE, {NewDataStore, NewDotCloud}}],
                  DecompList1 = lists:append(DecompList0, NewAWSet),
                  {DecompList1, state_causal_type:merge_dot_clouds(AccDotCloud0,
                                                                   NewDotCloud)}
          end, {[], ordsets:new()}, DataStoreDict),
    case ordsets:subtract(DotCloud0, AccDotCloud) of
        [] ->
            DecompList;
        RemovedDotCloud ->
            ordsets:fold(
              fun(Dot0, DecompList0) ->
                      lists:append(
                        DecompList0,
                        [{?TYPE,
                          {state_causal_type:new_data_store({dot_map, dot_set}),
                           [Dot0]}}])
              end, DecompList, RemovedDotCloud)
    end.

-spec encode(state_type:format(), delta_or_state()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> delta_or_state().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.

%% @private
add_elem_delta(Elem,
               Actor,
               {DataStore0, DotCloud0},
               {DeltaDataStore0, DeltaDotCloud0}) ->
    NewDotContext = state_causal_type:get_next_dot_context(Actor, DotCloud0),

    {ok, {dot_set, DotSet}} = state_causal_type:get_sub_data_store(Elem, DataStore0),
    DeltaDotCloud1 = state_causal_type:insert_dot_context(NewDotContext, DotSet),
    DeltaDotCloud = state_causal_type:merge_dot_clouds(DeltaDotCloud0,
                                                       DeltaDotCloud1),
    DeltaDataStore =
        state_causal_type:insert_object(Elem,
                                        {dot_set, DeltaDotCloud1},
                                        DeltaDataStore0),

    DotCloud = state_causal_type:insert_dot_context(NewDotContext, DotCloud0),

    {ok, {DataStore0, DotCloud}, {DeltaDataStore, DeltaDotCloud}}.

%% @private
remove_elem_delta(Elem, DataStore, {DeltaDataStore0, DeltaDotCloud0}) ->
    {ok, SubDataStore} = state_causal_type:get_sub_data_store(Elem, DataStore),
    case state_causal_type:is_bottom_data_store(SubDataStore) of
        false ->
            DeltaDotCloud = state_causal_type:merge_dot_clouds(
                              DeltaDotCloud0,
                              state_causal_type:get_dot_cloud(SubDataStore)),
            {ok, DataStore, {DeltaDataStore0, DeltaDotCloud}};
        true ->
            {error, {precondition, {not_present, Elem}}}
    end.

%% @private
remove_elems_delta([], DataStore, Delta) ->
    {ok, DataStore, Delta};
remove_elems_delta([Elem|Rest], DataStore, {DeltaDataStore0, DeltaDotCloud0}) ->
    case remove_elem_delta(Elem, DataStore, {DeltaDataStore0, DeltaDotCloud0}) of
        {ok, DataStore, Delta} -> remove_elems_delta(Rest, DataStore, Delta);
        Error         -> Error
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {{{dot_map, dot_set}, orddict:new()}, ordsets:new()}},
                 new()).

query_test() ->
    Set0 = new(),
    Set1 = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{a, 2}]}}]},
                    [{a, 1}, {a, 2}]}},
    ?assertEqual(sets:new(), query(Set0)),
    ?assertEqual(sets:from_list([<<"a">>]), query(Set1)).

delta_add_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate({add, <<"a">>}, Actor, Set0),
    Set1 = merge({?TYPE, Delta1}, Set0),
    {ok, {?TYPE, {delta, Delta2}}} = delta_mutate({add, <<"a">>}, Actor, Set1),
    Set2 = merge({?TYPE, Delta2}, Set1),
    {ok, {?TYPE, {delta, Delta3}}} = delta_mutate({add, <<"b">>}, Actor, Set2),
    Set3 = merge({?TYPE, Delta3}, Set2),
    ?assertEqual({?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                          [{1, 1}]}},
                 {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                          [{1, 1}]}},
                 Set1),
    ?assertEqual({?TYPE, {{{dot_map, dot_set},
                           [{<<"a">>, {dot_set, [{1, 1}, {1, 2}]}}]},
                          [{1, 1}, {1, 2}]}},
                 {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {{{dot_map, dot_set},
                           [{<<"a">>, {dot_set, [{1, 1}, {1, 2}]}}]},
                          [{1, 1}, {1, 2}]}},
                 Set2),
    ?assertEqual({?TYPE, {{{dot_map, dot_set}, [{<<"b">>, {dot_set, [{1, 3}]}}]},
                          [{1, 3}]}},
                 {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {{{dot_map, dot_set},
                           [{<<"a">>, {dot_set, [{1, 1}, {1, 2}]}},
                            {<<"b">>, {dot_set, [{1, 3}]}}]},
                          [{1, 1}, {1, 2}, {1, 3}]}},
                 Set3).

add_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"a">>}, Actor, Set0),
    {ok, Set2} = mutate({add, <<"a">>}, Actor, Set1),
    {ok, Set3} = mutate({add, <<"b">>}, Actor, Set2),
    ?assertEqual({?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                          [{1, 1}]}},
                 Set1),
    ?assertEqual({?TYPE, {{{dot_map, dot_set},
                           [{<<"a">>, {dot_set, [{1, 1}, {1, 2}]}}]},
                          [{1, 1}, {1, 2}]}},
                 Set2),
    ?assertEqual({?TYPE, {{{dot_map, dot_set},
                           [{<<"a">>, {dot_set, [{1, 1}, {1, 2}]}},
                            {<<"b">>, {dot_set, [{1, 3}]}}]},
                          [{1, 1}, {1, 2}, {1, 3}]}},
                 Set3).

rmv_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"a">>}, Actor, Set0),
    {error, _} = mutate({rmv, <<"b">>}, Actor, Set1),
    {ok, Set2} = mutate({rmv, <<"a">>}, Actor, Set1),
    ?assertEqual(sets:new(), query(Set2)).

add_all_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add_all, []}, Actor, Set0),
    {ok, Set2} = mutate({add_all, [<<"a">>, <<"b">>]}, Actor, Set0),
    {ok, Set3} = mutate({add_all, [<<"b">>, <<"c">>]}, Actor, Set2),
    ?assertEqual(sets:new(), query(Set1)),
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>]), query(Set2)),
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>, <<"c">>]), query(Set3)).

remove_all_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add_all, [<<"a">>, <<"b">>, <<"c">>]}, Actor, Set0),
    {ok, Set2} = mutate({rmv_all, [<<"a">>, <<"c">>]}, Actor, Set1),
    {error, _} = mutate({rmv_all, [<<"b">>, <<"d">>]}, Actor, Set2),
    {ok, Set3} = mutate({rmv_all, [<<"b">>]}, Actor, Set2),
    ?assertEqual(sets:from_list([<<"b">>]), query(Set2)),
    ?assertEqual(sets:new(), query(Set3)).

merge_idempontent_test() ->
    Set1 = {?TYPE, {{{dot_map, dot_set}, []}, [{1, 1}]}},
    Set2 = {?TYPE, {{{dot_map, dot_set}, [{<<"b">>, {dot_set, [{2, 1}]}}]},
                    [{2, 1}]}},
    Set3 = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                    [{1, 1}, {2, 1}]}},
    Set4 = merge(Set1, Set1),
    Set5 = merge(Set2, Set2),
    Set6 = merge(Set3, Set3),
    ?assertEqual({?TYPE, {{{dot_map, dot_set}, []}, [{1, 1}]}}, Set4),
    ?assertEqual({?TYPE, {{{dot_map, dot_set}, [{<<"b">>, {dot_set, [{2, 1}]}}]},
                          [{2, 1}]}},
                 Set5),
    ?assertEqual({?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                          [{1, 1}, {2, 1}]}},
                 Set6).

merge_commutative_test() ->
    Set1 = {?TYPE, {{{dot_map, dot_set}, []}, [{1, 1}]}},
    Set2 = {?TYPE, {{{dot_map, dot_set}, [{<<"b">>, {dot_set, [{2, 1}]}}]},
                    [{2, 1}]}},
    Set3 = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                    [{1, 1}, {2, 1}]}},
    Set4 = merge(Set1, Set2),
    Set5 = merge(Set2, Set1),
    Set6 = merge(Set1, Set3),
    Set7 = merge(Set3, Set1),
    Set8 = merge(Set2, Set3),
    Set9 = merge(Set3, Set2),
    Set10 = merge(Set1, merge(Set2, Set3)),
    Set1_2 = {?TYPE, {{{dot_map, dot_set}, [{<<"b">>, {dot_set, [{2, 1}]}}]},
                      [{1, 1}, {2, 1}]}},
    Set1_3 = {?TYPE, {{{dot_map, dot_set}, []}, [{1, 1}, {2, 1}]}},
    Set2_3 = Set3,
    ?assertEqual(Set1_2, Set4),
    ?assertEqual(Set1_2, Set5),
    ?assertEqual(Set1_3, Set6),
    ?assertEqual(Set1_3, Set7),
    ?assertEqual(Set2_3, Set8),
    ?assertEqual(Set2_3, Set9),
    ?assertEqual(Set1_3, Set10).

merge_delta_test() ->
    Set1 = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                    [{1, 1}]}},
    Delta1 = {?TYPE, {delta, {{{dot_map, dot_set}, []}, [{1, 1}]}}},
    Delta2 = {?TYPE, {delta, {{{dot_map, dot_set}, [{<<"b">>, {dot_set, [{2, 1}]}}]},
                              [{2, 1}]}}},
    Set2 = merge(Delta1, Set1),
    Set3 = merge(Set1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, {{{dot_map, dot_set}, []}, [{1, 1}]}}, Set2),
    ?assertEqual({?TYPE, {{{dot_map, dot_set}, []}, [{1, 1}]}}, Set3),
    ?assertEqual({?TYPE, {delta, {{{dot_map, dot_set},
                                   [{<<"b">>, {dot_set, [{2, 1}]}}]},
                                  [{1, 1}, {2, 1}]}}},
                 DeltaGroup).

equal_test() ->
    Set1 = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                    [{1, 1}]}},
    Set2 = {?TYPE, {{{dot_map, dot_set}, []}, [{1, 1}]}},
    Set3 = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                    [{1, 1}, {2, 1}]}},
    ?assert(equal(Set1, Set1)),
    ?assert(equal(Set2, Set2)),
    ?assert(equal(Set3, Set3)),
    ?assertNot(equal(Set1, Set2)),
    ?assertNot(equal(Set1, Set3)),
    ?assertNot(equal(Set2, Set3)).

is_bottom_test() ->
    Set0 = new(),
    Set1 = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                    [{1, 1}]}},
    ?assert(is_bottom(Set0)),
    ?assertNot(is_bottom(Set1)).

is_inflation_test() ->
    Set1 = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                    [{1, 1}]}},
    Set2 = {?TYPE, {{{dot_map, dot_set}, []}, [{1, 1}]}},
    Set3 = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                    [{1, 1}, {2, 1}]}},
    ?assert(is_inflation(Set1, Set1)),
    ?assert(is_inflation(Set1, Set2)),
    ?assertNot(is_inflation(Set2, Set1)),
    ?assert(is_inflation(Set1, Set3)),
    ?assertNot(is_inflation(Set2, Set3)),
    ?assertNot(is_inflation(Set3, Set2)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Set1, Set1)),
    ?assert(state_type:is_inflation(Set1, Set2)),
    ?assertNot(state_type:is_inflation(Set2, Set1)),
    ?assert(state_type:is_inflation(Set1, Set3)),
    ?assertNot(state_type:is_inflation(Set2, Set3)),
    ?assertNot(state_type:is_inflation(Set3, Set2)).

is_strict_inflation_test() ->
    Set1 = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                    [{1, 1}]}},
    Set2 = {?TYPE, {{{dot_map, dot_set}, []}, [{1, 1}]}},
    Set3 = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                    [{1, 1}, {2, 1}]}},
    ?assertNot(is_strict_inflation(Set1, Set1)),
    ?assert(is_strict_inflation(Set1, Set2)),
    ?assertNot(is_strict_inflation(Set2, Set1)),
    ?assert(is_strict_inflation(Set1, Set3)),
    ?assertNot(is_strict_inflation(Set2, Set3)),
    ?assertNot(is_strict_inflation(Set3, Set2)).

join_decomposition_test() ->
    Set1 = {?TYPE, {{{dot_map, dot_set}, []}, [{1, 1}]}},
    Set2 = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                    [{1, 1}, {2, 1}, {3, 1}]}},
    Decomp1 = join_decomposition(Set1),
    Decomp2 = join_decomposition(Set2),
    List = [{?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]},
                     [{1, 1}]}},
            {?TYPE, {{{dot_map, dot_set}, []}, [{2, 1}]}},
            {?TYPE, {{{dot_map, dot_set}, []}, [{3, 1}]}}],
    ?assertEqual([Set1], Decomp1),
    ?assertEqual(lists:sort(List), lists:sort(Decomp2)).

encode_decode_test() ->
    Set = {?TYPE, {{{dot_map, dot_set}, [{<<"a">>, {dot_set, [{1, 1}]}}]}, [{1, 1}, {2, 1}, {3, 1}]}},
    Binary = encode(erlang, Set),
    ESet = decode(erlang, Binary),
    ?assertEqual(Set, ESet).

-endif.
