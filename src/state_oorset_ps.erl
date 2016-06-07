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

%% @doc Optimised ORSet CRDT with provenance semirings:
%%      observed-remove set without tombstones
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(state_oorset_ps).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-behaviour(type).
-behaviour(state_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_inflation/2, is_strict_inflation/2]).
-export([join_decomposition/1]).

-export_type([state_oorset_ps/0, delta_state_oorset_ps/0, state_oorset_ps_op/0]).

-opaque state_oorset_ps() :: {?TYPE, payload()}.
-opaque delta_state_oorset_ps() :: {?TYPE, {delta, payload()}}.
-type delta_or_state() :: state_oorset_ps() | delta_state_oorset_ps().
-type payload() :: {ps_data_store(), ps_survived_events(), ps_all_events()}
                 | state_causal_type:causal_crdt().
-type element() :: term().
-type state_oorset_ps_op() :: {add, element()}
                            | {add_all, [element()]}
                            | {rmv, element()}
                            | {rmv_all, [element()]}.

%% Provenanve semirings related.
-export_type([ps_event_id/0]).

-type ps_object_id() :: binary().
-type ps_replica_id() :: term().
-type ps_event_id() :: {ps_object_id(), ps_replica_id()}.
-type ps_event() :: {ps_event_id(), pos_integer()}.
-type ps_dot() :: ordsets:ordsets(ps_event()).
-type ps_provenance() :: ordsets:ordsets(ps_dot()).
-type ps_data_store() :: orddict:orddict(element(), ps_provenance()).
-type ps_survived_events() :: ps_dot().
-type ps_all_events() :: ps_dot().

%% @doc Create a new, empty `state_oorset_ps()'
-spec new() -> state_oorset_ps().
new() ->
    {?TYPE, {orddict:new(), ordsets:new(), ordsets:new()}}.

%% @doc Create a new, empty `state_oorset_ps()'
-spec new([term()]) -> state_oorset_ps().
new([]) ->
    new().

%% @doc Mutate a `state_oorset_ps()'.
-spec mutate(state_oorset_ps_op(), type:id(), state_oorset_ps()) ->
    {ok, state_oorset_ps()} | {error, {precondition, {not_present, [element()]}}}.
mutate(Op, Actor, {?TYPE, _ORSet}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_oorset_ps()'.
%%      The first argument can be:
%%          - `{add, element()}'
%%          - `{rmv, element()}'
%%      The second argument is the event id ({object_id, replica_id}).
%%      The third argument is the `state_oorset_ps()' to be inflated.
-spec delta_mutate(state_oorset_ps_op(), type:id(), state_oorset_ps()) ->
    {ok, delta_state_oorset_ps()} | {error, {precondition, {not_present, element()}}}.
%% Adds a single element to `state_oorset_ps()'.
%% Delta: {[{Elem, {{NewEvent}}}], [NewEvent], [NewEvent]}
delta_mutate({add, Elem}, EventId, {?TYPE, {_DataStore, _SurvivedEvents, AllEvents}}) ->
    NewEvent = get_next_event(EventId, AllEvents),
    NewDot = ordsets:add_element(NewEvent, ordsets:new()),
    NewProvenance = ordsets:add_element(NewDot, ordsets:new()),
    DeltaDataStore = orddict:store(Elem, NewProvenance, orddict:new()),
    {ok, {?TYPE, {delta, {DeltaDataStore, NewDot, NewDot}}}};

%% @todo
%% Adds a list of elemenets to `state_oorset_ps()'.
delta_mutate({add_all, _Elems}, _Actor, {?TYPE, ORSet}) ->
    {ok, {?TYPE, {delta, ORSet}}};

%% Removes a single element in `state_oorset_ps()'.
%% Delta: {[], (ElemEvents - OwnedElemEvents), ElemEvents}
%% OwnedElemEvents = the object id is equal to the one of the `state_oorset_ps()'
delta_mutate({rmv, Elem},
             {ObjectId, _ReplicaId}=_EventId,
             {?TYPE, {DataStore, _SurvivedEvents, _AllEvents}}) ->
    case orddict:find(Elem, DataStore) of
        {ok, Provenance} ->
            ElemEvents = get_events_from_provenance(Provenance),
            ElemSurvivedEvents =
                ordsets:filter(fun({{ObjectId0, _ReplicaId0}, _Counter0}) ->
                                       ObjectId0 =/= ObjectId
                               end, ElemEvents),
            {ok, {?TYPE, {delta, {orddict:new(), ElemSurvivedEvents, ElemEvents}}}};
        error ->
            {error, {precondition, {not_present, Elem}}}
    end;

%% @todo
%% Removes a list of elemenets in `state_oorset_ps()'.
delta_mutate({rmv_all, _Elems}, _Actor, {?TYPE, ORSet}) ->
    {ok, {?TYPE, {delta, ORSet}}}.

%% @doc Returns the value of the `state_oorset_ps()'.
%% This value is a set with all the keys (elements) in the data store.
-spec query(state_oorset_ps()) -> sets:set(element()).
query({?TYPE, {DataStore, _SurvivedEvents, _AllEvents}}) ->
    Result = orddict:fetch_keys(DataStore),
    sets:from_list(Result).

%% @doc Merge two `state_oorset_ps()'.
%% Merging will be handled by the join_oorset_ps().
-spec merge(delta_or_state(), delta_or_state()) -> delta_or_state().
merge({?TYPE, {delta, Delta1}}, {?TYPE, {delta, Delta2}}) ->
    {?TYPE, DeltaGroup} = ?TYPE:merge({?TYPE, Delta1}, {?TYPE, Delta2}),
    {?TYPE, {delta, DeltaGroup}};
merge({?TYPE, {delta, Delta}}, {?TYPE, CRDT}) ->
    merge({?TYPE, Delta}, {?TYPE, CRDT});
merge({?TYPE, CRDT}, {?TYPE, {delta, Delta}}) ->
    merge({?TYPE, Delta}, {?TYPE, CRDT});
merge({?TYPE, ORSet1}, {?TYPE, ORSet2}) ->
    ORSet = join_oorset_ps(ORSet1, ORSet2),
    {?TYPE, ORSet}.

%% @doc Equality for `state_oorset_ps()'.
%% Since everything is ordered, == should work.
-spec equal(state_oorset_ps(), state_oorset_ps()) -> boolean().
equal({?TYPE, ORSet1}, {?TYPE, ORSet2}) ->
    ORSet1 == ORSet2.

%% @doc Given two `state_oorset_ps()', check if the second is and inflation of the first.
%% The inflation will be checked by the is_lattice_inflation() in the common library.
-spec is_inflation(state_oorset_ps(), state_oorset_ps()) -> boolean().
is_inflation({?TYPE, ORSet1}, {?TYPE, ORSet2}) ->
    is_lattice_inflation_oorset_ps(ORSet1, ORSet2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_oorset_ps(), state_oorset_ps()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @todo
%% @doc Join decomposition for `state_oorset_ps()'.
-spec join_decomposition(state_oorset_ps()) -> [state_oorset_ps()].
join_decomposition({?TYPE, ORSet}) ->
    [ORSet].

%% @private
-spec get_next_event(ps_event_id(), ps_all_events()) -> ps_event().
get_next_event(EventId, AllEvents) ->
    MaxCounter = ordsets:fold(fun({EventId0, EventCounter0}, MaxValue0) ->
                                      case (EventId0 == EventId) andalso
                                              (EventCounter0 > MaxValue0) of
                                          true ->
                                              EventCounter0;
                                          false ->
                                              MaxValue0
                                      end
                              end, 0, AllEvents),
    {EventId, MaxCounter + 1}.

%% @private
-spec get_events_from_provenance(ps_provenance()) -> ps_dot().
get_events_from_provenance(Provenance) ->
    ordsets:fold(fun(Dot0, Acc0) ->
                         ordsets:union(Acc0, Dot0)
                 end, ordsets:new(), Provenance).

%% @private
-spec is_lattice_inflation_oorset_ps(payload(), payload()) -> boolean().
is_lattice_inflation_oorset_ps({_DataStoreA, _SurvivedEventsA, AllEventsA},
                               {_DataStoreB, _SurvivedEventsB, AllEventsB}) ->
    AllEventsAList = get_maxs_for_all(AllEventsA),
    AllEventsBList = get_maxs_for_all(AllEventsB),
    lists:foldl(fun({EventId, Count}, Acc) ->
                        case lists:keyfind(EventId, 1, AllEventsBList) of
                            false ->
                                Acc andalso false;
                            {_EventId1, Count1} ->
                                Acc andalso (Count =< Count1)
                        end
                end, true, AllEventsAList).

%% @private
-spec join_oorset_ps(payload(), payload()) -> payload().
join_oorset_ps({DataStoreA, SurvivedEventsA, AllEventsA}=FstORSet,
               {DataStoreB, SurvivedEventsB, AllEventsB}=SndORSet) ->
    UnionElems = ordsets:union(ordsets:from_list(orddict:fetch_keys(DataStoreA)),
                               ordsets:from_list(orddict:fetch_keys(DataStoreB))),
    JoinedDataStore =
        ordsets:fold(
          fun(Elem, JoinedDataStore0) ->
                  {ok, ProvenanceA} = get_provenance(Elem, FstORSet),
                  {ok, ProvenanceB} = get_provenance(Elem, SndORSet),
                  JoinedProvenance =
                      join_provenance(ProvenanceA, SurvivedEventsA, AllEventsA,
                                      ProvenanceB, SurvivedEventsB, AllEventsB),
                  case ordsets:size(JoinedProvenance) of
                      0 ->
                          JoinedDataStore0;
                      _ ->
                          orddict:store(Elem, JoinedProvenance, JoinedDataStore0)
                  end
          end, orddict:new(), UnionElems),
    JoinedSurvivedEvents =
        ordsets:union([ordsets:intersection(SurvivedEventsA, SurvivedEventsB)] ++
                          [ordsets:subtract(SurvivedEventsA, AllEventsB)] ++
                          [ordsets:subtract(SurvivedEventsB, AllEventsA)]),
    {JoinedDataStore, JoinedSurvivedEvents, ordsets:union(AllEventsA, AllEventsB)}.

%% @private
-spec join_provenance(ps_provenance(), ps_survived_events(), ps_all_events(),
                      ps_provenance(), ps_survived_events(), ps_all_events()) ->
          ps_provenance().
join_provenance(ProvenanceA, SurvivedEventsA, AllEventsA,
                ProvenanceB, SurvivedEventsB, AllEventsB) ->
    JoinedProvenance0 = ordsets:intersection(ProvenanceA, ProvenanceB),
    InterSurvived = ordsets:intersection(SurvivedEventsA, SurvivedEventsB),
    JoinedProvenance1 = 
        ordsets:fold(fun(Dot0, Acc) ->
                             case is_valid_dot(Dot0, InterSurvived, AllEventsB) of
                                 false ->
                                     Acc;
                                 true ->
                                     ordsets:add_element(Dot0, Acc)
                             end
                     end, JoinedProvenance0, ProvenanceA),
    ordsets:fold(fun(Dot0, Acc) ->
                         case is_valid_dot(Dot0, InterSurvived, AllEventsA) of
                             false ->
                                 Acc;
                             true ->
                                 ordsets:add_element(Dot0, Acc)
                         end
                 end, JoinedProvenance1, ProvenanceB).

%% @private
-spec is_valid_dot(ps_dot(), ps_survived_events(), ps_all_events()) -> boolean().
is_valid_dot(Dot, InterSurvived, AllEvents) ->
    ordsets:fold(fun(Event0, IsValid0) ->
                         (ordsets:is_element(Event0, InterSurvived) orelse
                              (not ordsets:is_element(Event0, AllEvents)))
                         andalso IsValid0
                 end, true, Dot).

%% @private
-spec get_provenance(element(), payload()) -> ps_provenance().
get_provenance(Elem, {DataStore, _SurvivedEvents, _AllEvents}) ->
    case orddict:find(Elem, DataStore) of
        {ok, Provenance} ->
            {ok, Provenance};
        error ->
            {ok, ordsets:new()}
    end.

%% @private
get_maxs_for_all(AllEvents) ->
    ordsets:fold(fun({EventId0, EventCounter0}, MaxList) ->
                         {Counter, NewList} =
                             case lists:keytake(EventId0, 1, MaxList) of
                                 false ->
                                     {EventCounter0, MaxList};
                                 {value, {_EventId, C}, ModList} ->
                                     {max(EventCounter0, C), ModList}
                             end,
                         [{EventId0, Counter} | NewList]
                 end, [], AllEvents).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {orddict:new(), ordsets:new(), ordsets:new()}}, new()).

query_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId, 2}]]}],
                    [{EventId, 2}],
                    [{EventId, 1}, {EventId, 2}]}},
    ?assertEqual(sets:new(), query(Set0)),
    ?assertEqual(sets:from_list([<<"1">>]), query(Set1)).

delta_add_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate({add, <<"1">>}, EventId, Set0),
    Set1 = merge({?TYPE, Delta1}, Set0),
    {ok, {?TYPE, {delta, Delta2}}} = delta_mutate({add, <<"1">>}, EventId, Set1),
    Set2 = merge({?TYPE, Delta2}, Set1),
    {ok, {?TYPE, {delta, Delta3}}} = delta_mutate({add, <<"2">>}, EventId, Set2),
    Set3 = merge({?TYPE, Delta3}, Set2),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                          [{EventId, 1}],
                          [{EventId, 1}]}}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                          [{EventId, 1}],
                          [{EventId, 1}]}}, Set1),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 2}]]}],
                          [{EventId, 2}],
                          [{EventId, 2}]}}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]}],
                          [{EventId, 1}, {EventId, 2}],
                          [{EventId, 1}, {EventId, 2}]}}, Set2),
    ?assertEqual({?TYPE, {[{<<"2">>, [[{EventId, 3}]]}],
                          [{EventId, 3}],
                          [{EventId, 3}]}}, {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]},
                           {<<"2">>, [[{EventId, 3}]]}],
                          [{EventId, 1}, {EventId, 2}, {EventId, 3}],
                          [{EventId, 1}, {EventId, 2}, {EventId, 3}]}}, Set3).

add_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"1">>}, EventId, Set0),
    {ok, Set2} = mutate({add, <<"1">>}, EventId, Set1),
    {ok, Set3} = mutate({add, <<"2">>}, EventId, Set2),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                          [{EventId, 1}],
                          [{EventId, 1}]}}, Set1),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]}],
                          [{EventId, 1}, {EventId, 2}],
                          [{EventId, 1}, {EventId, 2}]}}, Set2),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]},
                           {<<"2">>, [[{EventId, 3}]]}],
                          [{EventId, 1}, {EventId, 2}, {EventId, 3}],
                          [{EventId, 1}, {EventId, 2}, {EventId, 3}]}}, Set3).

rmv_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"1">>}, EventId, Set0),
    {ok, Set2} = mutate({add, <<"1">>}, EventId, Set1),
    {error, _} = mutate({rmv, <<"2">>}, EventId, Set2),
    {ok, Set3} = mutate({rmv, <<"1">>}, EventId, Set2),
    ?assertEqual(sets:new(), query(Set3)).

%% @todo
%%add_all_test() ->
%%remove_all_test() ->

merge_idempontent_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}]]}],
                    [{EventId1, 1}],
                    [{EventId1, 1}]}},
    Set2 = {?TYPE, {[], [], [{EventId2, 1}]}},
    Set3 = {?TYPE, {[{<<"2">>, [[{EventId2, 1}]]}],
                    [{EventId2, 1}],
                    [{EventId1, 1}, {EventId2, 1}]}},
    Set4 = merge(Set1, Set1),
    Set5 = merge(Set2, Set2),
    Set6 = merge(Set3, Set3),
    ?assertEqual(Set1, Set4),
    ?assertEqual(Set2, Set5),
    ?assertEqual(Set3, Set6).

merge_commutative_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}]]}],
                    [{EventId1, 1}],
                    [{EventId1, 1}]}},
    Set2 = {?TYPE, {[], [], [{EventId2, 1}]}},
    Set3 = {?TYPE, {[{<<"2">>, [[{EventId2, 1}]]}],
                    [{EventId2, 1}],
                    [{EventId1, 1}, {EventId2, 1}]}},
    Set4 = merge(Set1, Set2),
    Set5 = merge(Set2, Set1),
    Set6 = merge(Set1, Set3),
    Set7 = merge(Set3, Set1),
    Set8 = merge(Set2, Set3),
    Set9 = merge(Set3, Set2),
    Set10 = merge(Set1, merge(Set2, Set3)),
    Set11 = merge(merge(Set2, Set3), Set1),
    ?assertEqual(Set4, Set5),
    ?assertEqual(Set6, Set7),
    ?assertEqual(Set8, Set9),
    ?assertEqual(Set10, Set11).

merge_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}, {EventId2, 1}]]},
                     {<<"2">>, [[{EventId1, 2}, {EventId2, 2}]]}],
                    [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}, {EventId2, 2}],
                    [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}, {EventId2, 2}]}},
    Set2 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}, {EventId2, 3}]]},
                     {<<"2">>, [[{EventId1, 2}, {EventId2, 4}]]}],
                    [{EventId1, 1}, {EventId1, 2}, {EventId2, 3}, {EventId2, 4}],
                    [{EventId1, 1}, {EventId1, 2}, {EventId2, 3}, {EventId2, 4}]}},
    Set3 = merge(Set1, Set2),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId1, 1}, {EventId2, 1}],
                                      [{EventId1, 1}, {EventId2, 3}]]},
                           {<<"2">>, [[{EventId1, 2}, {EventId2, 2}],
                                      [{EventId1, 2}, {EventId2, 4}]]}],
                    [{EventId1, 1}, {EventId1, 2}, {EventId2, 1},
                     {EventId2, 2}, {EventId2, 3}, {EventId2, 4}],
                    [{EventId1, 1}, {EventId1, 2}, {EventId2, 1},
                     {EventId2, 2}, {EventId2, 3}, {EventId2, 4}]}},
                 Set3).

merge_delta_test() ->
    EventId = {<<"object1">>, a},
    Set1 = {?TYPE, {[], [], [{EventId, 1}]}},
    Delta1 = {?TYPE, {delta, {[{<<"1">>, [[{EventId, 2}]]}],
                              [{EventId, 2}],
                              [{EventId, 2}]}}},
    Delta2 = {?TYPE, {delta, {[], [], [{EventId, 3}]}}},
    Set2 = merge(Delta1, Set1),
    Set3 = merge(Set1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 2}]]}],
                          [{EventId, 2}],
                          [{EventId, 1}, {EventId, 2}]}}, Set2),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 2}]]}],
                          [{EventId, 2}],
                          [{EventId, 1}, {EventId, 2}]}}, Set3),
    ?assertEqual({?TYPE, {delta, {[{<<"1">>, [[{EventId, 2}]]}],
                                  [{EventId, 2}],
                                  [{EventId, 2}, {EventId, 3}]}}}, DeltaGroup).

equal_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 = {?TYPE, {[], [], [{EventId1, 1}]}},
    %% Set2 = mutate({add, <<"1">>}, EventId1, Set1),
    Set2 = {?TYPE, {[{<<"1">>, [[{EventId1, 2}]]}],
                    [{EventId1, 2}],
                    [{EventId1, 1}, {EventId1, 2}]}},
    %% Set3 = mutate({add, <<"2">>}, EventId2, Set1),
    Set3 = {?TYPE, {[{<<"2">>, [[{EventId2, 1}]]}],
                    [{EventId2, 1}],
                    [{EventId1, 1}, {EventId2, 1}]}},
    ?assert(equal(Set1, Set1)),
    ?assert(equal(Set2, Set2)),
    ?assert(equal(Set3, Set3)),
    ?assertNot(equal(Set1, Set2)),
    ?assertNot(equal(Set1, Set3)),
    ?assertNot(equal(Set2, Set3)).

is_inflation_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 = {?TYPE, {[], [], [{EventId1, 1}]}},
    %% Set2 = mutate({add, <<"1">>}, EventId1, Set1),
    Set2 = {?TYPE, {[{<<"1">>, [[{EventId1, 2}]]}],
                    [{EventId1, 2}],
                    [{EventId1, 1}, {EventId1, 2}]}},
    %% Set3 = mutate({add, <<"2">>}, EventId2, Set1),
    Set3 = {?TYPE, {[{<<"2">>, [[{EventId2, 1}]]}],
                    [{EventId2, 1}],
                    [{EventId1, 1}, {EventId2, 1}]}},
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
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 = {?TYPE, {[], [], [{EventId1, 1}]}},
    %% Set2 = mutate({add, <<"1">>}, EventId1, Set1),
    Set2 = {?TYPE, {[{<<"1">>, [[{EventId1, 2}]]}],
                    [{EventId1, 2}],
                    [{EventId1, 1}, {EventId1, 2}]}},
    %% Set3 = mutate({add, <<"2">>}, EventId2, Set1),
    Set3 = {?TYPE, {[{<<"2">>, [[{EventId2, 1}]]}],
                    [{EventId2, 1}],
                    [{EventId1, 1}, {EventId2, 1}]}},
    ?assertNot(is_strict_inflation(Set1, Set1)),
    ?assert(is_strict_inflation(Set1, Set2)),
    ?assertNot(is_strict_inflation(Set2, Set1)),
    ?assert(is_strict_inflation(Set1, Set3)),
    ?assertNot(is_strict_inflation(Set2, Set3)),
    ?assertNot(is_strict_inflation(Set3, Set2)).

%% @todo
%%join_decomposition_test() ->

-endif.
