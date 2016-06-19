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

-module(state_oorset_ps_v3).
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

-export_type([state_oorset_ps_v3/0,
              delta_state_oorset_ps_v3/0,
              state_oorset_ps_v3_op/0]).

-opaque state_oorset_ps_v3() :: {?TYPE, payload()}.
-opaque delta_state_oorset_ps_v3() :: {?TYPE, {delta, payload()}}.
-type delta_or_state() :: state_oorset_ps_v3() | delta_state_oorset_ps_v3().
-type payload() :: {ps_data_store(), ps_filtered_out_events(), ps_all_events()}.
-type element() :: term().
-type state_oorset_ps_v3_op() :: {add, element()}
                               | {add_all, [element()]}
                               | {rmv, element()}
                               | {rmv_all, [element()]}.

%% Provenanve semiring related.
-export_type([ps_event_id/0]).

-type ps_object_id() :: binary().
-type ps_replica_id() :: term().
-type ps_event_id() :: {ps_object_id(), ps_replica_id()}.
-type ps_event() :: {ps_event_id(), pos_integer()}.
-type ps_dot() :: ordsets:ordsets(ps_event()).
-type ps_provenance() :: ordsets:ordsets(ps_dot()).
-type ps_data_store() :: orddict:orddict(element(), ps_provenance()).
-type ps_filtered_out_events() :: ps_dot().
-type ps_all_events() :: ps_dot()
                       | {vclock, [ps_event()]}.

%% @doc Create a new, empty `state_oorset_ps_v3()'
-spec new() -> state_oorset_ps_v3().
new() ->
    {?TYPE, {orddict:new(), ordsets:new(), {vclock, []}}}.

%% @doc Create a new, empty `state_oorset_ps_v3()'
-spec new([term()]) -> state_oorset_ps_v3().
new([]) ->
    new().

%% @doc Mutate a `state_oorset_ps_v3()'.
-spec mutate(state_oorset_ps_v3_op(), type:id(), state_oorset_ps_v3()) ->
    {ok, state_oorset_ps_v3()} | {error, {precondition, {not_present, [element()]}}}.
mutate(Op, Actor, {?TYPE, _ORSet}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_oorset_ps_v3()'.
%%      The first argument can be:
%%          - `{add, element()}'
%%          - `{add_all, [element()]}'
%%          - `{rmv, element()}'
%%      The second argument is the event id ({object_id, replica_id}).
%%      The third argument is the `state_oorset_ps_v3()' to be inflated.
-spec delta_mutate(state_oorset_ps_v3_op(), type:id(), state_oorset_ps_v3()) ->
    {ok, delta_state_oorset_ps_v3()} | {error, {precondition, {not_present, element()}}}.
%% Adds a single element to `state_oorset_ps_v3()'.
%% Delta: {[{Elem, {{NewEvent}}}], [], [NewEvent]}
delta_mutate({add, Elem},
             EventId,
             {?TYPE, {_DataStore, _FilteredOutEvents, AllEvents}}) ->
    {vclock, [NewEvent|_Rest]} = get_next_event(EventId, AllEvents),
    NewDot = ordsets:add_element(NewEvent, ordsets:new()),
    NewProvenance = ordsets:add_element(NewDot, ordsets:new()),
    DeltaDataStore = orddict:store(Elem, NewProvenance, orddict:new()),
    {ok, {?TYPE, {delta, {DeltaDataStore, ordsets:new(), NewDot}}}};

%% Adds a list of elemenets to `state_oorset_ps_v3()'.
delta_mutate({add_all, Elems},
             EventId,
             {?TYPE, {_DataStore, _FilteredOutEvents, AllEvents}}) ->
    {AccDelta, _AccAllEvents} =
        lists:foldl(
          fun(Elem, {AccDelta0, AccAllEvents0}) ->
                  {vclock, [NewEvent|Rest]} = get_next_event(EventId, AccAllEvents0),
                  NewDot = ordsets:add_element(NewEvent, ordsets:new()),
                  NewProvenance = ordsets:add_element(NewDot, ordsets:new()),
                  DeltaDataStore = orddict:store(Elem, NewProvenance, orddict:new()),
                  {merge(AccDelta0,
                         {?TYPE, {delta, {DeltaDataStore, ordsets:new(), NewDot}}}),
                   {vclock, [NewEvent|Rest]}}
          end,
          {{?TYPE, {delta, {orddict:new(), ordsets:new(), ordsets:new()}}},
           AllEvents},
          Elems),
    {ok, AccDelta};

%% Removes a single element in `state_oorset_ps_v3()'.
%% Delta: {[], (ElemEvents - OwnedElemEvents), ElemEvents}
%% OwnedElemEvents = the object id is equal to the one of the `state_oorset_ps_v3()'
delta_mutate({rmv, Elem},
             {ObjectId, _ReplicaId}=_EventId,
             {?TYPE, {DataStore, _FilteredOutEvents, _AllEvents}}) ->
    case orddict:find(Elem, DataStore) of
        {ok, Provenance} ->
            ElemEvents = get_events_from_provenance(Provenance),
            ElemFilteredOutEvents =
                ordsets:filter(fun({{ObjectId0, _ReplicaId0}, _Counter0}) ->
                                       ObjectId0 =/= ObjectId
                               end, ElemEvents),
            {ok, {?TYPE, {delta, {orddict:new(),
                                  ElemFilteredOutEvents,
                                  ElemEvents}}}};
        error ->
            {error, {precondition, {not_present, Elem}}}
    end;

%% @todo
%% Removes a list of elemenets in `state_oorset_ps_v3()'.
delta_mutate({rmv_all, _Elems}, _Actor, {?TYPE, ORSet}) ->
    {ok, {?TYPE, {delta, ORSet}}}.

%% @doc Returns the value of the `state_oorset_ps_v3()'.
%% This value is a set with all the keys (elements) in the data store.
-spec query(state_oorset_ps_v3()) -> sets:set(element()).
query({?TYPE, {DataStore, _FilteredOutEvents, _AllEvents}}) ->
    Result = orddict:fetch_keys(DataStore),
    sets:from_list(Result).

%% @doc Merge two `state_oorset_ps_v3()'.
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

%% @doc Equality for `state_oorset_ps_v3()'.
-spec equal(state_oorset_ps_v3(), state_oorset_ps_v3()) -> boolean().
equal({?TYPE, {DataStoreA, FilteredOutEventsA, {vclock, AllEventsA}}},
      {?TYPE, {DataStoreB, FilteredOutEventsB, {vclock, AllEventsB}}}) ->
    DataStoreA == DataStoreB andalso
        FilteredOutEventsA == FilteredOutEventsB andalso
        lists:sort(AllEventsA) == lists:sort(AllEventsB).

%% @doc Given two `state_oorset_ps_v3()', check if the second is and inflation of the first.
%% The inflation will be checked by the is_lattice_inflation() in the common library.
-spec is_inflation(state_oorset_ps_v3(), state_oorset_ps_v3()) -> boolean().
is_inflation({?TYPE, ORSet1}, {?TYPE, ORSet2}) ->
    is_lattice_inflation_oorset_ps(ORSet1, ORSet2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_oorset_ps_v3(), state_oorset_ps_v3()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @todo
%% @doc Join decomposition for `state_oorset_ps_v3()'.
-spec join_decomposition(state_oorset_ps_v3()) -> [state_oorset_ps_v3()].
join_decomposition({?TYPE, ORSet}) ->
    [ORSet].

%% @private
-spec get_next_event(ps_event_id(), ps_all_events()) -> ps_all_events().
get_next_event(EventId, {vclock, AllEvents}) ->
    {NextCounter, NewAllEvents} =
        case lists:keytake(EventId, 1, AllEvents) of
            false ->
                {1, AllEvents};
            {value, {_EId, Counter}, Rest} ->
                {Counter + 1, Rest}
        end,
    {vclock, [{EventId, NextCounter}|NewAllEvents]}.

%% @private
-spec get_counter(ps_event_id(), ps_all_events()) -> non_neg_integer().
get_counter(EventId, {vclock, AllEvents}) ->
    case lists:keyfind(EventId, 1, AllEvents) of
        {_, Counter} ->
            Counter;
        false ->
            0
    end.

%% @private
-spec get_events_from_provenance(ps_provenance()) -> ps_dot().
get_events_from_provenance(Provenance) ->
    ordsets:fold(fun(Dot0, Acc0) ->
                         ordsets:union(Acc0, Dot0)
                 end, ordsets:new(), Provenance).

%% @private
-spec is_lattice_inflation_oorset_ps(payload(), payload()) -> boolean().
is_lattice_inflation_oorset_ps(Payload, Payload) ->
    true;
is_lattice_inflation_oorset_ps({DataStoreA, FilteredOutEventsA, AllEventsA},
                               {DataStoreB, _FilteredOutEventsB, AllEventsB}) ->
    DataStoreEventsA = orddict:fold(
                         fun(_Elem, Provenance, DataStoreEventsA0) ->
                                 ordsets:union(
                                   DataStoreEventsA0,
                                   get_events_from_provenance(Provenance))
                         end, ordsets:new(), DataStoreA),
    DataStoreEventsB = orddict:fold(
                         fun(_Elem, Provenance, DataStoreEventsB0) ->
                                 ordsets:union(
                                   DataStoreEventsB0,
                                   get_events_from_provenance(Provenance))
                         end, ordsets:new(), DataStoreB),
    RemovedA = subtract_all_events(AllEventsA, ordsets:union(DataStoreEventsA,
                                                             FilteredOutEventsA)),
    is_all_events_inflation(AllEventsA, AllEventsB) andalso
        (ordsets:intersection(RemovedA, DataStoreEventsB) == []).

%% @private
-spec subtract_all_events(ps_all_events(), ps_dot()) -> ps_dot().
subtract_all_events({vclock, AllEvents}, Events) ->
    AllEventsSet = lists:foldl(
                     fun({EventId0, Counter0}, AllEventsSet0) ->
                             get_ordsets_from_vclock({EventId0, Counter0},
                                                     AllEventsSet0)
                     end, ordsets:new(), AllEvents),
    ordsets:subtract(AllEventsSet, Events).

%% @private
get_ordsets_from_vclock({EventId, Counter}, Ordset) ->
    case Counter of
        0 ->
            Ordset;
        _ ->
            Ordset1 = ordsets:add_element({EventId, Counter}, Ordset),
            get_ordsets_from_vclock({EventId, Counter - 1}, Ordset1)
    end.

%% @private
-spec is_all_events_inflation(ps_all_events(), ps_all_events()) -> boolean().
is_all_events_inflation({vclock, AllEventsA}, {vclock, AllEventsB}) ->
    is_all_events_inflation_private(AllEventsA, AllEventsB).

%% @private
is_all_events_inflation_private([], _) ->
    % all AllEvents are the inflation of the empty AllEvents
    true;
is_all_events_inflation_private(AllEventsA, AllEventsB) ->
    [{EventIdA, CounterA}|RestA] = AllEventsA,
    case lists:keyfind(EventIdA, 1, AllEventsB) of
        false ->
            false;
        {_, CounterB} ->
            case CounterA == CounterB of
                true ->
                    is_all_events_inflation_private(RestA, AllEventsB);
                false ->
                    CounterA < CounterB
            end
    end.

%% @private
-spec join_oorset_ps(payload(), payload()) -> payload().
join_oorset_ps({DataStoreA, FilteredOutEventsA, AllEventsA}=FstORSet,
               {DataStoreB, FilteredOutEventsB, AllEventsB}=SndORSet) ->
    DataStoreEventsA = orddict:fold(
                         fun(_Elem, Provenance, DataStoreEventsA0) ->
                                 ordsets:union(
                                   DataStoreEventsA0,
                                   get_events_from_provenance(Provenance))
                         end, ordsets:new(), DataStoreA),
    DataStoreEventsB = orddict:fold(
                         fun(_Elem, Provenance, DataStoreEventsB0) ->
                                 ordsets:union(
                                   DataStoreEventsB0,
                                   get_events_from_provenance(Provenance))
                         end, ordsets:new(), DataStoreB),
    UnionElems = ordsets:union(ordsets:from_list(orddict:fetch_keys(DataStoreA)),
                               ordsets:from_list(orddict:fetch_keys(DataStoreB))),
    {JoinedDataStore, ElemEvents} =
        ordsets:fold(
          fun(Elem, {JoinedDataStore0, ElemEvents0}) ->
                  {ok, ProvenanceA} = get_provenance(Elem, FstORSet),
                  {ok, ProvenanceB} = get_provenance(Elem, SndORSet),
                  JoinedProvenance =
                      join_provenance(ProvenanceA,
                                      DataStoreEventsA, FilteredOutEventsA, AllEventsA,
                                      ProvenanceB,
                                      DataStoreEventsB, FilteredOutEventsB, AllEventsB),
                  case ordsets:size(JoinedProvenance) of
                      0 ->
                          {JoinedDataStore0, ElemEvents0};
                      _ ->
                          {orddict:store(Elem, JoinedProvenance, JoinedDataStore0),
                           ordsets:union(ElemEvents0,
                                         get_events_from_provenance(JoinedProvenance))}
                  end
          end, {orddict:new(), ordsets:new()}, UnionElems),
    {JoinedDataStore,
     ordsets:subtract(ordsets:union(FilteredOutEventsA, FilteredOutEventsB),
                      ElemEvents),
     join_all_events(AllEventsA, AllEventsB)}.

%% @private
-spec join_all_events(ps_all_events(), ps_all_events()) -> ps_all_events().
join_all_events({vclock, AllEventsA}, {vclock, AllEventsB}) ->
    {JoinedAllEvents, RestB} =
        lists:foldl(
          fun({EventIdA, CounterA}, {JoinedAllEvents0, RestB0}) ->
                  case lists:keytake(EventIdA, 1, RestB0) of
                      false ->
                          {[{EventIdA, CounterA}|JoinedAllEvents0], RestB0};
                      {value, {_EventIdB, CounterB}, RestB1} ->
                          {[{EventIdA, max(CounterA, CounterB)}|JoinedAllEvents0],
                           RestB1}
                  end
          end, {[], AllEventsB}, AllEventsA),
    {vclock, JoinedAllEvents ++ RestB};
join_all_events({vclock, AllEventsA}, AllEventsB) ->
    JoinedAllEvents =
        ordsets:fold(
          fun({EventId0, Counter0}=Event0, AllEventsA0) ->
                  case lists:keytake(EventId0, 1, AllEventsA0) of
                      false ->
                          [Event0|AllEventsA0];
                      {value, {_EventIdA, CounterA}, RestAllEventsA0} ->
                          [{EventId0, max(CounterA, Counter0)}|RestAllEventsA0]
                  end
          end, AllEventsA, AllEventsB),
    {vclock, JoinedAllEvents};
join_all_events(AllEventsA, {vclock, AllEventsB}) ->
    JoinedAllEvents =
        ordsets:fold(
          fun({EventId0, Counter0}=Event0, AllEventsB0) ->
                  case lists:keytake(EventId0, 1, AllEventsB0) of
                      false ->
                          [Event0|AllEventsB0];
                      {value, {_EventIdB, CounterB}, RestAllEventsB0} ->
                          [{EventId0, max(CounterB, Counter0)}|RestAllEventsB0]
                  end
          end, AllEventsB, AllEventsA),
    {vclock, JoinedAllEvents};
join_all_events(AllEventsA, AllEventsB) ->
    ordsets:union(AllEventsA, AllEventsB).

%% @private
-spec join_provenance(ps_provenance(),
                      ps_dot(), ps_filtered_out_events(), ps_all_events(),
                      ps_provenance(),
                      ps_dot(), ps_filtered_out_events(), ps_all_events()) ->
          ps_provenance().
join_provenance(ProvenanceA, DataStoreEventsA, FilteredOutEventsA, AllEventsA,
                ProvenanceB, DataStoreEventsB, FilteredOutEventsB, AllEventsB) ->
    JoinedProvenance0 = ordsets:intersection(ProvenanceA, ProvenanceB),
    JoinedProvenance1 =
        ordsets:fold(fun(Dot0, Acc) ->
                             case is_valid_dot(Dot0, AllEventsB,
                                               DataStoreEventsB, FilteredOutEventsB) of
                                 false ->
                                     Acc;
                                 true ->
                                     ordsets:add_element(Dot0, Acc)
                             end
                     end, JoinedProvenance0, ProvenanceA),
    ordsets:fold(fun(Dot0, Acc) ->
                         case is_valid_dot(Dot0, AllEventsA,
                                           DataStoreEventsA, FilteredOutEventsA) of
                             false ->
                                 Acc;
                             true ->
                                 ordsets:add_element(Dot0, Acc)
                         end
                 end, JoinedProvenance1, ProvenanceB).

%% @private
-spec is_valid_dot(ps_dot(), ps_all_events(),
                   ps_dot(), ps_filtered_out_events()) -> boolean().
is_valid_dot(Dot, {vclock, AllEventsOther}, DataStoreEventsOther, FilteredOutOther) ->
    ValidOtherSet = ordsets:union(DataStoreEventsOther, FilteredOutOther),
    ordsets:fold(
      fun({EventId0, Counter0}=Event0, IsValid0) ->
              (ordsets:is_element(Event0, ValidOtherSet) orelse
               (Counter0 > get_counter(EventId0, {vclock, AllEventsOther}))) andalso
                  IsValid0
      end, true, Dot);
is_valid_dot(Dot, AllEventsOther, DataStoreEventsOther, FilteredOutOther) ->
    ValidOtherSet = ordsets:union(DataStoreEventsOther, FilteredOutOther),
    ordsets:fold(
      fun(Event0, IsValid0) ->
              (ordsets:is_element(Event0, ValidOtherSet) orelse
               (not ordsets:is_element(Event0, AllEventsOther))) andalso IsValid0
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

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {orddict:new(), ordsets:new(), {vclock, []}}}, new()).

query_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                    [],
                    {vclock, [{EventId, 2}]}}},
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
                          [],
                          [{EventId, 1}]}}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                          [],
                          {vclock, [{EventId, 1}]}}}, Set1),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 2}]]}],
                          [],
                          [{EventId, 2}]}}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]}],
                          [],
                          {vclock, [{EventId, 2}]}}}, Set2),
    ?assertEqual({?TYPE, {[{<<"2">>, [[{EventId, 3}]]}],
                          [],
                          [{EventId, 3}]}}, {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]},
                           {<<"2">>, [[{EventId, 3}]]}],
                          [],
                          {vclock, [{EventId, 3}]}}}, Set3).

add_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"1">>}, EventId, Set0),
    {ok, Set2} = mutate({add, <<"1">>}, EventId, Set1),
    {ok, Set3} = mutate({add, <<"2">>}, EventId, Set2),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                          [],
                          {vclock, [{EventId, 1}]}}}, Set1),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]}],
                          [],
                          {vclock, [{EventId, 2}]}}}, Set2),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]},
                           {<<"2">>, [[{EventId, 3}]]}],
                          [],
                          {vclock, [{EventId, 3}]}}}, Set3).

rmv_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"1">>}, EventId, Set0),
    {ok, Set2} = mutate({add, <<"1">>}, EventId, Set1),
    {error, _} = mutate({rmv, <<"2">>}, EventId, Set2),
    {ok, Set3} = mutate({rmv, <<"1">>}, EventId, Set2),
    ?assertEqual(sets:new(), query(Set3)).

add_all_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, Set1} = mutate({add_all, []}, EventId, Set0),
    {ok, Set2} = mutate({add_all, [<<"a">>, <<"b">>]}, EventId, Set0),
    {ok, Set3} = mutate({add_all, [<<"b">>, <<"c">>]}, EventId, Set2),
    ?assertEqual(sets:new(), query(Set1)),
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>]), query(Set2)),
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>, <<"c">>]), query(Set3)).

%% @todo
%%remove_all_test() ->

merge_idempontent_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 = {?TYPE, {[], [], {vclock, [{EventId1, 1}]}}},
    Set2 = {?TYPE, {[{<<"2">>, [[{EventId2, 1}]]}],
                    [],
                    {vclock, [{EventId2, 1}]}}},
    Set3 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}]]}],
                    [],
                    {vclock, [{EventId1, 1}, {EventId2, 1}]}}},
    Set4 = merge(Set1, Set1),
    Set5 = merge(Set2, Set2),
    Set6 = merge(Set3, Set3),
    ?assert(equal(Set1, Set4)),
    ?assert(equal(Set2, Set5)),
    ?assert(equal(Set3, Set6)).

merge_commutative_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 = {?TYPE, {[],
                    [],
                    {vclock, [{EventId1, 1}]}}},
    Set2 = {?TYPE, {[{<<"2">>, [[{EventId2, 1}]]}],
                    [],
                    {vclock, [{EventId2, 1}]}}},
    Set3 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}]]}],
                    [],
                    {vclock, [{EventId1, 1}, {EventId2, 1}]}}},
    Set4 = merge(Set1, Set2),
    Set5 = merge(Set2, Set1),
    Set6 = merge(Set1, Set3),
    Set7 = merge(Set3, Set1),
    Set8 = merge(Set2, Set3),
    Set9 = merge(Set3, Set2),
    Set10 = merge(Set1, merge(Set2, Set3)),
    Set1_2 = {?TYPE, {[{<<"2">>, [[{EventId2, 1}]]}],
                      [],
                      {vclock, [{EventId1, 1}, {EventId2, 1}]}}},
    Set1_3 = {?TYPE, {[],
                      [],
                      {vclock, [{EventId1, 1}, {EventId2, 1}]}}},
    Set2_3 = Set3,
    ?assert(equal(Set1_2, Set4)),
    ?assert(equal(Set1_2, Set5)),
    ?assert(equal(Set1_3, Set6)),
    ?assert(equal(Set1_3, Set7)),
    ?assert(equal(Set2_3, Set8)),
    ?assert(equal(Set2_3, Set9)),
    ?assert(equal(Set1_3, Set10)).

merge_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}, {EventId2, 1}]]},
                     {<<"2">>, [[{EventId1, 2}, {EventId2, 2}]]}],
                    [],
                    {vclock, [{EventId1, 2}, {EventId2, 2}]}}},
    Delta1 = {?TYPE, {delta, {[{<<"1">>, [[{EventId1, 1}, {EventId2, 3}]]},
                               {<<"2">>, [[{EventId1, 2}, {EventId2, 4}]]}],
                              [],
                              [{EventId1, 1}, {EventId1, 2},
                               {EventId2, 3}, {EventId2, 4}]}}},
    Set2 = merge(Set1, Delta1),
    ?assert(equal({?TYPE, {[{<<"1">>, [[{EventId1, 1}, {EventId2, 1}],
                                       [{EventId1, 1}, {EventId2, 3}]]},
                            {<<"2">>, [[{EventId1, 2}, {EventId2, 2}],
                                       [{EventId1, 2}, {EventId2, 4}]]}],
                           [],
                           {vclock, [{EventId1, 2}, {EventId2, 4}]}}},
                  Set2)).

merge_delta_test() ->
    EventId = {<<"object1">>, a},
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                    [],
                    {vclock, [{EventId, 1}]}}},
    Delta1 = {?TYPE, {delta, {[],
                              [],
                              [{EventId, 1}]}}},
    Delta2 = {?TYPE, {delta, {[{<<"2">>, [[{EventId, 2}]]}],
                              [],
                              [{EventId, 2}]}}},
    Set2 = merge(Delta1, Set1),
    Set3 = merge(Set1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, {[],
                          [],
                          {vclock, [{EventId, 1}]}}},
                 Set2),
    ?assertEqual({?TYPE, {[],
                          [],
                          {vclock, [{EventId, 1}]}}},
                 Set3),
    ?assertEqual({?TYPE, {delta, {[{<<"2">>, [[{EventId, 2}]]}],
                                  [],
                                  [{EventId, 1}, {EventId, 2}]}}},
                 DeltaGroup).

equal_test() ->
    EventId1 = {<<"object1">>, a},
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}]]}],
                    [],
                    {vclock, [{EventId1, 1}]}}},
    Set2 = {?TYPE, {[],
                    [],
                    {vclock, [{EventId1, 1}]}}},
    Set3 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}]]}],
                    [],
                    {vclock, [{EventId1, 2}]}}},
    ?assert(equal(Set1, Set1)),
    ?assert(equal(Set2, Set2)),
    ?assert(equal(Set3, Set3)),
    ?assertNot(equal(Set1, Set2)),
    ?assertNot(equal(Set1, Set3)),
    ?assertNot(equal(Set2, Set3)).

is_inflation_test() ->
    EventId1 = {<<"object1">>, a},
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}]]}],
                    [],
                    {vclock, [{EventId1, 1}]}}},
    Set2 = {?TYPE, {[],
                    [],
                    {vclock, [{EventId1, 1}]}}},
    Set3 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}]]}],
                    [],
                    {vclock, [{EventId1, 2}]}}},
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
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}]]}],
                    [],
                    {vclock, [{EventId1, 1}]}}},
    Set2 = {?TYPE, {[],
                    [],
                    {vclock, [{EventId1, 1}]}}},
    Set3 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}]]}],
                    [],
                    {vclock, [{EventId1, 2}]}}},
    ?assertNot(is_strict_inflation(Set1, Set1)),
    ?assert(is_strict_inflation(Set1, Set2)),
    ?assertNot(is_strict_inflation(Set2, Set1)),
    ?assert(is_strict_inflation(Set1, Set3)),
    ?assertNot(is_strict_inflation(Set2, Set3)),
    ?assertNot(is_strict_inflation(Set3, Set2)).

%% @todo
%%join_decomposition_test() ->

-endif.
