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

%% @doc Add-Wins Set CRDT with the provenance semiring:
%%      add-wins set without tombstones.

-module(state_awset_ps).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

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

-export([subtract_all_events/2,
         add_elem_with_dot/3,
         get_events_from_provenance/1,
         cross_provenance/2,
         subtract_removed/2]).

-export_type([state_awset_ps/0,
              state_awset_ps_op/0]).

-opaque state_awset_ps() :: {?TYPE, payload()}.
-type payload() :: {ps_data_store(), ps_filtered_out_events(), ps_all_events()}.
-type element() :: term().
-type state_awset_ps_op() :: {add, element()}
                           | {add_all, [element()]}
                           | {rmv, element()}
                           | {rmv_all, [element()]}.

%% Provenance semiring related.
-export_type([ps_event_id/0]).

-type ps_object_id() :: binary().
-type ps_replica_id() :: term().
-type ps_event_id() :: {ps_object_id(), ps_replica_id()}.
%% @doc A Event represents a single update for the addition of the set.
-type ps_event() :: {ps_event_id(), pos_integer()}.
%% @doc Generally a Dot contains a single Event, but it could have multiple Events
%% after set-related binary operations (such as intersection()/product()).
%% This type can also be used to represent a set of Events.
-type ps_dot() :: ordsets:ordset(ps_event()).
%% @doc A set of Dots.
-type ps_provenance() :: ordsets:ordset(ps_dot()).
%% @doc A DataStore for 'Element -> Provenance' pairs.
-type ps_data_store_elem() :: orddict:orddict(element(), ps_provenance()).
%% @doc A DataStore for 'Event -> a set of Elements (which contains the Event
%% in the Provenance)' pairs.
%% This is for making merge()/is_inflation() faster.
-type ps_data_store_event() :: orddict:orddict(ps_event(),
                                               ordsets:ordset(element())).
%% @doc DataStore section.
-type ps_data_store() :: {ps_data_store_elem(), ps_data_store_event()}.
%% @doc FilteredOutEvents section.
%% When elements are filtered-out by operations, every events in the provenance
%% will be moved to this section (filtered-out elements of the filter() or
%% uncommon elements of the intersection()).
-type ps_filtered_out_events() :: ps_dot().
%% @doc AllEvents section.
%% This section stores the causal context of the set.
-type ps_all_events() :: ps_dot()                %% for delta state
                       | {vclock, [ps_event()]}. %% for full state

%% @doc Create a new, empty `state_awset_ps()'
-spec new() -> state_awset_ps().
new() ->
    {?TYPE, {{orddict:new(), orddict:new()}, ordsets:new(), {vclock, []}}}.

%% @doc Create a new, empty `state_awset_ps()'
-spec new([term()]) -> state_awset_ps().
new([]) ->
    new().

%% @doc Mutate a `state_awset_ps()'.
-spec mutate(state_awset_ps_op(), type:id(), state_awset_ps()) ->
    {ok, state_awset_ps()}.
mutate(Op, Actor, {?TYPE, _AWSet}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_awset_ps()'.
%% The first argument can be:
%%     - `{add, element()}'
%%     - `{add_all, [element()]}'
%%     - `{rmv, element()}'
%%     - `{rmv_all, [element()]}'
%% The second argument is the event id ({object_id, replica_id}).
%% The third argument is the `state_awset_ps()' to be inflated.
-spec delta_mutate(state_awset_ps_op(), type:id(), state_awset_ps()) ->
    {ok, state_awset_ps()}.
%% Adds a single element to `state_awset_ps()'.
%% Delta: {{[{Elem, {{NewEvent}}}], [{NewEvent, Elem}]}, [], [NewEvent]}
delta_mutate({add, Elem},
             EventId,
             {?TYPE, {_DataStore, _FilteredOutEvents, AllEvents}}) ->
    {vclock, [NewEvent|_Rest]} = get_next_event(EventId, AllEvents),
    {ok, {?TYPE, {add_elem_with_dot(
                    Elem,
                    ordsets:add_element(NewEvent, ordsets:new()),
                    {orddict:new(), orddict:new()}),
                  ordsets:new(),
                  ordsets:add_element(NewEvent, ordsets:new())}}};

%% Adds a list of elements to `state_awset_ps()'.
delta_mutate({add_all, Elems},
             EventId,
             {?TYPE, {_DataStore, _FilteredOutEvents, AllEvents}}) ->
    {{AccDeltaElemDataStore, AccDeltaEventDataStore}, _AccAllEvents} =
        lists:foldl(
          fun(Elem,
              {{AccDeltaElemDataStore0, AccDeltaEventDataStore0}, AccAllEvents0}) ->
                  {vclock, [NewEvent|Rest]} = get_next_event(EventId, AccAllEvents0),
                  {add_elem_with_dot(
                     Elem,
                     ordsets:add_element(NewEvent, ordsets:new()),
                     {AccDeltaElemDataStore0, AccDeltaEventDataStore0}),
                   {vclock, [NewEvent|Rest]}}
          end, {{orddict:new(), orddict:new()}, AllEvents}, Elems),
    {ok, {?TYPE, {{AccDeltaElemDataStore, AccDeltaEventDataStore},
                  ordsets:new(),
                  ordsets:from_list(
                    orddict:fetch_keys(AccDeltaEventDataStore))}}};

%% Removes a single element in `state_awset_ps()'.
%% Delta: {[], [], ElemEvents}
delta_mutate({rmv, Elem},
             _EventId,
             {?TYPE, {{ElemDataStore, _EventDataStore},
                      _FilteredOutEvents,
                      _AllEvents}}) ->
    ElemEvents = case orddict:find(Elem, ElemDataStore) of
        {ok, Provenance} ->
            get_events_from_provenance(Provenance);
        error ->
            ordsets:new()
    end,
    {ok, {?TYPE, {{orddict:new(), orddict:new()},
                  ordsets:new(),
                  ElemEvents}}};

%% Removes a list of elements in `state_awset_ps()'.
delta_mutate({rmv_all, Elems},
             _EventId,
             {?TYPE, {{ElemDataStore, _EventDataStore},
                      _FilteredOutEvents,
                      _AllEvents}}) ->
    ElemEventsAll = remove_all_private(Elems, ElemDataStore, ordsets:new()),
    {ok, {?TYPE, {{orddict:new(), orddict:new()},
                  ordsets:new(),
                  ElemEventsAll}}}.

%% @doc Returns the value of the `state_awset_ps()'.
%% This value is a set with all the keys (elements) in the ElemDataStore.
-spec query(state_awset_ps()) -> sets:set(element()).
query({?TYPE, {{ElemDataStore, _EventDataStore}, _FilteredOutEvents, _AllEvents}}) ->
    Result = orddict:fetch_keys(ElemDataStore),
    sets:from_list(Result).

%% @doc Merge two `state_awset_ps()'.
%% Merging will be handled by the join_awset_ps().
-spec merge(state_awset_ps(), state_awset_ps()) -> state_awset_ps().
merge({?TYPE, AWSet1}, {?TYPE, AWSet2}) ->
    AWSet = join_awset_ps(AWSet1, AWSet2),
    {?TYPE, AWSet}.

%% @doc Equality for `state_awset_ps()'.
-spec equal(state_awset_ps(), state_awset_ps()) -> boolean().
equal({?TYPE, {DataStoreA, FilteredOutEventsA, {vclock, AllEventsA}}},
      {?TYPE, {DataStoreB, FilteredOutEventsB, {vclock, AllEventsB}}}) ->
    DataStoreA == DataStoreB andalso
        FilteredOutEventsA == FilteredOutEventsB andalso
        lists:sort(AllEventsA) == lists:sort(AllEventsB).

%% @doc Check if an AWSet is bottom.
-spec is_bottom(state_awset_ps()) -> boolean().
is_bottom({?TYPE, _AWSet}=FullAWSet) ->
    FullAWSet == new().

%% @doc Given two `state_awset_ps()', check if the second is and inflation of the first.
%% The inflation will be checked by the is_lattice_inflation() in the common library.
-spec is_inflation(state_awset_ps(), state_awset_ps()) -> boolean().
is_inflation({?TYPE, AWSet1}, {?TYPE, AWSet2}) ->
    is_inflation_awset_ps(AWSet1, AWSet2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_awset_ps(), state_awset_ps()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_awset_ps(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=A, B) ->
    state_type:irreducible_is_strict_inflation(A, B).

-spec digest(state_awset_ps()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_awset_ps()'.
%% @todo
-spec join_decomposition(state_awset_ps()) -> [state_awset_ps()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

%% @doc Delta calculation for `state_awset_ps()'.
-spec delta(state_awset_ps(), state_type:digest()) -> state_awset_ps().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_awset_ps()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_awset_ps().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.

%% @private
remove_all_private([], _ElemDataStore, ResultEvents) ->
    ResultEvents;
remove_all_private([Elem|RestElems], ElemDataStore, ResultEvents) ->
    ElemEvents = case orddict:find(Elem, ElemDataStore) of
        {ok, Provenance} ->
            get_events_from_provenance(Provenance);
        error ->
            ordsets:new()
    end,
    remove_all_private(RestElems,
                       ElemDataStore,
                       ordsets:union(ResultEvents, ElemEvents)).

%% @doc Subtract a set of events from AllEvents.
-spec subtract_all_events(ps_all_events(), ps_dot()) -> ps_dot().
subtract_all_events({vclock, AllEvents}, Events) ->
    %% Convert encoded AllEvents to a set of Events.
    AllEventsSet = lists:foldl(
                     fun({EventId0, Counter0}, AllEventsSet0) ->
                             get_ordsets_from_vclock({EventId0, Counter0},
                                                     AllEventsSet0)
                     end, ordsets:new(), AllEvents),
    ordsets:subtract(AllEventsSet, Events);
subtract_all_events(AllEvents, Events) ->
    ordsets:subtract(AllEvents, Events).

%% @doc Add a pair of an Elem and a Dot to DataStore.
-spec add_elem_with_dot(element(), ps_dot(), ps_data_store()) -> ps_data_store().
add_elem_with_dot(Elem, Dot, {ElemDataStore, EventDataStore}) ->
    %% Add a Dot to Elem's provenance in ElemDataStore.
    NewElemDataStore = add_elem_with_dot_private(Elem, Dot, ElemDataStore),
    %% Add {Event in Dot, Elem} pairs in EventDataStore.
    NewEventDataStore =
        ordsets:fold(
          fun(Event, NewEventDataStore0) ->
                  add_elem_with_dot_private(Event, Elem, NewEventDataStore0)
          end, EventDataStore, Dot),
    {NewElemDataStore, NewEventDataStore}.

%% @doc Return all events in a provenance.
-spec get_events_from_provenance(ps_provenance()) -> ps_dot().
get_events_from_provenance(Provenance) ->
    ordsets:fold(
      fun(Dot0, Acc0) ->
              ordsets:union(Acc0, Dot0)
      end, ordsets:new(), Provenance).

%% @doc Calculate the cross product provenance of two provenances, which
%% is a set of cross product dots in both provenances.
%% A cross product of two dots is an union set of two dots.
-spec cross_provenance(ps_provenance(), ps_provenance()) -> ps_provenance().
cross_provenance(ProvenanceL, ProvenanceR) ->
    ordsets:fold(
      fun(DotL, AccCrossProvenance0) ->
              ordsets:fold(
                fun(DotR, AccCrossProvenance1) ->
                        CrossDot = ordsets:union(DotL, DotR),
                        ordsets:add_element(CrossDot, AccCrossProvenance1)
                end, AccCrossProvenance0, ProvenanceR)
      end, ordsets:new(), ProvenanceL).

%% @doc Filter out the dots in Provenance which contains invalid (typically,
%% removed) events.
-spec subtract_removed(ps_provenance(), ps_dot()) -> ps_provenance().
subtract_removed(Provenance, ValidEvents) ->
    ordsets:fold(
      fun(Dot, AccNewProvenance0) ->
              case ordsets:intersection(Dot, ValidEvents) of
                  Dot ->
                      ordsets:add_element(Dot, AccNewProvenance0);
                  _ ->
                      AccNewProvenance0
              end
      end, ordsets:new(), Provenance).

%% @private
%% @doc Calculate the next event from the AllEvents and add it to the return value.
%% The next event is located on the head of the return value. The second parameter
%% should be the AllEvents of the full state version ({vclock, _}).
%% The return value: {vclock, [NextEvent|RestAllEvents]}.
-spec get_next_event(ps_event_id(), ps_all_events()) -> ps_all_events().
get_next_event(EventId, {vclock, AllEvents}) ->
    {NextCounter, RestAllEvents} =
        case lists:keytake(EventId, 1, AllEvents) of
            false ->
                {1, AllEvents};
            {value, {_EId, Counter}, Rest} ->
                {Counter + 1, Rest}
        end,
    {vclock, [{EventId, NextCounter}|RestAllEvents]}.

%% @private
add_elem_with_dot_private(Key, Value, DataStore) ->
    orddict:update(Key,
                   fun(Old) ->
                           ordsets:add_element(Value, Old)
                   end,
                   ordsets:add_element(Value, ordsets:new()),
                   DataStore).

%% @private
%% @doc Merge two AllEvents sections. The AllEvents can be a set of Events or
%% a vclock-like representation. Except merging two sets (delta + delta), this
%% function will return the vclock-like representation.
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
    join_all_events({vclock, AllEventsB}, AllEventsA);
join_all_events(AllEventsA, AllEventsB) ->
    ordsets:union(AllEventsA, AllEventsB).

%% @private
is_second_lager({vclock, AllEventsA}, {vclock, AllEventsB}) ->
    not is_inflation_all_events({vclock, AllEventsA}, {vclock, AllEventsB});
is_second_lager({vclock, _AllEventsA}, _AllEventsB) ->
    false;
is_second_lager(_AllEventsA, {vclock, _AllEventsB}) ->
    true;
is_second_lager(AllEventsA, AllEventsB) ->
    ordsets:size(AllEventsA) < ordsets:size(AllEventsB).

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
%% @doc Check whether an Event is a part of the second parameter (vclock or set).
is_valid_event({EventId, Counter}=_Event, {vclock, AllEvents}) ->
    Counter =< get_counter(EventId, {vclock, AllEvents});
is_valid_event(Event, EventsSet) ->
    ordsets:is_element(Event, EventsSet).

%% @private
is_addable_dot_private([], _ValidEventsOther, _AllEventsAnyOther) ->
    true;
is_addable_dot_private([Event|RestEvents], ValidEventsOther, AllEventsAnyOther) ->
    case is_valid_event(Event, AllEventsAnyOther) of
        false ->
            is_addable_dot_private(RestEvents, ValidEventsOther, AllEventsAnyOther);
        true ->
            case is_valid_event(Event, ValidEventsOther) of
                true ->
                    is_addable_dot_private(RestEvents,
                                           ValidEventsOther,
                                           AllEventsAnyOther);
                false ->
                    false
            end
    end.

%% @private
%% @doc Check whether a Dot is addable.
%% The addable dots consist of the evnets which are newly added (the Counter is
%% bigger than the one in the AllEventsAnyOther) or members of the ValidEventsOther.
is_addable_dot(Dot, ValidEventsOther, AllEventsAnyOther) ->
    is_addable_dot_private(Dot, ValidEventsOther, AllEventsAnyOther).

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
remove_elem_with_event(Elem, Event, {ElemDataStore, EventDataStore}) ->
    case orddict:find(Elem, ElemDataStore) of
        error ->
            {ElemDataStore, EventDataStore};
        {ok, Provenance} ->
            {NewProvenance, DeleteEvents} =
                ordsets:fold(
                  fun(Dot, {NewProvenance0, DeleteEvents0}) ->
                          case ordsets:del_element(Event, Dot) of
                              Dot ->
                                  {ordsets:add_element(Dot, NewProvenance0),
                                   DeleteEvents0};
                              _ ->
                                  {NewProvenance0,
                                   ordsets:union(DeleteEvents0, Dot)}
                          end
                  end, {ordsets:new(), ordsets:new()}, Provenance),
            NewElemDataStore =
                case NewProvenance of
                    [] ->
                        orddict:erase(Elem, ElemDataStore);
                    _ ->
                        orddict:update(Elem,
                                       fun(_Old) ->
                                               NewProvenance
                                       end,
                                       ElemDataStore)
                end,
            NewEventDataStore =
                ordsets:fold(
                  fun(OtherEvent, NewEventDataStore0) ->
                          case orddict:find(OtherEvent, NewEventDataStore0) of
                              error ->
                                  NewEventDataStore0;
                              {ok, Elems} ->
                                  NewElems = ordsets:del_element(Elem, Elems),
                                  case NewElems of
                                      [] ->
                                          orddict:erase(OtherEvent,
                                                        NewEventDataStore0);
                                      _ ->
                                          orddict:store(OtherEvent,
                                                        NewElems,
                                                        NewEventDataStore0)
                                  end
                          end
                  end, EventDataStore, DeleteEvents),
            {NewElemDataStore, NewEventDataStore}
    end.

%% @private
%% @doc Remove an event from the DataStore.
%% Dots which contain the will-be-removed event will also be removed and this
%% can cause the deletion of some elements as well.
-spec remove_event(ps_event(), ps_data_store()) -> ps_data_store().
remove_event(Event, {ElemDataStore, EventDataStore}) ->
    case orddict:find(Event, EventDataStore) of
        error ->
            {ElemDataStore, EventDataStore};
        {ok, Elems} ->
            ordsets:fold(
              fun(Elem, DataStore0) ->
                      remove_elem_with_event(Elem, Event, DataStore0)
              end, {ElemDataStore, EventDataStore}, Elems)
    end.

%% @private
%% @doc Join two DataStores.
%% This join algorithm will be very fast when the second parameter is smaller than
%% the first one (e.g., join the full state and the delta state).
-spec join_data_store(payload(), payload()) -> ps_data_store().
join_data_store({{_ElemDataStoreA, EventDataStoreA}=DataStoreA,
                 FilteredOutEventsA, AllEventsAnyA},
                {{ElemDataStoreB, EventDataStoreB}=_DataStoreB,
                 FilteredOutEventsB, AllEventsAnyB}) ->
    ValidEventsA = ordsets:from_list(orddict:fetch_keys(EventDataStoreA)),
    %% Find removed events in B.
    RemovedEventsB =
        subtract_all_events(AllEventsAnyB,
                            ordsets:union(
                              ordsets:from_list(orddict:fetch_keys(EventDataStoreB)),
                              FilteredOutEventsB)),
    %% Remove the removed events in B from A.
    {NewElemDataStoreA, NewEventDataStoreA} =
        ordsets:fold(
          fun(Event, DataStoreA0) ->
                  case is_valid_event(Event, AllEventsAnyA) of
                      false -> %% unknown event
                          DataStoreA0;
                      true ->
                          case is_valid_event(Event, ValidEventsA) of
                              false -> %% removed event
                                  DataStoreA0;
                              true ->
                                  remove_event(Event, DataStoreA0)
                          end
                  end
          end, DataStoreA, RemovedEventsB),
    %% Find new added ones in B.
    NewValidEventsA = ordsets:union(
                        ordsets:from_list(orddict:fetch_keys(NewEventDataStoreA)),
                        FilteredOutEventsA),
    NewAddedList =
        orddict:fold(
          fun(Elem, Provenance, NewAddedList0) ->
                  ordsets:fold(
                    fun(Dot, NewAddedList1) ->
                            case is_addable_dot(Dot,
                                                NewValidEventsA,
                                                AllEventsAnyA) of
                                true ->
                                    NewAddedList1 ++ [{Elem, Dot}];
                                false ->
                                    NewAddedList1
                            end
                    end, NewAddedList0, Provenance)
          end, [], ElemDataStoreB),
    %% Add new added ones to A.
    lists:foldl(
      fun({Elem, Dot}, NewDataStoreA0) ->
              add_elem_with_dot(Elem, Dot, NewDataStoreA0)
      end, {NewElemDataStoreA, NewEventDataStoreA}, NewAddedList).

%% @private
%% @doc Join two payloads of the awset_ps.
%% For the better performance, the second parameter of the join_data_store()
%% needs to be smaller that the first one.
-spec join_awset_ps(payload(), payload()) -> payload().
join_awset_ps(AWSet, AWSet) ->
    AWSet;
join_awset_ps({{[], []}, [], {vclock, []}},
              {DataStoreB, FilteredOutEventsB, AllEventsAnyB}) ->
    {DataStoreB, FilteredOutEventsB, join_all_events({vclock, []}, AllEventsAnyB)};
join_awset_ps({DataStoreA, FilteredOutEventsA, AllEventsAnyA},
              {{[], []}, [], {vclock, []}}) ->
    {DataStoreA, FilteredOutEventsA, join_all_events(AllEventsAnyA, {vclock, []})};
join_awset_ps({_DataStoreA, FilteredOutEventsA, AllEventsAnyA}=FstAWSet,
              {_DataStoreB, FilteredOutEventsB, AllEventsAnyB}=SndAWSet) ->
    {JoinedElemDataStore, JoinedEventDataStore} =
        case is_second_lager(AllEventsAnyA, AllEventsAnyB) of
            true ->
                join_data_store(SndAWSet, FstAWSet);
            false ->
                join_data_store(FstAWSet, SndAWSet)
        end,
    {{JoinedElemDataStore, JoinedEventDataStore},
     ordsets:subtract(ordsets:union(FilteredOutEventsA, FilteredOutEventsB),
                      ordsets:from_list(orddict:fetch_keys(JoinedEventDataStore))),
     join_all_events(AllEventsAnyA, AllEventsAnyB)}.

%% @private
is_inflation_all_events_private([], _) ->
    % all AllEvents are the inflation of the empty AllEvents
    true;
is_inflation_all_events_private(AllEventsA, AllEventsB) ->
    [{EventIdA, CounterA}|RestA] = AllEventsA,
    case lists:keyfind(EventIdA, 1, AllEventsB) of
        false ->
            false;
        {_, CounterB} ->
            case CounterA == CounterB of
                true ->
                    is_inflation_all_events_private(RestA, AllEventsB);
                false ->
                    CounterA < CounterB
            end
    end.

%% @private
%% @doc Check the inflation through the AllEvents section.
-spec is_inflation_all_events(ps_all_events(), ps_all_events()) -> boolean().
is_inflation_all_events({vclock, AllEventsA}, {vclock, AllEventsB}) ->
    is_inflation_all_events_private(AllEventsA, AllEventsB).

%% @private
%% @doc Check the inflation (whether B is an inflation of A).
%% If the comparison of the AllEvents sections is not an inflation, no need for
%% further checking.
%% If the AllEvents section is an inflation, need to check whether removed events
%% of A exist in B.
-spec is_inflation_awset_ps(payload(), payload()) -> boolean().
is_inflation_awset_ps(Payload, Payload) ->
    true;
is_inflation_awset_ps({{_ElemDataStoreA, EventDataStoreA}=_DataStoreA,
                       FilteredOutEventsA, AllEventsA},
                      {{_ElemDataStoreB, EventDataStoreB}=_DataStoreB,
                       _FilteredOutEventsB, AllEventsB}) ->
    case is_inflation_all_events(AllEventsA, AllEventsB) of
        false ->
            false;
        true ->
            RemovedEventsA =
                subtract_all_events(AllEventsA,
                                    ordsets:union(
                                      ordsets:from_list(
                                        orddict:fetch_keys(EventDataStoreA)),
                                      FilteredOutEventsA)),
            ordsets:intersection(RemovedEventsA,
                                 ordsets:from_list(
                                   orddict:fetch_keys(EventDataStoreB))) == []
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {{orddict:new(), orddict:new()},
                          ordsets:new(),
                          {vclock, []}}},
                 new()).

query_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    Set1 = {?TYPE, {{[{<<"1">>, [[{EventId, 1}]]}], [{{EventId, 1}, <<"1">>}]},
                    [],
                    {vclock, [{EventId, 2}]}}},
    ?assertEqual(sets:new(), query(Set0)),
    ?assertEqual(sets:from_list([<<"1">>]), query(Set1)).

delta_add_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate({add, <<"1">>}, EventId, Set0),
    Set1 = merge({?TYPE, Delta1}, Set0),
    {ok, {?TYPE, Delta2}} = delta_mutate({add, <<"1">>}, EventId, Set1),
    Set2 = merge({?TYPE, Delta2}, Set1),
    {ok, {?TYPE, Delta3}} = delta_mutate({add, <<"2">>}, EventId, Set2),
    Set3 = merge({?TYPE, Delta3}, Set2),
    ?assertEqual({?TYPE, {{[{<<"1">>, [[{EventId, 1}]]}],
                           [{{EventId, 1}, [<<"1">>]}]},
                          [],
                          [{EventId, 1}]}}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {{[{<<"1">>, [[{EventId, 1}]]}],
                           [{{EventId, 1}, [<<"1">>]}]},
                          [],
                          {vclock, [{EventId, 1}]}}}, Set1),
    ?assertEqual({?TYPE, {{[{<<"1">>, [[{EventId, 2}]]}],
                           [{{EventId, 2}, [<<"1">>]}]},
                          [],
                          [{EventId, 2}]}}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {{[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]}],
                           [{{EventId, 1}, [<<"1">>]},
                            {{EventId, 2}, [<<"1">>]}]},
                          [],
                          {vclock, [{EventId, 2}]}}}, Set2),
    ?assertEqual({?TYPE, {{[{<<"2">>, [[{EventId, 3}]]}],
                           [{{EventId, 3}, [<<"2">>]}]},
                          [],
                          [{EventId, 3}]}}, {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {{[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]},
                            {<<"2">>, [[{EventId, 3}]]}],
                           [{{EventId, 1}, [<<"1">>]},
                            {{EventId, 2}, [<<"1">>]},
                            {{EventId, 3}, [<<"2">>]}]},
                          [],
                          {vclock, [{EventId, 3}]}}}, Set3).

add_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"1">>}, EventId, Set0),
    {ok, Set2} = mutate({add, <<"1">>}, EventId, Set1),
    {ok, Set3} = mutate({add, <<"2">>}, EventId, Set2),
    ?assertEqual({?TYPE, {{[{<<"1">>, [[{EventId, 1}]]}],
                           [{{EventId, 1}, [<<"1">>]}]},
                          [],
                          {vclock, [{EventId, 1}]}}}, Set1),
    ?assertEqual({?TYPE, {{[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]}],
                           [{{EventId, 1}, [<<"1">>]},
                            {{EventId, 2}, [<<"1">>]}]},
                          [],
                          {vclock, [{EventId, 2}]}}}, Set2),
    ?assertEqual({?TYPE, {{[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]},
                            {<<"2">>, [[{EventId, 3}]]}],
                           [{{EventId, 1}, [<<"1">>]},
                            {{EventId, 2}, [<<"1">>]},
                            {{EventId, 3}, [<<"2">>]}]},
                          [],
                          {vclock, [{EventId, 3}]}}}, Set3).

rmv_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"1">>}, EventId, Set0),
    {ok, Set2} = mutate({add, <<"1">>}, EventId, Set1),
    {ok, Set2} = mutate({rmv, <<"2">>}, EventId, Set2),
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

remove_all_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, Set1} = mutate({add_all, [<<"a">>, <<"b">>, <<"c">>]}, EventId, Set0),
    {ok, Set2} = mutate({rmv_all, [<<"a">>, <<"c">>]}, EventId, Set1),
    {ok, Set3} = mutate({rmv_all, [<<"b">>, <<"d">>]}, EventId, Set2),
    {ok, Set3} = mutate({rmv_all, [<<"b">>]}, EventId, Set2),
    ?assertEqual(sets:from_list([<<"b">>]), query(Set2)),
    ?assertEqual(sets:new(), query(Set3)).

merge_idempontent_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 = {?TYPE, {{[], []}, [], {vclock, [{EventId1, 1}]}}},
    Set2 = {?TYPE, {{[{<<"2">>, [[{EventId2, 1}]]}],
                     [{{EventId2, 1}, [<<"2">>]}]},
                    [],
                    {vclock, [{EventId2, 1}]}}},
    Set3 = {?TYPE, {{[{<<"1">>, [[{EventId1, 1}]]}],
                     [{{EventId1, 1}, [<<"1">>]}]},
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
    Set1 = {?TYPE, {{[], []},
                    [],
                    {vclock, [{EventId1, 1}]}}},
    Set2 = {?TYPE, {{[{<<"2">>, [[{EventId2, 1}]]}],
                     [{{EventId2, 1}, [<<"2">>]}]},
                    [],
                    {vclock, [{EventId2, 1}]}}},
    Set3 = {?TYPE, {{[{<<"1">>, [[{EventId1, 1}]]}],
                     [{{EventId1, 1}, [<<"1">>]}]},
                    [],
                    {vclock, [{EventId1, 1}, {EventId2, 1}]}}},
    Set4 = merge(Set1, Set2),
    Set5 = merge(Set2, Set1),
    Set6 = merge(Set1, Set3),
    Set7 = merge(Set3, Set1),
    Set8 = merge(Set2, Set3),
    Set9 = merge(Set3, Set2),
    Set10 = merge(Set1, merge(Set2, Set3)),
    Set1_2 = {?TYPE, {{[{<<"2">>, [[{EventId2, 1}]]}],
                       [{{EventId2, 1}, [<<"2">>]}]},
                      [],
                      {vclock, [{EventId1, 1}, {EventId2, 1}]}}},
    Set1_3 = {?TYPE, {{[], []},
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
    Set1 = {?TYPE, {{[{<<"1">>, [[{EventId1, 1}, {EventId2, 1}]]},
                      {<<"2">>, [[{EventId1, 2}, {EventId2, 2}]]}],
                     [{{EventId1, 1}, [<<"1">>]}, {{EventId1, 2}, [<<"2">>]},
                      {{EventId2, 1}, [<<"1">>]}, {{EventId2, 2}, [<<"2">>]}]},
                    [],
                    {vclock, [{EventId1, 2}, {EventId2, 2}]}}},
    Delta1 = {?TYPE, {{[{<<"1">>, [[{EventId1, 1}, {EventId2, 3}]]},
                        {<<"2">>, [[{EventId1, 2}, {EventId2, 4}]]}],
                       [{{EventId1, 1}, [<<"1">>]},
                        {{EventId1, 2}, [<<"2">>]},
                        {{EventId2, 3}, [<<"1">>]},
                        {{EventId2, 4}, [<<"2">>]}]},
                      [],
                      [{EventId1, 1}, {EventId1, 2},
                       {EventId2, 3}, {EventId2, 4}]}},
    Set2 = merge(Set1, Delta1),
    ?assert(equal({?TYPE, {{[{<<"1">>, [[{EventId1, 1}, {EventId2, 1}],
                                        [{EventId1, 1}, {EventId2, 3}]]},
                             {<<"2">>, [[{EventId1, 2}, {EventId2, 2}],
                                        [{EventId1, 2}, {EventId2, 4}]]}],
                            [{{EventId1, 1}, [<<"1">>]},
                             {{EventId1, 2}, [<<"2">>]},
                             {{EventId2, 1}, [<<"1">>]},
                             {{EventId2, 2}, [<<"2">>]},
                             {{EventId2, 3}, [<<"1">>]},
                             {{EventId2, 4}, [<<"2">>]}]},
                           [],
                           {vclock, [{EventId1, 2}, {EventId2, 4}]}}},
                  Set2)).

merge_delta_test() ->
    EventId = {<<"object1">>, a},
    Set1 = {?TYPE, {{[{<<"1">>, [[{EventId, 1}]]}],
                     [{{EventId, 1}, [<<"1">>]}]},
                    [],
                    {vclock, [{EventId, 1}]}}},
    Delta1 = {?TYPE, {{[], []},
                      [],
                      [{EventId, 1}]}},
    Delta2 = {?TYPE, {{[{<<"2">>, [[{EventId, 2}]]}],
                       [{{EventId, 2}, [<<"2">>]}]},
                      [],
                      [{EventId, 2}]}},
    Set2 = merge(Delta1, Set1),
    Set3 = merge(Set1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, {{[], []},
                          [],
                          {vclock, [{EventId, 1}]}}},
                 Set2),
    ?assertEqual({?TYPE, {{[], []},
                          [],
                          {vclock, [{EventId, 1}]}}},
                 Set3),
    ?assertEqual({?TYPE, {{[{<<"2">>, [[{EventId, 2}]]}],
                           [{{EventId, 2}, [<<"2">>]}]},
                          [],
                          [{EventId, 1}, {EventId, 2}]}},
                          DeltaGroup).

equal_test() ->
    EventId = {<<"object1">>, a},
    Set1 = {?TYPE, {{[{<<"1">>, [[{EventId, 1}]]}],
                     [{{EventId, 1}, [<<"1">>]}]},
                    [],
                    {vclock, [{EventId, 1}]}}},
    Set2 = {?TYPE, {{[], []},
                    [],
                    {vclock, [{EventId, 1}]}}},
    Set3 = {?TYPE, {{[{<<"1">>, [[{EventId, 1}]]}],
                     [{{EventId, 1}, [<<"1">>]}]},
                    [],
                    {vclock, [{EventId, 2}]}}},
    ?assert(equal(Set1, Set1)),
    ?assert(equal(Set2, Set2)),
    ?assert(equal(Set3, Set3)),
    ?assertNot(equal(Set1, Set2)),
    ?assertNot(equal(Set1, Set3)),
    ?assertNot(equal(Set2, Set3)).

is_inflation_test() ->
    EventId = {<<"object1">>, a},
    Set1 = {?TYPE, {{[{<<"1">>, [[{EventId, 1}]]}],
                     [{{EventId, 1}, [<<"1">>]}]},
                    [],
                    {vclock, [{EventId, 1}]}}},
    Set2 = {?TYPE, {{[], []},
                    [],
                    {vclock, [{EventId, 1}]}}},
    Set3 = {?TYPE, {{[{<<"1">>, [[{EventId, 1}]]}],
                     [{{EventId, 1}, [<<"1">>]}]},
                    [],
                    {vclock, [{EventId, 2}]}}},
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
    EventId = {<<"object1">>, a},
    Set1 = {?TYPE, {{[{<<"1">>, [[{EventId, 1}]]}],
                     [{{EventId, 1}, [<<"1">>]}]},
                    [],
                    {vclock, [{EventId, 1}]}}},
    Set2 = {?TYPE, {{[], []},
                    [],
                    {vclock, [{EventId, 1}]}}},
    Set3 = {?TYPE, {{[{<<"1">>, [[{EventId, 1}]]}],
                     [{{EventId, 1}, [<<"1">>]}]},
                    [],
                    {vclock, [{EventId, 2}]}}},
    ?assertNot(is_strict_inflation(Set1, Set1)),
    ?assert(is_strict_inflation(Set1, Set2)),
    ?assertNot(is_strict_inflation(Set2, Set1)),
    ?assert(is_strict_inflation(Set1, Set3)),
    ?assertNot(is_strict_inflation(Set2, Set3)),
    ?assertNot(is_strict_inflation(Set3, Set2)).

join_decomposition_test() ->
    %% @todo
    ok.

encode_decode_test() ->
    Set = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}, {<<"b">>, [{<<"token2">>, false}]}]},
    Binary = encode(erlang, Set),
    ESet = decode(erlang, Binary),
    ?assertEqual(Set, ESet).

-endif.
