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

%% @doc Observed-remove map CRDT with the provenance semiring:
%%      observed-remove map without tombstones.

-module(state_ps_ormap_naive).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-behaviour(type).
-behaviour(state_ps_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1,
         mutate/3,
         query/1,
         equal/2]).
-export([delta_mutate/3,
         merge/2,
         is_bottom/1,
         is_inflation/2,
         is_strict_inflation/2,
         encode/2,
         decode/2,
         new_provenance_store/1,
         get_events_from_provenance_store/1]).

-export_type([state_ps_ormap_naive/0,
              state_ps_ormap_naive_op/0]).

-type key() :: term().
-type value() :: term().
-type value_type() :: {state_ps_type:state_ps_type() | state_type:state_type(),
                       []}
                    | {?TYPE, [value_type()]}.
-type state_ps_provenance_store() :: {value_type(),
                                      orddict:orddict(key(), value())}.
-type payload() :: {state_ps_provenance_store(),
                    state_ps_type:state_ps_subset_events(),
                    state_ps_type:state_ps_all_events()}.
-opaque state_ps_ormap_naive() :: {?TYPE, payload()}.
-type value_op() :: term().
-type state_ps_ormap_naive_op() :: {apply, key(), value_op()}
                                 | {rmv, key()}.

%% @doc Create a new, empty provenance store for `state_ps_ormap_naive()'.
-spec new_provenance_store([value_type()]) -> state_ps_provenance_store().
new_provenance_store([{ValueType, SubValueType}]) ->
    {{ValueType, SubValueType}, orddict:new()}.

%% @doc Return all events in a provenance store.
-spec get_events_from_provenance_store(state_ps_provenance_store()) ->
    ordsets:ordset(state_ps_type:state_ps_event()).
get_events_from_provenance_store({{ValueType, _SubValueType},
                                  ValueProvenanceStore}) ->
    orddict:fold(
        fun(_Key, SubProvenanceStore, AccInEvents) ->
            EventsFromSubProvenanceStore =
                case state_ps_type:is_ps_type(ValueType) of
                    true ->
                        ValueType:get_events_from_provenance_store(
                            SubProvenanceStore);
                    false ->
                        ordsets:fold(
                            fun({_ValueDeltaPayload, ValueProvenance},
                                AccInEventsFromSubProvenanceStore) ->
                                EventsFromSubPS =
                                    state_ps_type:get_events_from_provenance(
                                            ValueProvenance),
                                ordsets:union(
                                    AccInEventsFromSubProvenanceStore,
                                    EventsFromSubPS)
                            end,
                            ordsets:new(),
                            SubProvenanceStore)
                end,
            ordsets:union(AccInEvents, EventsFromSubProvenanceStore)
        end,
        ordsets:new(),
        ValueProvenanceStore).

%% @doc Create a new, empty `state_ps_ormap_naive()'.
%%      By default the values are a state_ps_orset_naive CRDT.
-spec new() -> state_ps_ormap_naive().
new() ->
    new([{state_ps_orset_naive, []}]).

%% @doc Create a new, empty `state_ps_ormap_naive()'
-spec new([term()]) -> state_ps_ormap_naive().
new([{ValueType, SubValueType}]) ->
    {?TYPE, {new_provenance_store([{ValueType, SubValueType}]),
             ordsets:new(),
             {ev_set, ordsets:new()}}}.

%% @doc Mutate a `state_ps_ormap_naive()'.
-spec mutate(state_ps_ormap_naive_op(), type:id(), state_ps_ormap_naive()) ->
    {ok, state_ps_ormap_naive()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_ps_type:mutate(Op, Actor, CRDT).

%% @doc Returns the value of the `state_ps_ormap_naive()'.
%%      This value is a dictionary where keys are the same as the
%%      `state_ps_ormap_naive()' and values are the return values of `query/1'.
-spec query(state_ps_ormap_naive()) -> term().
query({?TYPE, {{{ValueType, _SubValueType}, ValueProvenanceStore},
               SubsetEvents,
               AllEvents}}) ->
    orddict:fold(
        fun(Key, SubProvenanceStore, AccInResultMap) ->
            Value =
                case state_ps_type:is_ps_type(ValueType) of
                    true ->
                        ValueType:query(
                            {ValueType, {SubProvenanceStore,
                                         SubsetEvents,
                                         AllEvents}});
                    false ->
                        %% Get the local payload.
                        ValueLocalPayload =
                            get_value_local_payload(
                                Key, ValueType, SubProvenanceStore),
                        ValueType:query(ValueLocalPayload)
                end,
            orddict:store(Key, Value, AccInResultMap)
        end,
        orddict:new(),
        ValueProvenanceStore).

%% @doc Equality for `state_ps_ormap_naive()'.
%%      Since everything is ordered, == should work.
-spec equal(state_ps_ormap_naive(), state_ps_ormap_naive()) -> boolean().
equal({?TYPE, {{ValueTypeA, ValueProvenanceStoreA},
               SubsetEventsA,
               AllEventsA}},
      {?TYPE, {{ValueTypeB, ValueProvenanceStoreB},
               SubsetEventsB,
               AllEventsB}}) ->
    ValueTypeA == ValueTypeB andalso
    ValueProvenanceStoreA == ValueProvenanceStoreB andalso
    SubsetEventsA == SubsetEventsB andalso
    state_ps_type:equal_all_events(AllEventsA, AllEventsB).

%% @doc Delta-mutate a `state_ps_ormap_naive()'.
%%      The first argument can be:
%%          - `{apply, Key, Op}'
%%          - `{rmv, Key}'
%%          `apply' receives an operation that will be applied to the
%%          value of the key. This operation has to be a valid operation
%%          in the CRDT chose to be in the values (by default an
%%          state_ps_orset_naive).
%%      The second argument is the event id ({object_id, replica_id}).
%%      The third argument is the `state_ps_ormap_naive()' to be inflated.
-spec delta_mutate(state_ps_ormap_naive_op(),
                   type:id(),
                   state_ps_ormap_naive()) -> {ok, state_ps_ormap_naive()}.
delta_mutate({apply, Key, Op},
             Actor,
             {?TYPE, {{{ValueType, SubValueType}, ValueProvenanceStore},
                      SubsetEvents,
                      AllEvents}}) ->
    SubProvenanceStore =
        case orddict:find(Key, ValueProvenanceStore) of
            {ok, SubProvenanceStore0} ->
                SubProvenanceStore0;
            error ->
                %% `apply' with a new Key.
                case state_ps_type:is_ps_type(ValueType) of
                    true ->
                        ValueType:new_provenance_store(SubValueType);
                    false ->
                        ordsets:new()
                end
        end,
    {DeltaProvenanceStore, DeltaSubsetEvents, DeltaAllEvents} =
        case state_ps_type:is_ps_type(ValueType) of
            true ->
                {ok, {ValueType, {DeltaSubProvenanceStore0,
                                  DeltaSubsetEvents0,
                                  DeltaAllEvents0}}} =
                    ValueType:delta_mutate(
                        Op, Actor, {ValueType, {SubProvenanceStore,
                                                SubsetEvents,
                                                AllEvents}}),
                DeltaProvenanceStore0 =
                    case DeltaSubProvenanceStore0 ==
                         ValueType:new_provenance_store(SubValueType) of
                        true ->
                            {{ValueType, SubValueType}, orddict:new()};
                        false ->
                            {{ValueType, SubValueType},
                             orddict:store(
                                 Key, DeltaSubProvenanceStore0, orddict:new())}
                    end,
                {DeltaProvenanceStore0, DeltaSubsetEvents0, DeltaAllEvents0};
            false ->
                %% Get next Event from AllEvents.
                NextEvent =
                    state_ps_type:get_next_event(Actor, AllEvents),
                %% Make a new Provenance from the Event.
                ValueProvenance = ordsets:add_element(
                    ordsets:add_element(NextEvent, ordsets:new()),
                    ordsets:new()),
                %% Get the local payload.
                ValueLocalPayload =
                    get_value_local_payload(
                        Key, ValueType, SubProvenanceStore),
                {ok, {ValueType, ValuePayload}} =
                    ValueType:delta_mutate(Op, Actor, ValueLocalPayload),
                DeltaProvenanceStore1 =
                    case {ValueType, ValuePayload} ==
                         ValueType:new() of
                        true ->
                            {{ValueType, SubValueType}, orddict:new()};
                        false ->
                            {{ValueType, SubValueType},
                             orddict:store(
                                 Key,
                                 ordsets:add_element(
                                     {ValuePayload, ValueProvenance},
                                     ordsets:new()),
                                 orddict:new())}
                    end,
                {DeltaProvenanceStore1,
                 ordsets:add_element(NextEvent, ordsets:new()),
                 {ev_set, ordsets:add_element(NextEvent, ordsets:new())}}
        end,
    {ok, {?TYPE, {DeltaProvenanceStore, DeltaSubsetEvents, DeltaAllEvents}}};

delta_mutate({rmv, Key},
             _Actor,
             {?TYPE, {{{ValueType, SubValueType}, ProvenanceStore},
                      _SubsetEvents,
                      _AllEvents}}) ->
    case orddict:find(Key, ProvenanceStore) of
        {ok, SubProvenanceStore} ->
            RemovedAllEvents =
                case state_ps_type:is_ps_type(ValueType) of
                    true ->
                        ValueType:get_events_from_provenance_store(
                            SubProvenanceStore);
                    false ->
                        ordsets:fold(
                            fun({_ValueDeltaPayload0, ValueProvenance0},
                                AccInRemovedAllEvents) ->
                                EventsFromValueProvenance =
                                    state_ps_type:get_events_from_provenance(
                                        ValueProvenance0),
                                ordsets:union(
                                    AccInRemovedAllEvents,
                                    EventsFromValueProvenance)
                            end,
                            ordsets:new(),
                            SubProvenanceStore)
                end,
            {ok, {?TYPE, {new_provenance_store([{ValueType, SubValueType}]),
                          ordsets:new(),
                          {ev_set, RemovedAllEvents}}}};
        error ->
            {ok, new([{ValueType, SubValueType}])}
    end.

%% @doc Merge two `state_ps_ormap_naive()'.
-spec merge(state_ps_ormap_naive(), state_ps_ormap_naive()) ->
    state_ps_ormap_naive().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun merge_survived_ev_set_all_events/2,
    state_ps_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Check if a `state_ps_ormap_naive()' is bottom
-spec is_bottom(state_ps_ormap_naive()) -> boolean().
is_bottom({?TYPE, {{ValueType, _ProvenanceStore},
                   _SubsetEvents,
                   _AllEvents}}=CRDT) ->
    CRDT == new([ValueType]).

%% @doc Given two `state_ps_ormap_naive()', check if the second is an inflation
%%      of the first.
-spec is_inflation(state_ps_ormap_naive(), state_ps_ormap_naive()) -> boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_ps_ormap_naive(), state_ps_ormap_naive()) ->
    boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_strict_inflation(CRDT1, CRDT2).

-spec encode(state_ps_type:format(), state_ps_ormap_naive()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_ps_type:format(), binary()) -> state_ps_ormap_naive().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.

%% @private
merge_survived_ev_set_all_events(
    {?TYPE, {{{ValueType, SubValueType}, ValueProvenanceStoreA},
             SubsetEventsSurvivedA,
             {ev_set, AllEventsEVA}}},
    {?TYPE, {{{ValueType, SubValueType}, ValueProvenanceStoreB},
             SubsetEventsSurvivedB,
             {ev_set, AllEventsEVB}}}) ->
    MergedAllEventsEV = {ev_set, ordsets:union(AllEventsEVA, AllEventsEVB)},
    MergedSubsetEventsSurvived =
        ordsets:union(
            [ordsets:intersection(
                SubsetEventsSurvivedA,
                SubsetEventsSurvivedB)] ++
            [ordsets:subtract(SubsetEventsSurvivedA, AllEventsEVB)] ++
            [ordsets:subtract(SubsetEventsSurvivedB, AllEventsEVA)]),
    MergedKeys =
        ordsets:union(
            ordsets:from_list(orddict:fetch_keys(ValueProvenanceStoreA)),
            ordsets:from_list(orddict:fetch_keys(ValueProvenanceStoreB))),
    MergedValueProvenanceStore =
        ordsets:fold(
            fun(Key, AccInMergedProvenanceStore) ->
                SubProvenanceStoreA =
                    orddict:find(Key, ValueProvenanceStoreA),
                SubProvenanceStoreB =
                    orddict:find(Key, ValueProvenanceStoreB),
                MergedSubProvenanceStore =
                    merge_sub_provenance_store(
                        {ValueType, SubValueType},
                        {SubProvenanceStoreA,
                         SubsetEventsSurvivedA,
                         {ev_set, AllEventsEVA}},
                        {SubProvenanceStoreB,
                         SubsetEventsSurvivedB,
                         {ev_set, AllEventsEVB}}),
                case is_bottom_provenance_store(
                    {ValueType, SubValueType}, MergedSubProvenanceStore) of
                    true ->
                        AccInMergedProvenanceStore;
                    false ->
                        orddict:store(
                            Key,
                            MergedSubProvenanceStore,
                            AccInMergedProvenanceStore)
                end
            end,
            orddict:new(),
            MergedKeys),
    {?TYPE, {{{ValueType, SubValueType}, MergedValueProvenanceStore},
             MergedSubsetEventsSurvived,
             MergedAllEventsEV}}.

%% @private
is_bottom_provenance_store({ValueType, SubValueType}, ProvenanceStore) ->
    case state_ps_type:is_ps_type(ValueType) of
        true ->
            ProvenanceStore ==
                ValueType:new_provenance_store(SubValueType);
        false ->
            ProvenanceStore == ordsets:new()
    end.

%% @private
merge_sub_provenance_store({ValueType, _SubValueType},
                           {{ok, SubProvenanceStoreA},
                            SubsetEventsSurvivedA,
                            {ev_set, AllEventsEVA}},
                           {{ok, SubProvenanceStoreB},
                            SubsetEventsSurvivedB,
                            {ev_set, AllEventsEVB}}) ->
    case state_ps_type:is_ps_type(ValueType) of
        true ->
            %% @todo Make this to the function.
            {ValueType, {MergedProvenanceStore0, _, _}} =
                ValueType:merge(
                    {ValueType, {SubProvenanceStoreA,
                                 SubsetEventsSurvivedA,
                                 {ev_set, AllEventsEVA}}},
                    {ValueType, {SubProvenanceStoreB,
                                 SubsetEventsSurvivedB,
                                 {ev_set, AllEventsEVB}}}),
            MergedProvenanceStore0;
        false ->
            RemovedEventsA =
                ordsets:subtract(AllEventsEVA, SubsetEventsSurvivedA),
            RemovedEventsB =
                ordsets:subtract(AllEventsEVB, SubsetEventsSurvivedB),
            SubProvenanceStoreA0 =
                %% @todo Make this to the function.
                ordsets:fold(
                    fun({ValueDeltaPayload0, ValueProvenance0},
                        AccInSubProvenanceStoreA0) ->
                        case ordsets:is_subset(
                            state_ps_type:get_events_from_provenance(
                                ValueProvenance0),
                            RemovedEventsB) of
                            true ->
                                AccInSubProvenanceStoreA0;
                            false ->
                                ordsets:add_element(
                                    {ValueDeltaPayload0, ValueProvenance0},
                                    AccInSubProvenanceStoreA0)
                        end
                    end,
                    ordsets:new(),
                    SubProvenanceStoreA),
            SubProvenanceStoreB0 =
                %% @todo Make this to the function.
                ordsets:fold(
                    fun({ValueDeltaPayload1, ValueProvenance1},
                        AccInSubProvenanceStoreB0) ->
                        case ordsets:is_subset(
                            state_ps_type:get_events_from_provenance(
                                ValueProvenance1),
                            RemovedEventsA) of
                            true ->
                                AccInSubProvenanceStoreB0;
                            false ->
                                ordsets:add_element(
                                    {ValueDeltaPayload1, ValueProvenance1},
                                    AccInSubProvenanceStoreB0)
                        end
                    end,
                    ordsets:new(),
                    SubProvenanceStoreB),
            ordsets:union(SubProvenanceStoreA0, SubProvenanceStoreB0)
    end;
merge_sub_provenance_store({ValueType, SubValueType},
                           {error,
                            SubsetEventsSurvivedA,
                            {ev_set, AllEventsEVA}},
                           {{ok, SubProvenanceStoreB},
                            SubsetEventsSurvivedB,
                            {ev_set, AllEventsEVB}}) ->
    case state_ps_type:is_ps_type(ValueType) of
        true ->
            %% @todo Make this to the function.
            {ValueType, {MergedProvenanceStore0, _, _}} =
                ValueType:merge(
                    {ValueType, {ValueType:new_provenance_store(SubValueType),
                                 SubsetEventsSurvivedA,
                                 {ev_set, AllEventsEVA}}},
                    {ValueType, {SubProvenanceStoreB,
                                 SubsetEventsSurvivedB,
                                 {ev_set, AllEventsEVB}}}),
            MergedProvenanceStore0;
        false ->
            %% Find the removed events from A.
            RemovedEventsA =
                ordsets:subtract(AllEventsEVA, SubsetEventsSurvivedA),
            %% Removed the `RemovedEventsA' from B's SubProvenanceStore.
            %% @todo Make this to the function.
            ordsets:fold(
                fun({ValueDeltaPayload0, ValueProvenance0},
                    AccInSubProvenanceStoreB0) ->
                    case ordsets:is_subset(
                        state_ps_type:get_events_from_provenance(
                            ValueProvenance0),
                        RemovedEventsA) of
                        true ->
                            AccInSubProvenanceStoreB0;
                        false ->
                            ordsets:add_element(
                                {ValueDeltaPayload0, ValueProvenance0},
                                AccInSubProvenanceStoreB0)
                    end
                end,
                ordsets:new(),
                SubProvenanceStoreB)
    end;
merge_sub_provenance_store({ValueType, SubValueType},
                           {{ok, SubProvenanceStoreA},
                            SubsetEventsSurvivedA,
                            {ev_set, AllEventsEVA}},
                           {error,
                            SubsetEventsSurvivedB,
                            {ev_set, AllEventsEVB}}) ->
    case state_ps_type:is_ps_type(ValueType) of
        true ->
            %% @todo Make this to the function.
            {ValueType, {MergedProvenanceStore0, _, _}} =
                ValueType:merge(
                    {ValueType, {SubProvenanceStoreA,
                                 SubsetEventsSurvivedA,
                                 {ev_set, AllEventsEVA}}},
                    {ValueType, {ValueType:new_provenance_store(SubValueType),
                                 SubsetEventsSurvivedB,
                                 {ev_set, AllEventsEVB}}}),
            MergedProvenanceStore0;
        false ->
            %% Find the removed events from B.
            RemovedEventsB =
                ordsets:subtract(AllEventsEVB, SubsetEventsSurvivedB),
            %% Removed the `RemovedEventsB' from A's SubProvenanceStore.
            %% @todo Make this to the function.
            ordsets:fold(
                fun({ValueDeltaPayload0, ValueProvenance0},
                    AccInSubProvenanceStoreA0) ->
                    case ordsets:is_subset(
                        state_ps_type:get_events_from_provenance(
                            ValueProvenance0),
                        RemovedEventsB) of
                        true ->
                            AccInSubProvenanceStoreA0;
                        false ->
                            ordsets:add_element(
                                {ValueDeltaPayload0, ValueProvenance0},
                                AccInSubProvenanceStoreA0)
                    end
                end,
                ordsets:new(),
                SubProvenanceStoreA)
    end.

%% @private
get_value_local_payload(_Key, ValueType, ProvenanceStore) ->
    %% @todo
    %% This is not the optimised way to get the local payload of the value.
    %% The local payloads need to be stored to/retrieved from the storage.
    ordsets:fold(
        fun({ValueDeltaPayload0, _ValueProvenance0}, AccInLocalPayload) ->
            ValueType:merge(
                AccInLocalPayload, {ValueType, ValueDeltaPayload0})
        end,
        ValueType:new(),
        ProvenanceStore).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    %% A default map.
    ?assertEqual(
        {?TYPE, {{{state_ps_orset_naive, []},
                  orddict:new()},
                 ordsets:new(),
                 {ev_set, ordsets:new()}}},
        new()),

    %% A map with the `state_gcounter'.
    ?assertEqual(
        {?TYPE, {{{state_gcounter, []},
                  orddict:new()},
                 ordsets:new(),
                 {ev_set, ordsets:new()}}},
        new([{state_gcounter, []}])),

    %% A nested map with the map with the `state_ps_orset_naive'.
    ?assertEqual(
        {?TYPE, {{{state_ps_ormap_naive, [{state_ps_orset_naive, []}]},
                  orddict:new()},
                 ordsets:new(),
                 {ev_set, ordsets:new()}}},
        new([{state_ps_ormap_naive, [{state_ps_orset_naive, []}]}])).

query_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    %% A default map.
    Map0 = new(),
    Map1 = {?TYPE, {{{state_ps_orset_naive, []},
                     [{"a", [{17, [[{EventId1, 2}]]}]},
                      {"b", [{3, [[{EventId1, 1}]]},
                             {13, [[{EventId1, 3}]]}]}]},
                    [{EventId1, 1}, {EventId1, 2}, {EventId1, 3}],
                    {ev_set, [{EventId1, 1}, {EventId1, 2}, {EventId1, 3}]}}},
    ?assertEqual(orddict:new(), query(Map0)),
    ?assertEqual([{"a", sets:from_list([17])}, {"b", sets:from_list([3, 13])}],
                 query(Map1)),

    %% A map with the `state_gcounter'.
    Map2 = new([{state_gcounter, []}]),
    Map3 = {?TYPE, {{{state_gcounter, []},
                     [{"a", [{[{EventId1, 1}], [[{EventId1, 2}]]}]},
                      {"b", [{[{EventId1, 1}], [[{EventId1, 1}]]},
                             {[{EventId1, 2}], [[{EventId1, 3}]]},
                             {[{EventId2, 1}], [[{EventId2, 1}]]}]}]},
                    [{EventId1, 1}, {EventId1, 2},
                     {EventId1, 3}, {EventId2, 1}],
                    {ev_set, [{EventId1, 1}, {EventId1, 2},
                              {EventId1, 3}, {EventId2, 1}]}}},
    ?assertEqual(orddict:new(), query(Map2)),
    ?assertEqual([{"a", 1}, {"b", 3}], query(Map3)),

    %% A nested map with the map with the `state_ps_orset_naive'.
    Map4 = new([{state_ps_ormap_naive, [{state_ps_orset_naive, []}]}]),
    Map5 = {?TYPE, {{{state_ps_ormap_naive, [{state_ps_orset_naive, []}]},
                     [{"a", {{state_ps_orset_naive, []},
                             [{"a1", [{17, [[{EventId1, 2}]]}]},
                              {"a2", [{3, [[{EventId1, 1}]]},
                                      {13, [[{EventId2, 2}]]}]}]}},
                      {"b", {{state_ps_orset_naive, []},
                             [{"b1", [{7, [[{EventId2, 1}]]}]},
                              {"b2", [{23, [[{EventId1, 3}]]}]}]}}]},
                    [{EventId1, 1}, {EventId1, 2}, {EventId1, 3},
                     {EventId2, 1}, {EventId2, 2}],
                    {ev_set, [{EventId1, 1}, {EventId1, 2}, {EventId1, 3},
                              {EventId2, 1}, {EventId2, 2}]}}},
    ?assertEqual(orddict:new(), query(Map4)),
    ?assertEqual([{"a", [{"a1", sets:from_list([17])},
                         {"a2", sets:from_list([3, 13])}]},
                  {"b", [{"b1", sets:from_list([7])},
                         {"b2", sets:from_list([23])}]}],
                 query(Map5)).

delta_apply_test() ->
    EventId = {<<"object1">>, a},
    %% A default map.
    Map0 = new(),
    {ok, {?TYPE, Delta1}} =
        delta_mutate({apply, "b", {add, 3}}, EventId, Map0),
    Map1 = merge({?TYPE, Delta1}, Map0),
    {ok, {?TYPE, Delta2}} =
        delta_mutate({apply, "a", {add, 17}}, EventId, Map1),
    Map2 = merge({?TYPE, Delta2}, Map1),
    {ok, {?TYPE, Delta3}} =
        delta_mutate({apply, "b", {add, 13}}, EventId, Map2),
    Map3 = merge({?TYPE, Delta3}, Map2),
    {ok, {?TYPE, Delta4}} =
        delta_mutate({apply, "b", {rmv, 3}}, EventId, Map3),
    Map4 = merge({?TYPE, Delta4}, Map3),
    ?assertEqual({?TYPE, {{{state_ps_orset_naive, []},
                           [{"b", [{3, [[{EventId, 1}]]}]}]},
                          [{EventId, 1}],
                          {ev_set, [{EventId, 1}]}}},
                 {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {{{state_ps_orset_naive, []},
                           [{"b", [{3, [[{EventId, 1}]]}]}]},
                          [{EventId, 1}],
                          {ev_set, [{EventId, 1}]}}},
                 Map1),
    ?assertEqual({?TYPE, {{{state_ps_orset_naive, []},
                           [{"a", [{17, [[{EventId, 2}]]}]}]},
                          [{EventId, 2}],
                          {ev_set, [{EventId, 2}]}}},
                 {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {{{state_ps_orset_naive, []},
                           [{"a", [{17, [[{EventId, 2}]]}]},
                            {"b", [{3, [[{EventId, 1}]]}]}]},
                          [{EventId, 1}, {EventId, 2}],
                          {ev_set, [{EventId, 1}, {EventId, 2}]}}},
                 Map2),
    ?assertEqual({?TYPE, {{{state_ps_orset_naive, []},
                           [{"b", [{13, [[{EventId, 3}]]}]}]},
                          [{EventId, 3}],
                          {ev_set, [{EventId, 3}]}}},
                 {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {{{state_ps_orset_naive, []},
                           [{"a", [{17, [[{EventId, 2}]]}]},
                            {"b", [{3, [[{EventId, 1}]]},
                                   {13, [[{EventId, 3}]]}]}]},
                          [{EventId, 1}, {EventId, 2}, {EventId, 3}],
                          {ev_set, [{EventId, 1},
                                    {EventId, 2},
                                    {EventId, 3}]}}},
                 Map3),
    ?assertEqual({?TYPE, {{{state_ps_orset_naive, []},
                           []},
                          [],
                          {ev_set, [{EventId, 1}]}}},
                 {?TYPE, Delta4}),
    ?assertEqual({?TYPE, {{{state_ps_orset_naive, []},
                           [{"a", [{17, [[{EventId, 2}]]}]},
                            {"b", [{13, [[{EventId, 3}]]}]}]},
                          [{EventId, 2}, {EventId, 3}],
                          {ev_set, [{EventId, 1},
                                    {EventId, 2},
                                    {EventId, 3}]}}},
                 Map4).

apply_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    %% A map with the `state_gcounter'.
    Map0 = new([{state_gcounter, []}]),
    {ok, Map1} = mutate({apply, "b", increment}, EventId1, Map0),
    {ok, Map2} = mutate({apply, "a", increment}, EventId1, Map1),
    {ok, Map3} = mutate({apply, "b", increment}, EventId2, Map2),
    {ok, Map4} = mutate({apply, "b", increment}, EventId1, Map3),
    ?assertEqual({?TYPE, {{{state_gcounter, []},
                           [{"b", [{[{EventId1, 1}], [[{EventId1, 1}]]}]}]},
                          [{EventId1, 1}],
                          {ev_set, [{EventId1, 1}]}}},
                 Map1),
    ?assertEqual({?TYPE, {{{state_gcounter, []},
                           [{"a", [{[{EventId1, 1}], [[{EventId1, 2}]]}]},
                            {"b", [{[{EventId1, 1}], [[{EventId1, 1}]]}]}]},
                          [{EventId1, 1}, {EventId1, 2}],
                          {ev_set, [{EventId1, 1}, {EventId1, 2}]}}},
                 Map2),
    ?assertEqual({?TYPE, {{{state_gcounter, []},
                           [{"a", [{[{EventId1, 1}], [[{EventId1, 2}]]}]},
                            {"b", [{[{EventId1, 1}], [[{EventId1, 1}]]},
                                   {[{EventId2, 1}], [[{EventId2, 1}]]}]}]},
                          [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                          {ev_set, [{EventId1, 1},
                                    {EventId1, 2},
                                    {EventId2, 1}]}}},
                 Map3),
    ?assertEqual({?TYPE, {{{state_gcounter, []},
                           [{"a", [{[{EventId1, 1}], [[{EventId1, 2}]]}]},
                            {"b", [{[{EventId1, 1}], [[{EventId1, 1}]]},
                                   {[{EventId1, 2}], [[{EventId1, 3}]]},
                                   {[{EventId2, 1}], [[{EventId2, 1}]]}]}]},
                          [{EventId1, 1}, {EventId1, 2},
                           {EventId1, 3}, {EventId2, 1}],
                          {ev_set, [{EventId1, 1},
                                    {EventId1, 2},
                                    {EventId1, 3},
                                    {EventId2, 1}]}}},
                 Map4).

apply_rmv_with_nested_map_test() ->
    EventId = {<<"object1">>, a},
    %% A nested map with the map with the `state_ps_orset_naive'.
    Map0 = new([{state_ps_ormap_naive, [{state_ps_orset_naive, []}]}]),
    {ok, Map1} = mutate({apply, "a", {apply, "a1", {add, 3}}}, EventId, Map0),
    {ok, Map2} = mutate({apply, "a", {apply, "a2", {add, 7}}}, EventId, Map1),
    {ok, Map3} = mutate({apply, "b", {apply, "b1", {add, 17}}}, EventId, Map2),
    {ok, Map4} = mutate({apply, "a", {rmv, "a1"}}, EventId, Map3),
    {ok, Map5} = mutate({rmv, "b"}, EventId, Map4),
    {ok, Map6} = mutate({apply, "a", {apply, "a3", {add, 23}}}, EventId, Map5),
    {ok, Map7} = mutate({apply, "a", {rmv, "a2"}}, EventId, Map6),
    {ok, Map8} = mutate({apply, "a", {rmv, "a3"}}, EventId, Map7),
    Query3 = query(Map3),
    Query4 = query(Map4),
    Query5 = query(Map5),
    Query6 = query(Map6),
    Query7 = query(Map7),
    Query8 = query(Map8),

    ?assertEqual([{"a", [{"a1", sets:from_list([3])},
                         {"a2", sets:from_list([7])}]},
                  {"b", [{"b1", sets:from_list([17])}]}],
                 Query3),
    ?assertEqual([{"a", [{"a2", sets:from_list([7])}]},
                  {"b", [{"b1", sets:from_list([17])}]}],
                 Query4),
    ?assertEqual([{"a", [{"a2", sets:from_list([7])}]}],
                 Query5),
    ?assertEqual([{"a", [{"a2", sets:from_list([7])},
                         {"a3", sets:from_list([23])}]}],
                 Query6),
    ?assertEqual([{"a", [{"a3", sets:from_list([23])}]}],
                 Query7),
    ?assertEqual(orddict:new(), Query8).

equal_test() ->
    EventId = {<<"object1">>, a},
    Map1 = {?TYPE, {{{state_ps_orset_naive, []},
                     [{"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}]}}},
    Map2 = {?TYPE, {{{state_ps_orset_naive, []},
                     [{"a", [{17, [[{EventId, 2}]]}]},
                      {"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}, {EventId, 2}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    Map3 = {?TYPE, {{{state_ps_orset_naive, []},
                     [{"a", [{17, [[{EventId, 2}]]}]},
                      {"b", [{3, [[{EventId, 1}]]},
                             {13, [[{EventId, 3}]]}]}]},
                    [{EventId, 1}, {EventId, 2}, {EventId, 3}],
                    {ev_set, [{EventId, 1},
                              {EventId, 2},
                              {EventId, 3}]}}},
    ?assert(equal(Map1, Map1)),
    ?assertNot(equal(Map1, Map2)),
    ?assertNot(equal(Map1, Map3)).

is_bottom_test() ->
    EventId = {<<"object1">>, a},
    Map0 = new(),
    Map1 = {?TYPE, {{{state_ps_orset_naive, []},
                     [{"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}]}}},
    ?assert(is_bottom(Map0)),
    ?assertNot(is_bottom(Map1)).

is_inflation_test() ->
    EventId = {<<"object1">>, a},
    Map1 = {?TYPE, {{{state_ps_orset_naive, []},
                     [{"a", [{17, [[{EventId, 2}]]}]},
                      {"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}, {EventId, 2}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    %% mutate({apply, "b", {add, 13}}, EventId, Map1),
    Map2 = {?TYPE, {{{state_ps_orset_naive, []},
                     [{"a", [{17, [[{EventId, 2}]]}]},
                      {"b", [{3, [[{EventId, 1}]]},
                             {13, [[{EventId, 3}]]}]}]},
                    [{EventId, 1}, {EventId, 2}, {EventId, 3}],
                    {ev_set, [{EventId, 1},
                              {EventId, 2},
                              {EventId, 3}]}}},
    %% mutate({remove, "a"}, EventId, Map1),
    Map3 = {?TYPE, {{{state_ps_orset_naive, []},
                     [{"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    ?assert(is_inflation(Map1, Map1)),
    ?assertNot(is_inflation(Map2, Map3)),
    ?assertNot(is_inflation(Map3, Map2)),
    ?assert(is_inflation(Map1, Map2)),
    ?assert(is_inflation(Map1, Map3)),
    %% check inflation with merge
    ?assert(state_ps_type:is_inflation(Map1, Map1)),
    ?assertNot(state_ps_type:is_inflation(Map2, Map3)),
    ?assertNot(state_ps_type:is_inflation(Map3, Map2)),
    ?assert(state_ps_type:is_inflation(Map1, Map2)),
    ?assert(state_ps_type:is_inflation(Map1, Map3)).

is_strict_inflation_test() ->
    EventId = {<<"object1">>, a},
    Map1 = {?TYPE, {{{state_ps_orset_naive, []},
                     [{"a", [{17, [[{EventId, 2}]]}]},
                      {"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}, {EventId, 2}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    %% mutate({apply, "b", {add, 13}}, EventId, Map1),
    Map2 = {?TYPE, {{{state_ps_orset_naive, []},
                     [{"a", [{17, [[{EventId, 2}]]}]},
                      {"b", [{3, [[{EventId, 1}]]},
                             {13, [[{EventId, 3}]]}]}]},
                    [{EventId, 1}, {EventId, 2}, {EventId, 3}],
                    {ev_set, [{EventId, 1},
                              {EventId, 2},
                              {EventId, 3}]}}},
    %% mutate({remove, "a"}, EventId, Map1),
    Map3 = {?TYPE, {{{state_ps_orset_naive, []},
                     [{"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    ?assertNot(is_strict_inflation(Map1, Map1)),
    ?assertNot(is_strict_inflation(Map2, Map3)),
    ?assertNot(is_strict_inflation(Map3, Map2)),
    ?assert(is_strict_inflation(Map1, Map2)),
    ?assert(is_strict_inflation(Map1, Map3)).

encode_decode_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Map = {?TYPE, {{{state_gcounter, []},
                    [{"a", [{[{EventId1, 1}], [[{EventId1, 2}]]}]},
                     {"b", [{[{EventId1, 1}], [[{EventId1, 1}]]},
                            {[{EventId1, 1}], [[{EventId1, 3}]]},
                            {[{EventId2, 1}], [[{EventId2, 1}]]}]}]},
                   [{EventId1, 1}, {EventId1, 2},
                    {EventId1, 3}, {EventId2, 1}],
                   {ev_set, [{EventId1, 1},
                             {EventId1, 2},
                             {EventId1, 3},
                             {EventId2, 1}]}}},
    Binary = encode(erlang, Map),
    ESet = decode(erlang, Binary),
    ?assertEqual(Map, ESet).

-endif.
