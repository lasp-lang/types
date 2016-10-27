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

-module(state_ormap_ps).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-behaviour(type).
-behaviour(state_type).
-behaviour(state_provenance_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new_provenance_store/1,
         get_events_from_provenance_store/1]).
-export([new/0, new/1,
         mutate/3,
         query/1,
         equal/2]).
-export([new_delta/0, new_delta/1,
         is_delta/1,
         delta_mutate/3,
         merge/2,
         is_bottom/1,
         is_inflation/2,
         is_strict_inflation/2,
         join_decomposition/1,
         encode/2,
         decode/2]).

-export_type([state_ormap_ps/0,
              delta_state_ormap_ps/0,
              state_ormap_ps_op/0]).

-type key() :: term().
-type value() :: term().
-type value_type() :: {state_type:state_type(), []}
                    | {?TYPE, [value_type()]}.
-type ps_provenance_store() :: {value_type(),
                                orddict:orddict(key(), value())}.
-type payload() :: {ps_provenance_store(),
                    state_provenance_type:ps_subset_events(),
                    state_provenance_type:ps_all_events()}.
-opaque state_ormap_ps() :: {?TYPE, payload()}.
-opaque delta_state_ormap_ps() :: {?TYPE, {delta, payload()}}.
-type delta_or_state() :: state_ormap_ps()
                        | delta_state_ormap_ps().
-type value_op() :: term().
-type state_ormap_ps_op() :: {apply, key(), value_op()}
                           | {rmv, key()}.

%% @doc Create a new, empty provenance store for `state_ormap_ps()'.
-spec new_provenance_store([value_type()]) -> ps_provenance_store().
new_provenance_store([{ValueType, SubValueType}]) ->
    {{ValueType, SubValueType}, orddict:new()}.

%% @doc Return all events in a provenance store.
-spec get_events_from_provenance_store(ps_provenance_store()) ->
    ordsets:ordsets(state_provenance_type:ps_event()).
get_events_from_provenance_store({{ValueType, _SubValueType},
                                  ValueProvenanceStore}) ->
    orddict:fold(
        fun(_Key, SubProvenanceStore, AccInEvents) ->
            EventsFromSubProvenanceStore =
                case state_provenance_type:is_provenance_type(ValueType) of
                    true ->
                        ValueType:get_events_from_provenance_store(
                            SubProvenanceStore);
                    false ->
                        ordsets:fold(
                            fun({_ValueDeltaPayload, ValueProvenance},
                                AccInEventsFromSubProvenanceStore) ->
                                EventsFromSubPS =
                                    state_provenance_type:
                                        get_events_from_provenance(
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

%% @doc Create a new, empty `state_ormap_ps()'.
%%      By default the values are a state_orset_ps CRDT.
-spec new() -> state_ormap_ps().
new() ->
    new([{state_orset_ps, []}]).

%% @doc Create a new, empty `state_ormap_ps()'
-spec new([term()]) -> state_ormap_ps().
new([{ValueType, SubValueType}]) ->
    {?TYPE, {new_provenance_store([{ValueType, SubValueType}]),
             ordsets:new(),
             {ev_set, ordsets:new()}}}.

%% @doc Mutate a `state_ormap_ps()'.
-spec mutate(state_ormap_ps_op(), type:id(), state_ormap_ps()) ->
    {ok, state_ormap_ps()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Returns the value of the `state_ormap_ps()'.
%%      This value is a dictionary where keys are the same as the
%%      `state_ormap_ps()' and values are the return values of `query/1'.
-spec query(state_ormap_ps()) -> term().
query({?TYPE, {{{ValueType, _SubValueType}, ValueProvenanceStore},
               SubsetEvents,
               AllEvents}}) ->
    orddict:fold(
        fun(Key, SubProvenanceStore, AccInResultMap) ->
            Value =
                case state_provenance_type:is_provenance_type(ValueType) of
                    true ->
                        ValueType:query({ValueType, {SubProvenanceStore,
                                                     SubsetEvents,
                                                     AllEvents}});
                    false ->
                        ValueTypeCRDT =
                            ordsets:fold(
                                fun({ValueDeltaPayload, _ValueProvenance},
                                    AccInValueTypeCRDT) ->
                                    ValueType:merge(
                                        AccInValueTypeCRDT,
                                        {ValueType, ValueDeltaPayload})
                                end,
                                ValueType:new(),
                                SubProvenanceStore),
                        ValueType:query(ValueTypeCRDT)
                end,
            orddict:store(Key, Value, AccInResultMap)
        end,
        orddict:new(),
        ValueProvenanceStore).

%% @doc Equality for `state_ormap_ps()'.
%%      Since everything is ordered, == should work.
-spec equal(state_ormap_ps(), state_ormap_ps()) -> boolean().
equal({?TYPE, {{ValueTypeA, ValueProvenanceStoreA},
               SubsetEventsA,
               AllEventsA}},
      {?TYPE, {{ValueTypeB, ValueProvenanceStoreB},
               SubsetEventsB,
               AllEventsB}}) ->
    ValueTypeA == ValueTypeB andalso
    ValueProvenanceStoreA == ValueProvenanceStoreB andalso
    SubsetEventsA == SubsetEventsB andalso
    state_provenance_type:equal_all_events(AllEventsA, AllEventsB).

-spec new_delta() -> delta_state_ormap_ps().
new_delta() ->
    new_delta([{state_orset_ps, []}]).

-spec new_delta([term()]) -> delta_state_ormap_ps().
new_delta([{ValueType, SubValueType}]) ->
    {?TYPE, {delta, {new_provenance_store([{ValueType, SubValueType}]),
                     ordsets:new(),
                     {ev_set, ordsets:new()}}}}.

-spec is_delta(delta_or_state()) -> boolean().
is_delta({?TYPE, _}=CRDT) ->
    state_type:is_delta(CRDT).

%% @doc Delta-mutate a `state_ormap_ps()'.
%%      The first argument can be:
%%          - `{apply, Key, Op}'
%%          - `{rmv, Key}'
%%          `apply' receives an operation that will be applied to the
%%          value of the key. This operation has to be a valid operation
%%          in the CRDT chose to be in the values
%%          (by default an state_orset_ps).
%%      The second argument is the event id ({object_id, replica_id}).
%%      The third argument is the `state_ormap_ps()' to be inflated.
-spec delta_mutate(state_ormap_ps_op(), type:id(), state_ormap_ps()) ->
    {ok, delta_state_ormap_ps()}.
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
                case state_provenance_type:is_provenance_type(ValueType) of
                    true ->
                        ValueType:new_provenance_store(SubValueType);
                    false ->
                        ordsets:new()
                end
        end,
    {DeltaProvenanceStore, DeltaSubsetEvents, DeltaAllEvents} =
        case state_provenance_type:is_provenance_type(ValueType) of
            true ->
                {ok, {ValueType, {delta, {DeltaSubProvenanceStore0,
                                          DeltaSubsetEvents0,
                                          DeltaAllEvents0}}}} =
                    ValueType:delta_mutate(Op,
                                           Actor,
                                           {ValueType, {SubProvenanceStore,
                                                        SubsetEvents,
                                                        AllEvents}}),
                DeltaProvenanceStore0 =
                    case DeltaSubProvenanceStore0 ==
                         ValueType:new_provenance_store(SubValueType) of
                        true ->
                            {{ValueType, SubValueType}, orddict:new()};
                        false ->
                            {{ValueType, SubValueType},
                             orddict:store(Key,
                                           DeltaSubProvenanceStore0,
                                           orddict:new())}
                    end,
                {DeltaProvenanceStore0, DeltaSubsetEvents0, DeltaAllEvents0};
            false ->
                %% Get next Event from AllEvents.
                NextEvent =
                    state_provenance_type:get_next_event(Actor, AllEvents),
                %% Make a new Provenance from the Event.
                ValueProvenance = ordsets:add_element(
                    ordsets:add_element(NextEvent, ordsets:new()),
                    ordsets:new()),
                {ok, {ValueType, {delta, ValuePayload}}} =
                    ValueType:delta_mutate(Op, Actor, ValueType:new()),
                DeltaProvenanceStore1 =
                    case {ValueType, {delta, ValuePayload}} ==
                         ValueType:new_delta() of
                        true ->
                            {{ValueType, SubValueType}, orddict:new()};
                        false ->
                            {{ValueType, SubValueType},
                             orddict:store(
                                 Key,
                                 ordsets:add_element(
                                     {{delta, ValuePayload}, ValueProvenance},
                                     ordsets:new()),
                                 orddict:new())}
                    end,
                {DeltaProvenanceStore1,
                 ordsets:add_element(NextEvent, ordsets:new()),
                 {ev_set, ordsets:add_element(NextEvent, ordsets:new())}}
        end,
    {ok, {?TYPE, {delta, {DeltaProvenanceStore,
                          DeltaSubsetEvents,
                          DeltaAllEvents}}}};

delta_mutate({rmv, Key},
             _Actor,
             {?TYPE, {{{ValueType, SubValueType}, ProvenanceStore},
                      _SubsetEvents,
                      _AllEvents}}) ->
    case orddict:find(Key, ProvenanceStore) of
        {ok, SubProvenanceStore} ->
            RemovedAllEvents =
                case state_provenance_type:is_provenance_type(ValueType) of
                    true ->
                        ValueType:get_events_from_provenance_store(
                            SubProvenanceStore);
                    false ->
                        ordsets:fold(
                            fun({_ValueDeltaPayload0, ValueProvenance0},
                                AccInRemovedAllEvents) ->
                                EventsFromValueProvenance =
                                    state_provenance_type:
                                        get_events_from_provenance(
                                            ValueProvenance0),
                                ordsets:union(
                                    AccInRemovedAllEvents,
                                    EventsFromValueProvenance)
                            end,
                            ordsets:new(),
                            SubProvenanceStore)
                end,
            {ok, {?TYPE, {delta, {new_provenance_store(
                                    [{ValueType, SubValueType}]),
                                  ordsets:new(),
                                  {ev_set, RemovedAllEvents}}}}};
        error ->
            {ok, new_delta([{ValueType, SubValueType}])}
    end.

%% @doc Merge two `state_ormap_ps()'.
-spec merge(delta_or_state(), delta_or_state()) -> delta_or_state().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun merge_survived_ev_set_all_events/2,
    state_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Check if a `state_ormap_ps()' is bottom
-spec is_bottom(delta_or_state()) -> boolean().
is_bottom({?TYPE, {delta, {{ValueType, _ProvenanceStore},
                           _SubsetEvents,
                           _AllEvents}}}=CRDT) ->
    CRDT == new_delta([ValueType]);
is_bottom({?TYPE, {{ValueType, _ProvenanceStore},
                   _SubsetEvents,
                   _AllEvents}}=CRDT) ->
    CRDT == new([ValueType]).

%% @doc Given two `state_ormap_ps()', check if the second is an inflation
%%      of the first.
-spec is_inflation(delta_or_state(), state_ormap_ps()) -> boolean().
is_inflation({?TYPE, {delta, Map1}}, {?TYPE, Map2}) ->
    is_inflation({?TYPE, Map1}, {?TYPE, Map2});
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(delta_or_state(), state_ormap_ps()) -> boolean().
is_strict_inflation({?TYPE, {delta, Map1}}, {?TYPE, Map2}) ->
    is_strict_inflation({?TYPE, Map1}, {?TYPE, Map2});
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @todo
%% @doc Join decomposition for `state_ormap_ps()'.
-spec join_decomposition(state_ormap_ps()) -> [state_ormap_ps()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

-spec encode(state_type:format(), delta_or_state()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> delta_or_state().
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
        ordsets:union([ordsets:intersection(SubsetEventsSurvivedA,
                                            SubsetEventsSurvivedB)] ++
                      [ordsets:subtract(SubsetEventsSurvivedA,
                                        AllEventsEVB)] ++
                      [ordsets:subtract(SubsetEventsSurvivedB,
                                        AllEventsEVA)]),
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
                        orddict:store(Key,
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
    case state_provenance_type:is_provenance_type(ValueType) of
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
    case state_provenance_type:is_provenance_type(ValueType) of
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
                            state_provenance_type:get_events_from_provenance(
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
                            state_provenance_type:get_events_from_provenance(
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
    case state_provenance_type:is_provenance_type(ValueType) of
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
                        state_provenance_type:get_events_from_provenance(
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
    case state_provenance_type:is_provenance_type(ValueType) of
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
                        state_provenance_type:get_events_from_provenance(
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

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    %% A default map.
    ?assertEqual({?TYPE, {{{state_orset_ps, []}, orddict:new()},
                          ordsets:new(),
                          {ev_set, ordsets:new()}}},
                 new()),
    ?assertEqual({?TYPE, {delta, {{{state_orset_ps, []}, orddict:new()},
                                  ordsets:new(),
                                  {ev_set, ordsets:new()}}}},
                 new_delta()),

    %% A map with the `state_gcounter'.
    ?assertEqual({?TYPE, {{{state_gcounter, []}, orddict:new()},
                          ordsets:new(),
                          {ev_set, ordsets:new()}}},
                 new([{state_gcounter, []}])),
    ?assertEqual({?TYPE, {delta, {{{state_gcounter, []}, orddict:new()},
                                  ordsets:new(),
                                  {ev_set, ordsets:new()}}}},
                 new_delta([{state_gcounter, []}])),

    %% A map with the map with the `state_orset_ps'.
    ?assertEqual({?TYPE, {{{state_ormap_ps, [{state_orset_ps, []}]},
                           orddict:new()},
                          ordsets:new(),
                          {ev_set, ordsets:new()}}},
                 new([{state_ormap_ps, [{state_orset_ps, []}]}])),
    ?assertEqual({?TYPE, {delta, {{{state_ormap_ps, [{state_orset_ps, []}]},
                                   orddict:new()},
                                  ordsets:new(),
                                  {ev_set, ordsets:new()}}}},
                 new_delta([{state_ormap_ps, [{state_orset_ps, []}]}])).

query_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    %% A default map.
    Map0 = new(),
    Map1 = {?TYPE, {{{state_orset_ps, []},
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
                     [{"a", [{{delta, [{EventId1, 1}]},
                              [[{EventId1, 2}]]}]},
                      {"b", [{{delta, [{EventId1, 1}]},
                              [[{EventId1, 1}]]},
                             {{delta, [{EventId2, 1}]},
                              [[{EventId2, 1}]]}]}]},
                    [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                    {ev_set, [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}]}}},
    ?assertEqual(orddict:new(), query(Map2)),
    ?assertEqual([{"a", 1}, {"b", 2}], query(Map3)),

    %% A map with the map with the `state_orset_ps'.
    Map4 = new([{state_ormap_ps, [{state_orset_ps, []}]}]),
    Map5 = {?TYPE, {{{state_ormap_ps, [{state_orset_ps, []}]},
                     [{"a", {{state_orset_ps, []},
                             [{"a1", [{17, [[{EventId1, 2}]]}]},
                              {"a2", [{3, [[{EventId1, 1}]]},
                                      {13, [[{EventId2, 2}]]}]}]}},
                      {"b", {{state_orset_ps, []},
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
    {ok, {?TYPE, {delta, Delta1}}} =
        delta_mutate({apply, "b", {add, 3}}, EventId, Map0),
    Map1 = merge({?TYPE, Delta1}, Map0),
    {ok, {?TYPE, {delta, Delta2}}} =
        delta_mutate({apply, "a", {add, 17}}, EventId, Map1),
    Map2 = merge({?TYPE, Delta2}, Map1),
    {ok, {?TYPE, {delta, Delta3}}} =
        delta_mutate({apply, "b", {add, 13}}, EventId, Map2),
    Map3 = merge({?TYPE, Delta3}, Map2),
    {ok, {?TYPE, {delta, Delta4}}} =
        delta_mutate({apply, "b", {rmv, 3}}, EventId, Map3),
    Map4 = merge({?TYPE, Delta4}, Map3),
    ?assertEqual({?TYPE, {{{state_orset_ps, []},
                           [{"b", [{3, [[{EventId, 1}]]}]}]},
                          [{EventId, 1}],
                          {ev_set, [{EventId, 1}]}}},
                 {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {{{state_orset_ps, []},
                           [{"b", [{3, [[{EventId, 1}]]}]}]},
                          [{EventId, 1}],
                          {ev_set, [{EventId, 1}]}}},
                 Map1),
    ?assertEqual({?TYPE, {{{state_orset_ps, []},
                           [{"a", [{17, [[{EventId, 2}]]}]}]},
                          [{EventId, 2}],
                          {ev_set, [{EventId, 2}]}}},
                 {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {{{state_orset_ps, []},
                           [{"a", [{17, [[{EventId, 2}]]}]},
                            {"b", [{3, [[{EventId, 1}]]}]}]},
                          [{EventId, 1}, {EventId, 2}],
                          {ev_set, [{EventId, 1}, {EventId, 2}]}}},
                 Map2),
    ?assertEqual({?TYPE, {{{state_orset_ps, []},
                           [{"b", [{13, [[{EventId, 3}]]}]}]},
                          [{EventId, 3}],
                          {ev_set, [{EventId, 3}]}}},
                 {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {{{state_orset_ps, []},
                           [{"a", [{17, [[{EventId, 2}]]}]},
                            {"b", [{3, [[{EventId, 1}]]},
                                   {13, [[{EventId, 3}]]}]}]},
                          [{EventId, 1}, {EventId, 2}, {EventId, 3}],
                          {ev_set, [{EventId, 1},
                                    {EventId, 2},
                                    {EventId, 3}]}}},
                 Map3),
    ?assertEqual({?TYPE, {{{state_orset_ps, []},
                           []},
                          [],
                          {ev_set, [{EventId, 1}]}}},
                 {?TYPE, Delta4}),
    ?assertEqual({?TYPE, {{{state_orset_ps, []},
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
                           [{"b", [{{delta, [{EventId1, 1}]},
                                    [[{EventId1, 1}]]}]}]},
                          [{EventId1, 1}],
                          {ev_set, [{EventId1, 1}]}}},
                 Map1),
    ?assertEqual({?TYPE, {{{state_gcounter, []},
                           [{"a", [{{delta, [{EventId1, 1}]},
                                    [[{EventId1, 2}]]}]},
                            {"b", [{{delta, [{EventId1, 1}]},
                                    [[{EventId1, 1}]]}]}]},
                          [{EventId1, 1}, {EventId1, 2}],
                          {ev_set, [{EventId1, 1}, {EventId1, 2}]}}},
                 Map2),
    ?assertEqual({?TYPE, {{{state_gcounter, []},
                           [{"a", [{{delta, [{EventId1, 1}]},
                                    [[{EventId1, 2}]]}]},
                            {"b", [{{delta, [{EventId1, 1}]},
                                    [[{EventId1, 1}]]},
                                   {{delta, [{EventId2, 1}]},
                                    [[{EventId2, 1}]]}]}]},
                          [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                          {ev_set, [{EventId1, 1},
                                    {EventId1, 2},
                                    {EventId2, 1}]}}},
                 Map3),
    ?assertEqual({?TYPE, {{{state_gcounter, []},
                           [{"a", [{{delta, [{EventId1, 1}]},
                                    [[{EventId1, 2}]]}]},
                            {"b", [{{delta, [{EventId1, 1}]},
                                    [[{EventId1, 1}]]},
                                   {{delta, [{EventId1, 1}]},
                                    [[{EventId1, 3}]]},
                                   {{delta, [{EventId2, 1}]},
                                    [[{EventId2, 1}]]}]}]},
                          [{EventId1, 1}, {EventId1, 2},
                           {EventId1, 3}, {EventId2, 1}],
                          {ev_set, [{EventId1, 1},
                                    {EventId1, 2},
                                    {EventId1, 3},
                                    {EventId2, 1}]}}},
                 Map4).

apply_rmv_with_nested_map_test() ->
    EventId = {<<"object1">>, a},
    %% A map with the map with the `state_orset_ps'.
    Map0 = new([{state_ormap_ps, [{state_orset_ps, []}]}]),
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
    Map1 = {?TYPE, {{{state_orset_ps, []},
                     [{"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}]}}},
    Map2 = {?TYPE, {{{state_orset_ps, []},
                     [{"a", [{17, [[{EventId, 2}]]}]},
                      {"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}, {EventId, 2}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    Map3 = {?TYPE, {{{state_orset_ps, []},
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
    Map1 = {?TYPE, {{{state_orset_ps, []},
                     [{"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}]}}},
    ?assert(is_bottom(Map0)),
    ?assertNot(is_bottom(Map1)).

is_inflation_test() ->
    EventId = {<<"object1">>, a},
    Map1 = {?TYPE, {{{state_orset_ps, []},
                     [{"a", [{17, [[{EventId, 2}]]}]},
                      {"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}, {EventId, 2}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    %% mutate({apply, "b", {add, 13}}, EventId, Map1),
    Map2 = {?TYPE, {{{state_orset_ps, []},
                     [{"a", [{17, [[{EventId, 2}]]}]},
                      {"b", [{3, [[{EventId, 1}]]},
                             {13, [[{EventId, 3}]]}]}]},
                    [{EventId, 1}, {EventId, 2}, {EventId, 3}],
                    {ev_set, [{EventId, 1},
                              {EventId, 2},
                              {EventId, 3}]}}},
    %% mutate({remove, "a"}, EventId, Map1),
    Map3 = {?TYPE, {{{state_orset_ps, []},
                     [{"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    ?assert(is_inflation(Map1, Map1)),
    ?assertNot(is_inflation(Map2, Map3)),
    ?assertNot(is_inflation(Map3, Map2)),
    ?assert(is_inflation(Map1, Map2)),
    ?assert(is_inflation(Map1, Map3)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Map1, Map1)),
    ?assertNot(state_type:is_inflation(Map2, Map3)),
    ?assertNot(state_type:is_inflation(Map3, Map2)),
    ?assert(state_type:is_inflation(Map1, Map2)),
    ?assert(state_type:is_inflation(Map1, Map3)).

is_strict_inflation_test() ->
    EventId = {<<"object1">>, a},
    Map1 = {?TYPE, {{{state_orset_ps, []},
                     [{"a", [{17, [[{EventId, 2}]]}]},
                      {"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}, {EventId, 2}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    %% mutate({apply, "b", {add, 13}}, EventId, Map1),
    Map2 = {?TYPE, {{{state_orset_ps, []},
                     [{"a", [{17, [[{EventId, 2}]]}]},
                      {"b", [{3, [[{EventId, 1}]]},
                             {13, [[{EventId, 3}]]}]}]},
                    [{EventId, 1}, {EventId, 2}, {EventId, 3}],
                    {ev_set, [{EventId, 1},
                              {EventId, 2},
                              {EventId, 3}]}}},
    %% mutate({remove, "a"}, EventId, Map1),
    Map3 = {?TYPE, {{{state_orset_ps, []},
                     [{"b", [{3, [[{EventId, 1}]]}]}]},
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    ?assertNot(is_strict_inflation(Map1, Map1)),
    ?assertNot(is_strict_inflation(Map2, Map3)),
    ?assertNot(is_strict_inflation(Map3, Map2)),
    ?assert(is_strict_inflation(Map1, Map2)),
    ?assert(is_strict_inflation(Map1, Map3)).

%%join_decomposition_test() ->
%%    %% @todo
%%    ok.

encode_decode_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Map = {?TYPE, {{{state_gcounter, []},
                    [{"a", [{{delta, [{EventId1, 1}]},
                             [[{EventId1, 2}]]}]},
                     {"b", [{{delta, [{EventId1, 1}]},
                             [[{EventId1, 1}]]},
                            {{delta, [{EventId1, 1}]},
                             [[{EventId1, 3}]]},
                            {{delta, [{EventId2, 1}]},
                             [[{EventId2, 1}]]}]}]},
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
