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

%% @doc LWWRegister CRDT with the provenance semiring:
%%      last write win register.

-module(state_ps_lwwregister_naive).
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

-export_type([state_ps_lwwregister_naive/0,
              state_ps_lwwregister_naive_op/0]).

-type value() :: term().
-type knowledge() :: ordsets:ordset(state_ps_type:state_ps_event()).
-type state_ps_provenance_store() :: {value(),
                                      knowledge(),
                                      state_ps_type:state_ps_provenance()}.
-type payload() :: {state_ps_provenance_store(),
                    state_ps_type:state_ps_subset_events(),
                    state_ps_type:state_ps_all_events()}.
-opaque state_ps_lwwregister_naive() :: {?TYPE, payload()}.
-type state_ps_lwwregister_naive_op() :: {set, value()}.

%% @doc Create a new, empty provenance store for `state_ps_lwwregister_naive()'.
-spec new_provenance_store([term()]) -> state_ps_provenance_store().
new_provenance_store([]) ->
    {undefined, ordsets:new(), ordsets:new()}.

%% @doc Return all events in a provenance store.
-spec get_events_from_provenance_store(state_ps_provenance_store()) ->
    ordsets:ordset(state_ps_type:state_ps_event()).
get_events_from_provenance_store({undefined, _Knowledge, _Provenance}) ->
    ordsets:new();
get_events_from_provenance_store({_Value, Knowledge, _Provenance}) ->
    Knowledge.

%% @doc Create a new, empty `state_ps_lwwregister_naive()'.
-spec new() -> state_ps_lwwregister_naive().
new() ->
    {?TYPE, {new_provenance_store([]),
             ordsets:new(),
             {ev_set, ordsets:new()}}}.

%% @doc Create a new, empty `state_ps_lwwregister_naive()'
-spec new([term()]) -> state_ps_lwwregister_naive().
new([_]) ->
    new().

%% @doc Mutate a `state_ps_lwwregister_naive()'.
-spec mutate(
    state_ps_lwwregister_naive_op(), type:id(), state_ps_lwwregister_naive()) ->
    {ok, state_ps_lwwregister_naive()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_ps_type:mutate(Op, Actor, CRDT).

%% @doc Returns the value of the `state_ps_lwwregister_naive()'.
-spec query(state_ps_lwwregister_naive()) -> term().
query({?TYPE, {{Value, _Knowledge, _Provenance}, _SubsetEvents, _AllEvents}}) ->
    Value.

%% @doc Equality for `state_ps_lwwregister_naive()'.
%%      Since everything is ordered, == should work.
-spec equal(state_ps_lwwregister_naive(), state_ps_lwwregister_naive()) ->
    boolean().
equal({?TYPE, {ProvenanceStoreA, SubsetEventsA, AllEventsA}},
      {?TYPE, {ProvenanceStoreB, SubsetEventsB, AllEventsB}}) ->
    ProvenanceStoreA == ProvenanceStoreB andalso
    SubsetEventsA == SubsetEventsB andalso
    state_ps_type:equal_all_events(AllEventsA, AllEventsB).

%% @doc Delta-mutate a `state_ps_lwwregister_naive()'.
%%      The first argument can only be `{set, value()}'.
%%      The second argument is the event id ({object_id, replica_id}).
%%      The third argument is the `state_ps_lwwregister_naive()' to be inflated.
-spec delta_mutate(state_ps_lwwregister_naive_op(),
                   type:id(),
                   state_ps_lwwregister_naive()) ->
    {ok, state_ps_lwwregister_naive()}.
%% Write the value of the `state_ps_lwwregister_naive()'.
%% Delta: {{Value, Knowledge, [[NewEvent]]}, [NewEvent], {ev_set, [NewEvent]}}
delta_mutate({set, Value},
             Actor,
             {?TYPE, {{_PrevValue, PrevKnowledge, _PrevProvenance},
                      _SubsetEventsSurvived,
                      {ev_set, AllEventsEV}}}) ->
    %% Get next Event from AllEvents.
    NextEvent = state_ps_type:get_next_event(Actor, {ev_set, AllEventsEV}),
    %% Make a new Provenance from the Event.
    DeltaProvenance =
        ordsets:add_element(
            ordsets:add_element(NextEvent, ordsets:new()), ordsets:new()),

    Knowledge =
        ordsets:add_element(NextEvent, PrevKnowledge),

    DeltaProvenanceStore =
        {Value, Knowledge, DeltaProvenance},
    {ok, {?TYPE, {DeltaProvenanceStore,
                  ordsets:add_element(NextEvent, ordsets:new()),
                  {ev_set, ordsets:add_element(NextEvent, ordsets:new())}}}}.

%% @doc Merge two `state_ps_lwwregister_naive()'.
-spec merge(state_ps_lwwregister_naive(), state_ps_lwwregister_naive()) ->
    state_ps_lwwregister_naive().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun merge_survived_ev_set_all_events/2,
    state_ps_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Check if a `state_ps_lwwregister_naive()' is bottom
-spec is_bottom(state_ps_lwwregister_naive()) -> boolean().
is_bottom({?TYPE, _}=CRDT) ->
    CRDT == new().

%% @doc Given two `state_ps_lwwregister_naive()', check if the second is an
%%      inflation of the first.
-spec is_inflation(
    state_ps_lwwregister_naive(), state_ps_lwwregister_naive()) -> boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(
    state_ps_lwwregister_naive(), state_ps_lwwregister_naive()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_strict_inflation(CRDT1, CRDT2).

-spec encode(state_ps_type:format(), state_ps_lwwregister_naive()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_ps_type:format(), binary()) -> state_ps_lwwregister_naive().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.

%% @private
merge_survived_ev_set_all_events(
    {?TYPE, {{ValueA, KnowledgeA, ProvenanceA},
             SubsetEventsSurvivedA,
             {ev_set, AllEventsEVA}}},
    {?TYPE, {{ValueB, KnowledgeB, ProvenanceB},
             SubsetEventsSurvivedB,
             {ev_set, AllEventsEVB}}}) ->
    MergedAllEventsEV = {ev_set, ordsets:union(AllEventsEVA, AllEventsEVB)},
    MergedSubsetEventsSurvived =
        ordsets:union(
            [ordsets:intersection(
                SubsetEventsSurvivedA, SubsetEventsSurvivedB)] ++
            [ordsets:subtract(
                SubsetEventsSurvivedA, AllEventsEVB)] ++
            [ordsets:subtract(
                SubsetEventsSurvivedB, AllEventsEVA)]),
    ProvenanceStoreA =
        case ProvenanceA of
            [] ->
                new_provenance_store([]);
            [[EventA]] ->
                case ordsets:is_element(EventA, MergedSubsetEventsSurvived) of
                    false ->
                        new_provenance_store([]);
                    true ->
                        {ValueA, KnowledgeA, ProvenanceA}
                end
        end,
    ProvenanceStoreB =
        case ProvenanceB of
            [] ->
                new_provenance_store([]);
            [[EventB]] ->
                case ordsets:is_element(EventB, MergedSubsetEventsSurvived) of
                    false ->
                        new_provenance_store([]);
                    true ->
                        {ValueB, KnowledgeB, ProvenanceB}
                end
        end,
    {MergedValue, MergedKnowledge, MergedProvenance} =
        find_later_write(
            ProvenanceStoreA, ProvenanceStoreB, MergedSubsetEventsSurvived),
    {?TYPE, {{MergedValue, MergedKnowledge, MergedProvenance},
             MergedSubsetEventsSurvived,
             MergedAllEventsEV}}.

%% @private
find_later_write(
    {undefined, _, _},
    {ValueB, KnowledgeB, ProvenanceB},
    SubsetEventsSurvived) ->
    {ValueB,
     ordsets:intersection(KnowledgeB, SubsetEventsSurvived),
     ProvenanceB};
find_later_write(
    {ValueA, KnowledgeA, ProvenanceA},
    {undefined, _, _},
    SubsetEventsSurvived) ->
    {ValueA,
     ordsets:intersection(KnowledgeA, SubsetEventsSurvived),
     ProvenanceA};
find_later_write(
    {ValueA, KnowledgeA, [[EventA]]=ProvenanceA},
    {ValueB, KnowledgeB, [[EventB]]=ProvenanceB},
    SubsetEventsSurvived) ->
    MergedKnowledge =
        ordsets:intersection(
            ordsets:union(KnowledgeA, KnowledgeB), SubsetEventsSurvived),
    case ordsets:is_element(EventA, KnowledgeB) of
        true ->
            {ValueB, MergedKnowledge, ProvenanceB};
        false ->
            case ordsets:is_element(EventB, KnowledgeA) of
                true ->
                    {ValueA, MergedKnowledge, ProvenanceA};
                false ->
                    %% Conflict updates?
                    TotalCntA = ordsets:size(KnowledgeA),
                    TotalCntB = ordsets:size(KnowledgeB),
                    case TotalCntA == TotalCntB of
                        true ->
                            {{_, ReplicaIdA}, _} = EventA,
                            {{_, ReplicaIdB}, _} = EventB,
                            case ReplicaIdA > ReplicaIdB of
                                true ->
                                    {ValueA, MergedKnowledge, ProvenanceA};
                                false ->
                                    {ValueB, MergedKnowledge, ProvenanceB}
                            end;
                        false ->
                            case TotalCntA > TotalCntB of
                                true ->
                                    {ValueA, MergedKnowledge, ProvenanceA};
                                false ->
                                    {ValueB, MergedKnowledge, ProvenanceB}
                            end
                    end
            end
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual(
        {?TYPE, {new_provenance_store([]),
                 ordsets:new(),
                 {ev_set, ordsets:new()}}},
        new()).

query_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Register0 = new(),
    Register1 =
        {?TYPE, {{5, [{EventId1, 1}, {EventId2, 1}], [[{EventId2, 1}]]},
                 [{EventId1, 1}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId2, 1}]}}},
    ?assertEqual(undefined, query(Register0)),
    ?assertEqual(5, query(Register1)).

delta_set_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Register0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate({set, 8}, EventId1, Register0),
    Register1 = merge({?TYPE, Delta1}, Register0),
    {ok, {?TYPE, Delta2}} = delta_mutate({set, 1}, EventId2, Register1),
    Register2 = merge({?TYPE, Delta2}, Register1),
    {ok, {?TYPE, Delta3}} = delta_mutate({set, 5}, EventId1, Register2),
    Register3 = merge({?TYPE, Delta3}, Register2),
    ?assertEqual({?TYPE, {{8, [{EventId1, 1}], [[{EventId1, 1}]]},
                          [{EventId1, 1}],
                          {ev_set, [{EventId1, 1}]}}}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {{8, [{EventId1, 1}], [[{EventId1, 1}]]},
                          [{EventId1, 1}],
                          {ev_set, [{EventId1, 1}]}}}, Register1),
    ?assertEqual(
        {?TYPE, {{1, [{EventId1, 1}, {EventId2, 1}], [[{EventId2, 1}]]},
                 [{EventId2, 1}],
                 {ev_set, [{EventId2, 1}]}}}, {?TYPE, Delta2}),
    ?assertEqual(
        {?TYPE, {{1, [{EventId1, 1}, {EventId2, 1}], [[{EventId2, 1}]]},
                 [{EventId1, 1}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId2, 1}]}}}, Register2),
    ?assertEqual(
        {?TYPE, {{5,
                  [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                  [[{EventId1, 2}]]},
                 [{EventId1, 2}],
                 {ev_set, [{EventId1, 2}]}}}, {?TYPE, Delta3}),
    ?assertEqual(
        {?TYPE, {{5,
                  [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                  [[{EventId1, 2}]]},
                 [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}]}}},
        Register3).

set_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Register0 = new(),
    {ok, Register1} = mutate({set, 8}, EventId1, Register0),
    {ok, Register2} = mutate({set, 1}, EventId2, Register1),
    {ok, Register3} = mutate({set, 5}, EventId1, Register2),
    ?assertEqual({?TYPE, {{8, [{EventId1, 1}], [[{EventId1, 1}]]},
                          [{EventId1, 1}],
                          {ev_set, [{EventId1, 1}]}}}, Register1),
    ?assertEqual(
        {?TYPE, {{1, [{EventId1, 1}, {EventId2, 1}], [[{EventId2, 1}]]},
                 [{EventId1, 1}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId2, 1}]}}}, Register2),
    ?assertEqual(
        {?TYPE, {{5,
                  [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                  [[{EventId1, 2}]]},
                 [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}]}}},
        Register3).

merge_idempotent_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Register1 =
        {?TYPE, {{1, [{EventId1, 1}, {EventId2, 1}], [[{EventId2, 1}]]},
                 [{EventId1, 1}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId2, 1}]}}},
    Register2 =
        {?TYPE, {{5,
                  [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                  [[{EventId1, 2}]]},
                 [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}]}}},
    Register3 = merge(Register1, Register1),
    Register4 = merge(Register2, Register2),
    ?assertEqual(Register1, Register3),
    ?assertEqual(Register2, Register4).

merge_commutative_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Register1 =
        {?TYPE, {{1, [{EventId1, 1}, {EventId2, 1}], [[{EventId2, 1}]]},
                 [{EventId1, 1}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId2, 1}]}}},
    Register2 =
        {?TYPE, {{5,
                  [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                  [[{EventId1, 2}]]},
                 [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}]}}},
    Register3 = merge(Register1, Register2),
    Register4 = merge(Register2, Register1),
    ?assertEqual(Register2, Register3),
    ?assertEqual(Register2, Register4).

equal_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Register1 =
        {?TYPE, {{1, [{EventId1, 1}, {EventId2, 1}], [[{EventId2, 1}]]},
                 [{EventId1, 1}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId2, 1}]}}},
    Register2 =
        {?TYPE, {{5,
                  [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                  [[{EventId1, 2}]]},
                 [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}]}}},
    ?assert(equal(Register1, Register1)),
    ?assertNot(equal(Register1, Register2)).

is_bottom_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Register0 = new(),
    Register1 =
        {?TYPE, {{5, [{EventId1, 1}, {EventId2, 1}], [[{EventId2, 1}]]},
                 [{EventId1, 1}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId2, 1}]}}},
    ?assert(is_bottom(Register0)),
    ?assertNot(is_bottom(Register1)).

is_inflation_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Register1 =
        {?TYPE, {{1, [{EventId1, 1}, {EventId2, 1}], [[{EventId2, 1}]]},
                 [{EventId1, 1}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId2, 1}]}}},
    Register2 =
        {?TYPE, {{5,
                  [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                  [[{EventId1, 2}]]},
                 [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}]}}},
    ?assert(is_inflation(Register1, Register1)),
    ?assert(is_inflation(Register1, Register2)),
    ?assertNot(is_inflation(Register2, Register1)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Register1, Register1)),
    ?assert(state_type:is_inflation(Register1, Register2)),
    ?assertNot(state_type:is_inflation(Register2, Register1)).

is_strict_inflation_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Register1 =
        {?TYPE, {{1, [{EventId1, 1}, {EventId2, 1}], [[{EventId2, 1}]]},
                 [{EventId1, 1}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId2, 1}]}}},
    Register2 =
        {?TYPE, {{5,
                  [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                  [[{EventId1, 2}]]},
                 [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}]}}},
    ?assertNot(is_strict_inflation(Register1, Register1)),
    ?assert(is_strict_inflation(Register1, Register2)),
    ?assertNot(is_strict_inflation(Register2, Register1)).

encode_decode_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Register =
        {?TYPE, {{5,
                  [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                  [[{EventId1, 2}]]},
                 [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}],
                 {ev_set, [{EventId1, 1}, {EventId1, 2}, {EventId2, 1}]}}},
    Binary = encode(erlang, Register),
    ECounter = decode(erlang, Binary),
    ?assertEqual(Register, ECounter).

-endif.
