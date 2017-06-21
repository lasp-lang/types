%%
%% Copyright (c) 2015-2017 Christopher Meiklejohn.  All Rights Reserved.
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

%% @doc Add-Win Observed-Remove Set CRDT with the POE OR Set design:
%%     add-wins observed-remove set without tombstones.

-module(state_ps_aworset_naive).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-behaviour(type).
-behaviour(state_ps_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    new/0, new/1,
    mutate/3,
    query/1,
    equal/2]).
-export([
    delta_mutate/3,
    merge/2,
    is_inflation/2,
    is_strict_inflation/2,
    encode/2,
    decode/2,
    get_next_event/2]).
-export([
    flatten/1,
    unflatten/1,
    length/1]).

-export_type([
    state_ps_aworset_naive/0,
    state_ps_aworset_naive_op/0]).

-type element() :: term().
-type payload() :: state_ps_poe_orset:state_ps_poe_orset().
-opaque state_ps_aworset_naive() :: {?TYPE, payload()}.
-type state_ps_aworset_naive_op() :: {add, element()}
                                   | {rmv, element()}.

%% @doc Create a new, empty `state_ps_aworset_naive()'.
-spec new() -> state_ps_aworset_naive().
new() ->
    {?TYPE, state_ps_poe_orset:new()}.

%% @doc Create a new, empty `state_ps_aworset_naive()'
-spec new([term()]) -> state_ps_aworset_naive().
new([_]) ->
    new().

%% @doc Mutate a `state_ps_aworset_naive()'.
-spec mutate(
    state_ps_aworset_naive_op(),
    type:id(),
    state_ps_aworset_naive()) -> {ok, state_ps_aworset_naive()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_ps_type:mutate(Op, Actor, CRDT).

%% @doc Returns the value of the `state_ps_aworset_naive()'.
%%      This value is a set of not-removed elements.
-spec query(state_ps_aworset_naive()) -> term().
query({?TYPE, Payload}) ->
    state_ps_poe_orset:read(Payload).

%% @doc Equality for `state_ps_aworset_naive()'.
-spec equal(state_ps_aworset_naive(), state_ps_aworset_naive()) -> boolean().
equal({?TYPE, PayloadA}, {?TYPE, PayloadB}) ->
    state_ps_poe_orset:equal(PayloadA, PayloadB).

%% @doc Delta-mutate a `state_ps_aworset_naive()'.
%%      The first argument can be:
%%          - `{add, element()}'
%%          - `{rmv, element()}'
%%      The second argument is the event id ({object_id, replica_id}).
%%      The third argument is the `state_ps_aworset_naive()' to be inflated.
-spec delta_mutate(
    state_ps_aworset_naive_op(),
    type:id(),
    state_ps_aworset_naive()) -> {ok, state_ps_aworset_naive()}.
%% Add a single element to `state_ps_aworset_naive()'.
%% Delta: {[{Elem, {{NewEvent}}}], [NewEvent], [NewEvent]}
delta_mutate({add, Elem}, Actor, {?TYPE, Payload}) ->
    %% Get next Event from AllEvents.
    NextEvent = get_next_event(Actor, Payload),
    %% Get a delta object.
    DeltaPayload =
        state_ps_poe_orset:delta_insert(NextEvent, Elem, Payload),
    {ok, {?TYPE, DeltaPayload}};

%% Remove a single element to `state_ps_aworset_naive()'.
%% Delta: {[], [], ElemEvents}
delta_mutate({rmv, Elem}, _Actor, {?TYPE, Payload}) ->
    %% Get a delta object.
    DeltaPayload = state_ps_poe_orset:delta_delete(Elem, Payload),
    {ok, {?TYPE, DeltaPayload}}.

%% @doc Merge two `state_ps_aworset_naive()'.
-spec merge(state_ps_aworset_naive(), state_ps_aworset_naive()) ->
    state_ps_aworset_naive().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun merge_state_ps_aworset_naive/2,
    state_ps_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Given two `state_ps_aworset_naive()', check if the second is an
%%     inflation of the first.
-spec is_inflation(state_ps_aworset_naive(), state_ps_aworset_naive()) ->
    boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_ps_aworset_naive(), state_ps_aworset_naive()) ->
    boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_strict_inflation(CRDT1, CRDT2).

-spec encode(state_ps_type:format(), state_ps_aworset_naive()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_ps_type:format(), binary()) -> state_ps_aworset_naive().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.

%% @doc Calculate the next event from the AllEvents.
-spec get_next_event(
    state_ps_type:state_ps_event_id(),
    state_ps_type:state_ps_payload()) -> state_ps_type:state_ps_event().
get_next_event(EventId, {_, _, AllEvents}=_Payload) ->
    MaxCnt =
        ordsets:fold(
            fun({EventType0, {EventId0, Counter0}}, AccInMaxCnt) ->
                case EventType0 == state_ps_event_partial_order_independent
                    andalso EventId0 == EventId of
                    true ->
                        max(AccInMaxCnt, Counter0);
                    false ->
                        AccInMaxCnt
                end
            end,
            0,
            AllEvents),
    {state_ps_event_partial_order_independent, {EventId, MaxCnt + 1}}.

%% @doc @todo
-spec flatten(state_ps_aworset_naive()) -> state_ps_aworset_naive().
flatten({?TYPE, {ProvenanceStore, SubsetEvents, AllEvents}=_Payload}) ->
    {FlattenedElem, FlattenedProvenance} =
        orddict:fold(
            fun(Elem, Provenance, {AccFlattenedElem, AccFlattenedProvenance}) ->
                NewElem = lists:append(AccFlattenedElem, Elem),
                NewProvenance =
                    case AccFlattenedProvenance of
                        [] ->
                            Provenance;
                        _ ->
                            state_ps_type:cross_provenance(
                                AccFlattenedProvenance, Provenance)
                    end,
                {NewElem, NewProvenance}
            end,
            {[], ordsets:new()},
            ProvenanceStore),
    NewProvenanceStore =
        orddict:store(FlattenedElem, FlattenedProvenance, orddict:new()),
    NewPayload = {NewProvenanceStore, SubsetEvents, AllEvents},
    {?TYPE, NewPayload}.

%% @doc @todo
-spec unflatten(state_ps_aworset_naive()) -> state_ps_aworset_naive().
unflatten({?TYPE, {ProvenanceStore, SubsetEvents, AllEvents}=_Payload}) ->
    [{ListElem, Provenance}] = ProvenanceStore,
    NewProvenanceStore =
        lists:foldl(
            fun(Elem, AccNewProvenanceStore) ->
                orddict:store([Elem], Provenance, AccNewProvenanceStore)
            end,
            orddict:new(),
            ListElem),
    NewPayload = {NewProvenanceStore, SubsetEvents, AllEvents},
    {?TYPE, NewPayload}.

%% @doc @todo
-spec length(state_ps_aworset_naive()) -> state_ps_type:crdt().
length({?TYPE, {ProvenanceStore, SubsetEvents, AllEvents}=_Payload}) ->
    {{SizeTPS, SizeTSE, SizeTAE}, EventsInPS} =
        orddict:fold(
            fun(_Elem, Provenance, {AccSizeTPayload, AccEventsInPS}) ->
                NewAccEventsInPS =
                    ordsets:union(
                        AccEventsInPS,
                        state_ps_type:get_events_from_provenance(Provenance)),
                NewSizeTEvent =
                    {state_ps_event_partial_order_provenance, Provenance},
                ValueAsProvenance =
                    ordsets:add_element(
                        ordsets:add_element(NewSizeTEvent, ordsets:new()),
                        ordsets:new()),
                NewAccSizeTPayload =
                    state_ps_poe_orset:insert(
                        NewSizeTEvent, ValueAsProvenance, AccSizeTPayload),
                {NewAccSizeTPayload, NewAccEventsInPS}
            end,
            {state_ps_poe_orset:new(), ordsets:new()},
            ProvenanceStore),
    FilteredOutEvents = ordsets:subtract(SubsetEvents, EventsInPS),
    LeastDominantFilteredOutProvenances =
        ordsets:fold(
            fun(FilteredOutEvent, AccIn) ->
                LeastDominantFilteredOutProvenance =
                    ordsets:add_element(
                        ordsets:add_element(FilteredOutEvent, ordsets:new()),
                        ordsets:new()),
                NewSizeTEvent =
                    {state_ps_event_partial_order_provenance,
                        LeastDominantFilteredOutProvenance},
                ordsets:add_element(NewSizeTEvent, AccIn)
            end,
            ordsets:new(),
            FilteredOutEvents),
    FinalSizeTSE = ordsets:union(SizeTSE, LeastDominantFilteredOutProvenances),
    RemovedEvents = ordsets:subtract(AllEvents, SubsetEvents),
    FinalSizeTAE =
        case RemovedEvents of
            [] ->
                ordsets:union(SizeTAE, LeastDominantFilteredOutProvenances);
            [Event] ->
                RemovedProvenance =
                    {state_ps_event_partial_order_provenance,
                        ordsets:add_element(
                            ordsets:add_element(Event, ordsets:new()),
                            ordsets:new())},
                ordsets:add_element(
                    RemovedProvenance,
                    ordsets:union(SizeTAE, LeastDominantFilteredOutProvenances));
            _ ->
                MostDominantRemovedProvenance =
                    {state_ps_event_partial_order_provenance,
                        {most_dominant, RemovedEvents}},
                ordsets:add_element(
                    MostDominantRemovedProvenance,
                    ordsets:union(SizeTAE, LeastDominantFilteredOutProvenances))
        end,
    {state_ps_size_t_naive, {SizeTPS, FinalSizeTSE, FinalSizeTAE}}.

%% @private
merge_state_ps_aworset_naive({?TYPE, PayloadA}, {?TYPE, PayloadB}) ->
    MergedPayload = state_ps_poe_orset:join(PayloadA, PayloadB),
    {?TYPE, MergedPayload}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(EVENT_TYPE, state_ps_event_partial_order_independent).

new_test() ->
    ?assertEqual({?TYPE, state_ps_poe_orset:new()}, new()).

query_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    Set1 =
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}],
            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}]}},
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
    ?assertEqual(
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}],
            [{?EVENT_TYPE, {EventId, 1}}]}},
        {?TYPE, Delta1}),
    ?assertEqual(
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}],
            [{?EVENT_TYPE, {EventId, 1}}]}},
        Set1),
    ?assertEqual(
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 2}}]]}],
            [{?EVENT_TYPE, {EventId, 2}}],
            [{?EVENT_TYPE, {EventId, 2}}]}},
        {?TYPE, Delta2}),
    ?assertEqual(
        {?TYPE, {
            [{<<"1">>, [
                [{?EVENT_TYPE, {EventId, 1}}],
                [{?EVENT_TYPE, {EventId, 2}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}],
            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}]}},
        Set2),
    ?assertEqual(
        {?TYPE, {
            [{<<"2">>, [[{?EVENT_TYPE, {EventId, 3}}]]}],
            [{?EVENT_TYPE, {EventId, 3}}],
            [{?EVENT_TYPE, {EventId, 3}}]}},
        {?TYPE, Delta3}),
    ?assertEqual(
        {?TYPE, {
            [
                {<<"1">>, [
                    [{?EVENT_TYPE, {EventId, 1}}],
                    [{?EVENT_TYPE, {EventId, 2}}]]},
                {<<"2">>, [[{?EVENT_TYPE, {EventId, 3}}]]}],
            [{?EVENT_TYPE, {EventId, 1}},
                {?EVENT_TYPE, {EventId, 2}},
                {?EVENT_TYPE, {EventId, 3}}],
            [{?EVENT_TYPE, {EventId, 1}},
                {?EVENT_TYPE, {EventId, 2}},
                {?EVENT_TYPE, {EventId, 3}}]}},
        Set3).

add_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"1">>}, EventId, Set0),
    {ok, Set2} = mutate({add, <<"1">>}, EventId, Set1),
    {ok, Set3} = mutate({add, <<"2">>}, EventId, Set2),
    ?assertEqual(
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}],
            [{?EVENT_TYPE, {EventId, 1}}]}},
        Set1),
    ?assertEqual(
        {?TYPE, {
            [{<<"1">>, [
                [{?EVENT_TYPE, {EventId, 1}}],
                [{?EVENT_TYPE, {EventId, 2}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}],
            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}]}},
        Set2),
    ?assertEqual(
        {?TYPE, {
            [
                {<<"1">>, [
                    [{?EVENT_TYPE, {EventId, 1}}],
                    [{?EVENT_TYPE, {EventId, 2}}]]},
                {<<"2">>, [[{?EVENT_TYPE, {EventId, 3}}]]}],
            [{?EVENT_TYPE, {EventId, 1}},
                {?EVENT_TYPE, {EventId, 2}},
                {?EVENT_TYPE, {EventId, 3}}],
            [{?EVENT_TYPE, {EventId, 1}},
                {?EVENT_TYPE, {EventId, 2}},
                {?EVENT_TYPE, {EventId, 3}}]}},
        Set3).

rmv_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"1">>}, EventId, Set0),
    {ok, Set2} = mutate({add, <<"1">>}, EventId, Set1),
    {ok, Set2} = mutate({rmv, <<"2">>}, EventId, Set2),
    {ok, Set3} = mutate({rmv, <<"1">>}, EventId, Set2),
    ?assertEqual(sets:new(), query(Set3)).

merge_idempotent_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 =
        {?TYPE, {
            [],
            [],
            [{?EVENT_TYPE, {EventId1, 1}}]}},
    Set2 =
        {?TYPE, {
            [{<<"2">>, [[{?EVENT_TYPE, {EventId2, 1}}]]}],
            [{?EVENT_TYPE, {EventId2, 1}}],
            [{?EVENT_TYPE, {EventId2, 1}}]}},
    Set3 =
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId1, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 1}}],
            [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId2, 1}}]}},
    Set4 = merge(Set1, Set1),
    Set5 = merge(Set2, Set2),
    Set6 = merge(Set3, Set3),
    ?assert(equal(Set1, Set4)),
    ?assert(equal(Set2, Set5)),
    ?assert(equal(Set3, Set6)).

merge_commutative_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 =
        {?TYPE, {
            [],
            [],
            [{?EVENT_TYPE, {EventId1, 1}}]}},
    Set2 =
        {?TYPE, {
            [{<<"2">>, [[{?EVENT_TYPE, {EventId2, 1}}]]}],
            [{?EVENT_TYPE, {EventId2, 1}}],
            [{?EVENT_TYPE, {EventId2, 1}}]}},
    Set3 =
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId1, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 1}}],
            [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId2, 1}}]}},
    Set4 = merge(Set1, Set2),
    Set5 = merge(Set2, Set1),
    Set6 = merge(Set1, Set3),
    Set7 = merge(Set3, Set1),
    Set8 = merge(Set2, Set3),
    Set9 = merge(Set3, Set2),
    Set10 = merge(Set1, merge(Set2, Set3)),
    Set1_2 =
        {?TYPE, {
            [{<<"2">>, [[{?EVENT_TYPE, {EventId2, 1}}]]}],
            [{?EVENT_TYPE, {EventId2, 1}}],
            [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId2, 1}}]}},
    Set1_3 =
        {?TYPE, {
            [],
            [],
            [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId2, 1}}]}},
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
    Set1 =
        {?TYPE, {
            [
                {<<"1">>, [
                    [{?EVENT_TYPE, {EventId1, 1}},
                        {?EVENT_TYPE, {EventId2, 1}}]]},
                {<<"2">>, [
                    [{?EVENT_TYPE, {EventId1, 2}},
                        {?EVENT_TYPE, {EventId2, 2}}]]}],
            [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId1, 2}},
                {?EVENT_TYPE, {EventId2, 1}}, {?EVENT_TYPE, {EventId2, 2}}],
            [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId1, 2}},
                {?EVENT_TYPE, {EventId2, 1}}, {?EVENT_TYPE, {EventId2, 2}}]}},
    Delta1 =
        {?TYPE, {
            [
                {<<"1">>, [
                    [{?EVENT_TYPE, {EventId1, 1}},
                        {?EVENT_TYPE, {EventId2, 3}}]]},
                {<<"2">>, [
                    [{?EVENT_TYPE, {EventId1, 2}},
                        {?EVENT_TYPE, {EventId2, 4}}]]}],
            [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId1, 2}},
                {?EVENT_TYPE, {EventId2, 3}}, {?EVENT_TYPE, {EventId2, 4}}],
            [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId1, 2}},
                {?EVENT_TYPE, {EventId2, 3}}, {?EVENT_TYPE, {EventId2, 4}}]}},
    Set2 = merge(Set1, Delta1),
    ?assert(
        equal(
            {?TYPE, {
                [
                    {<<"1">>, [
                        [{?EVENT_TYPE, {EventId1, 1}},
                            {?EVENT_TYPE, {EventId2, 1}}],
                        [{?EVENT_TYPE, {EventId1, 1}},
                            {?EVENT_TYPE, {EventId2, 3}}]]},
                    {<<"2">>, [
                        [{?EVENT_TYPE, {EventId1, 2}},
                            {?EVENT_TYPE, {EventId2, 2}}],
                        [{?EVENT_TYPE, {EventId1, 2}},
                            {?EVENT_TYPE, {EventId2, 4}}]]}],
                [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId1, 2}},
                    {?EVENT_TYPE, {EventId2, 1}}, {?EVENT_TYPE, {EventId2, 2}},
                    {?EVENT_TYPE, {EventId2, 3}}, {?EVENT_TYPE, {EventId2, 4}}],
                [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId1, 2}},
                    {?EVENT_TYPE, {EventId2, 1}}, {?EVENT_TYPE, {EventId2, 2}},
                    {?EVENT_TYPE, {EventId2, 3}}, {?EVENT_TYPE, {EventId2, 4}}]}},
            Set2)).

merge_delta_test() ->
    EventId = {<<"object1">>, a},
    Set1 =
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}],
            [{?EVENT_TYPE, {EventId, 1}}]}},
    Delta1 =
        {?TYPE, {
            [],
            [],
            [{?EVENT_TYPE, {EventId, 1}}]}},
    Delta2 =
        {?TYPE, {
            [{<<"2">>, [[{?EVENT_TYPE, {EventId, 2}}]]}],
            [{?EVENT_TYPE, {EventId, 2}}],
            [{?EVENT_TYPE, {EventId, 2}}]}},
    Set2 = merge(Delta1, Set1),
    Set3 = merge(Set1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual(
        {?TYPE, {
            [],
            [],
            [{?EVENT_TYPE, {EventId, 1}}]}},
        Set2),
    ?assertEqual(
        {?TYPE, {
            [],
            [],
            [{?EVENT_TYPE, {EventId, 1}}]}},
        Set3),
    ?assertEqual(
        {?TYPE, {
            [{<<"2">>, [[{?EVENT_TYPE, {EventId, 2}}]]}],
            [{?EVENT_TYPE, {EventId, 2}}],
            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}]}},
        DeltaGroup).

equal_test() ->
    EventId = {<<"object1">>, a},
    Set1 =
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}],
            [{?EVENT_TYPE, {EventId, 1}}]}},
    Set2 =
        {?TYPE, {
            [],
            [],
            [{?EVENT_TYPE, {EventId, 1}}]}},
    Set3 =
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}],
            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}]}},
    ?assert(equal(Set1, Set1)),
    ?assert(equal(Set2, Set2)),
    ?assert(equal(Set3, Set3)),
    ?assertNot(equal(Set1, Set2)),
    ?assertNot(equal(Set1, Set3)),
    ?assertNot(equal(Set2, Set3)).

is_inflation_test() ->
    EventId = {<<"object1">>, a},
    Set1 =
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}],
            [{?EVENT_TYPE, {EventId, 1}}]}},
    Set2 =
        {?TYPE, {
            [],
            [],
            [{?EVENT_TYPE, {EventId, 1}}]}},
    Set3 =
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}],
            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}]}},
    ?assert(is_inflation(Set1, Set1)),
    ?assert(is_inflation(Set1, Set2)),
    ?assertNot(is_inflation(Set2, Set1)),
    ?assert(is_inflation(Set1, Set3)),
    ?assertNot(is_inflation(Set2, Set3)),
    ?assertNot(is_inflation(Set3, Set2)),
    %% check inflation with merge
    ?assert(state_ps_type:is_inflation(Set1, Set1)),
    ?assert(state_ps_type:is_inflation(Set1, Set2)),
    ?assertNot(state_ps_type:is_inflation(Set2, Set1)),
    ?assert(state_ps_type:is_inflation(Set1, Set3)),
    ?assertNot(state_ps_type:is_inflation(Set2, Set3)),
    ?assertNot(state_ps_type:is_inflation(Set3, Set2)).

is_strict_inflation_test() ->
    EventId = {<<"object1">>, a},
    Set1 =
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}],
            [{?EVENT_TYPE, {EventId, 1}}]}},
    Set2 =
        {?TYPE, {
            [],
            [],
            [{?EVENT_TYPE, {EventId, 1}}]}},
    Set3 =
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}],
            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}]}},
    ?assertNot(is_strict_inflation(Set1, Set1)),
    ?assert(is_strict_inflation(Set1, Set2)),
    ?assertNot(is_strict_inflation(Set2, Set1)),
    ?assert(is_strict_inflation(Set1, Set3)),
    ?assertNot(is_strict_inflation(Set2, Set3)),
    ?assertNot(is_strict_inflation(Set3, Set2)).

encode_decode_test() ->
    EventId = {<<"object1">>, a},
    Set =
        {?TYPE, {
            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
            [{?EVENT_TYPE, {EventId, 1}}],
            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}]}},
    Binary = encode(erlang, Set),
    ESet = decode(erlang, Binary),
    ?assertEqual(Set, ESet).

-endif.
