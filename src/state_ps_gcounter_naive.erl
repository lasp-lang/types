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

%% @doc G Counter CRDT with the POE OR Set design:
%%     grow only counter.

-module(state_ps_gcounter_naive).

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

-export_type([
    state_ps_gcounter_naive/0,
    state_ps_gcounter_naive_op/0]).

-type payload() :: state_ps_poe_orset:state_ps_poe_orset().
-opaque state_ps_gcounter_naive() :: {?TYPE, payload()}.
-type state_ps_gcounter_naive_op() :: increment.

%% @doc Create a new, empty `state_ps_gcounter_naive()'.
-spec new() -> state_ps_gcounter_naive().
new() ->
    {?TYPE, state_ps_poe_orset:new()}.

%% @doc Create a new, empty `state_ps_gcounter_naive()'
-spec new([term()]) -> state_ps_gcounter_naive().
new([_]) ->
    new().

%% @doc Mutate a `state_ps_gcounter_naive()'.
-spec mutate(
    state_ps_gcounter_naive_op(), type:id(), state_ps_gcounter_naive()) ->
    {ok, state_ps_gcounter_naive()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_ps_type:mutate(Op, Actor, CRDT).

%% @doc Returns the value of the `state_ps_gcounter_naive()'.
-spec query(state_ps_gcounter_naive()) -> term().
query({?TYPE, Payload}) ->
    InternalSet = sets:to_list(state_ps_poe_orset:read(Payload)),
    case InternalSet of
        [] ->
            0;
        [ValueAsProvenance] ->
            Value = state_ps_type:get_events_from_provenance(ValueAsProvenance),
            ordsets:fold(
                fun({state_ps_event_partial_order_downward_closed, {_, Counter}},
                    AccResult) ->
                    AccResult + Counter
                end,
                0,
                Value)
    end.

%% @doc Equality for `state_ps_gcounter_naive()'.
%%      Since everything is ordered, == should work.
-spec equal(state_ps_gcounter_naive(), state_ps_gcounter_naive()) -> boolean().
equal({?TYPE, PayloadA}, {?TYPE, PayloadB}) ->
    state_ps_poe_orset:equal(PayloadA, PayloadB).

%% @doc Delta-mutate a `state_ps_gcounter_naive()'.
%%      The first argument can only be `increment'.
%%      The second argument is the event id ({object_id, replica_id}).
%%      The third argument is the `state_ps_gcounter_naive()' to be inflated.
-spec delta_mutate(
    state_ps_gcounter_naive_op(),
    type:id(),
    state_ps_gcounter_naive()) -> {ok, state_ps_gcounter_naive()}.
%% Increase the value of the `state_ps_gcounter_naive()'.
delta_mutate(increment, Actor, {?TYPE, Payload}) ->
    %% Get next Event from AllEvents.
    NextEvent = get_next_event(Actor, Payload),
    ValueAsProvenance =
        ordsets:add_element(
            ordsets:add_element(NextEvent, ordsets:new()), ordsets:new()),
    %% Get a delta object.
    DeltaPayload =
        state_ps_poe_orset:delta_insert(NextEvent, ValueAsProvenance, Payload),
    {ok, {?TYPE, DeltaPayload}}.

%% @doc Merge two `state_ps_gcounter_naive()'.
-spec merge(state_ps_gcounter_naive(), state_ps_gcounter_naive()) ->
    state_ps_gcounter_naive().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun merge_state_ps_gcounter_naive/2,
    state_ps_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Given two `state_ps_gcounter_naive()', check if the second is an
%%      inflation of the first.
-spec is_inflation(state_ps_gcounter_naive(), state_ps_gcounter_naive()) ->
    boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(
    state_ps_gcounter_naive(), state_ps_gcounter_naive()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_strict_inflation(CRDT1, CRDT2).

-spec encode(state_ps_type:format(), state_ps_gcounter_naive()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_ps_type:format(), binary()) -> state_ps_gcounter_naive().
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
                case EventType0 == state_ps_event_partial_order_downward_closed
                    andalso EventId0 == EventId of
                    true ->
                        max(AccInMaxCnt, Counter0);
                    false ->
                        AccInMaxCnt
                end
            end,
            0,
            AllEvents),
    {state_ps_event_partial_order_downward_closed, {EventId, MaxCnt + 1}}.

%% @private
merge_state_ps_gcounter_naive({?TYPE, PayloadA}, {?TYPE, PayloadB}) ->
    {ProvenanceStore, SubsetEvents, AllEvents} =
        state_ps_poe_orset:join(PayloadA, PayloadB),
    MergedProvenance =
        orddict:fold(
            fun(_Elem, Provenance, AccMergedProvenance) ->
                state_ps_type:plus_provenance(AccMergedProvenance, Provenance)
            end,
            ordsets:new(),
            ProvenanceStore),
    MergedProvenanceStore =
        orddict:store(MergedProvenance, MergedProvenance, orddict:new()),
    {?TYPE, {MergedProvenanceStore, SubsetEvents, AllEvents}}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(EVENT_TYPE, state_ps_event_partial_order_downward_closed).

new_test() ->
    ?assertEqual({?TYPE, state_ps_poe_orset:new()}, new()).

query_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Counter0 = new(),
    Counter1 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 3}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 3}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 3}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 3}}]}},
    ?assertEqual(0, query(Counter0)),
    ?assertEqual(5, query(Counter1)).

delta_increment_test() ->
    EventId1 = {<<"object1">>, a},
    Counter0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate(increment, EventId1, Counter0),
    Counter1 = merge({?TYPE, Delta1}, Counter0),
    EventId2 = {<<"object1">>, b},
    {ok, {?TYPE, Delta2}} = delta_mutate(increment, EventId2, Counter1),
    Counter2 = merge({?TYPE, Delta2}, Counter1),
    {ok, {?TYPE, Delta3}} = delta_mutate(increment, EventId1, Counter2),
    Counter3 = merge({?TYPE, Delta3}, Counter2),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 1}}],
            [{?EVENT_TYPE, {EventId1, 1}}]}},
        {?TYPE, Delta1}),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 1}}],
            [{?EVENT_TYPE, {EventId1, 1}}]}},
        Counter1),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId2, 1}}]],
                [[{?EVENT_TYPE, {EventId2, 1}}]]}],
            [{?EVENT_TYPE, {EventId2, 1}}],
            [{?EVENT_TYPE, {EventId2, 1}}]}},
        {?TYPE, Delta2}),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 1}}], [{?EVENT_TYPE, {EventId2, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 1}}], [{?EVENT_TYPE, {EventId2, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId2, 1}}],
            [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId2, 1}}]}},
        Counter2),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}],
            [{?EVENT_TYPE, {EventId1, 2}}]}},
        {?TYPE, Delta3}),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}}]}},
        Counter3).

increment_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Counter0 = new(),
    {ok, Counter1} = mutate(increment, EventId1, Counter0),
    {ok, Counter2} = mutate(increment, EventId2, Counter1),
    {ok, Counter3} = mutate(increment, EventId1, Counter2),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 1}}],
            [{?EVENT_TYPE, {EventId1, 1}}]}},
        Counter1),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 1}}], [{?EVENT_TYPE, {EventId2, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 1}}], [{?EVENT_TYPE, {EventId2, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId2, 1}}],
            [{?EVENT_TYPE, {EventId1, 1}}, {?EVENT_TYPE, {EventId2, 1}}]}},
        Counter2),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}}]}},
        Counter3).

merge_idempotent_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    EventId3 = {<<"object1">>, c},
    Counter1 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 3}}]],
                [[{?EVENT_TYPE, {EventId1, 3}}]]}],
            [{?EVENT_TYPE, {EventId1, 3}}],
            [{?EVENT_TYPE, {EventId1, 3}}]}},
    Counter2 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId2, 4}}], [{?EVENT_TYPE, {EventId3, 2}}]],
                [[{?EVENT_TYPE, {EventId2, 4}}], [{?EVENT_TYPE, {EventId3, 2}}]]}],
            [{?EVENT_TYPE, {EventId2, 4}}, {?EVENT_TYPE, {EventId3, 2}}],
            [{?EVENT_TYPE, {EventId2, 4}}, {?EVENT_TYPE, {EventId3, 2}}]}},
    Counter3 = merge(Counter1, Counter1),
    Counter4 = merge(Counter2, Counter2),
    ?assertEqual(Counter1, Counter3),
    ?assertEqual(Counter2, Counter4).

merge_commutative_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    EventId3 = {<<"object1">>, c},
    Counter1 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 3}}]],
                [[{?EVENT_TYPE, {EventId1, 3}}]]}],
            [{?EVENT_TYPE, {EventId1, 3}}],
            [{?EVENT_TYPE, {EventId1, 3}}]}},
    Counter2 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId2, 4}}], [{?EVENT_TYPE, {EventId3, 2}}]],
                [[{?EVENT_TYPE, {EventId2, 4}}], [{?EVENT_TYPE, {EventId3, 2}}]]}],
            [{?EVENT_TYPE, {EventId2, 4}}, {?EVENT_TYPE, {EventId3, 2}}],
            [{?EVENT_TYPE, {EventId2, 4}}, {?EVENT_TYPE, {EventId3, 2}}]}},
    Counter3 = merge(Counter1, Counter2),
    Counter4 = merge(Counter2, Counter1),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 3}}], [{?EVENT_TYPE, {EventId2, 4}}],
                    [{?EVENT_TYPE, {EventId3, 2}}]],
                [[{?EVENT_TYPE, {EventId1, 3}}], [{?EVENT_TYPE, {EventId2, 4}}],
                    [{?EVENT_TYPE, {EventId3, 2}}]]}],
            [{?EVENT_TYPE, {EventId1, 3}}, {?EVENT_TYPE, {EventId2, 4}},
                {?EVENT_TYPE, {EventId3, 2}}],
            [{?EVENT_TYPE, {EventId1, 3}}, {?EVENT_TYPE, {EventId2, 4}},
                {?EVENT_TYPE, {EventId3, 2}}]}},
        Counter3),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 3}}], [{?EVENT_TYPE, {EventId2, 4}}],
                    [{?EVENT_TYPE, {EventId3, 2}}]],
                [[{?EVENT_TYPE, {EventId1, 3}}], [{?EVENT_TYPE, {EventId2, 4}}],
                    [{?EVENT_TYPE, {EventId3, 2}}]]}],
            [{?EVENT_TYPE, {EventId1, 3}}, {?EVENT_TYPE, {EventId2, 4}},
                {?EVENT_TYPE, {EventId3, 2}}],
            [{?EVENT_TYPE, {EventId1, 3}}, {?EVENT_TYPE, {EventId2, 4}},
                {?EVENT_TYPE, {EventId3, 2}}]}},
        Counter4).

merge_same_id_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Counter1 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 4}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 4}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 4}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 4}}]}},
    Counter2 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 3}}], [{?EVENT_TYPE, {EventId2, 3}}]],
                [[{?EVENT_TYPE, {EventId1, 3}}], [{?EVENT_TYPE, {EventId2, 3}}]]}],
            [{?EVENT_TYPE, {EventId1, 3}}, {?EVENT_TYPE, {EventId2, 3}}],
            [{?EVENT_TYPE, {EventId1, 3}}, {?EVENT_TYPE, {EventId2, 3}}]}},
    Counter3 = merge(Counter1, Counter2),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 3}}], [{?EVENT_TYPE, {EventId2, 4}}]],
                [[{?EVENT_TYPE, {EventId1, 3}}], [{?EVENT_TYPE, {EventId2, 4}}]]}],
            [{?EVENT_TYPE, {EventId1, 3}}, {?EVENT_TYPE, {EventId2, 4}}],
            [{?EVENT_TYPE, {EventId1, 3}}, {?EVENT_TYPE, {EventId2, 4}}]}},
        Counter3).

merge_deltas_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Counter1 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 3}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 3}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 3}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 3}}]}},
    Delta1 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 4}}], [{?EVENT_TYPE, {EventId2, 4}}]],
                [[{?EVENT_TYPE, {EventId1, 4}}], [{?EVENT_TYPE, {EventId2, 4}}]]}],
            [{?EVENT_TYPE, {EventId1, 4}}, {?EVENT_TYPE, {EventId2, 4}}],
            [{?EVENT_TYPE, {EventId1, 4}}, {?EVENT_TYPE, {EventId2, 4}}]}},
    Delta2 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 3}}], [{?EVENT_TYPE, {EventId2, 5}}]],
                [[{?EVENT_TYPE, {EventId1, 3}}], [{?EVENT_TYPE, {EventId2, 5}}]]}],
            [{?EVENT_TYPE, {EventId1, 3}}, {?EVENT_TYPE, {EventId2, 5}}],
            [{?EVENT_TYPE, {EventId1, 3}}, {?EVENT_TYPE, {EventId2, 5}}]}},
    Counter2 = merge(Delta1, Counter1),
    Counter3 = merge(Counter1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 4}}], [{?EVENT_TYPE, {EventId2, 4}}]],
                [[{?EVENT_TYPE, {EventId1, 4}}], [{?EVENT_TYPE, {EventId2, 4}}]]}],
            [{?EVENT_TYPE, {EventId1, 4}}, {?EVENT_TYPE, {EventId2, 4}}],
            [{?EVENT_TYPE, {EventId1, 4}}, {?EVENT_TYPE, {EventId2, 4}}]}},
        Counter2),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 4}}], [{?EVENT_TYPE, {EventId2, 4}}]],
                [[{?EVENT_TYPE, {EventId1, 4}}], [{?EVENT_TYPE, {EventId2, 4}}]]}],
            [{?EVENT_TYPE, {EventId1, 4}}, {?EVENT_TYPE, {EventId2, 4}}],
            [{?EVENT_TYPE, {EventId1, 4}}, {?EVENT_TYPE, {EventId2, 4}}]}},
        Counter3),
    ?assertEqual(
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 4}}], [{?EVENT_TYPE, {EventId2, 5}}]],
                [[{?EVENT_TYPE, {EventId1, 4}}], [{?EVENT_TYPE, {EventId2, 5}}]]}],
            [{?EVENT_TYPE, {EventId1, 4}}, {?EVENT_TYPE, {EventId2, 5}}],
            [{?EVENT_TYPE, {EventId1, 4}}, {?EVENT_TYPE, {EventId2, 5}}]}},
        DeltaGroup).

equal_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    EventId3 = {<<"object1">>, c},
    EventId4 = {<<"object1">>, d},
    Counter1 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}]}},
    Counter2 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}], [{?EVENT_TYPE, {EventId4, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}], [{?EVENT_TYPE, {EventId4, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}, {?EVENT_TYPE, {EventId4, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}, {?EVENT_TYPE, {EventId4, 1}}]}},
    Counter3 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 2}}],
                    [{?EVENT_TYPE, {EventId3, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 2}}],
                    [{?EVENT_TYPE, {EventId3, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 2}},
                {?EVENT_TYPE, {EventId3, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 2}},
                {?EVENT_TYPE, {EventId3, 1}}]}},
    Counter4 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}}]}},
    ?assert(equal(Counter1, Counter1)),
    ?assertNot(equal(Counter1, Counter2)),
    ?assertNot(equal(Counter1, Counter3)),
    ?assertNot(equal(Counter1, Counter4)).

is_inflation_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    EventId3 = {<<"object1">>, c},
    EventId4 = {<<"object1">>, d},
    Counter1 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}]}},
    Counter2 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}], [{?EVENT_TYPE, {EventId4, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}], [{?EVENT_TYPE, {EventId4, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}, {?EVENT_TYPE, {EventId4, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}, {?EVENT_TYPE, {EventId4, 1}}]}},
    Counter3 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 2}}],
                    [{?EVENT_TYPE, {EventId3, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 2}}],
                    [{?EVENT_TYPE, {EventId3, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 2}},
                {?EVENT_TYPE, {EventId3, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 2}},
                {?EVENT_TYPE, {EventId3, 1}}]}},
    Counter4 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}}]}},
    ?assert(is_inflation(Counter1, Counter1)),
    ?assert(is_inflation(Counter1, Counter2)),
    ?assert(is_inflation(Counter1, Counter3)),
    ?assertNot(is_inflation(Counter1, Counter4)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Counter1, Counter1)),
    ?assert(state_type:is_inflation(Counter1, Counter2)),
    ?assert(state_type:is_inflation(Counter1, Counter3)),
    ?assertNot(state_type:is_inflation(Counter1, Counter4)).

is_strict_inflation_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    EventId3 = {<<"object1">>, c},
    EventId4 = {<<"object1">>, d},
    Counter1 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}]}},
    Counter2 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}], [{?EVENT_TYPE, {EventId4, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}], [{?EVENT_TYPE, {EventId4, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}, {?EVENT_TYPE, {EventId4, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}, {?EVENT_TYPE, {EventId4, 1}}]}},
    Counter3 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 2}}],
                    [{?EVENT_TYPE, {EventId3, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 2}}],
                    [{?EVENT_TYPE, {EventId3, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 2}},
                {?EVENT_TYPE, {EventId3, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 2}},
                {?EVENT_TYPE, {EventId3, 1}}]}},
    Counter4 =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}}]}},
    ?assertNot(is_strict_inflation(Counter1, Counter1)),
    ?assert(is_strict_inflation(Counter1, Counter2)),
    ?assert(is_strict_inflation(Counter1, Counter3)),
    ?assertNot(is_strict_inflation(Counter1, Counter4)).

encode_decode_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    EventId3 = {<<"object1">>, c},
    EventId4 = {<<"object1">>, d},
    Counter =
        {?TYPE, {
            [{
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}], [{?EVENT_TYPE, {EventId4, 1}}]],
                [[{?EVENT_TYPE, {EventId1, 2}}], [{?EVENT_TYPE, {EventId2, 1}}],
                    [{?EVENT_TYPE, {EventId3, 1}}], [{?EVENT_TYPE, {EventId4, 1}}]]}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}, {?EVENT_TYPE, {EventId4, 1}}],
            [{?EVENT_TYPE, {EventId1, 2}}, {?EVENT_TYPE, {EventId2, 1}},
                {?EVENT_TYPE, {EventId3, 1}}, {?EVENT_TYPE, {EventId4, 1}}]}},
    Binary = encode(erlang, Counter),
    ECounter = decode(erlang, Binary),
    ?assertEqual(Counter, ECounter).

-endif.
