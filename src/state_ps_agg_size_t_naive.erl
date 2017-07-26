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

%% @doc Size CRDT for the POE OR Set with the POE OR Set design:
%%     size_t type.

-module(state_ps_agg_size_t_naive).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-behaviour(type).
-behaviour(state_ps_agg_type).

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
    state_ps_agg_size_t_naive/0,
    state_ps_agg_size_t_naive_op/0]).

-type payload() :: state_ps_agg_poe_orset:state_ps_agg_poe_orset().
-opaque state_ps_agg_size_t_naive() :: {?TYPE, payload()}.
-type state_ps_agg_size_t_naive_op() :: no_op.

%% @doc Create a new, empty `state_ps_agg_size_t_naive()'.
-spec new() -> state_ps_agg_size_t_naive().
new() ->
    {?TYPE, state_ps_agg_poe_orset:new()}.

%% @doc Create a new, empty `state_ps_agg_size_t_naive()'
-spec new([term()]) -> state_ps_agg_size_t_naive().
new([_]) ->
    new().

%% @doc Mutate a `state_ps_agg_size_t_naive()'.
-spec mutate(
    state_ps_agg_size_t_naive_op(), type:id(), state_ps_agg_size_t_naive()) ->
    {ok, state_ps_agg_size_t_naive()}.
mutate(_Op, _Actor, {?TYPE, _}=CRDT) ->
    {ok, CRDT}.

%% @doc Returns the value of the `state_ps_agg_size_t_naive()'.
-spec query(state_ps_agg_size_t_naive()) -> term().
query({?TYPE, Payload}) ->
    InternalSet = sets:to_list(state_ps_agg_poe_orset:read(Payload)),
    case InternalSet of
        [] ->
            0;
        [Size] ->
            Size
    end.

%% @doc Equality for `state_ps_agg_size_t_naive()'.
%%      Since everything is ordered, == should work.
-spec equal(state_ps_agg_size_t_naive(), state_ps_agg_size_t_naive()) ->
    boolean().
equal({?TYPE, PayloadA}, {?TYPE, PayloadB}) ->
    state_ps_agg_poe_orset:equal(PayloadA, PayloadB).

%% @doc Delta-mutate a `state_ps_agg_size_t_naive()'.
%%      The first argument can only be `increment'.
%%      The second argument is the event id ({object_id, replica_id}).
%%      The third argument is the `state_ps_agg_size_t_naive()' to be inflated.
-spec delta_mutate(
    state_ps_agg_size_t_naive_op(),
    type:id(),
    state_ps_agg_size_t_naive()) -> {ok, state_ps_agg_size_t_naive()}.
%% Increase the value of the `state_ps_agg_size_t_naive()'.
delta_mutate(no_op, _Actor, {?TYPE, Payload}) ->
    {ok, {?TYPE, Payload}}.

%% @doc Merge two `state_ps_agg_size_t_naive()'.
-spec merge(state_ps_agg_size_t_naive(), state_ps_agg_size_t_naive()) ->
    state_ps_agg_size_t_naive().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun merge_state_ps_agg_size_t_naive/2,
    state_ps_agg_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Given two `state_ps_agg_size_t_naive()', check if the second is an
%%      inflation of the first.
-spec is_inflation(state_ps_agg_size_t_naive(), state_ps_agg_size_t_naive()) ->
    boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_agg_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(
    state_ps_agg_size_t_naive(), state_ps_agg_size_t_naive()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_agg_type:is_strict_inflation(CRDT1, CRDT2).

-spec encode(state_ps_agg_type:format(), state_ps_agg_size_t_naive()) ->
    binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_ps_agg_type:format(), binary()) ->
    state_ps_agg_size_t_naive().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.

%% @doc Calculate the next event from the AllEvents.
-spec get_next_event(
    state_ps_agg_type:state_ps_agg_event_id(),
    state_ps_agg_type:state_ps_agg_payload()) ->
    state_ps_agg_type:state_ps_agg_event().
get_next_event(_EventId, _Payload) ->
    {state_ps_agg_event_bottom, undefined}.

%% @private
merge_state_ps_agg_size_t_naive({?TYPE, PayloadA}, {?TYPE, PayloadB}) ->
    {ProvenanceStore, SubsetEvents, AllEvents} =
        state_ps_agg_poe_orset:join(PayloadA, PayloadB),
    MergedProvenance =
        orddict:fold(
            fun(_Elem, Provenance, AccMergedProvenance) ->
                state_ps_agg_type:plus_provenance(
                    AccMergedProvenance, Provenance)
            end,
            {tensors, orddict:new()},
            ProvenanceStore),
    MergedProvenanceStore =
        case state_ps_agg_type:is_bottom_provenance(MergedProvenance) of
            true ->
                orddict:new();
            false ->
                {tensors, MergedProvenanceTensorDict} = MergedProvenance,
                orddict:store(
                    orddict:size(MergedProvenanceTensorDict),
                    MergedProvenance,
                    orddict:new())
        end,
    {?TYPE, {MergedProvenanceStore, SubsetEvents, AllEvents}}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(SET_EVENT_TYPE, state_ps_agg_event_partial_order_independent).

new_test() ->
    ?assertEqual({?TYPE, state_ps_agg_poe_orset:new()}, new()).

query_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    SizeT0 = new(),
    SizeT1 =
        {?TYPE, {
            [{2, {tensors, [
                {<<"1">>, {dots, [
                    [{?SET_EVENT_TYPE, {EventId1, 1}}],
                    [{?SET_EVENT_TYPE, {EventId2, 1}}]]}},
                {<<"2">>, {dots, [[{?SET_EVENT_TYPE, {EventId1, 2}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId1, 2}},
                {?SET_EVENT_TYPE, {EventId2, 1}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId1, 2}},
                {?SET_EVENT_TYPE, {EventId2, 1}}]}},
    ?assertEqual(0, query(SizeT0)),
    ?assertEqual(2, query(SizeT1)).

merge_idempotent_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    EventId3 = {<<"object1">>, c},
    SizeT1 =
        {?TYPE, {
            [{1, {tensors, [
                {<<"1">>, {dots, [[{?SET_EVENT_TYPE, {EventId1, 1}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}}]}},
    SizeT2 =
        {?TYPE, {
            [{2, {tensors, [
                {<<"2">>, {dots, [
                    [{?SET_EVENT_TYPE, {EventId2, 1}}],
                    [{?SET_EVENT_TYPE, {EventId3, 1}}]]}},
                {<<"3">>, {dots, [[{?SET_EVENT_TYPE, {EventId2, 2}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId2, 1}},
                {?SET_EVENT_TYPE, {EventId2, 2}},
                {?SET_EVENT_TYPE, {EventId3, 1}}],
            [{?SET_EVENT_TYPE, {EventId2, 1}},
                {?SET_EVENT_TYPE, {EventId2, 2}},
                {?SET_EVENT_TYPE, {EventId3, 1}}]}},
    SizeT3 = merge(SizeT1, SizeT1),
    SizeT4 = merge(SizeT2, SizeT2),
    ?assertEqual(SizeT1, SizeT3),
    ?assertEqual(SizeT2, SizeT4).

merge_commutative_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    EventId3 = {<<"object1">>, c},
    SizeT1 =
        {?TYPE, {
            [{1, {tensors, [
                {<<"1">>, {dots, [[{?SET_EVENT_TYPE, {EventId1, 1}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}}]}},
    SizeT2 =
        {?TYPE, {
            [{2, {tensors, [
                {<<"2">>, {dots, [
                    [{?SET_EVENT_TYPE, {EventId2, 1}}],
                    [{?SET_EVENT_TYPE, {EventId3, 1}}]]}},
                {<<"3">>, {dots, [[{?SET_EVENT_TYPE, {EventId2, 2}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId2, 1}},
                {?SET_EVENT_TYPE, {EventId3, 1}},
                {?SET_EVENT_TYPE, {EventId2, 2}}],
            [{?SET_EVENT_TYPE, {EventId2, 1}},
                {?SET_EVENT_TYPE, {EventId3, 1}},
                {?SET_EVENT_TYPE, {EventId2, 2}}]}},
    SizeT3 = merge(SizeT1, SizeT2),
    SizeT4 = merge(SizeT2, SizeT1),
    ?assertEqual(
        {?TYPE, {
            [{3, {tensors, [
                {<<"1">>, {dots, [[{?SET_EVENT_TYPE, {EventId1, 1}}]]}},
                {<<"2">>, {dots, [
                    [{?SET_EVENT_TYPE, {EventId2, 1}}],
                    [{?SET_EVENT_TYPE, {EventId3, 1}}]]}},
                {<<"3">>, {dots, [[{?SET_EVENT_TYPE, {EventId2, 2}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId2, 1}},
                {?SET_EVENT_TYPE, {EventId2, 2}},
                {?SET_EVENT_TYPE, {EventId3, 1}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId2, 1}},
                {?SET_EVENT_TYPE, {EventId2, 2}},
                {?SET_EVENT_TYPE, {EventId3, 1}}]}},
        SizeT3),
    ?assertEqual(SizeT3, SizeT4).

merge_ordered_provenance_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    EventId3 = {<<"object1">>, c},
    SizeT1 =
        {?TYPE, {
            [{2, {tensors, [
                {<<"1">>, {dots, [
                    [{?SET_EVENT_TYPE, {EventId1, 1}}],
                    [{?SET_EVENT_TYPE, {EventId2, 1}}]]}},
                {<<"2">>, {dots, [[{?SET_EVENT_TYPE, {EventId1, 2}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId1, 2}},
                {?SET_EVENT_TYPE, {EventId2, 1}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId1, 2}},
                {?SET_EVENT_TYPE, {EventId2, 1}}]}},
    SizeT2 =
        {?TYPE, {
            [{2, {tensors, [
                {<<"1">>, {dots, [[{?SET_EVENT_TYPE, {EventId1, 1}}]]}},
                {<<"2">>, {dots, [
                    [{?SET_EVENT_TYPE, {EventId1, 2}}],
                    [{?SET_EVENT_TYPE, {EventId3, 1}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId1, 2}},
                {?SET_EVENT_TYPE, {EventId3, 1}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId1, 2}},
                {?SET_EVENT_TYPE, {EventId3, 1}}]}},
    SizeT3 = merge(SizeT1, SizeT2),
    ?assertEqual(
        {?TYPE, {
            [{2, {tensors, [
                {<<"1">>, {dots, [
                    [{?SET_EVENT_TYPE, {EventId1, 1}}],
                    [{?SET_EVENT_TYPE, {EventId2, 1}}]]}},
                {<<"2">>, {dots, [
                    [{?SET_EVENT_TYPE, {EventId1, 2}}],
                    [{?SET_EVENT_TYPE, {EventId3, 1}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId1, 2}},
                {?SET_EVENT_TYPE, {EventId2, 1}},
                {?SET_EVENT_TYPE, {EventId3, 1}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId1, 2}},
                {?SET_EVENT_TYPE, {EventId2, 1}},
                {?SET_EVENT_TYPE, {EventId3, 1}}]}},
        SizeT3).

merge_after_elem_removed_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    SizeT1 =
        {?TYPE, {
            [{1, {tensors, [
                {{<<"1">>, <<"a">>}, {dots, [
                    [{?SET_EVENT_TYPE, {EventId1, 1}},
                        {?SET_EVENT_TYPE, {EventId2, 1}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId2, 1}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId2, 1}}]}},
    SizeT2 =
        {?TYPE, {
            [],
            [{?SET_EVENT_TYPE, {EventId2, 1}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId2, 1}}]}},
    SizeT3 = merge(SizeT1, SizeT2),
    ?assertEqual(
        {?TYPE, {
            [],
            [{?SET_EVENT_TYPE, {EventId2, 1}}],
            [{?SET_EVENT_TYPE, {EventId1, 1}},
                {?SET_EVENT_TYPE, {EventId2, 1}}]}},
        SizeT3).

equal_test() ->
    EventId = {<<"object1">>, a},
%%    Set1 =
%%        {?TYPE, {
%%            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
%%            [{?EVENT_TYPE, {EventId, 1}}],
%%            [{?EVENT_TYPE, {EventId, 1}}]}},
    SizeT1 =
        {?TYPE, {
            [{1, {tensors, [
                {<<"1">>, {dots, [[{?SET_EVENT_TYPE, {EventId, 1}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId, 1}}],
            [{?SET_EVENT_TYPE, {EventId, 1}}]}},
%%    Set2 =
%%        {?TYPE, {
%%            [],
%%            [],
%%            [{?EVENT_TYPE, {EventId, 1}}]}},
    SizeT2 =
        {?TYPE, {
            [],
            [],
            [{?SET_EVENT_TYPE, {EventId, 1}}]}},
%%    Set3 =
%%        {?TYPE, {
%%            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
%%            [{?EVENT_TYPE, {EventId, 1}}],
%%            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}]}},
    SizeT3 =
        {?TYPE, {
            [{1, {tensors, [
                {<<"1">>, {dots, [[{?SET_EVENT_TYPE, {EventId, 1}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId, 1}}],
            [{?SET_EVENT_TYPE, {EventId, 1}},
                {?SET_EVENT_TYPE, {EventId, 2}}]}},
    ?assert(equal(SizeT1, SizeT1)),
    ?assert(equal(SizeT2, SizeT2)),
    ?assert(equal(SizeT3, SizeT3)),
    ?assertNot(equal(SizeT1, SizeT2)),
    ?assertNot(equal(SizeT1, SizeT3)),
    ?assertNot(equal(SizeT2, SizeT3)).

is_inflation_test() ->
    EventId = {<<"object1">>, a},
%%    Set1 =
%%        {?TYPE, {
%%            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
%%            [{?EVENT_TYPE, {EventId, 1}}],
%%            [{?EVENT_TYPE, {EventId, 1}}]}},
    SizeT1 =
        {?TYPE, {
            [{1, {tensors, [
                {<<"1">>, {dots, [[{?SET_EVENT_TYPE, {EventId, 1}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId, 1}}],
            [{?SET_EVENT_TYPE, {EventId, 1}}]}},
%%    Set2 =
%%        {?TYPE, {
%%            [],
%%            [],
%%            [{?EVENT_TYPE, {EventId, 1}}]}},
    SizeT2 =
        {?TYPE, {
            [],
            [],
            [{?SET_EVENT_TYPE, {EventId, 1}}]}},
%%    Set3 =
%%        {?TYPE, {
%%            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
%%            [{?EVENT_TYPE, {EventId, 1}}],
%%            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}]}},
    SizeT3 =
        {?TYPE, {
            [{1, {tensors, [
                {<<"1">>, {dots, [[{?SET_EVENT_TYPE, {EventId, 1}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId, 1}}],
            [{?SET_EVENT_TYPE, {EventId, 1}},
                {?SET_EVENT_TYPE, {EventId, 2}}]}},
    ?assert(is_inflation(SizeT1, SizeT1)),
    ?assert(is_inflation(SizeT1, SizeT2)),
    ?assertNot(is_inflation(SizeT2, SizeT1)),
    ?assert(is_inflation(SizeT1, SizeT3)),
    ?assertNot(is_inflation(SizeT2, SizeT3)),
    ?assertNot(is_inflation(SizeT3, SizeT2)),
    %% check inflation with merge
    ?assert(state_ps_type:is_inflation(SizeT1, SizeT1)),
    ?assert(state_ps_type:is_inflation(SizeT1, SizeT2)),
    ?assertNot(state_ps_type:is_inflation(SizeT2, SizeT1)),
    ?assert(state_ps_type:is_inflation(SizeT1, SizeT3)),
    ?assertNot(state_ps_type:is_inflation(SizeT2, SizeT3)),
    ?assertNot(state_ps_type:is_inflation(SizeT3, SizeT2)).

is_strict_inflation_test() ->
    EventId = {<<"object1">>, a},
%%    Set1 =
%%        {?TYPE, {
%%            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
%%            [{?EVENT_TYPE, {EventId, 1}}],
%%            [{?EVENT_TYPE, {EventId, 1}}]}},
    SizeT1 =
        {?TYPE, {
            [{1, {tensors, [
                {<<"1">>, {dots, [[{?SET_EVENT_TYPE, {EventId, 1}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId, 1}}],
            [{?SET_EVENT_TYPE, {EventId, 1}}]}},
%%    Set2 =
%%        {?TYPE, {
%%            [],
%%            [],
%%            [{?EVENT_TYPE, {EventId, 1}}]}},
    SizeT2 =
        {?TYPE, {
            [],
            [],
            [{?SET_EVENT_TYPE, {EventId, 1}}]}},
%%    Set3 =
%%        {?TYPE, {
%%            [{<<"1">>, [[{?EVENT_TYPE, {EventId, 1}}]]}],
%%            [{?EVENT_TYPE, {EventId, 1}}],
%%            [{?EVENT_TYPE, {EventId, 1}}, {?EVENT_TYPE, {EventId, 2}}]}},
    SizeT3 =
        {?TYPE, {
            [{1, {tensors, [
                {<<"1">>, {dots, [[{?SET_EVENT_TYPE, {EventId, 1}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId, 1}}],
            [{?SET_EVENT_TYPE, {EventId, 1}},
                {?SET_EVENT_TYPE, {EventId, 2}}]}},
    ?assertNot(is_strict_inflation(SizeT1, SizeT1)),
    ?assert(is_strict_inflation(SizeT1, SizeT2)),
    ?assertNot(is_strict_inflation(SizeT2, SizeT1)),
    ?assert(is_strict_inflation(SizeT1, SizeT3)),
    ?assertNot(is_strict_inflation(SizeT2, SizeT3)),
    ?assertNot(is_strict_inflation(SizeT3, SizeT2)).

encode_decode_test() ->
    EventId = {<<"object1">>, a},
    SizeT =
        {?TYPE, {
            [{1, {tensors, [
                {<<"1">>, {dots, [[{?SET_EVENT_TYPE, {EventId, 1}}]]}}]}}],
            [{?SET_EVENT_TYPE, {EventId, 1}}],
            [{?SET_EVENT_TYPE, {EventId, 1}},
                {?SET_EVENT_TYPE, {EventId, 2}}]}},
    Binary = encode(erlang, SizeT),
    ESizeT = decode(erlang, Binary),
    ?assertEqual(SizeT, ESizeT).

-endif.
