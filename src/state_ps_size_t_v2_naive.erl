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

%% @doc Singleton Observed-Remove Set CRDT with the POE OR Set design:
%%     singleton observed-remove set without tombstones.

-module(state_ps_size_t_v2_naive).

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
    length/1,
    threshold_met/2,
    threshold_met_strict/2]).

-export_type([
    state_ps_size_t_v2_naive/0,
    state_ps_size_t_v2_naive_op/0]).

-type payload() :: state_ps_poe_orset:state_ps_poe_orset().
-opaque state_ps_size_t_v2_naive() :: {?TYPE, payload()}.
-type state_ps_size_t_v2_naive_op() :: no_op.

%% @doc Create a new, empty `state_ps_size_t_v2_naive()'.
-spec new() -> state_ps_size_t_v2_naive().
new() ->
    {?TYPE, state_ps_poe_orset:new()}.

%% @doc Create a new, empty `state_ps_size_t_v2_naive()'
-spec new([term()]) -> state_ps_size_t_v2_naive().
new([_]) ->
    new().

%% @doc Mutate a `state_ps_size_t_v2_naive()'.
-spec mutate(
    state_ps_size_t_v2_naive_op(),
    type:id(),
    state_ps_size_t_v2_naive()) -> {ok, state_ps_size_t_v2_naive()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_ps_type:mutate(Op, Actor, CRDT).

%% @doc Returns the value of the `state_ps_size_t_v2_naive()'.
%%      This value is a set of not-removed elements.
-spec query(state_ps_size_t_v2_naive()) -> term().
query({?TYPE, Payload}) ->
    InternalSet = sets:to_list(state_ps_poe_orset:read(Payload)),
    case InternalSet of
        [] ->
            0;
        [Length] ->
            Length
    end.

%% @doc Equality for `state_ps_size_t_v2_naive()'.
-spec equal(
    state_ps_size_t_v2_naive(), state_ps_size_t_v2_naive()) ->
    boolean().
equal({?TYPE, PayloadA}, {?TYPE, PayloadB}) ->
    state_ps_poe_orset:equal(PayloadA, PayloadB).

%% @doc Delta-mutate a `state_ps_size_t_v2_naive()'.
%%      The first argument can be:
%%          - `{add, element()}'
%%          - `{rmv, element()}'
%%      The second argument is the event id ({object_id, replica_id}).
%%      The third argument is the `state_ps_size_t_v2_naive()' to be
%%          inflated.
-spec delta_mutate(
    state_ps_size_t_v2_naive_op(),
    type:id(),
    state_ps_size_t_v2_naive()) -> {ok, state_ps_size_t_v2_naive()}.
delta_mutate(no_op, _Actor, {?TYPE, Payload}) ->
    {ok, {?TYPE, Payload}}.

%% @doc Merge two `state_ps_size_t_v2_naive()'.
-spec merge(
    state_ps_size_t_v2_naive(), state_ps_size_t_v2_naive()) ->
    state_ps_size_t_v2_naive().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun merge_state_ps_size_t_v2_naive/2,
    state_ps_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Given two `state_ps_size_t_v2_naive()', check if the second is an
%%     inflation of the first.
-spec is_inflation(
    state_ps_size_t_v2_naive(), state_ps_size_t_v2_naive()) ->
    boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(
    state_ps_size_t_v2_naive(), state_ps_size_t_v2_naive()) ->
    boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_strict_inflation(CRDT1, CRDT2).

-spec encode(
    state_ps_type:format(), state_ps_size_t_v2_naive()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(
    state_ps_type:format(), binary()) -> state_ps_size_t_v2_naive().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.

%% @doc Calculate the next event from the AllEvents.
-spec get_next_event(
    state_ps_type:state_ps_event_id(),
    state_ps_type:state_ps_payload()) -> state_ps_type:state_ps_event().
get_next_event(_EventId, _Payload) ->
    {state_ps_event_bottom, undefined}.

%% @doc @todo
-spec length(state_ps_type:crdt()) -> state_ps_size_t_v2_naive().
length(
    {state_ps_aworset_naive,
        {ProvenanceStore, SubsetEvents, AllEvents}=_Payload}) ->
    {Length, LengthProvenance} =
        orddict:fold(
            fun(_Elem, Provenance, {AccLength, AccLengthProvenance}) ->
                NewProvenance =
                    case AccLengthProvenance of
                        [] ->
                            Provenance;
                        _ ->
                            state_ps_type:cross_provenance(
                                AccLengthProvenance, Provenance)
                    end,
                {AccLength + 1, NewProvenance}
            end,
            {0, ordsets:new()},
            ProvenanceStore),
    {NewLengthProvenance, AddedEvents} =
        ordsets:fold(
            fun(Dot, {AccNewLengthProvenance, AccAddedEvents}) ->
                DotWithoutESet =
                    ordsets:fold(
                        fun({EventType, _EventInfo}=Event, AccDotWithoutESet) ->
                            case EventType of
                                state_ps_event_partial_order_event_set ->
                                    AccDotWithoutESet;
                                _ ->
                                    ordsets:add_element(
                                        Event, AccDotWithoutESet)
                            end
                        end,
                        ordsets:new(),
                        Dot),
                NewEvent =
                    {state_ps_event_partial_order_event_set, DotWithoutESet},
                NewDot =
                    state_ps_type:events_max(
                        ordsets:add_element(NewEvent, Dot)),
                NewProvenance =
                    ordsets:add_element(NewDot, AccNewLengthProvenance),
                NewAddedEvents = ordsets:add_element(NewEvent, AccAddedEvents),
                {NewProvenance, NewAddedEvents}
            end,
            {ordsets:new(), ordsets:new()},
            LengthProvenance),
    NewProvenanceStore =
        case NewLengthProvenance of
            [] ->
                orddict:new();
            _ ->
                orddict:store(
                    Length, NewLengthProvenance, orddict:new())
        end,
    NewSubsetEvents =
        state_ps_type:events_max(
            state_ps_type:events_union(
                SubsetEvents,
                state_ps_type:add_event_to_events(
                    AddedEvents, state_ps_type:new_subset_events()))),
    NewAllEvents =
        state_ps_type:events_max(
            state_ps_type:events_union(
                AllEvents,
                state_ps_type:add_event_to_events(
                    AddedEvents, state_ps_type:new_all_events()))),
    NewPayload = {NewProvenanceStore, NewSubsetEvents, NewAllEvents},
    {?TYPE, NewPayload}.

%% @doc Determine if a threshold is met.
threshold_met(Threshold, {?TYPE, {ProvenanceStore, _, _}=_Payload}=CRDT) ->
    case orddict:size(ProvenanceStore) > 1 of
        false ->
            is_inflation(Threshold, CRDT);
        true ->
            false
    end.

%% @doc Determine if a threshold is met.
threshold_met_strict(
    Threshold, {?TYPE, {ProvenanceStore, _, _}=_Payload}=CRDT) ->
    case orddict:size(ProvenanceStore) > 1 of
        false ->
            is_strict_inflation(Threshold, CRDT);
        true ->
            false
    end.

%% @private
merge_state_ps_size_t_v2_naive({?TYPE, PayloadA}, {?TYPE, PayloadB}) ->
    MergedPayload = state_ps_poe_orset:join(PayloadA, PayloadB),
    {?TYPE, MergedPayload}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, state_ps_poe_orset:new()}, new()).

-endif.
