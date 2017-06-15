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

%% @doc @todo

-module(state_ps_type).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-export([
    mutate/3,
    merge/3,
    is_inflation/2,
    is_strict_inflation/2]).
-export([
    is_dominant/2,
    max_events/1,
    minus_events/2]).
-export([
    get_events_from_provenance/1,
    plus_provenance/2,
    cross_provenance/2,
    new_subset_events/0,
    new_all_events/0]).

-export_type([
    crdt/0,
    format/0]).
-export_type([
    state_ps_type/0,
    state_ps_event_type/0,
    state_ps_event_info/0,
    state_ps_event_id/0,
    state_ps_event/0,
    state_ps_dot/0,
    state_ps_provenance/0,
    state_ps_provenance_store/0,
    state_ps_subset_events/0,
    state_ps_all_events/0,
    state_ps_payload/0]).

%% Define some initial types.
-type crdt() :: {state_ps_type(), type:payload()}.
%% Supported serialization formats.
-type format() :: erlang.
%% A list of types with the provenance semiring.
-type state_ps_type() :: state_ps_aworset_naive
                       | state_ps_gcounter_naive
                       | state_ps_lwwregister_naive
                       | state_ps_size_t_naive.
%% A list of types of events.
-type state_ps_event_type() :: state_ps_event_partial_order_independent
                             | state_ps_event_partial_order_downward_closed
                             | state_ps_event_partial_order_provenance
                             | state_ps_event_total_order.
%% The contents of an event.
-type state_ps_event_info() :: term().
%% An identification for each object (must be unique).
-type state_ps_object_id() :: binary().
%% An identification for each replica (must be comparable/can be ordered).
-type state_ps_replica_id() :: term().
-type state_ps_event_id() :: {state_ps_object_id(), state_ps_replica_id()}.
%% An event: this will be generated on related update operations.
%%     Even though all events use the same representation, how they can be
%%     ordered can be different based on the types of events.
-type state_ps_event() ::
    {state_ps_event_type(), state_ps_event_info()}.
%% A dot: a single reason to exist.
%%     Generally a dot contains a single event, but it could have multiple
%%     events after binary operations (such as product() in set-related
%%     operations).
-type state_ps_dot() :: ordsets:ordset(state_ps_event()).
%% A provenance: a set of dots.
-type state_ps_provenance() :: ordsets:ordset(state_ps_dot()).
%% A function from the set of elements to that of provenances.
-type state_ps_provenance_store() :: term().
%% A set of survived events.
-type state_ps_subset_events() :: ordsets:ordset(state_ps_event()).
%% A set of the entire events.
-type state_ps_all_events() :: ordsets:ordset(state_ps_event()).
-type state_ps_payload() ::
    {state_ps_provenance_store(),
        state_ps_subset_events(),
        state_ps_all_events()}.

%% Perform a delta mutation.
-callback delta_mutate(type:operation(), type:id(), crdt()) ->
    {ok, crdt()} | {error, type:error()}.
%% Merge two replicas.
%% If we merge two CRDTs, the result is a CRDT.
%% If we merge a delta and a CRDT, the result is a CRDT.
%% If we merge two deltas, the result is a delta (delta group).
-callback merge(crdt(), crdt()) -> crdt().
%% Inflation testing.
-callback is_inflation(crdt(), crdt()) -> boolean().
-callback is_strict_inflation(crdt(), crdt()) -> boolean().
%% @todo These should be moved to type.erl
%% Encode and Decode.
-callback encode(format(), crdt()) -> binary().
-callback decode(format(), binary()) -> crdt().
%% @doc Calculate the next event from the AllEvents.
-callback get_next_event(state_ps_event_id(), state_ps_payload()) ->
    state_ps_event().

%% @doc Generic Mutate.
-spec mutate(type:operation(), type:id(), crdt()) ->
    {ok, crdt()} | {error, type:error()}.
mutate(Op, Actor, {Type, _}=CRDT) ->
    case Type:delta_mutate(Op, Actor, CRDT) of
        {ok, {Type, Delta}} ->
            {ok, Type:merge({Type, Delta}, CRDT)};
        Error ->
            Error
    end.

%% @doc Generic Merge.
-spec merge(crdt(), crdt(), function()) -> crdt().
merge({Type, CRDT1}, {Type, CRDT2}, MergeFun) ->
    MergeFun({Type, CRDT1}, {Type, CRDT2}).

%% @doc Generic check for inflation.
-spec is_inflation(crdt(), crdt()) -> boolean().
is_inflation({Type, _}=CRDT1, {Type, _}=CRDT2) ->
    Type:equal(Type:merge(CRDT1, CRDT2), CRDT2).

%% @doc Generic check for strict inflation.
%%     We have a strict inflation if:
%%         - we have an inflation
%%         - we have different CRDTs
-spec is_strict_inflation(crdt(), crdt()) -> boolean().
is_strict_inflation({Type, _}=CRDT1, {Type, _}=CRDT2) ->
    Type:is_inflation(CRDT1, CRDT2) andalso
        not Type:equal(CRDT1, CRDT2).

%% @doc @todo
-spec is_dominant(state_ps_event(), state_ps_event()) -> boolean().
is_dominant(
    {state_ps_event_partial_order_independent, _}=EventL,
    {state_ps_event_partial_order_independent, _}=EventR) ->
    EventL == EventR;
is_dominant(
    {
        state_ps_event_partial_order_downward_closed,
        {{ObjectId, ReplicaIdL}, CounterL}}=EventL,
    {
        state_ps_event_partial_order_downward_closed,
        {{ObjectId, ReplicaIdR}, CounterR}}=EventR) ->
    EventL == EventR
        orelse (ReplicaIdL == ReplicaIdR andalso CounterL =< CounterR);
is_dominant(
    {state_ps_event_partial_order_provenance,
        {most_dominant, EventsSetL}}=EventL,
    {state_ps_event_partial_order_provenance,
        {most_dominant, EventsSetR}}=EventR) ->
    EventL == EventR
        orelse ordsets:is_subset(EventsSetL, EventsSetR);
is_dominant(
    {state_ps_event_partial_order_provenance,
        {most_dominant, _EventsSetL}}=_EventL,
    {state_ps_event_partial_order_provenance, _ProvenanceR}=_EventR) ->
    false;
is_dominant(
    {state_ps_event_partial_order_provenance, ProvenanceL}=_EventL,
    {state_ps_event_partial_order_provenance,
        {most_dominant, EventsSetR}}=_EventR) ->
    EventsInProvenanceL = get_events_from_provenance(ProvenanceL),
    ordsets:is_subset(EventsInProvenanceL, EventsSetR);
is_dominant(
    {state_ps_event_partial_order_provenance, ProvenanceL}=EventL,
    {state_ps_event_partial_order_provenance, ProvenanceR}=EventR) ->
    EventL == EventR
        orelse ordsets:is_subset(ProvenanceL, ProvenanceR);
is_dominant(
    {state_ps_event_total_order, {{ObjectId, ReplicaIdL}, CounterL}}=EventL,
    {state_ps_event_total_order, {{ObjectId, ReplicaIdR}, CounterR}}=EventR) ->
    EventL == EventR
        orelse CounterL < CounterR
        orelse (CounterL == CounterR andalso ReplicaIdL < ReplicaIdR).

%% @doc @todo
-spec max_events(ordsets:ordset(state_ps_event())) ->
    ordsets:ordset(state_ps_event()).
max_events(EventSet) ->
    ordsets:fold(
        fun(CurEvent, AccMaxEvents) ->
            FoundDominant =
                ordsets:fold(
                    fun(OtherEvent, AccFoundDominant) ->
                        {CurEventType, _} = CurEvent,
                        {OtherEventType, _} = OtherEvent,
                        AccFoundDominant orelse
                            (CurEventType == OtherEventType andalso
                                is_dominant(CurEvent, OtherEvent) andalso
                                CurEvent /= OtherEvent)
                    end,
                    false,
                    EventSet),
            case FoundDominant of
                false ->
                    ordsets:add_element(CurEvent, AccMaxEvents);
                true ->
                    AccMaxEvents
            end
        end,
        ordsets:new(),
        EventSet).

%% @doc @todo
-spec minus_events(
    ordsets:ordset(state_ps_event()), ordsets:ordset(state_ps_event())) ->
    ordsets:ordset(state_ps_event()).
minus_events(EventSetL, EventSetR) ->
    ordsets:fold(
        fun(EventL, AccMinusEvents) ->
            FoundDominant =
                ordsets:fold(
                    fun(EventR, AccFoundDominant) ->
                        {EventLType, _} = EventL,
                        {EventRType, _} = EventR,
                        AccFoundDominant orelse
                            (EventLType == EventRType andalso
                                is_dominant(EventL, EventR))
                    end,
                    false,
                    EventSetR),
            case FoundDominant of
                false ->
                    ordsets:add_element(EventL, AccMinusEvents);
                true ->
                    AccMinusEvents
            end
        end,
        ordsets:new(),
        EventSetL).

%% @doc Return all events in a provenance.
-spec get_events_from_provenance(state_ps_provenance()) ->
    ordsets:ordset(state_ps_event()).
get_events_from_provenance(Provenance) ->
    ordsets:fold(
        fun(Dot, AccInEvents) ->
            ordsets:union(AccInEvents, Dot)
        end,
        ordsets:new(),
        Provenance).

%% @doc Calculate the plus operation of two provenances, which is an union set
%%     of dots in both provenances.
-spec plus_provenance(state_ps_provenance(), state_ps_provenance()) ->
    state_ps_provenance().
plus_provenance(ProvenanceL, ProvenanceR) ->
    ordsets:union(ProvenanceL, ProvenanceR).

%% @doc Calculate the cross operation of two provenances, which is a set of
%%     cross product dots in both provenances. A cross product of two dots is an
%%     union set of two dots.
-spec cross_provenance(state_ps_provenance(), state_ps_provenance()) ->
    state_ps_provenance().
cross_provenance(ProvenanceL, ProvenanceR) ->
    ordsets:fold(
        fun(DotL, AccCrossProvenance0) ->
            ordsets:fold(
                fun(DotR, AccCrossProvenance1) ->
                    CrossDot = ordsets:union(DotL, DotR),
                    ordsets:add_element(CrossDot, AccCrossProvenance1)
                end, AccCrossProvenance0, ProvenanceR)
        end, ordsets:new(), ProvenanceL).

%% @doc @todo
-spec new_subset_events() -> state_ps_subset_events().
new_subset_events() ->
    ordsets:new().

%% @doc @todo
-spec new_all_events() -> state_ps_all_events().
new_all_events() ->
    ordsets:new().
