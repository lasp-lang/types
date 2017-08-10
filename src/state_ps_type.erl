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
    is_related_to/2,
    event_set_union/2,
    event_set_intersection/2,
    event_set_minus/2,
    event_set_max/1]).
-export([
    get_event_set_from_provenance/1,
    plus_provenance/2,
    cross_provenance/2,
    new_subset_events/0,
    join_subset_events/4,
    new_all_events/0,
    join_all_events/2]).

-export_type([
    crdt/0,
    format/0]).
-export_type([
    state_ps_type/0,
    state_ps_object_id/0,
    state_ps_event_type/0,
    state_ps_event_info/0,
    state_ps_event_id/0,
    state_ps_event/0,
    state_ps_event_set/0,
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
-type state_ps_type() ::
    state_ps_aworset_naive |
    state_ps_gcounter_naive |
    state_ps_lwwregister_naive |
    state_ps_singleton_orset_naive |
    state_ps_size_t_naive.
%% A list of types of events.
-type state_ps_event_type() ::
    state_ps_event_bottom |
    state_ps_event_partial_order_independent |
    state_ps_event_partial_order_downward_closed |
    state_ps_event_partial_order_event_set |
    state_ps_event_total_order.
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
-type state_ps_event_set() :: ordsets:ordset(state_ps_event()).
%% A dot: a single reason to exist.
%%     Generally a dot contains a single event, but it could have multiple
%%     events after binary operations (such as product() in set-related
%%     operations).
-type state_ps_dot() :: state_ps_event_set().
%% A provenance: a set of dots.
-type state_ps_provenance() :: ordsets:ordset(state_ps_dot()).
%% A function from the set of elements to that of provenances.
-type state_ps_provenance_store() :: term().
%% A set of survived events.
-type state_ps_subset_events() :: state_ps_event_set().
%% A set of the entire events.
-type state_ps_all_events() :: state_ps_event_set().
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
-spec is_related_to(state_ps_event(), state_ps_event()) -> boolean().
is_related_to({state_ps_event_bottom, _}=_EventL, _EventR) ->
    true;
is_related_to(
    {state_ps_event_partial_order_independent, _}=EventL,
    {state_ps_event_partial_order_independent, _}=EventR) ->
    EventL == EventR;
is_related_to(
    {state_ps_event_partial_order_downward_closed,
        {{_ObjectIdL, _ReplicaIdL}=EventIdL, EventCounterL}}=EventL,
    {state_ps_event_partial_order_downward_closed,
        {{_ObjectIdR, _ReplicaIdR}=EventIdR, EventCounterR}}=EventR) ->
    EventL == EventR orelse
        (EventIdL == EventIdR andalso EventCounterL =< EventCounterR);
is_related_to(
    {state_ps_event_partial_order_event_set, {ObjectIdL, EventSetL}}=EventL,
    {state_ps_event_partial_order_event_set, {ObjectIdR, EventSetR}}=EventR) ->
    EventL == EventR orelse
        (ObjectIdL == ObjectIdR andalso
            ordsets:is_subset(EventSetL, EventSetR));
is_related_to(
    {state_ps_event_total_order, {{ObjectId, ReplicaIdL}, CounterL}}=EventL,
    {state_ps_event_total_order, {{ObjectId, ReplicaIdR}, CounterR}}=EventR) ->
    EventL == EventR orelse
        CounterL < CounterR orelse
        (CounterL == CounterR andalso ReplicaIdL < ReplicaIdR);
is_related_to(_EventL, _EventR) ->
    false.

%% @doc @todo
-spec event_set_union(
    state_ps_subset_events() | state_ps_all_events(),
    state_ps_subset_events() | state_ps_all_events()) ->
    state_ps_subset_events() | state_ps_all_events().
event_set_union(EventsL, EventsR) ->
    ordsets:union(EventsL, EventsR).

%% @doc @todo
-spec event_set_intersection(
    state_ps_subset_events() | state_ps_all_events(),
    state_ps_subset_events() | state_ps_all_events()) ->
    state_ps_subset_events() | state_ps_all_events().
event_set_intersection(EventsL, EventsR) ->
    ordsets:intersection(EventsL, EventsR).

%% @doc @todo
-spec event_set_minus(
    state_ps_subset_events() | state_ps_all_events(),
    state_ps_subset_events() | state_ps_all_events()) ->
    state_ps_subset_events() | state_ps_all_events().
event_set_minus(EventsL, EventsR) ->
    ordsets:fold(
        fun(CurEvent, AccMaxEvents) ->
            FoundRelatedTo =
                ordsets:fold(
                    fun(OtherEvent, AccFoundRelatedTo) ->
                        AccFoundRelatedTo orelse
                            is_related_to(CurEvent, OtherEvent)
                    end,
                    false,
                    EventsR),
            case FoundRelatedTo of
                false ->
                    ordsets:add_element(CurEvent, AccMaxEvents);
                true ->
                    AccMaxEvents
            end
        end,
        ordsets:new(),
        EventsL).

%% @doc @todo
-spec event_set_max(state_ps_subset_events() | state_ps_all_events()) ->
    state_ps_subset_events() | state_ps_all_events().
event_set_max(Events) ->
    ordsets:fold(
        fun(CurEvent, AccMaxEvents) ->
            FoundRelatedTo =
                ordsets:fold(
                    fun(OtherEvent, AccFoundRelatedTo) ->
                        AccFoundRelatedTo orelse
                            (CurEvent /= OtherEvent andalso
                                is_related_to(CurEvent, OtherEvent))
                    end,
                    false,
                    Events),
            case FoundRelatedTo of
                false ->
                    ordsets:add_element(CurEvent, AccMaxEvents);
                true ->
                    AccMaxEvents
            end
        end,
        ordsets:new(),
        Events).

%% @doc Return all events in a provenance.
-spec get_event_set_from_provenance(state_ps_provenance()) ->
    state_ps_event_set().
get_event_set_from_provenance(Provenance) ->
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
    {state_ps_provenance(), state_ps_event_set()}.
cross_provenance(ProvenanceL, ProvenanceR) ->
    ordsets:fold(
        fun(DotL, {AccCrossedProvenance0, AccNewEvents0}) ->
            ordsets:fold(
                fun(DotR, {AccCrossedProvenance1, AccNewEvents1}) ->
                    {CrossedDot, CrossedNewEvents} = cross_dot(DotL, DotR),
                    {ordsets:add_element(CrossedDot, AccCrossedProvenance1),
                        ordsets:union(AccNewEvents1, CrossedNewEvents)}
                end,
                {AccCrossedProvenance0, AccNewEvents0},
                ProvenanceR)
        end,
        {ordsets:new(), ordsets:new()},
        ProvenanceL).

%% @doc @todo
-spec new_subset_events() -> state_ps_subset_events().
new_subset_events() ->
    ordsets:new().

%% @doc @todo
-spec join_subset_events(
    state_ps_subset_events(), state_ps_all_events(),
    state_ps_subset_events(), state_ps_all_events()) -> state_ps_subset_events().
join_subset_events(SubsetEventsL, AllEventsL, SubsetEventsR, AllEventsR) ->
    EventSet =
        event_set_union(
            event_set_intersection(SubsetEventsL, SubsetEventsR),
            event_set_union(
                event_set_minus(SubsetEventsL, AllEventsR),
                event_set_minus(SubsetEventsR, AllEventsL))),
    prune_event_set(EventSet).

%% @doc @todo
-spec new_all_events() -> state_ps_all_events().
new_all_events() ->
    ordsets:new().

%% @doc @todo
-spec join_all_events(state_ps_all_events(), state_ps_all_events()) ->
    state_ps_all_events().
join_all_events(AllEventsL, AllEventsR) ->
    event_set_max(event_set_union(AllEventsL, AllEventsR)).

%% @private
-spec cross_dot(state_ps_dot(), state_ps_dot()) ->
    {state_ps_dot(), state_ps_event_set()}.
cross_dot(DotL, DotR) ->
    {CrossedDotL, EventSetsL} =
        ordsets:fold(
            fun({EventTypeL, _EventInfoL}=EventL,
                {AccCrossedDotL, AccEventSetsL}) ->
                case EventTypeL of
                    state_ps_event_partial_order_event_set ->
                        {AccCrossedDotL,
                            ordsets:add_element(EventL, AccEventSetsL)};
                    _ ->
                        {ordsets:add_element(EventL, AccCrossedDotL),
                            AccEventSetsL}
                end
            end,
            {ordsets:new(), ordsets:new()},
            DotL),
    {CrossedDotLR, EventSetsLR} =
        ordsets:fold(
            fun({EventTypeR, _EventInfoR}=EventR,
                {AccCrossedDotLR, AccEventSetsLR}) ->
                case EventTypeR of
                    state_ps_event_partial_order_event_set ->
                        {AccCrossedDotLR,
                            ordsets:add_element(EventR, AccEventSetsLR)};
                    _ ->
                        {ordsets:add_element(EventR, AccCrossedDotLR),
                            AccEventSetsLR}
                end
            end,
            {CrossedDotL, EventSetsL},
            DotR),
    CrossedEventSetsLRDict =
        ordsets:fold(
            fun({state_ps_event_partial_order_event_set, {ObjectId, EventSet}},
                AccCrossedEventSetsLRDict) ->
                orddict:update(
                    ObjectId,
                    fun(OldEventSet) ->
                        ordsets:union(OldEventSet, EventSet)
                    end,
                    EventSet,
                    AccCrossedEventSetsLRDict)
            end,
            orddict:new(),
            EventSetsLR),
    CrossedEventSetsWithNewEvents =
        orddict:fold(
            fun(ObjectId, EventSet, AccCrossedEventSetsWithNewEvents) ->
                ordsets:add_element(
                    {state_ps_event_partial_order_event_set,
                        {ObjectId, EventSet}},
                    AccCrossedEventSetsWithNewEvents)
            end,
            ordsets:new(),
            CrossedEventSetsLRDict),
    {ordsets:union(CrossedDotLR, CrossedEventSetsWithNewEvents),
        CrossedEventSetsWithNewEvents}.

%% @private
-spec prune_event_set(state_ps_event_set()) -> state_ps_event_set().
prune_event_set(EventSet) ->
    ordsets:fold(
        fun({EventType, EventInfo}, AccIn) ->
            case EventType of
                state_ps_event_partial_order_event_set ->
                    {_ObjectId, EventSetInEvent} = EventInfo,
                    Survived =
                        ordsets:fold(
                            fun(Event, AccSurvived) ->
                                AccSurvived andalso
                                    ordsets:is_element(Event, EventSet)
                            end,
                            true,
                            EventSetInEvent),
                    case Survived of
                        false ->
                            AccIn;
                        true ->
                            ordsets:add_element({EventType, EventInfo}, AccIn)
                    end;
                _ ->
                    ordsets:add_element({EventType, EventInfo}, AccIn)
            end
        end,
        ordsets:new(),
        EventSet).
