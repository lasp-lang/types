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
    events_max/1,
    events_union/2]).
-export([
    get_events_from_provenance/1,
    plus_provenance/2,
    cross_provenance/2,
    new_subset_events/0,
    join_subset_events/4,
    new_all_events/0,
    join_all_events/2,
    add_event_to_events/2]).

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
                       | state_ps_singleton_orset_naive
                       | state_ps_size_t_naive.
%% A list of types of events.
-type state_ps_event_type() :: state_ps_event_bottom
                             | state_ps_event_partial_order_independent
                             | state_ps_event_partial_order_downward_closed
                             | state_ps_event_partial_order_provenance
                             | state_ps_event_partial_order_dot
                             | state_ps_event_partial_order_event_set
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
is_dominant({state_ps_event_bottom, _}=_EventL, _EventR) ->
    true;
is_dominant(
    {state_ps_event_partial_order_independent, _}=EventL,
    {state_ps_event_partial_order_independent, _}=EventR) ->
    EventL == EventR;
is_dominant(
    {state_ps_event_partial_order_downward_closed,
        {{_ObjectIdL, _ReplicaIdL}=EventIdL, EventCounterL}}=EventL,
    {state_ps_event_partial_order_downward_closed,
        {{_ObjectIdR, _ReplicaIdR}=EventIdR, EventCounterR}}=EventR) ->
    EventL == EventR orelse
        (EventIdL == EventIdR andalso EventCounterL =< EventCounterR);
is_dominant(
    {state_ps_event_partial_order_provenance,
        {most_dominant, EventsSetL}}=EventL,
    {state_ps_event_partial_order_provenance,
        {most_dominant, EventsSetR}}=EventR) ->
    EventL == EventR orelse
        ordsets:is_subset(EventsSetL, EventsSetR);
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
    ordsets:intersection(EventsInProvenanceL, EventsSetR) /= [];
is_dominant(
    {state_ps_event_partial_order_provenance, ProvenanceL}=EventL,
    {state_ps_event_partial_order_provenance, ProvenanceR}=EventR) ->
    EventL == EventR orelse
        is_dominant_provenances_private(ProvenanceL, ProvenanceR);
is_dominant(
    {state_ps_event_partial_order_dot, DotL}=EventL,
    {state_ps_event_partial_order_dot, DotR}=EventR) ->
    EventL == EventR orelse
        (ordsets:size(DotL) == ordsets:size(DotR) andalso
            is_dominant_dots_private(DotL, DotR));
is_dominant(
    {state_ps_event_partial_order_event_set, EventSetL}=EventL,
    {state_ps_event_partial_order_event_set, EventSetR}=EventR) ->
    EventL == EventR orelse
        ordsets:is_subset(EventSetL, EventSetR);
is_dominant(
    {state_ps_event_total_order, {{ObjectId, ReplicaIdL}, CounterL}}=EventL,
    {state_ps_event_total_order, {{ObjectId, ReplicaIdR}, CounterR}}=EventR) ->
    EventL == EventR orelse
        CounterL < CounterR orelse
        (CounterL == CounterR andalso ReplicaIdL < ReplicaIdR);
is_dominant(_EventL, _EventR) ->
    false.

%% @private
is_dominant_dots_private(DotL, DotR) ->
    {IsDominant, Others} =
        ordsets:fold(
            fun(EventInDotL, {AccIsDominant, AccOthers}) ->
                case AccIsDominant of
                    true ->
                        NewAccOthers =
                            ordsets:fold(
                                fun(EventInDotR, AccNewAccOthers) ->
                                    case is_dominant(EventInDotL, EventInDotR) of
                                        true ->
                                            AccNewAccOthers;
                                        false ->
                                            ordsets:add_element(
                                                EventInDotR,
                                                AccNewAccOthers)
                                    end
                                end,
                                ordsets:new(),
                                AccOthers),
                        case NewAccOthers == AccOthers of
                            true ->
                                {false, AccOthers};
                            false ->
                                {true, NewAccOthers}
                        end;
                    false ->
                        {false, AccOthers}
                end
            end,
            {true, DotR},
            DotL),
    IsDominant andalso Others == [].

%% @private
is_dominant_provenances_private(ProvenanceL, ProvenanceR) ->
    NotDominantDots =
        ordsets:fold(
            fun(DotL, AccNotDominantDots) ->
                ordsets:fold(
                    fun(DotR, AccNotDominantDots0) ->
                        case is_dominant(
                            {state_ps_event_partial_order_dot, DotL},
                            {state_ps_event_partial_order_dot, DotR}) of
                            true ->
                                ordsets:add_element(DotL, AccNotDominantDots0);
                            false ->
                                AccNotDominantDots0
                        end
                    end,
                    AccNotDominantDots,
                    ProvenanceR)
            end,
            ordsets:new(),
            ProvenanceL),
    NotDominantDots == ProvenanceL.

%% @doc @todo
-spec events_union(
    state_ps_subset_events() | state_ps_all_events(),
    state_ps_subset_events() | state_ps_all_events()) ->
    state_ps_subset_events() | state_ps_all_events().
events_union(EventsL, EventsR) ->
    {Union, MostDominant} =
        ordsets:fold(
            fun({EventType, EventInfo}=Event,
                {AccUnion, AccMostDominant}) ->
                case EventType == state_ps_event_partial_order_provenance of
                    true ->
                        case EventInfo of
                            {most_dominant, _} ->
                                {AccUnion,
                                    ordsets:add_element(
                                        Event, AccMostDominant)};
                            _ ->
                                {ordsets:add_element(Event, AccUnion),
                                    AccMostDominant}
                        end;
                    false ->
                        {ordsets:add_element(Event, AccUnion),
                            AccMostDominant}
                end
            end,
            {ordsets:new(), ordsets:new()},
            ordsets:union(EventsL, EventsR)),
    case ordsets:size(MostDominant) of
        2 ->
            [
                {state_ps_event_partial_order_provenance,
                    {most_dominant, SetL}},
                {state_ps_event_partial_order_provenance,
                    {most_dominant, SetR}}] = MostDominant,
            NewMostDominant =
                {state_ps_event_partial_order_provenance,
                    {most_dominant, ordsets:union(SetL, SetR)}},
            ordsets:add_element(NewMostDominant, Union);
        _ -> %% shouldn't be more than 2.
            ordsets:union(Union, MostDominant)
    end.

%% @doc @todo
-spec events_intersection(
    state_ps_subset_events() | state_ps_all_events(),
    state_ps_subset_events() | state_ps_all_events()) ->
    state_ps_subset_events() | state_ps_all_events().
events_intersection(EventsL, EventsR) ->
    ordsets:intersection(EventsL, EventsR).

%% @doc @todo
-spec events_minus(
    state_ps_subset_events() | state_ps_all_events(),
    state_ps_subset_events() | state_ps_all_events()) ->
    state_ps_subset_events() | state_ps_all_events().
events_minus(EventsL, EventsR) ->
    ordsets:fold(
        fun(CurEvent, AccMaxEvents) ->
            FoundDominant =
                ordsets:fold(
                    fun(OtherEvent, AccFoundDominant) ->
                        AccFoundDominant orelse
                            is_dominant(CurEvent, OtherEvent)
                    end,
                    false,
                    EventsR),
            case FoundDominant of
                false ->
                    ordsets:add_element(CurEvent, AccMaxEvents);
                true ->
                    AccMaxEvents
            end
        end,
        ordsets:new(),
        EventsL).

%% @doc @todo
-spec events_max(state_ps_subset_events() | state_ps_all_events()) ->
    state_ps_subset_events() | state_ps_all_events().
events_max(Events) ->
    ordsets:fold(
        fun(CurEvent, AccMaxEvents) ->
            FoundDominant =
                ordsets:fold(
                    fun(OtherEvent, AccFoundDominant) ->
                        AccFoundDominant orelse
                            (CurEvent /= OtherEvent andalso
                                is_dominant(CurEvent, OtherEvent))
                    end,
                    false,
                    Events),
            case FoundDominant of
                false ->
                    ordsets:add_element(CurEvent, AccMaxEvents);
                true ->
                    AccMaxEvents
            end
        end,
        ordsets:new(),
        Events).

%% @doc @todo
-spec prune_event_set(
    ordsets:ordset(state_ps_event())) -> ordsets:ordset(state_ps_event()).
prune_event_set(EventSet) ->
    ordsets:fold(
        fun({EventType, EventInfo}, AccIn) ->
            case EventType of
                state_ps_event_partial_order_event_set ->
                    Survived =
                        ordsets:fold(
                            fun(Event, AccSurvived) ->
                                AccSurvived andalso
                                    ordsets:is_element(Event, EventSet)
                            end,
                            true,
                            EventInfo),
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
-spec join_subset_events(
    state_ps_subset_events(), state_ps_all_events(),
    state_ps_subset_events(), state_ps_all_events()) -> state_ps_subset_events().
join_subset_events(SubsetEventsL, AllEventsL, SubsetEventsR, AllEventsR) ->
    EventSet =
        events_union(
            events_intersection(SubsetEventsL, SubsetEventsR),
            events_union(
                events_minus(SubsetEventsL, AllEventsR),
                events_minus(SubsetEventsR, AllEventsL))),
    prune_event_set(EventSet).

%% @doc @todo
-spec new_all_events() -> state_ps_all_events().
new_all_events() ->
    ordsets:new().

%% @doc @todo
-spec join_all_events(state_ps_all_events(), state_ps_all_events()) ->
    state_ps_all_events().
join_all_events(AllEventsL, AllEventsR) ->
    events_max(events_union(AllEventsL, AllEventsR)).

%% @doc @todo
-spec add_event_to_events(
    ordsets:ordset(state_ps_event()),
    state_ps_subset_events() | state_ps_all_events()) ->
    state_ps_subset_events() | state_ps_all_events().
add_event_to_events(Events, EventSet) ->
    ordsets:union(Events, EventSet).
