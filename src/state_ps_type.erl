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

-module(state_ps_type).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-export([mutate/3,
         merge/3,
         is_inflation/2,
         is_strict_inflation/2]).

-export([equal_all_events/2,
         get_events_from_provenance/1,
         get_next_event/2,
         is_ps_type/1,
         merge_all_events/2,
         is_inflation_all_events/2,
         cross_provenance/2]).

-export_type([state_ps_type/0,
              crdt/0,
              format/0]).

-export_type([state_ps_event/0,
              state_ps_provenance/0,
              state_ps_subset_events/0,
              state_ps_all_events/0,
              state_ps_payload/0]).

%% Define some initial types.
-type state_ps_type() :: state_ps_gcounter_full
                       | state_ps_gcounter_naive
                       | state_ps_lwwregister_full
                       | state_ps_lwwregister_naive
                       | state_ps_ormap_full
                       | state_ps_ormap_naive
                       | state_ps_orset_full
                       | state_ps_orset_naive.
-type crdt() :: {state_ps_type(), type:payload()}.

%% Supported serialization formats.
-type format() :: erlang.

-type state_ps_object_id() :: binary().
-type state_ps_replica_id() :: term().
-type state_ps_event_id() :: {state_ps_object_id(), state_ps_replica_id()}.

%% @doc An Event.
-type state_ps_event() :: {state_ps_event_id(), pos_integer()}.
%% @doc Generally a Dot contains a single Event, but it could have multiple
%%      Events after binary operations (such as product() in set-related
%%      operations).
-type state_ps_dot() :: ordsets:ordset(state_ps_event()).
%% @doc A provenance: a set of Dots.
-type state_ps_provenance() :: ordsets:ordset(state_ps_dot()).

-type state_ps_provenance_store() :: term().
-type state_ps_subset_events() :: ordsets:ordset(state_ps_event()).
-type state_ps_all_events() :: {ev_set, ordsets:ordset(state_ps_event())}
                             | {vv_set, ordsets:ordset(state_ps_event())}.

-type state_ps_payload() :: {state_ps_provenance_store(),
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

%% Check if a some state is bottom
-callback is_bottom(crdt()) -> boolean().

%% Inflation testing.
-callback is_inflation(crdt(), crdt()) -> boolean().
-callback is_strict_inflation(crdt(), crdt()) -> boolean().

%% @todo These should be moved to type.erl
%% Encode and Decode.
-callback encode(format(), crdt()) -> binary().
-callback decode(format(), binary()) -> crdt().

%% @todo
-callback new_provenance_store([term()]) -> state_ps_provenance_store().
%% @todo
-callback get_events_from_provenance_store(state_ps_provenance_store()) ->
    ordsets:ordset(state_ps_event()).

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
%%      We have a strict inflation if:
%%          - we have an inflation
%%          - we have different CRDTs
-spec is_strict_inflation(crdt(), crdt()) -> boolean().
is_strict_inflation({Type, _}=CRDT1, {Type, _}=CRDT2) ->
    Type:is_inflation(CRDT1, CRDT2) andalso
    not Type:equal(CRDT1, CRDT2).

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

%% @doc Equality for `state_ps_all_events()'.
-spec equal_all_events(state_ps_all_events(), state_ps_all_events()) ->
    boolean().
equal_all_events({TypeA, AllEventsA}, {TypeB, AllEventsB}) ->
    TypeA == TypeB andalso AllEventsA == AllEventsB.

%% @doc Calculate the next event from the AllEvents.
-spec get_next_event(state_ps_event_id(), state_ps_all_events()) ->
    state_ps_event().
get_next_event(EventId, {ev_set, AllEvents}) ->
    MaxCnt = ordsets:fold(
        fun({EventId0, Counter0}, AccInMaxCnt) ->
            case EventId0 of
                EventId ->
                    max(AccInMaxCnt, Counter0);
                _ ->
                    AccInMaxCnt
            end
        end,
        0,
        AllEvents),
    {EventId, MaxCnt + 1}.

%% @doc Check if the Type is ps CRDT.
-spec is_ps_type(state_ps_type() | state_type:state_type()) -> boolean().
is_ps_type(Type) ->
    ordsets:is_element(Type, state_ps_types()).

%% @doc Merge two `state_ps_all_events()'s.
-spec merge_all_events(state_ps_all_events(), state_ps_all_events()) ->
    state_ps_all_events().
merge_all_events({ev_set, AllEventsA}, {ev_set, AllEventsB}) ->
    {ev_set, ordsets:union(AllEventsA, AllEventsB)}.

%% @doc Check the inflation through the AllEvents section.
-spec is_inflation_all_events(state_ps_all_events(), state_ps_all_events()) ->
    boolean().
is_inflation_all_events({ev_set, AllEventsA}, {ev_set, AllEventsB}) ->
    ordsets:is_subset(AllEventsA, AllEventsB).

%% @doc Calculate the cross product provenance of two provenances, which
%%      is a set of cross product dots in both provenances.
%%      A cross product of two dots is an union set of two dots.
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

%% @private
state_ps_types() ->
    [
        state_ps_gcounter_full,
        state_ps_gcounter_naive,
        state_ps_lwwregister_full,
        state_ps_lwwregister_naive,
        state_ps_ormap_full,
        state_ps_ormap_naive,
        state_ps_orset_full,
        state_ps_orset_naive
    ].
