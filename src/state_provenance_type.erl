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

%% @doc Common library for provenance CRDTs.

-module(state_provenance_type).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([is_provenance_type/1,
         equal_all_events/2,
         get_events_from_provenance/1,
         get_next_event/2,
         merge_all_events/2,
         is_inflation_all_events/2]).

-export_type([ps_provenance/0,
              ps_subset_events/0,
              ps_all_events/0,
              ps_payload/0]).

-type ps_object_id() :: binary().
-type ps_replica_id() :: term().
-type ps_event_id() :: {ps_object_id(), ps_replica_id()}.

%% @doc An Event.
-type ps_event() :: {ps_event_id(), pos_integer()}.
%% @doc Generally a Dot contains a single Event, but it could have multiple
%%      Events after binary operations (such as product() in set-related
%%      operations).
-type ps_dot() :: ordsets:ordsets(ps_event()).
%% @doc A provenance: a set of Dots.
-type ps_provenance() :: ordsets:ordsets(ps_dot()).

-type ps_provenance_store() :: term().
-type ps_subset_events() :: ordsets:ordsets(ps_event()).
-type ps_all_events() :: {ev_set, ordsets:ordsets(ps_event())}
                       | {vv_set, ordsets:ordsets(ps_event())}.

-type ps_payload() :: {ps_provenance_store(),
                       ps_subset_events(),
                       ps_all_events()}.

-callback new_provenance_store([term()]) -> ps_provenance_store().
-callback get_events_from_provenance_store(ps_provenance_store()) ->
    ordsets:ordsets(ps_event()).

%% @doc Check if the Type is provenance CRDT.
-spec is_provenance_type(state_type:state_type()) -> boolean().
is_provenance_type(Type) ->
    ordsets:is_element(Type, state_provenance_types()).

%% @doc Equality for `ps_all_events()'.
-spec equal_all_events(ps_all_events(), ps_all_events()) -> boolean().
equal_all_events({TypeA, AllEventsA}, {TypeB, AllEventsB}) ->
    TypeA == TypeB andalso AllEventsA == AllEventsB.

%% @doc Return all events in a provenance.
-spec get_events_from_provenance(ps_provenance()) -> ordsets:ordsets(ps_event()).
get_events_from_provenance(Provenance) ->
    ordsets:fold(
        fun(Dot, AccInEvents) ->
            ordsets:union(AccInEvents, Dot)
        end,
        ordsets:new(),
        Provenance).

%% @doc Calculate the next event from the AllEvents.
-spec get_next_event(ps_event_id(), ps_all_events()) -> ps_event().
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

%% @doc Merge two `ps_all_events()'s.
-spec merge_all_events(ps_all_events(), ps_all_events()) -> ps_all_events().
merge_all_events({ev_set, AllEventsA}, {ev_set, AllEventsB}) ->
    {ev_set, ordsets:union(AllEventsA, AllEventsB)}.

%% @doc Check the inflation through the AllEvents section.
-spec is_inflation_all_events(ps_all_events(), ps_all_events()) -> boolean().
is_inflation_all_events({ev_set, AllEventsA}, {ev_set, AllEventsB}) ->
    ordsets:is_subset(AllEventsA, AllEventsB).

%% @private
state_provenance_types() ->
    [
        state_orset_ps,
        state_ormap_ps
    ].
