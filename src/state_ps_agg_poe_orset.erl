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

-module(state_ps_agg_poe_orset).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    new/0,
    equal/2,
    insert/3,
    delta_insert/3,
    delete/2,
    delta_delete/2,
    read/1,
    join/2,
    map/2,
    filter/2,
    union/2,
    product/2]).

-export_type([
    state_ps_agg_poe_orset/0]).

-type element() :: term().
%% A function from the set of elements to that of provenances.
-type state_ps_agg_provenance_store() ::
    orddict:orddict(element(), state_ps_agg_type:state_ps_agg_provenance()).
-type state_ps_agg_poe_orset() ::
    {state_ps_agg_provenance_store(),
        state_ps_agg_type:state_ps_agg_subset_events(),
        state_ps_agg_type:state_ps_agg_all_events()}.

%% @doc Create a new, empty `state_ps_agg_poe_orset()'.
-spec new() -> state_ps_agg_poe_orset().
new() ->
    {orddict:new(),
        state_ps_agg_type:new_subset_events(),
        state_ps_agg_type:new_all_events()}.

%% @doc Equality for `state_ps_agg_poe_orset()'.
%%      Since everything is ordered, == should work.
-spec equal(state_ps_agg_poe_orset(), state_ps_agg_poe_orset()) -> boolean().
equal(
    {ProvenanceStoreL, SubsetEventsL, AllEventsL}=_ORSetL,
    {ProvenanceStoreR, SubsetEventsR, AllEventsR}=_ORSetR) ->
    ProvenanceStoreL == ProvenanceStoreR andalso
        SubsetEventsL == SubsetEventsR andalso
        AllEventsL == AllEventsR.

%% @doc @todo
-spec insert(
    state_ps_agg_type:state_ps_agg_event(),
    element(),
    state_ps_agg_poe_orset()) -> state_ps_agg_poe_orset().
insert(Event, Elem, ORSet) ->
    join(ORSet, delta_insert(Event, Elem, ORSet)).

%% @doc @todo
-spec delta_insert(
    state_ps_agg_type:state_ps_agg_event(),
    element(),
    state_ps_agg_poe_orset()) -> state_ps_agg_poe_orset().
delta_insert(
    Event,
    Elem,
    {_ProvenanceStore, _SubsetEvents, AllEvents}=ORSet) ->
    CanBeSkipped =
        ordsets:fold(
            fun(MaxEvent, AccCanBeSkipped) ->
                AccCanBeSkipped orelse
                    (Event /= MaxEvent andalso
                        state_ps_agg_type:is_partially_ordered(Event, MaxEvent))
            end,
            false,
            AllEvents),
    case CanBeSkipped of
        true ->
            ORSet;
        false ->
            NewDots =
                ordsets:add_element(
                    ordsets:add_element(Event, ordsets:new()), ordsets:new()),
            NewProvenance = {dots, NewDots},
            {orddict:store(Elem, NewProvenance, orddict:new()),
                state_ps_agg_type:add_event_to_events(
                    ordsets:add_element(Event, ordsets:new()),
                    state_ps_agg_type:new_subset_events()),
                state_ps_agg_type:add_event_to_events(
                    ordsets:add_element(Event, ordsets:new()),
                    state_ps_agg_type:new_all_events())}
    end.

%% @doc @todo
-spec delete(element(), state_ps_agg_poe_orset()) -> state_ps_agg_poe_orset().
delete(Elem, ORSet) ->
    join(ORSet, delta_delete(Elem, ORSet)).

%% @doc @todo
-spec delta_delete(element(), state_ps_agg_poe_orset()) ->
    state_ps_agg_poe_orset().
delta_delete(
    Elem, {ProvenanceStore, _SubsetEvents, _AllEvents}=ORSet) ->
    case orddict:find(Elem, ProvenanceStore) of
        {ok, Provenance} ->
            DeletedEvents =
                state_ps_agg_type:get_events_from_provenance(Provenance),
            {orddict:new(),
                state_ps_agg_type:new_subset_events(),
                state_ps_agg_type:add_event_to_events(
                    DeletedEvents,
                    state_ps_agg_type:new_all_events())};
        error ->
            ORSet
    end.

%% @doc @todo
-spec read(state_ps_agg_poe_orset()) -> sets:set().
read({ProvenanceStore, _SubsetEvents, _AllEvents}=_ORSet) ->
    orddict:fold(
        fun(Elem, Provenance, AccResultSet) ->
            case Provenance of
                {dots, _Dots} ->
                    sets:add_element(Elem, AccResultSet);
                {tensors, TensorDict} ->
                    AggElems =
                        orddict:fold(
                            fun(AggElem, _AggProvenance, AccAggElems) ->
                                case AggElem of
                                    undefined ->
                                        AccAggElems;
                                    _ ->
                                        sets:add_element(AggElem, AccAggElems)
                                end
                            end,
                            sets:new(),
                            TensorDict),
                    case AggElems of
                        [] ->
                            sets:add_element(Elem, AccResultSet);
                        _ ->
                            sets:add_element({Elem, AggElems}, AccResultSet)
                    end
            end
        end,
        sets:new(),
        ProvenanceStore).

%% @doc @todo
-spec join(state_ps_agg_poe_orset(), state_ps_agg_poe_orset()) ->
    state_ps_agg_poe_orset().
join(
    {ProvenanceStoreL, SubsetEventsL, AllEventsL}=_ORSetL,
    {ProvenanceStoreR, SubsetEventsR, AllEventsR}=_ORSetR) ->
    JoinedAllEvents = state_ps_agg_type:join_all_events(AllEventsL, AllEventsR),
    JoinedSubsetEvents =
        state_ps_agg_type:join_subset_events(
            SubsetEventsL, AllEventsL, SubsetEventsR, AllEventsR),
    MergedProvenanceStore =
        orddict:merge(
            fun(_Elem, ProvenanceL, ProvenanceR) ->
                state_ps_agg_type:plus_provenance(ProvenanceL, ProvenanceR)
            end,
            ProvenanceStoreL,
            ProvenanceStoreR),
    JoinedProvenanceStore =
        prune_provenance_store(MergedProvenanceStore, JoinedSubsetEvents),
    {JoinedProvenanceStore, JoinedSubsetEvents, JoinedAllEvents}.

%% @doc @todo
-spec map(function(), state_ps_agg_poe_orset()) -> state_ps_agg_poe_orset().
map(Function, {ProvenanceStore, SubsetEvents, AllEvents}=_ORSet) ->
    MapProvenanceStore =
        orddict:fold(
            fun(Elem, Provenance, AccMapProvenanceStore) ->
                orddict:update(
                    Function(Elem),
                    fun(OldProvenance) ->
                        state_ps_agg_type:plus_provenance(
                            OldProvenance, Provenance)
                    end,
                    Provenance,
                    AccMapProvenanceStore)
            end,
            orddict:new(),
            ProvenanceStore),
    {MapProvenanceStore, SubsetEvents, AllEvents}.

%% @doc @todo
-spec filter(function(), state_ps_agg_poe_orset()) -> state_ps_agg_poe_orset().
filter(Function, {ProvenanceStore, SubsetEvents, AllEvents}=_ORSet) ->
    FilterProvenanceStore =
        orddict:fold(
            fun(Elem, Provenance, AccFilterProvenanceStore) ->
                case Function(Elem) of
                    true ->
                        orddict:store(
                            Elem,
                            Provenance,
                            AccFilterProvenanceStore);
                    false ->
                        AccFilterProvenanceStore
                end
            end,
            orddict:new(),
            ProvenanceStore),
    {FilterProvenanceStore, SubsetEvents, AllEvents}.

%% @doc @todo
-spec union(state_ps_agg_poe_orset(), state_ps_agg_poe_orset()) ->
    state_ps_agg_poe_orset().
union(ORSetL, ORSetR) ->
    join(ORSetL, ORSetR).

%% @doc @todo
-spec product(state_ps_agg_poe_orset(), state_ps_agg_poe_orset()) ->
    state_ps_agg_poe_orset().
product(
    {ProvenanceStoreL, SubsetEventsL, AllEventsL}=_ORSetL,
    {ProvenanceStoreR, SubsetEventsR, AllEventsR}=_ORSetR) ->
    ProductAllEvents =
        state_ps_agg_type:join_all_events(AllEventsL, AllEventsR),
    ProductSubsetEvents =
        state_ps_agg_type:join_subset_events(
            SubsetEventsL, AllEventsL, SubsetEventsR, AllEventsR),
    CrossedProvenanceStore =
        orddict:fold(
            fun(ElemL, ProvenanceL, AccProductProvenanceStoreL) ->
                orddict:fold(
                    fun(ElemR,
                        ProvenanceR,
                        AccProductProvenanceStoreR) ->
                        ProductElem = {ElemL, ElemR},
                        ProductProvenance =
                            state_ps_agg_type:cross_provenance(
                                ProvenanceL, ProvenanceR),
                        orddict:store(
                            ProductElem,
                            ProductProvenance,
                            AccProductProvenanceStoreR)
                    end,
                    AccProductProvenanceStoreL,
                    ProvenanceStoreR)
            end,
            orddict:new(),
            ProvenanceStoreL),
    ProductProvenanceStore =
        prune_provenance_store(CrossedProvenanceStore, ProductSubsetEvents),
    {ProductProvenanceStore, ProductSubsetEvents, ProductAllEvents}.

%% @private
prune_provenance_store(ProvenanceStore, Events) ->
    orddict:fold(
        fun(Elem, Provenance, AccPruneProvenanceStore) ->
            NewProvenance =
                state_ps_agg_type:prune_provenance(Provenance, Events),
            case state_ps_agg_type:is_bottom_provenance(NewProvenance) of
                true ->
                    AccPruneProvenanceStore;
                false ->
                    orddict:store(Elem, NewProvenance, AccPruneProvenanceStore)
            end
        end,
        orddict:new(),
        ProvenanceStore).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual(
        {orddict:new(),
            state_ps_agg_type:new_subset_events(),
            state_ps_agg_type:new_all_events()},
        new()).

-endif.
