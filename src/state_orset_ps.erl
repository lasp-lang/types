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

%% @doc Observed-remove set CRDT with the provenance semiring:
%%      observed-remove set without tombstones.

-module(state_orset_ps).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-behaviour(type).
-behaviour(state_type).
-behaviour(state_provenance_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new_provenance_store/1,
         get_events_from_provenance_store/1]).
-export([new/0, new/1,
         mutate/3,
         query/1,
         equal/2]).
-export([new_delta/0, new_delta/1,
         is_delta/1,
         delta_mutate/3,
         merge/2,
         is_bottom/1,
         is_inflation/2,
         is_strict_inflation/2,
         join_decomposition/1,
         encode/2,
         decode/2]).

-export_type([state_orset_ps/0,
              delta_state_orset_ps/0,
              state_orset_ps_op/0]).

-type element() :: term().
-type ps_provenance_store() ::
    orddict:orddict(element(),
                    state_provenance_type:ps_provenance()).
-type payload() :: {ps_provenance_store(),
                    state_provenance_type:ps_subset_events(),
                    state_provenance_type:ps_all_events()}.
-opaque state_orset_ps() :: {?TYPE, payload()}.
-opaque delta_state_orset_ps() :: {?TYPE, {delta, payload()}}.
-type delta_or_state() :: state_orset_ps()
                        | delta_state_orset_ps().
-type state_orset_ps_op() :: {add, element()}
%                           | {add_all, [element()]}
%                           | {rmv_all, [element()]}
                           | {rmv, element()}.

%% @doc Create a new, empty provenance store for `state_orset_ps()'.
-spec new_provenance_store([term()]) -> ps_provenance_store().
new_provenance_store([]) ->
    orddict:new().

%% @doc Return all events in a provenance store.
-spec get_events_from_provenance_store(ps_provenance_store()) ->
    ordsets:ordsets(state_provenance_type:ps_event()).
get_events_from_provenance_store(ProvenanceStore) ->
    orddict:fold(
        fun(_Elem, Provenance, AccInEvents) ->
            ordsets:union(
                AccInEvents,
                state_provenance_type:get_events_from_provenance(Provenance))
        end,
        ordsets:new(),
        ProvenanceStore).

%% @doc Create a new, empty `state_orset_ps()'.
%%      By default the values are a AWSet_PS CRDT.
-spec new() -> state_orset_ps().
new() ->
    {?TYPE, {new_provenance_store([]),
             ordsets:new(),
             {ev_set, ordsets:new()}}}.

%% @doc Create a new, empty `state_orset_ps()'
-spec new([term()]) -> state_orset_ps().
new([_]) ->
    new().

%% @doc Mutate a `state_orset_ps()'.
-spec mutate(state_orset_ps_op(), type:id(), state_orset_ps()) ->
    {ok, state_orset_ps()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Returns the value of the `state_orset_ps()'.
%%      This value is a dictionary where each key sets to the
%%      result of `query/1' over the current value.
-spec query(state_orset_ps()) -> term().
query({?TYPE, {ProvenanceStore, _SubsetEvents, _AllEvents}}) ->
    orddict:fold(
        fun(Elem, _Provenance, AccInSet) ->
            sets:add_element(Elem, AccInSet)
        end,
        sets:new(),
        ProvenanceStore).

%% @doc Equality for `state_orset_ps()'.
%%      Since everything is ordered, == should work.
-spec equal(state_orset_ps(), state_orset_ps()) -> boolean().
equal({?TYPE, {ProvenanceStoreA, SubsetEventsA, AllEventsA}},
      {?TYPE, {ProvenanceStoreB, SubsetEventsB, AllEventsB}}) ->
    ProvenanceStoreA == ProvenanceStoreB andalso
    SubsetEventsA == SubsetEventsB andalso
    state_provenance_type:equal_all_events(AllEventsA, AllEventsB).

-spec new_delta() -> delta_state_orset_ps().
new_delta() ->
    {?TYPE, {delta, {new_provenance_store([]),
                     ordsets:new(),
                     {ev_set, ordsets:new()}}}}.

-spec new_delta([term()]) -> delta_state_orset_ps().
new_delta(_Args) ->
    new_delta().

-spec is_delta(delta_or_state()) -> boolean().
is_delta({?TYPE, _}=CRDT) ->
    state_type:is_delta(CRDT).

%% @doc Delta-mutate a `state_orset_ps()'.
%%      The first argument can be:
%%          - `{add, element()}'
%%          %- `{add_all, [element()]}'
%%          - `{rmv, element()}'
%%          %- `{rmv_all, [element()]}'
%%      The second argument is the event id ({object_id, replica_id}).
%%      The third argument is the `state_orset_ps()' to be inflated.
-spec delta_mutate(state_orset_ps_op(), type:id(), state_orset_ps()) ->
    {ok, delta_state_orset_ps()}.
%% Add a single element to `state_orset_ps()'.
%% Delta: {[{Elem, {{NewEvent}}}], [NewEvent], {ev_set, [NewEvent]}}
delta_mutate({add, Elem},
             Actor,
             {?TYPE, {_ProvenanceStore, _SubsetEventsSurvived, AllEvents}}) ->
    %% Get next Event from AllEvents.
    NextEvent = state_provenance_type:get_next_event(Actor, AllEvents),
    %% Make a new Provenance from the Event.
    DeltaProvenance = ordsets:add_element(
                        ordsets:add_element(
                            NextEvent, ordsets:new()),
                        ordsets:new()),
    DeltaProvenanceStore = orddict:store(
                            Elem, DeltaProvenance, orddict:new()),
    {ok, {?TYPE, {delta, {DeltaProvenanceStore,
                          ordsets:add_element(NextEvent, ordsets:new()),
                          {ev_set, ordsets:add_element(NextEvent,
                                                       ordsets:new())}}}}};

%% Remove a single element to `state_orset_ps()'.
%% Delta: {[], [], ElemEvents}
delta_mutate({rmv, Elem},
             _Actor,
             {?TYPE, {ProvenanceStore, _SubsetEventsSurvived, _AllEvents}}) ->
    case orddict:find(Elem, ProvenanceStore) of
        {ok, Provenance} ->
            ElemEvents = state_provenance_type:get_events_from_provenance(
                            Provenance),
            {ok, {?TYPE, {delta, {orddict:new(),
                                  ordsets:new(),
                                  {ev_set, ElemEvents}}}}};
        error ->
            {ok, new_delta()}
    end.

%% @doc Merge two `state_orset_ps()'.
-spec merge(delta_or_state(), delta_or_state()) -> delta_or_state().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun merge_survived_ev_set_all_events/2,
    state_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Check if a `state_orset_ps()' is bottom
-spec is_bottom(delta_or_state()) -> boolean().
is_bottom({?TYPE, {delta, _}}=CRDT) ->
    CRDT == new_delta();
is_bottom({?TYPE, _}=CRDT) ->
    CRDT == new().

%% @doc Given two `state_orset_ps()', check if the second is an inflation
%%      of the first.
-spec is_inflation(delta_or_state(), state_orset_ps()) -> boolean().
is_inflation({?TYPE, {delta, Set1}}, {?TYPE, Set2}) ->
    is_inflation({?TYPE, Set1}, {?TYPE, Set2});
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(delta_or_state(), state_orset_ps()) -> boolean().
is_strict_inflation({?TYPE, {delta, Set1}}, {?TYPE, Set2}) ->
    is_strict_inflation({?TYPE, Set1}, {?TYPE, Set2});
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @todo
%% @doc Join decomposition for `state_orset_ps()'.
-spec join_decomposition(state_orset_ps()) -> [state_orset_ps()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

-spec encode(state_type:format(), delta_or_state()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> delta_or_state().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.

%% @private
merge_survived_ev_set_all_events({?TYPE, {ProvenanceStoreA,
                                          SubsetEventsSurvivedA,
                                          {ev_set, AllEventsEVA}}},
                                 {?TYPE, {ProvenanceStoreB,
                                          SubsetEventsSurvivedB,
                                          {ev_set, AllEventsEVB}}}) ->
    MergedAllEventsEV = {ev_set, ordsets:union(AllEventsEVA, AllEventsEVB)},
    MergedSubsetEventsSurvived =
        ordsets:union([ordsets:intersection(SubsetEventsSurvivedA,
                                            SubsetEventsSurvivedB)] ++
                      [ordsets:subtract(SubsetEventsSurvivedA,
                                        AllEventsEVB)] ++
                      [ordsets:subtract(SubsetEventsSurvivedB,
                                        AllEventsEVA)]),
    MergedProvenanceStore0 =
        orddict:merge(
            fun(_Elem, ProvenanceA, ProvenanceB) ->
                ordsets:union(ProvenanceA, ProvenanceB)
            end,
            ProvenanceStoreA,
            ProvenanceStoreB),
    MergedProvenanceStore =
        orddict:fold(
            fun(Elem, Provenance, AccInMergedProvenanceStore) ->
                NewProvenance =
                    ordsets:fold(
                        fun(Dot, AccInNewProvenance) ->
                            case ordsets:is_subset(
                                    Dot, MergedSubsetEventsSurvived) of
                                true ->
                                    ordsets:add_element(
                                        Dot, AccInNewProvenance);
                                false ->
                                    AccInNewProvenance
                            end
                        end,
                        ordsets:new(),
                        Provenance),
                case ordsets:size(NewProvenance) of
                    0 ->
                        AccInMergedProvenanceStore;
                    _ ->
                        orddict:store(Elem,
                                      NewProvenance,
                                      AccInMergedProvenanceStore)
                end
            end,
            orddict:new(),
            MergedProvenanceStore0),
    {?TYPE, {MergedProvenanceStore,
             MergedSubsetEventsSurvived,
             MergedAllEventsEV}}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {orddict:new(),
                          ordsets:new(),
                          {ev_set, ordsets:new()}}},
                 new()),
    ?assertEqual({?TYPE, {delta, {orddict:new(),
                                  ordsets:new(),
                                  {ev_set, ordsets:new()}}}},
                 new_delta()).

query_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    ?assertEqual(sets:new(), query(Set0)),
    ?assertEqual(sets:from_list([<<"1">>]), query(Set1)).

delta_add_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate({add, <<"1">>}, EventId, Set0),
    Set1 = merge({?TYPE, Delta1}, Set0),
    {ok, {?TYPE, {delta, Delta2}}} = delta_mutate({add, <<"1">>}, EventId, Set1),
    Set2 = merge({?TYPE, Delta2}, Set1),
    {ok, {?TYPE, {delta, Delta3}}} = delta_mutate({add, <<"2">>}, EventId, Set2),
    Set3 = merge({?TYPE, Delta3}, Set2),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                          [{EventId, 1}],
                          {ev_set, [{EventId, 1}]}}},
                 {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                          [{EventId, 1}],
                          {ev_set, [{EventId, 1}]}}},
                 Set1),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 2}]]}],
                          [{EventId, 2}],
                          {ev_set, [{EventId, 2}]}}},
                 {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]}],
                          [{EventId, 1}, {EventId, 2}],
                          {ev_set, [{EventId, 1}, {EventId, 2}]}}},
                 Set2),
    ?assertEqual({?TYPE, {[{<<"2">>, [[{EventId, 3}]]}],
                          [{EventId, 3}],
                          {ev_set, [{EventId, 3}]}}},
                 {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]},
                            {<<"2">>, [[{EventId, 3}]]}],
                          [{EventId, 1}, {EventId, 2}, {EventId, 3}],
                          {ev_set, [{EventId, 1},
                                    {EventId, 2},
                                    {EventId, 3}]}}},
                 Set3).

add_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"1">>}, EventId, Set0),
    {ok, Set2} = mutate({add, <<"1">>}, EventId, Set1),
    {ok, Set3} = mutate({add, <<"2">>}, EventId, Set2),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                          [{EventId, 1}],
                          {ev_set, [{EventId, 1}]}}},
                 Set1),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]}],
                          [{EventId, 1}, {EventId, 2}],
                          {ev_set, [{EventId, 1}, {EventId, 2}]}}},
                 Set2),
    ?assertEqual({?TYPE, {[{<<"1">>, [[{EventId, 1}], [{EventId, 2}]]},
                           {<<"2">>, [[{EventId, 3}]]}],
                          [{EventId, 1}, {EventId, 2}, {EventId, 3}],
                          {ev_set, [{EventId, 1},
                                    {EventId, 2},
                                    {EventId, 3}]}}},
                 Set3).

rmv_test() ->
    EventId = {<<"object1">>, a},
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"1">>}, EventId, Set0),
    {ok, Set2} = mutate({add, <<"1">>}, EventId, Set1),
    {ok, Set2} = mutate({rmv, <<"2">>}, EventId, Set2),
    {ok, Set3} = mutate({rmv, <<"1">>}, EventId, Set2),
    ?assertEqual(sets:new(), query(Set3)).

%%add_all_test() ->
%%    EventId = {<<"object1">>, a},
%%    Set0 = new(),
%%    {ok, Set1} = mutate({add_all, []}, EventId, Set0),
%%    {ok, Set2} = mutate({add_all, [<<"a">>, <<"b">>]}, EventId, Set0),
%%    {ok, Set3} = mutate({add_all, [<<"b">>, <<"c">>]}, EventId, Set2),
%%    ?assertEqual(sets:new(), query(Set1)),
%%    ?assertEqual(sets:from_list([<<"a">>, <<"b">>]), query(Set2)),
%%    ?assertEqual(sets:from_list([<<"a">>, <<"b">>, <<"c">>]), query(Set3)).

%%remove_all_test() ->
%%    EventId = {<<"object1">>, a},
%%    Set0 = new(),
%%    {ok, Set1} = mutate({add_all, [<<"a">>, <<"b">>, <<"c">>]}, EventId, Set0),
%%    {ok, Set2} = mutate({rmv_all, [<<"a">>, <<"c">>]}, EventId, Set1),
%%    {ok, Set3} = mutate({rmv_all, [<<"b">>, <<"d">>]}, EventId, Set2),
%%    {ok, Set3} = mutate({rmv_all, [<<"b">>]}, EventId, Set2),
%%    ?assertEqual(sets:from_list([<<"b">>]), query(Set2)),
%%    ?assertEqual(sets:new(), query(Set3)).

merge_idempotent_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 = {?TYPE, {[], [], {ev_set, [{EventId1, 1}]}}},
    Set2 = {?TYPE, {[{<<"2">>, [[{EventId2, 1}]]}],
                    [{EventId2, 1}],
                    {ev_set, [{EventId2, 1}]}}},
    Set3 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}]]}],
                    [{EventId1, 1}],
                    {ev_set, [{EventId1, 1}, {EventId2, 1}]}}},
    Set4 = merge(Set1, Set1),
    Set5 = merge(Set2, Set2),
    Set6 = merge(Set3, Set3),
    ?assert(equal(Set1, Set4)),
    ?assert(equal(Set2, Set5)),
    ?assert(equal(Set3, Set6)).

merge_commutative_test() ->
    EventId1 = {<<"object1">>, a},
    EventId2 = {<<"object1">>, b},
    Set1 = {?TYPE, {[], [], {ev_set, [{EventId1, 1}]}}},
    Set2 = {?TYPE, {[{<<"2">>, [[{EventId2, 1}]]}],
                    [{EventId2, 1}],
                    {ev_set, [{EventId2, 1}]}}},
    Set3 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}]]}],
                    [{EventId1, 1}],
                    {ev_set, [{EventId1, 1}, {EventId2, 1}]}}},
    Set4 = merge(Set1, Set2),
    Set5 = merge(Set2, Set1),
    Set6 = merge(Set1, Set3),
    Set7 = merge(Set3, Set1),
    Set8 = merge(Set2, Set3),
    Set9 = merge(Set3, Set2),
    Set10 = merge(Set1, merge(Set2, Set3)),
    Set1_2 = {?TYPE, {[{<<"2">>, [[{EventId2, 1}]]}],
                      [{EventId2, 1}],
                      {ev_set, [{EventId1, 1}, {EventId2, 1}]}}},
    Set1_3 = {?TYPE, {[],
                      [],
                      {ev_set, [{EventId1, 1}, {EventId2, 1}]}}},
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
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId1, 1}, {EventId2, 1}]]},
                     {<<"2">>, [[{EventId1, 2}, {EventId2, 2}]]}],
                    [{EventId1, 1}, {EventId1, 2},
                     {EventId2, 1}, {EventId2, 2}],
                    {ev_set, [{EventId1, 1}, {EventId1, 2},
                              {EventId2, 1}, {EventId2, 2}]}}},
    Delta1 = {?TYPE, {delta, {[{<<"1">>, [[{EventId1, 1}, {EventId2, 3}]]},
                               {<<"2">>, [[{EventId1, 2}, {EventId2, 4}]]}],
                              [{EventId1, 1}, {EventId1, 2},
                               {EventId2, 3}, {EventId2, 4}],
                              {ev_set, [{EventId1, 1}, {EventId1, 2},
                                        {EventId2, 3}, {EventId2, 4}]}}}},
    Set2 = merge(Set1, Delta1),
    ?assert(equal({?TYPE, {[{<<"1">>, [[{EventId1, 1}, {EventId2, 1}],
                                       [{EventId1, 1}, {EventId2, 3}]]},
                            {<<"2">>, [[{EventId1, 2}, {EventId2, 2}],
                                       [{EventId1, 2}, {EventId2, 4}]]}],
                           [{EventId1, 1}, {EventId1, 2},
                            {EventId2, 1}, {EventId2, 2},
                            {EventId2, 3}, {EventId2, 4}],
                           {ev_set, [{EventId1, 1}, {EventId1, 2},
                                     {EventId2, 1}, {EventId2, 2},
                                     {EventId2, 3}, {EventId2, 4}]}}},
                  Set2)).

merge_delta_test() ->
    EventId = {<<"object1">>, a},
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}]}}},
    Delta1 = {?TYPE, {delta, {[],
                              [],
                              {ev_set, [{EventId, 1}]}}}},
    Delta2 = {?TYPE, {delta, {[{<<"2">>, [[{EventId, 2}]]}],
                              [{EventId, 2}],
                              {ev_set, [{EventId, 2}]}}}},
    Set2 = merge(Delta1, Set1),
    Set3 = merge(Set1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, {[],
                          [],
                          {ev_set, [{EventId, 1}]}}},
                 Set2),
    ?assertEqual({?TYPE, {[],
                          [],
                          {ev_set, [{EventId, 1}]}}},
                 Set3),
    ?assertEqual({?TYPE, {delta, {[{<<"2">>, [[{EventId, 2}]]}],
                                  [{EventId, 2}],
                                  {ev_set, [{EventId, 1}, {EventId, 2}]}}}},
                 DeltaGroup).

equal_test() ->
    EventId = {<<"object1">>, a},
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}]}}},
    Set2 = {?TYPE, {[],
                    [],
                    {ev_set, [{EventId, 1}]}}},
    Set3 = {?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    ?assert(equal(Set1, Set1)),
    ?assert(equal(Set2, Set2)),
    ?assert(equal(Set3, Set3)),
    ?assertNot(equal(Set1, Set2)),
    ?assertNot(equal(Set1, Set3)),
    ?assertNot(equal(Set2, Set3)).

is_inflation_test() ->
    EventId = {<<"object1">>, a},
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}]}}},
    Set2 = {?TYPE, {[],
                    [],
                    {ev_set, [{EventId, 1}]}}},
    Set3 = {?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    ?assert(is_inflation(Set1, Set1)),
    ?assert(is_inflation(Set1, Set2)),
    ?assertNot(is_inflation(Set2, Set1)),
    ?assert(is_inflation(Set1, Set3)),
    ?assertNot(is_inflation(Set2, Set3)),
    ?assertNot(is_inflation(Set3, Set2)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Set1, Set1)),
    ?assert(state_type:is_inflation(Set1, Set2)),
    ?assertNot(state_type:is_inflation(Set2, Set1)),
    ?assert(state_type:is_inflation(Set1, Set3)),
    ?assertNot(state_type:is_inflation(Set2, Set3)),
    ?assertNot(state_type:is_inflation(Set3, Set2)).

is_strict_inflation_test() ->
    EventId = {<<"object1">>, a},
    Set1 = {?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}]}}},
    Set2 = {?TYPE, {[],
                    [],
                    {ev_set, [{EventId, 1}]}}},
    Set3 = {?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                    [{EventId, 1}],
                    {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    ?assertNot(is_strict_inflation(Set1, Set1)),
    ?assert(is_strict_inflation(Set1, Set2)),
    ?assertNot(is_strict_inflation(Set2, Set1)),
    ?assert(is_strict_inflation(Set1, Set3)),
    ?assertNot(is_strict_inflation(Set2, Set3)),
    ?assertNot(is_strict_inflation(Set3, Set2)).

%%join_decomposition_test() ->
%%    %% @todo
%%    ok.

encode_decode_test() ->
    EventId = {<<"object1">>, a},
    Set = {?TYPE, {[{<<"1">>, [[{EventId, 1}]]}],
                   [{EventId, 1}],
                   {ev_set, [{EventId, 1}, {EventId, 2}]}}},
    Binary = encode(erlang, Set),
    ESet = decode(erlang, Binary),
    ?assertEqual(Set, ESet).

-endif.
