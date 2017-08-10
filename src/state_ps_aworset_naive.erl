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

%% @doc Add-Win Observed-Remove Set CRDT with the POE OR Set design:
%%     add-wins observed-remove set without tombstones.

-module(state_ps_aworset_naive).

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
    state_ps_aworset_naive/0,
    state_ps_aworset_naive_op/0]).

-type element() :: term().
-type payload() :: state_ps_poe_orset:state_ps_poe_orset().
-opaque state_ps_aworset_naive() :: {?TYPE, payload()}.
-type state_ps_aworset_naive_op() ::
    {add, element()} |
    {rmv, element()}.

%% @doc Create a new, empty `state_ps_aworset_naive()'.
-spec new() -> state_ps_aworset_naive().
new() ->
    {?TYPE, state_ps_poe_orset:new()}.

%% @doc Create a new, empty `state_ps_aworset_naive()'
-spec new([term()]) -> state_ps_aworset_naive().
new([_]) ->
    new().

%% @doc Mutate a `state_ps_aworset_naive()'.
-spec mutate(
    state_ps_aworset_naive_op(),
    type:id(),
    state_ps_aworset_naive()) -> {ok, state_ps_aworset_naive()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_ps_type:mutate(Op, Actor, CRDT).

%% @doc Returns the value of the `state_ps_aworset_naive()'.
%%      This value is a set of not-removed elements.
-spec query(state_ps_aworset_naive()) -> term().
query({?TYPE, Payload}) ->
    state_ps_poe_orset:read(Payload).

%% @doc Equality for `state_ps_aworset_naive()'.
-spec equal(state_ps_aworset_naive(), state_ps_aworset_naive()) -> boolean().
equal({?TYPE, PayloadA}, {?TYPE, PayloadB}) ->
    state_ps_poe_orset:equal(PayloadA, PayloadB).

%% @doc Delta-mutate a `state_ps_aworset_naive()'.
%%      The first argument can be:
%%          - `{add, element()}'
%%          - `{rmv, element()}'
%%      The second argument is the event id ({object_id, replica_id}).
%%      The third argument is the `state_ps_aworset_naive()' to be inflated.
-spec delta_mutate(
    state_ps_aworset_naive_op(),
    type:id(),
    state_ps_aworset_naive()) -> {ok, state_ps_aworset_naive()}.
%% Add a single element to `state_ps_aworset_naive()'.
%% Delta: {[{Elem, {{NewEvent}}}], [NewEvent], [NewEvent]}
delta_mutate({add, Elem}, Actor, {?TYPE, Payload}) ->
    %% Get next Event from AllEvents.
    NextEvent = get_next_event(Actor, Payload),
    %% Get a delta object.
    DeltaPayload =
        state_ps_poe_orset:delta_insert(NextEvent, Elem, Payload),
    {ok, {?TYPE, DeltaPayload}};

%% Remove a single element to `state_ps_aworset_naive()'.
%% Delta: {[], [], ElemEvents}
delta_mutate({rmv, Elem}, _Actor, {?TYPE, Payload}) ->
    %% Get a delta object.
    DeltaPayload = state_ps_poe_orset:delta_delete(Elem, Payload),
    {ok, {?TYPE, DeltaPayload}}.

%% @doc Merge two `state_ps_aworset_naive()'.
-spec merge(state_ps_aworset_naive(), state_ps_aworset_naive()) ->
    state_ps_aworset_naive().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun merge_state_ps_aworset_naive/2,
    state_ps_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Given two `state_ps_aworset_naive()', check if the second is an
%%     inflation of the first.
-spec is_inflation(state_ps_aworset_naive(), state_ps_aworset_naive()) ->
    boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_ps_aworset_naive(), state_ps_aworset_naive()) ->
    boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_strict_inflation(CRDT1, CRDT2).

-spec encode(state_ps_type:format(), state_ps_aworset_naive()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_ps_type:format(), binary()) -> state_ps_aworset_naive().
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
                case EventType0 == state_ps_event_partial_order_independent
                    andalso EventId0 == EventId of
                    true ->
                        max(AccInMaxCnt, Counter0);
                    false ->
                        AccInMaxCnt
                end
            end,
            0,
            AllEvents),
    {state_ps_event_partial_order_independent, {EventId, MaxCnt + 1}}.

%% @private
merge_state_ps_aworset_naive({?TYPE, PayloadA}, {?TYPE, PayloadB}) ->
    MergedPayload = state_ps_poe_orset:join(PayloadA, PayloadB),
    {?TYPE, MergedPayload}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, state_ps_poe_orset:new()}, new()).

-endif.
