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

%% @doc LWW Register CRDT with the POE OR Set design:
%%     last write wins register.

-module(state_ps_lwwregister_naive).

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
    state_ps_lwwregister_naive/0,
    state_ps_lwwregister_naive_op/0]).

-type value() :: term().
-type payload() :: state_ps_poe_orset:state_ps_poe_orset().
-opaque state_ps_lwwregister_naive() :: {?TYPE, payload()}.
-type state_ps_lwwregister_naive_op() :: {set, value()}.

%% @doc Create a new, empty `state_ps_lwwregister_naive()'.
-spec new() -> state_ps_lwwregister_naive().
new() ->
    {?TYPE, state_ps_poe_orset:new()}.

%% @doc Create a new, empty `state_ps_lwwregister_naive()'
-spec new([term()]) -> state_ps_lwwregister_naive().
new([_]) ->
    new().

%% @doc Mutate a `state_ps_lwwregister_naive()'.
-spec mutate(
    state_ps_lwwregister_naive_op(), type:id(), state_ps_lwwregister_naive()) ->
    {ok, state_ps_lwwregister_naive()}.
mutate(Op, Actor, {?TYPE, _}=CRDT) ->
    state_ps_type:mutate(Op, Actor, CRDT).

%% @doc Returns the value of the `state_ps_lwwregister_naive()'.
-spec query(state_ps_lwwregister_naive()) -> term().
query({?TYPE, Payload}) ->
    Value = state_ps_poe_orset:read(Payload),
    case sets:to_list(Value) of
        [] ->
            undefined;
        [Result] ->
            Result
    end.

%% @doc Equality for `state_ps_lwwregister_naive()'.
%%      Since everything is ordered, == should work.
-spec equal(state_ps_lwwregister_naive(), state_ps_lwwregister_naive()) ->
    boolean().
equal({?TYPE, PayloadA}, {?TYPE, PayloadB}) ->
    state_ps_poe_orset:equal(PayloadA, PayloadB).

%% @doc Delta-mutate a `state_ps_lwwregister_naive()'.
%%      The first argument can only be `{set, value()}'.
%%      The second argument is the event id ({object_id, replica_id}).
%%      The third argument is the `state_ps_lwwregister_naive()' to be inflated.
-spec delta_mutate(
    state_ps_lwwregister_naive_op(),
    type:id(),
    state_ps_lwwregister_naive()) -> {ok, state_ps_lwwregister_naive()}.
%% Write the value of the `state_ps_lwwregister_naive()'.
delta_mutate({set, Value}, Actor, {?TYPE, Payload}) ->
    %% Get next Event from AllEvents.
    NextEvent = get_next_event(Actor, Payload),
    %% Get a delta object.
    DeltaPayload =
        state_ps_poe_orset:delta_insert(NextEvent, Value, Payload),
    {ok, {?TYPE, DeltaPayload}}.

%% @doc Merge two `state_ps_lwwregister_naive()'.
-spec merge(state_ps_lwwregister_naive(), state_ps_lwwregister_naive()) ->
    state_ps_lwwregister_naive().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun merge_state_ps_lwwregister_naive/2,
    state_ps_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Given two `state_ps_lwwregister_naive()', check if the second is an
%%      inflation of the first.
-spec is_inflation(
    state_ps_lwwregister_naive(), state_ps_lwwregister_naive()) -> boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_inflation(CRDT1, CRDT2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(
    state_ps_lwwregister_naive(), state_ps_lwwregister_naive()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_ps_type:is_strict_inflation(CRDT1, CRDT2).

-spec encode(state_ps_type:format(), state_ps_lwwregister_naive()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_ps_type:format(), binary()) -> state_ps_lwwregister_naive().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.

%% @doc Calculate the next event from the AllEvents.
-spec get_next_event(
    state_ps_type:state_ps_event_id(),
    state_ps_type:state_ps_payload()) -> state_ps_type:state_ps_event().
get_next_event(EventId, {_, _, AllEvents}=_Payload) ->
    case AllEvents of
        [] ->
            {state_ps_event_total_order, {EventId, 1}};
        [{state_ps_event_total_order, {_, MaxCnt}}] ->
            {state_ps_event_total_order, {EventId, MaxCnt + 1}}
    end.

%% @private
merge_state_ps_lwwregister_naive({?TYPE, PayloadA}, {?TYPE, PayloadB}) ->
    MergedPayload = state_ps_poe_orset:join(PayloadA, PayloadB),
    {?TYPE, MergedPayload}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, state_ps_poe_orset:new()}, new()).

-endif.
