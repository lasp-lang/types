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

-module(state_type).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-export([new/1,
         new_delta/1,
         new_delta/2,
         is_delta/1]).
-export([mutate/3,
         merge/3,
         is_inflation/2,
         is_strict_inflation/2,
         irreducible_is_strict_inflation/2]).
-export([delta/3]).
-export([extract_args/1]).

-export_type([state_type/0,
              crdt/0,
              format/0,
              delta_method/0]).

%% Define some initial types.
-type state_type() :: state_awset |
                      state_awset_ps |
                      state_bcounter |
                      state_boolean |
                      state_dwflag |
                      state_ewflag |
                      state_gcounter |
                      state_gmap |
                      state_gset |
                      state_ivar |
                      state_lexcounter |
                      state_lwwregister |
                      state_max_int |
                      state_mvregister |
                      state_mvmap |
                      state_ormap |
                      state_orset |
                      state_pair |
                      state_pncounter |
                      state_twopset.
-type crdt() :: {state_type(), type:payload()}.
-type delta_crdt() :: {state_type(), {delta, type:payload()}}.
-type delta_or_state() :: crdt() | delta_crdt().
-type delta_method() :: state_driven | digest_driven.

%% Supported serialization formats.
-type format() :: erlang.

%% Creates an empty delta
-callback new_delta() -> delta_crdt().
-callback new_delta([term()]) -> delta_crdt().

%% Check if it's a delta
-callback is_delta(delta_or_state()) -> boolean().

%% Perform a delta mutation.
-callback delta_mutate(type:operation(), type:id(), crdt()) ->
    {ok, delta_crdt()} | {error, type:error()}.

%% Merge two replicas.
%% If we merge two CRDTs, the result is a CRDT.
%% If we merge a delta and a CRDT, the result is a CRDT.
%% If we merge two deltas, the result is a delta (delta group).
-callback merge(delta_or_state(), delta_or_state()) -> delta_or_state().

%% Check if a some state is bottom
-callback is_bottom(delta_or_state()) -> boolean().

%% Inflation testing.
-callback is_inflation(delta_or_state(), crdt()) -> boolean().
-callback is_strict_inflation(delta_or_state(), crdt()) -> boolean().

%% Let A be the first argument.
%% Let B be the second argument.
%% A is a join-irreducible state.
%% This functions checks if A will strictly inflate B.
%% "B < A \join B"
-callback irreducible_is_strict_inflation(crdt(), crdt()) -> boolean().

%% Join decomposition.
-callback join_decomposition(delta_or_state()) -> [crdt()].

%% Let A be the second argument.
%% Let B be the third argument.
%% This function returns a ∆ from A that inflates B.
%% "The join of all s in join_decomposition(A) such that s strictly inflates B"
-callback delta(delta_method(), delta_or_state(), delta_or_state()) -> crdt().

%% @todo These should be moved to type.erl
%% Encode and Decode.
-callback encode(format(), crdt()) -> binary().
-callback decode(format(), binary()) -> crdt().

%% @todo These functions are for the incremental interface.
%% -type iterator() :: term().
%% -type decomposition() :: ok | {ok, {crdt(), iterator()}}.
%% -callback join_decomposition(crdt()) -> decomposition().
%% -callback join_decomposition(iterator(), crdt()) -> decomposition().

%% @doc Builds a new CRDT from a given CRDT
-spec new(crdt()) -> any(). %% @todo Fix this any()
new({Type, {delta, Payload}}) ->
    new({Type, Payload});
new({state_awset, _Payload}) ->
    state_awset:new();
new({state_awset_ps, _Payload}) ->
    state_awset_ps:new();
new({state_bcounter, _Payload}) ->
    state_bcounter:new();
new({state_boolean, _Payload}) ->
    state_boolean:new();
new({state_gcounter, _Payload}) ->
    state_gcounter:new();
new({state_gmap, {ValuesType, _Payload}}) ->
    state_gmap:new([ValuesType]);
new({state_gset, _Payload}) ->
    state_gset:new();
new({state_ivar, _Payload}) ->
    state_ivar:new();
new({state_lexcounter, _Payload}) ->
    state_lexcounter:new();
new({state_max_int, _Payload}) ->
    state_max_int:new();
new({state_orset, _Payload}) ->
    state_orset:new();
new({state_pair, {Fst, Snd}}) ->
    {state_pair, {new(Fst), new(Snd)}};
new({state_pncounter, _Payload}) ->
    state_pncounter:new();
new({state_twopset, _Payload}) ->
    state_twopset:new().

%% Generic new_delta
-spec new_delta(state_type()) -> delta_crdt().
new_delta(Type) ->
    new_delta(Type, []).

-spec new_delta(state_type(), [term()]) -> delta_crdt().
new_delta(Type, Args) ->
    {Type, Payload} = Type:new(Args),
    {Type, {delta, Payload}}.

%% Check if it's a delta
-spec is_delta(delta_or_state()) -> boolean().
is_delta({_Type, {delta, _Payload}}) ->
    true;
is_delta({_Type, _Payload}) ->
    false.

%% @doc Generic Join composition.
-spec mutate(type:operation(), type:id(), crdt()) ->
    {ok, crdt()} | {error, type:error()}.
mutate(Op, Actor, {Type, _}=CRDT) ->
    case Type:delta_mutate(Op, Actor, CRDT) of
        {ok, {Type, {delta, Delta}}} ->
            {ok, Type:merge({Type, Delta}, CRDT)};
        Error ->
            Error
    end.

%% @doc Generic Merge.
-spec merge(delta_or_state(), delta_or_state(), function()) -> delta_or_state().
merge({Type, {delta, Delta1}}, {Type, {delta, Delta2}}, MergeFun) ->
    {Type, DeltaGroup} = MergeFun({Type, Delta1}, {Type, Delta2}),
    {Type, {delta, DeltaGroup}};
merge({Type, {delta, Delta}}, {Type, CRDT}, MergeFun) ->
    MergeFun({Type, Delta}, {Type, CRDT});
merge({Type, CRDT}, {Type, {delta, Delta}}, MergeFun) ->
    MergeFun({Type, Delta}, {Type, CRDT});
merge({Type, CRDT1}, {Type, CRDT2}, MergeFun) ->
    MergeFun({Type, CRDT1}, {Type, CRDT2}).

%% @doc Generic check for inflation.
-spec is_inflation(delta_or_state(), crdt()) -> boolean().
is_inflation({Type, _}=CRDT1, {Type, _}=CRDT2) ->
    Type:equal(Type:merge(CRDT1, CRDT2), CRDT2).

%% @doc Generic check for strict inflation.
%%      We have a strict inflation if:
%%          - we have an inflation
%%          - we have different CRDTs
-spec is_strict_inflation(delta_or_state(), crdt()) -> boolean().
is_strict_inflation({Type, _}=CRDT1, {Type, _}=CRDT2) ->
    Type:is_inflation(CRDT1, CRDT2) andalso
    not Type:equal(CRDT1, CRDT2).

%% @doc Generic check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(crdt(), crdt()) -> boolean().
irreducible_is_strict_inflation({Type, _}=Irreducible, {Type, _}=CRDT) ->
    Merged = Type:merge(Irreducible, CRDT),
    Type:is_strict_inflation(CRDT, Merged).

%% @doc Generic delta calculation.
-spec delta(delta_method(), crdt(), crdt()) -> crdt().
delta(state_driven, {Type, _}=A, {Type, _}=B) ->
    Inflations = lists:filter(
        fun(Irreducible) ->
            Type:irreducible_is_strict_inflation(Irreducible, B)
        end,
        Type:join_decomposition(A)
    ),
    lists:foldl(
        fun(Irreducible, Acc) ->
            Type:merge(Acc, Irreducible)
        end,
        new(A),
        Inflations
    ).

%% @doc extract arguments from complex (composite) types
extract_args({Type, Args}) ->
    {Type, Args};
extract_args(Type) ->
    {Type, []}.
