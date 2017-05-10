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
         mutate/3,
         merge/3,
         is_inflation/2,
         is_strict_inflation/2,
         irreducible_is_strict_inflation/3]).
-export([delta/3]).
-export([extract_args/1]).

-export_type([state_type/0,
              crdt/0,
              digest/0,
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
-type digest() :: term().
-type crdt_or_digest() :: crdt() | digest().
-type delta_method() :: state | digest.

%% Supported serialization formats.
-type format() :: erlang.

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

%% Let A be the first argument.
%% Let B be the second argument.
%% A is a join-irreducible state.
%% This functions checks if A will strictly inflate B.
%% B can be a CRDT or a digest of a CRDT.
-callback irreducible_is_strict_inflation(delta_method(),
                                          crdt(), crdt_or_digest()) ->
    boolean().

%% Join decomposition.
-callback join_decomposition(crdt()) -> [crdt()].

%% Let A be the second argument.
%% Let B be the third argument.
%% This function returns a ∆ from A that inflates B.
%% "The join of all s in join_decomposition(A) such that s strictly
%% inflates B"
-callback delta(delta_method(), crdt(), crdt()) -> crdt().

%% @todo These should be moved to type.erl
%% Encode and Decode.
-callback encode(format(), crdt()) -> binary().
-callback decode(format(), binary()) -> crdt().

%% @doc Builds a new CRDT from a given CRDT
-spec new(crdt()) -> any(). %% @todo Fix this any()
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

%% @doc Generic Join composition.
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

%% @doc Generic check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(delta_method(), crdt(),
                                      crdt_or_digest()) ->
    boolean().
irreducible_is_strict_inflation(state, {Type, _}=Irreducible, {Type, _}=CRDT) ->
    Merged = Type:merge(Irreducible, CRDT),
    Type:is_strict_inflation(CRDT, Merged).

%% @doc Generic delta calculation.
-spec delta(delta_method(), crdt(), crdt_or_digest()) -> crdt().
delta(Method, {Type, _}=A, B) ->
    Inflations = lists:filter(
        fun(Irreducible) ->
            Type:irreducible_is_strict_inflation(Method,
                                                 Irreducible,
                                                 B)
        end,
        Type:join_decomposition(A)
    ),
    merge_all(Inflations).

%% @doc extract arguments from complex (composite) types
extract_args({Type, Args}) ->
    {Type, Args};
extract_args(Type) ->
    {Type, []}.

%% @private
-spec merge_all(list(crdt())) -> crdt().
merge_all([H|_]=L) ->
    lists:foldl(
        fun({Type, _}=CRDT, Acc) ->
            Type:merge(CRDT, Acc)
        end,
        new(H),
        L
    ).
