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

-include("state_type.hrl").

-export([new/1,
         mutate/3,
         is_inflation/2,
         is_strict_inflation/2,
         irreducible_is_strict_inflation/2]).
-export([delta/2]).
-export([extract_args/1]).
-export([crdt_size/1]).

-export_type([state_type/0,
              crdt/0,
              digest/0,
              format/0,
              delta_method/0]).

%% Define some initial types.
-type state_type() :: state_awmap |
                      state_awset |
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
                      state_orset |
                      state_pair |
                      state_pncounter |
                      state_twopset.
-type crdt() :: {state_type(), type:payload()}.
-type digest() :: {state, crdt()} | %% state as digest
                  {mdata, term()}.  %% metadata as digest
-type delta_method() :: state | mdata.

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
-callback irreducible_is_strict_inflation(crdt(),
                                          digest()) -> boolean().

%% CRDT digest (which can be the CRDT state itself).
-callback digest(crdt()) -> digest().

%% Join decomposition.
-callback join_decomposition(crdt()) -> [crdt()].

%% Let A be the first argument.
%% Let B be the second argument.
%% This function returns a ∆ from A that inflates B.
%% "The join of all s in join_decomposition(A) such that s strictly
%% inflates B"
-callback delta(crdt(), digest()) -> crdt().

%% @todo These should be moved to type.erl
%% Encode and Decode.
-callback encode(format(), crdt()) -> binary().
-callback decode(format(), binary()) -> crdt().

%% @doc Builds a new CRDT from a given CRDT
-spec new(crdt()) -> any(). %% @todo Fix this any()
new({?GMAP_TYPE, {ValuesType, _Payload}}) ->
    ?GMAP_TYPE:new([ValuesType]);
new({?PAIR_TYPE, {Fst, Snd}}) ->
    {?PAIR_TYPE, {new(Fst), new(Snd)}};
new({Type, _Payload}) ->
    Type:new().

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
-spec irreducible_is_strict_inflation(crdt(),
                                      digest()) -> boolean().
irreducible_is_strict_inflation({Type, _}=Irreducible,
                                {state, {Type, _}=CRDT}) ->
    Merged = Type:merge(Irreducible, CRDT),
    Type:is_strict_inflation(CRDT, Merged).

%% @doc Generic delta calculation.
-spec delta(crdt(), digest()) -> crdt().
delta({Type, _}=A, B) ->
    lists:foldl(
        fun(Irreducible, Acc) ->
            case Type:irreducible_is_strict_inflation(Irreducible,
                                                      B) of
                true ->
                    Type:merge(Irreducible, Acc);
                false ->
                    Acc
            end
        end,
        new(A),
        Type:join_decomposition(A)
    ).

%% @doc extract arguments from complex (composite) types
extract_args({Type, Args}) ->
    {Type, Args};
extract_args(Type) ->
    {Type, []}.

%% @doc Term size.
crdt_size({?AWMAP_TYPE, {_CType, CRDT}}) -> crdt_size(CRDT);
crdt_size({?AWSET_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?BCOUNTER_TYPE, {CRDT1, CRDT2}}) ->
    crdt_size(CRDT1) + crdt_size(CRDT2);
crdt_size({?BOOLEAN_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?DWFLAG_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?EWFLAG_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?GCOUNTER_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?GMAP_TYPE, {_CType, CRDT}}) -> crdt_size(CRDT);
crdt_size({?GSET_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?IVAR_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?LEXCOUNTER_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?LWWREGISTER_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?MAX_INT_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?MVMAP_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?MVREGISTER_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?ORSET_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?PAIR_TYPE, {CRDT1, CRDT2}}) ->
    crdt_size(CRDT1) + crdt_size(CRDT2);
crdt_size({?PNCOUNTER_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?TWOPSET_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size(T) ->
    erts_debug:flat_size(T).
