%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015-2016 Christopher Meiklejohn.  All Rights Reserved.
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

-export([mutate/3, is_inflation/2, is_strict_inflation/2]).

-export_type([state_type/0]).

%% Define some initial types.
-type state_type() :: state_bcounter |
                      state_gcounter |
                      state_gmap |
                      state_gset |
                      state_ivar |
                      state_lexcounter |
                      state_max_int |
                      state_orset |
                      state_pair |
                      state_pncounter |
                      state_twopset |
                      state_oorset.
-type crdt() :: {state_type(), type:payload()}.
-type delta_crdt() :: {state_type(), {delta, type:payload()}}.
-type delta_or_state() :: crdt() | delta_crdt().

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

%% Join decomposition.
-callback join_decomposition(crdt()) -> [crdt()].

%% @todo These functions are for the incremental interface.
%% -type iterator() :: term().
%% -type decomposition() :: ok | {ok, {crdt(), iterator()}}.
%% -callback join_decomposition(crdt()) -> decomposition().
%% -callback join_decomposition(iterator(), crdt()) -> decomposition().

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

