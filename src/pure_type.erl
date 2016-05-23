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

%% @doc Inspired by both the Lasp data type behaviour and the Riak data
%%      type behaviour.

-module(pure_type).
-author("Georges Younes <georges.r.younes@gmail.com>").

%-export([update/3]).
-export_type([polog/0, version_vector/0, element/0]).


%% Define some initial types.
-type pure_type() :: pure_gcounter | pure_pncounter | pure_gset | pure_twopset | pure_aworset.
-type polog() :: orddict:orddict().
-type pure_payload() :: {polog(), term()}.
-type version_vector() :: orddict:orddict().
-type pure_crdt() :: {pure_type(), pure_payload()}.
-type pure_operation() :: term().
-type element() :: term().
-type value() :: term().
-type error() :: term().

%% Initialize a Pure op-based CRDT.
-callback new() -> pure_crdt().

%% Unified interface for allowing parameterized Pure op-based CRDTs.
-callback new([term()]) -> pure_crdt().

%% Perform an update.
-callback update(pure_operation(), version_vector(), pure_crdt()) ->
    {ok, pure_crdt()} | {error, error()}.

%% Get the value of a Pure op-based CRDT.
-callback query(pure_crdt()) -> value().

%% Compare equality.
-callback equal(pure_crdt(), pure_crdt()) -> boolean().

