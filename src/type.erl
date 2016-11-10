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

%% @doc Inspired by both the Lasp data type behaviour and the Riak data
%%      type behaviour.

-module(type).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export_type([crdt/0,
              id/0,
              payload/0,
              operation/0,
              error/0]).

%% Define some initial types.
-type type() :: state_type:state_type() | pure_type:pure_type().
-type payload() :: term().
-type crdt() :: {type(), payload()}.
-type operation() :: term().
-type id() :: term().
-type value() :: term().
-type error() :: term().

%% Initialize a CRDT.
-callback new() -> crdt().

%% Unified interface for allowing parameterized CRDTs.
-callback new([term()]) -> crdt().

%% Perform a mutation.
-callback mutate(operation(), id(), crdt()) ->
    {ok, crdt()} | {error, error()}.

%% Get the value of a CRDT.
-callback query(crdt()) -> value().

%% Compare equality.
-callback equal(crdt(), crdt()) -> boolean().
