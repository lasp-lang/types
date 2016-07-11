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

-export_type([pure_type/0, polog/0, id/0, element/0]).

-export([reset/2]).

%% Define some initial types.
-type pure_type() :: pure_gcounter | pure_pncounter | pure_gset | pure_twopset | pure_aworset | pure_rworset | pure_ewflag | pure_dwflag | pure_mvreg.
-type polog() :: orddict:orddict().
-type id() :: orddict:orddict().
-type element() :: term().

%% @doc Clear/reset the state to initial state.
-spec reset(pure_type:id(), type:crdt()) -> type:crdt().
reset(VV, {Type, {POLog, Crystal}}) ->
    Type:reset(VV, {Type, {POLog, Crystal}}).