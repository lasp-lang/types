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

-module(pure_type).
-author("Georges Younes <georges.r.younes@gmail.com>").

-include("pure_type.hrl").

-export_type([pure_type/0,
              crdt/0,
              polog/0,
              id/0,
              element/0]).

-export([reset/2]).
-export([crdt_size/1]).

%% Define some initial types.
-type pure_type() :: pure_awset |
                     pure_dwflag |
                     pure_ewflag |
                     pure_gcounter |
                     pure_gset |
                     pure_mvregister |
                     pure_pncounter |
                     pure_rwset |
                     pure_twopset.
-type crdt() :: {pure_type(), payload()}.
-type payload() :: {polog(), term()}.
-type polog() :: orddict:orddict().
-type id() :: orddict:orddict().
-type element() :: term().

%% Reset the data type
-callback reset(pure_type:id(), crdt()) -> crdt().

%% @doc Clear/reset the state to initial state.
-spec reset(pure_type:id(), crdt()) -> crdt().
reset(VV, {Type, {POLog, _Crystal}}) ->
    {Type, {_POLog, Crystal}} = Type:new(),
    POLog1 = orddict:filter(
        fun(VV1, _Op) ->
            not pure_trcb:happened_before(VV1, VV)
        end,
        POLog
    ),
    {Type, {POLog1, Crystal}}.

-define(AWSET_TYPE, pure_awset).
-define(DWFLAG_TYPE, pure_dwflag).
-define(EWFLAG_TYPE, pure_ewflag).
-define(GCOUNTER_TYPE, pure_gcounter).
-define(GSET_TYPE, pure_gset).
-define(MVREGISTER_TYPE, pure_mvregister).
-define(PNCOUNTER_TYPE, pure_pncounter).
-define(RWSET_TYPE, pure_rwset).
-define(TWOPSET_TYPE, pure_twopset).

%% @doc Term size.
crdt_size({?PURE_AWSET_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?PURE_DWFLAG_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?PURE_EWFLAG_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?PURE_GCOUNTER_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?PURE_GSET_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?PURE_MVREGISTER_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?PURE_RWSET_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?PURE_PNCOUNTER_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size({?PURE_TWOPSET_TYPE, CRDT}) -> crdt_size(CRDT);
crdt_size(T) ->
    erts_debug:flat_size(T).
