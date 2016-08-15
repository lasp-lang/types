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

%% @doc POLog: Partially Ordered Log
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_polog).
-author("Georges Younes <georges.r.younes@gmail.com>").

-export([remove_redundant/2]).

%% @doc Check redundant operations.
-callback redundant({pure_type:id(), type:operation()}, {pure_type:id(), type:operation()}) ->
    atom().

%% @doc Remove redundant operations from the crystal.
-callback remove_redundant_crystal({pure_type:id(), type:operation()}, type:crdt()) -> {boolean(), type:crdt()}.

%% @doc Remove redundant operations from the POLog.
-callback remove_redundant_polog({pure_type:id(), type:operation()}, type:crdt()) -> {boolean(), type:crdt()}.

%% @doc Check stable operations and move them from the POLog to the crystal
-callback check_stability(pure_type:id(), type:crdt()) -> type:crdt().

%% @doc Remove redundant operations.
-spec remove_redundant({pure_type:id(), type:operation()}, type:crdt()) -> {boolean(), type:crdt()}.
remove_redundant({VV1, Op}, {Type, {POLog, Crystal}}) ->
    {Add0, {Type, {POLog0, Crystal0}}} = Type:remove_redundant_crystal({VV1, Op}, {Type, {POLog, Crystal}}),
    {Add1, {Type, {POLog1, Crystal1}}} = Type:remove_redundant_polog({VV1, Op}, {Type, {POLog0, Crystal0}}),
    {Add0 andalso Add1, {Type, {POLog1, Crystal1}}}.


