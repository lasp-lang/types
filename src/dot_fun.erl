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

%% @doc DotFun.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(dot_fun).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-include("state_type.hrl").

-behaviour(dot_store).

-export([
         new/0,
         is_empty/1,
         is_element/2,
         fetch/2,
         store/3,
         to_list/1,
         filter/2
        ]).

-type dot_fun() :: dot_store:dot_fun().

%% @doc Create an empty DotFun.
-spec new() -> dot_fun().
new() ->
    orddict:new().

%% @doc Check if a DotFun is empty.
-spec is_empty(dot_fun()) -> boolean().
is_empty(DotFun) ->
    orddict:is_empty(DotFun).

%% @doc Check if a dot belongs to the DotFun.
-spec is_element(dot_store:dot(), dot_fun()) -> boolean().
is_element(Dot, DotFun) ->
    orddict:is_key(Dot, DotFun).

%% @doc Given a Dot and a DotFun, get the correspondent CRDT value.
-spec fetch(dot_store:dot(), dot_fun()) -> state_type:crdt().
fetch(Dot, DotFun) ->
    orddict:fetch(Dot, DotFun).

%% @doc Stores a new CRDT in the DotFun with Dot as key.
-spec store(dot_store:dot(), state_type:crdt(), dot_fun()) ->
    dot_fun().
store(Dot, CRDT, DotFun) ->
    orddict:store(Dot, CRDT, DotFun).

%% @doc Converts the DotFun to a list of pairs {Dot, CRDT}.
-spec to_list(dot_fun()) -> list({dot_store:dot(), state_type:crdt()}).
to_list(DotFun) ->
    orddict:to_list(DotFun).

%% @doc Filter DotFun.
-spec filter(function(), dot_fun()) -> dot_fun().
filter(F, DotFun) ->
    orddict:filter(F, DotFun).
