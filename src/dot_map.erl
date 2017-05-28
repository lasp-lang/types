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

%% @doc DotMap.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(dot_map).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-behaviour(dot_store).

-export([
         new/0,
         is_empty/1,
         fetch_keys/1,
         fetch/3,
         store/3,
         merge/3,
         filter/2,
         to_list/1
        ]).

-type dot_map() :: dot_store:dot_map().

%% @doc Create an empty DotMap.
-spec new() -> dot_map().
new() ->
    orddict:new().

%% @doc Check if a DotMap is empty.
-spec is_empty(dot_map()) -> boolean().
is_empty(DotMap) ->
    orddict:is_empty(DotMap).

%% @doc Given a key, a DotMap and a default,
%%      return:
%%        - the correspondent value, if key present in the DotMap
%%        - default, otherwise
-spec fetch(term(), dot_map(), dot_store:dot_store() | undefined) ->
    dot_store:dot_store().
fetch(Key, DotMap, Default) ->
    orddict_ext:fetch(Key, DotMap, Default).

%% @doc Get a list of keys in the DotMap.
-spec fetch_keys(dot_store:dot_map()) -> [term()].
fetch_keys(DotMap) ->
    orddict:fetch_keys(DotMap).

%% @doc Stores a new {Key, DotStore} pair in the DotMap.
%%      If `Key` already in the DotMap, then its value is replaced.
-spec store(term(), dot_store:dot_store(), dot_map()) -> dot_map().
store(Key, DotStore, DotMap) ->
    orddict:store(Key, DotStore, DotMap).

%% @doc Merge two DotMap.
-spec merge(function(), dot_map(), dot_map()) -> dot_map().
merge(Fun, DotMapA, DotMapB) ->
    orddict:merge(Fun, DotMapA, DotMapB).

%% @doc Filter a DotMap.
-spec filter(function(), dot_map()) -> dot_map().
filter(Fun, DotMap) ->
    orddict:filter(Fun, DotMap).

%% @doc Convert a DotMap to a list.
-spec to_list(dot_map()) -> [{term(), dot_store:dot_store()}].
to_list(DotMap) ->
    orddict:to_list(DotMap).
