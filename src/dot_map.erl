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
         dots/2,
         fetch_keys/1,
         fetch/3,
         store/3
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

%% @doc Create a DotSet from a list of dots.
-spec dots(dot_set | dot_fun | dot_map, dot_map()) ->
    dot_store:dot_set().
dots(DSType, DotMap) ->
    orddict:fold(
        fun(_, DotStore, DotSet) ->
            %% @todo nested DotMap
            dot_set:union(DotSet, DSType:dots(DotStore))
        end,
        dot_set:new(),
        DotMap
    ).

-spec fetch(term(), dot_map(), dot_store:dot_store()) ->
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
