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
         merge/4,
         any/2,
         fold/3
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
-spec merge(function(), dot_store:dot_store(),
            dot_map(), dot_map()) -> dot_map().
merge(_Fun, _Default, [], []) ->
    [];
merge(Fun, Default, [{Key, ValueA} | RestA], []) ->
    do_merge(Fun, Default, Key, ValueA, Default, RestA, []);
merge(Fun, Default, [], [{Key, ValueB} | RestB]) ->
    do_merge(Fun, Default, Key, Default, ValueB, [], RestB);
merge(Fun, Default, [{Key, ValueA} | RestA],
                    [{Key, ValueB} | RestB]) ->
    do_merge(Fun, Default, Key, ValueA, ValueB, RestA, RestB);
merge(Fun, Default, [{KeyA, ValueA} | RestA],
                    [{KeyB, _} | _]=RestB) when KeyA < KeyB ->
    do_merge(Fun, Default, KeyA, ValueA, Default, RestA, RestB);
merge(Fun, Default, [{KeyA, _} | _]=RestA,
                    [{KeyB, ValueB} | RestB]) when KeyA > KeyB ->
    do_merge(Fun, Default, KeyB, Default, ValueB, RestA, RestB).

do_merge(Fun, Default, Key, ValueA, ValueB, RestA, RestB) ->
    case Fun(ValueA, ValueB) of
        Default ->
            merge(Fun, Default, RestA, RestB);
        Value ->
            [{Key, Value} | merge(Fun, Default, RestA, RestB)]
    end.

%% @doc True if Pred is true for at least one entry in the DotMap.
-spec any(function(), dot_map()) -> boolean().
any(Pred, DotMap) ->
    lists:any(Pred, DotMap).

%% @doc Fold a DotMap.
-spec fold(function(), term(), dot_map()) -> term().
fold(Fun, AccIn, DotMap) ->
    orddict:fold(Fun, AccIn, DotMap).
