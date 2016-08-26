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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0,
         new/1,
         is_empty/1]).

%% DotMap related (following the same API as `orddict')
-export([fetch/2,
         fetch_keys/1,
         store/3]).

%% @doc Create an empty DotMap (by default with a DotSet as DotStore in values)
-spec new() -> dot_store:dot_map().
new() ->
    new(dot_set).

%% @doc Create an empty DotMap with a given DotStore type in values
-spec new(term()) -> dot_store:dot_map().
new(DotStoreType) ->
    {{dot_map, DotStoreType}, orddict:new()}.

%% @doc Check if a DotMap is empty.
-spec is_empty(dot_store:dot_map()) -> boolean().
is_empty({{dot_map, _DotStoreType}, DotMap}) ->
    orddict:is_empty(DotMap).


%% DotMap API
%% @doc Given a key and a DotMap, get the correspondent DotStore.
%%      If the key is not found, an empty DotStore will be returned.
-spec fetch(term(), dot_store:dot_map()) -> dot_store:dot_store().
fetch(Key, {{dot_map, DotStoreType}, DotMap}) ->
    case orddict:find(Key, DotMap) of
        {ok, DotStore} ->
            DotStore;
        error ->
            case DotStoreType of
                dot_set ->
                    dot_set:new();
                {dot_fun, CRDTType} ->
                    dot_fun:new(CRDTType);
                CCausalCRDTType ->
                    {CausalCRDTType, Args} = state_type:extract_args(CCausalCRDTType),
                    {CausalCRDTType, {EmptyDotStore, _CC}} = CausalCRDTType:new(Args),
                    %% DotMap's can only embed DotStore's.
                    %% For example, an ORMap<K, AWSet<E>>
                    %% never actually stores an AWSet in the values
                    %% only the underlying DotStore (and that's why
                    %% an ORMap can only embed Causal CRDTs)
                    %% We know that an AWSet is <DotStore, CausalContext>
                    %% but the CausalContext is not stored per key-value
                    %% because all the key-value pairs in the ORMap
                    %% share the same CausalContext.
                    %% This behaviour can easily be observed in
                    %% `state_ormap:query/1' function.
                    EmptyDotStore
            end
    end.

%% @doc Get a list of keys in the DotMap.
-spec fetch_keys(dot_store:dot_map()) -> [term()].
fetch_keys({{dot_map, _DotStoreType}, DotMap}) ->
    orddict:fetch_keys(DotMap).

%% @doc Stores a new {Key, DotStore} pair in the DotMap.
%%      If `Key` already in the DotMap, then its value is replaced.
-spec store(term(), dot_store:dot_store(), dot_store:dot_map()) -> dot_store:dot_map().
store(Key, SubDotStore, {{dot_map, DotStoreType}, DotMap}) ->
    {{dot_map, DotStoreType}, orddict:store(Key, SubDotStore, DotMap)}.
