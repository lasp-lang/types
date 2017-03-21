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

%% @doc Common library for causal CRDTs.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(state_causal_type).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-include("state_type.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/1,
         merge/3,
         get_dot_store_type/1]).

-export_type([causal_crdt/0]).

-type causal_crdt() :: {
                            dot_store:dot_store(),
                            causal_context:causal_context()
                       }.

%% @doc Create an empty CausalCRDT.
-spec new(dot_set | dot_fun | dot_map) -> causal_crdt().
new(DotStoreType) ->
    {DotStoreType:new(), causal_context:new()}.

%% @doc Universal merge for a two CausalCRDTs.
-spec merge(dot_store:type(), causal_crdt(), causal_crdt()) ->
    causal_crdt().
merge(_Type, {DotStore, CausalContext}, {DotStore, CausalContext}) ->
    {DotStore, CausalContext};

merge(dot_set, {DotSetA, CausalContextA}, {DotSetB, CausalContextB}) ->

    Intersection = dot_set:intersection(DotSetA, DotSetB),
    Subtract1 = dot_set:subtract_causal_context(DotSetA,
                                                CausalContextB),
    Subtract2 = dot_set:subtract_causal_context(DotSetB,
                                                CausalContextA),

    DotStore = dot_set:union(Intersection,
                             dot_set:union(Subtract1, Subtract2)),

    CausalContext = causal_context:union(CausalContextA,
                                         CausalContextB),

    {DotStore, CausalContext};

merge({dot_fun, CRDTType}, {DotFunA, CausalContextA},
                           {DotFunB, CausalContextB}) ->

    Filtered1 = dot_fun:filter(
        fun(Dot, _) ->
            not causal_context:is_element(Dot, CausalContextB)
        end,
        DotFunA
    ),

    Filtered2 = dot_fun:filter(
        fun(Dot, _) ->
            not causal_context:is_element(Dot, CausalContextA)
        end,
        DotFunB
    ),

    DotSetA = dot_set:from_dots(dot_fun:dots(DotFunA)),
    DotSetB = dot_set:from_dots(dot_fun:dots(DotFunB)),
    Intersection = dot_set:intersection(DotSetA, DotSetB),

    DotStore0 = lists:foldl(
        fun(Dot, DotFun) ->
            ValueA = dot_fun:fetch(Dot, DotFunA),
            ValueB = dot_fun:fetch(Dot, DotFunB),
            {CRDTType, Value} = CRDTType:merge({CRDTType, ValueA},
                                               {CRDTType, ValueB}),
            dot_fun:store(Dot, Value, DotFun)
        end,
        dot_fun:new(),
        dot_set:to_list(Intersection)
    ),

    DotStore1 = lists:foldl(
        fun({Dot, Value}, DotFun) ->
            dot_fun:store(Dot, Value, DotFun)
        end,
        DotStore0,
        dot_fun:to_list(Filtered1) ++ dot_fun:to_list(Filtered2)
    ),

    CausalContext = causal_context:union(CausalContextA,
                                         CausalContextB),

    {DotStore1, CausalContext};

merge({dot_map, Type}, {DotMapA, CausalContextA},
                       {DotMapB, CausalContextB}) ->

    %% Type can be:
    %% - DotStoreType
    %% - CRDTType (causal)
    DotStoreType = get_dot_store_type(Type),
    Default = DotStoreType:new(),

    KeysA = dot_map:fetch_keys(DotMapA),
    KeysB = dot_map:fetch_keys(DotMapB),
    Keys = ordsets:union(
        ordsets:from_list(KeysA),
        ordsets:from_list(KeysB)
    ),

    DotStore = ordsets:fold(
        fun(Key, DotMap) ->
            KeyDotStoreA = dot_map:fetch(Key, DotMapA, Default),
            KeyDotStoreB = dot_map:fetch(Key, DotMapB, Default),

            {VK, _} = merge(get_type(Type),
                {KeyDotStoreA, CausalContextA},
                {KeyDotStoreB, CausalContextB}
            ),

            case DotStoreType:is_empty(VK) of
                true ->
                    DotMap;
                false ->
                    dot_map:store(Key, VK, DotMap)
            end
        end,
        dot_map:new(),
        Keys
    ),

    CausalContext = causal_context:union(CausalContextA,
                                         CausalContextB),

    {DotStore, CausalContext}.

%% @doc Get the DotStore type.
get_dot_store_type(T) ->
    case get_type(T) of
        dot_set ->
            dot_set;
        {DS, _} ->
            DS
    end.

%% @private
get_type(dot_set=T) ->
    T;
get_type({dot_fun, _}=T) ->
    T;
get_type({dot_map, _}=T) ->
    T;
get_type(state_awset) ->
    {dot_map, dot_set};
get_type(state_dwflag) ->
    dot_set;
get_type(state_ewflag) ->
    dot_set;
get_type(state_mvregister) ->
    {dot_fun, ?IVAR_TYPE}.
