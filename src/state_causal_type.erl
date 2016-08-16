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

-export([new/1,
         merge/2]).

-export_type([causal_crdt/0]).

-type causal_crdt() :: {
                            dot_store:dot_store(),
                            causal_context:causal_context()
                       }.

%% @doc Create an empty CausalCRDT.
-spec new(dot_store:type()) -> causal_crdt().
new(CType) ->
    {DotStoreType, Args} = state_type:extract_args(CType),
    {DotStoreType:new(Args), causal_context:new()}.

%% @doc Universal merge for a two CausalCRDTs.
-spec merge(causal_crdt(), causal_crdt()) -> causal_crdt().
merge({DotStore, CausalContext}, {DotStore, CausalContext}) ->
    {DotStore, CausalContext};

merge({{dot_set, _}=DotSetA, CausalContextA},
      {{dot_set, _}=DotSetB, CausalContextB}) ->

    DotSetL = dot_set:intersection(DotSetA, DotSetB),
    DotSetM = dot_set:subtract(
        DotSetA,
        causal_context:to_dot_set(CausalContextB)
    ),
    DotSetR = dot_set:subtract(
        DotSetB,
        causal_context:to_dot_set(CausalContextA)
    ),

    DotStore = dot_set:union(
        dot_set:union(DotSetL, DotSetM),
        DotSetR
    ),
    CausalContext = causal_context:merge(CausalContextA, CausalContextB),

    {DotStore, CausalContext};

merge({{{dot_fun, CRDTType}, _}=DotFunA, CausalContextA},
      {{{dot_fun, CRDTType}, _}=DotFunB, CausalContextB}) ->

    DotSetA = causal_context:to_dot_set(
        dot_fun:to_causal_context(DotFunA)
    ),
    DotSetB = causal_context:to_dot_set(
        dot_fun:to_causal_context(DotFunB)
    ),
    CCDotSetA = causal_context:to_dot_set(CausalContextA),
    CCDotSetB = causal_context:to_dot_set(CausalContextB),

    CommonDotSet = dot_set:intersection(DotSetA, DotSetB),

    DotFun0 = lists:foldl(
        fun(Dot, DotFun) ->
            ValueA = dot_fun:fetch(Dot, DotFunA),
            ValueB = dot_fun:fetch(Dot, DotFunB),
            Value = CRDTType:merge(ValueA, ValueB),
            dot_fun:store(Dot, Value, DotFun)
        end,
        dot_fun:new([CRDTType]),
        dot_set:to_list(CommonDotSet)
    ),

    DotFun1 = lists:foldl(
        fun(Dot, DotFun) ->
            case dot_set:is_element(Dot, CCDotSetB) of
                true ->
                    DotFun;
                false ->
                    Value = dot_fun:fetch(Dot, DotFunA),
                    dot_fun:store(Dot, Value, DotFun)
            end
        end,
        DotFun0,
        dot_fun:fetch_keys(DotFunA)
    ),

    DotFun2 = lists:foldl(
        fun(Dot, DotFun) ->
            case dot_set:is_element(Dot, CCDotSetA) of
                true ->
                    DotFun;
                false ->
                    Value = dot_fun:fetch(Dot, DotFunB),
                    dot_fun:store(Dot, Value, DotFun)
            end
        end,
        DotFun1,
        dot_fun:fetch_keys(DotFunB)
    ),

    DotStore = DotFun2,
    CausalContext = causal_context:merge(CausalContextA, CausalContextB),
    {DotStore, CausalContext};

merge({{{dot_map, DotStoreType}, _}=DotMapA, CausalContextA},
      {{{dot_map, DotStoreType}, _}=DotMapB, CausalContextB}) ->

    KeysA = dot_map:fetch_keys(DotMapA),
    KeysB = dot_map:fetch_keys(DotMapB),
    Keys = ordsets:union(
        ordsets:from_list(KeysA),
        ordsets:from_list(KeysB)
    ),

    DotStore = ordsets:fold(
        fun(Key, DotMap) ->
            KeyDotStoreA = dot_map:fetch(Key, DotMapA),
            KeyDotStoreB = dot_map:fetch(Key, DotMapB),

            {VK, _} = merge(
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
        dot_map:new(dot_set),
        Keys
    ),
    CausalContext = causal_context:merge(CausalContextA, CausalContextB),

    {DotStore, CausalContext}.
