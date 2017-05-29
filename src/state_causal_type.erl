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
         ds_bottom/1,
         is_element/3,
         dots/2]).

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
merge(Type, A, B) ->
    merge(Type, A, B, true).

merge(dot_set, {DotSetA, CausalContextA}, {DotSetB, CausalContextB}, MergeCausalContext) ->

    Intersection = dot_set:intersection(DotSetA, DotSetB),
    Subtract1 = dot_set:subtract_causal_context(DotSetA,
                                                CausalContextB),
    Subtract2 = dot_set:subtract_causal_context(DotSetB,
                                                CausalContextA),

    DotStore = dot_set:union(Intersection,
                             dot_set:union(Subtract1, Subtract2)),

    CausalContext = case MergeCausalContext of
        true ->
            causal_context:union(CausalContextA,
                                 CausalContextB);
        false ->
            undefined
    end,

    {DotStore, CausalContext};


merge({dot_fun, CRDTType}=DSType, {DotFunA, CausalContextA},
                                  {DotFunB, CausalContextB}, MergeCausalContext) ->

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

    DotSetA = dots(DSType, DotFunA),
    DotSetB = dots(DSType, DotFunB),
    Intersection = dot_set:intersection(DotSetA, DotSetB),

    DotStore0 = dot_set:fold(
        fun(Dot, DotFun) ->
            ValueA = dot_fun:fetch(Dot, DotFunA),
            ValueB = dot_fun:fetch(Dot, DotFunB),
            {CRDTType, Value} = CRDTType:merge({CRDTType, ValueA},
                                               {CRDTType, ValueB}),
            dot_fun:store(Dot, Value, DotFun)
        end,
        dot_fun:new(),
        Intersection
    ),

    DotStore1 = lists:foldl(
        fun({Dot, Value}, DotFun) ->
            dot_fun:store(Dot, Value, DotFun)
        end,
        DotStore0,
        dot_fun:to_list(Filtered1) ++ dot_fun:to_list(Filtered2)
    ),

    CausalContext = case MergeCausalContext of
        true ->
            causal_context:union(CausalContextA,
                                 CausalContextB);
        false ->
            undefined
    end,

    {DotStore1, CausalContext};

merge({dot_map, Type}, {DotMapA, CausalContextA},
                       {DotMapB, CausalContextB}, MergeCausalContext) ->

    Default = ds_bottom(Type),
    DotStoreType = get_type(Type),

    %% merge two dot maps
    DotStore = dot_map:merge(
        fun(KeyDotStoreA, KeyDotStoreB) ->
            {VK, _} = merge(DotStoreType,
                {KeyDotStoreA, CausalContextA},
                {KeyDotStoreB, CausalContextB},
                false %% don't merge the CausalContext
            ),
            VK
        end,
        Default,
        DotMapA,
        DotMapB
    ),

    CausalContext = case MergeCausalContext of
        true ->
            causal_context:union(CausalContextA,
                                 CausalContextB);
        false ->
            undefined
    end,

    {DotStore, CausalContext}.

%% @doc Get an empty DotStore.
-spec ds_bottom(dot_store:type()) -> dot_store:dot_store().
ds_bottom(T) ->
    DSType = case get_type(T) of
        dot_set ->
            dot_set;
        {DS, _} ->
            DS
    end,
    DSType:new().

%% @doc Check if a dot belongs to the DotStore.
-spec is_element(dot_store:type(), dot_store:dot(),
                 dot_store:dot_store()) -> boolean().
is_element(dot_set, Dot, DotSet) ->
    dot_set:is_element(Dot, DotSet);
is_element({dot_fun, _}, Dot, DotFun) ->
    dot_fun:is_element(Dot, DotFun);
is_element({dot_map, T}, Dot, DotMap) ->
    dot_map:any(
        fun({_Key, DotStore}) ->
            is_element(get_type(T), Dot, DotStore)
        end,
        DotMap
    ).

%% @doc Get dots from a DotStore.
-spec dots(dot_store:type(), dot_store:dot_store()) ->
    dot_store:dot_set().
dots(dot_set, DotSet) ->
    DotSet;
dots({dot_fun, _}, DotFun) ->
    Dots = [Dot || {Dot, _} <- dot_fun:to_list(DotFun)],
    dot_set:from_dots(Dots);
dots({dot_map, T}, DotMap) ->
    dot_map:fold(
        fun(_, DS, Acc) ->
            DotSet = dots(T, DS),
            dot_set:union(DotSet, Acc)
        end,
        dot_set:new(),
        DotMap
    );
dots(Type, DS) ->
    dots(get_type(Type), DS).

%% @private
get_type(dot_set) ->
    dot_set;
get_type({dot_fun, T}) ->
    {dot_fun, T};
get_type({dot_map, T}) ->
    {dot_map, get_type(T)};
get_type(?AWSET_TYPE) ->
    {dot_map, dot_set};
get_type(?DWFLAG_TYPE) ->
    dot_set;
get_type(?EWFLAG_TYPE) ->
    dot_set;
get_type(?MVREGISTER_TYPE) ->
    {dot_fun, ?IVAR_TYPE};
get_type({?AWMAP_TYPE, [T]}) ->
    {dot_map, get_type(T)}.
