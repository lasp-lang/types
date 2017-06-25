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

%% @doc DotSet.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(dot_set).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-behaviour(dot_store).

-export([
         new/0,
         from_dots/1,
         add_dot/2,
         is_empty/1,
         is_element/2,
         union/2,
         intersection/2,
         subtract/2,
         subtract_causal_context/2,
         fold/3
        ]).

-type dot_set() :: dot_store:dot_set().

%% @doc Create an empty DotSet.
-spec new() -> dot_set().
new() ->
    ordsets:new().

%% @doc Create a DotSet from a list of dots.
-spec from_dots(list(dot_store:dot())) -> dot_set().
from_dots(Dots) ->
    lists:foldl(
        fun(Dot, DotSet) ->
            dot_set:add_dot(Dot, DotSet)
        end,
        dot_set:new(),
        Dots
    ).

%% @doc Add a dot to the DotSet.
-spec add_dot(dot_store:dot(), dot_set()) -> dot_set().
add_dot(Dot, DotSet) ->
    ordsets:add_element(Dot, DotSet).

%% @doc Check if a DotSet is empty.
-spec is_empty(dot_set()) -> boolean().
is_empty(DotSet) ->
    ordsets:size(DotSet) == 0.

%% @doc Check if a dot belongs to the DotSet.
-spec is_element(dot_store:dot(), dot_set()) -> boolean().
is_element(Dot, DotSet) ->
    ordsets:is_element(Dot, DotSet).

%% @doc Union two DotSets.
-spec union(dot_set(), dot_set()) -> dot_set().
union(DotSetA, DotSetB) ->
    ordsets:union(DotSetA, DotSetB).

%% @doc Intersect two DotSets.
-spec intersection(dot_set(), dot_set()) -> dot_set().
intersection(DotSetA, DotSetB) ->
    ordsets:intersection(DotSetA, DotSetB).

%% @doc Subtract a DotSet from a DotSet.
-spec subtract(dot_set(), dot_set()) -> dot_set().
subtract(DotSetA, DotSetB) ->
    ordsets:subtract(DotSetA, DotSetB).

%% @doc Subtract a CausalContext from a DotSet.
-spec subtract_causal_context(dot_set(),
                              causal_context:causal_context()) ->
    dot_set().
subtract_causal_context(DotSet, CausalContext) ->
    ordsets:filter(
        fun(Dot) ->
            not causal_context:is_element(Dot, CausalContext)
        end,
        DotSet
    ).

%% @doc Fold a DotSet.
-spec fold(function(), term(), dot_set()) -> term().
fold(Fun, AccIn, DotSet) ->
    ordsets:fold(Fun, AccIn, DotSet).
