%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015-2016 Christopher Meiklejohn.  All Rights Reserved.
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

-export([new/0,
         new/1,
         is_empty/1,
         to_causal_context/1]).

%% DotSet related (following the same API as `ordsets`)
-export([add_element/2,
         intersection/2,
         is_element/2,
         subtract/2,
         to_list/1,
         union/2]).

%% @doc Create an empty DotSet.
-spec new() -> dot_store:dot_set().
new() ->
    {dot_set, ordsets:new()}.

-spec new(term()) -> dot_store:dot_set().
new([]) ->
    new().

%% @doc Check if a DotSet is empty.
-spec is_empty(dot_store:dot_set()) -> boolean().
is_empty({dot_set, DotSet}) ->
    ordsets:size(DotSet) == 0.

%% @doc Given a DotSet, extract a Causal Context.
-spec to_causal_context(dot_store:dot_set()) -> causal_context:causal_context().
to_causal_context({dot_set, DotSet}) ->
    ordsets:fold(
        fun(Dot, CausalContext) ->
            causal_context:add_dot(Dot, CausalContext)
        end,
        causal_context:new(),
        DotSet
    ).


%% DotSet API
%% @doc Add a Dot to the DotSet.
-spec add_element(dot_store:dot(), dot_store:dot_set()) -> dot_store:dot_set().
add_element(Dot, {dot_set, DotSet}) ->
    {dot_set, ordsets:add_element(Dot, DotSet)}.

%% @doc Intersect two DotSets.
-spec intersection(dot_store:dot_set(), dot_store:dot_set()) -> dot_store:dot_set().
intersection({dot_set, DotSetA}, {dot_set, DotSetB}) ->
    {dot_set, ordsets:intersection(DotSetA, DotSetB)}.

%% @doc Check if a Dot belongs to the DotSet.
-spec is_element(dot_store:dot(), dot_store:dot_set()) -> boolean().
is_element(Dot, {dot_set, DotSet}) ->
    ordsets:is_element(Dot, DotSet).

%% @doc Subtract the second DotSet to the first.
-spec subtract(dot_store:dot_set(), dot_store:dot_set()) -> dot_store:dot_set().
subtract({dot_set, DotSetA}, {dot_set, DotSetB}) ->
    {dot_set, ordsets:subtract(DotSetA, DotSetB)}.

%% @doc Convert a DotSet to a list of Dots
-spec to_list(dot_store:dot_set()) -> [dot_store:dot()].
to_list({dot_set, DotSet}) ->
    ordsets:to_list(DotSet).

%% @doc Union two DotSets.
-spec union(dot_store:dot_set(), dot_store:dot_set()) -> dot_store:dot_set().
union({dot_set, DotSetA}, {dot_set, DotSetB}) ->
    {dot_set, ordsets:union(DotSetA, DotSetB)}.
