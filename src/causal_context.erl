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

%% @doc Causal Context.
%%      The current implementation does not have any optimisations such as
%%      the causal context compression.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(causal_context).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0,
         merge/2,
         add_dot/2,
         max_dot/2,
         next_dot/2,
         to_dot_set/1,
         to_causal_context/1]).

-export_type([causal_context/0]).

-type causal_context() :: ordsets:ordset(dot_store:dot()).

%% @doc Create an empty Causal Context.
-spec new() -> causal_context().
new() ->
    ordsets:new().

%% @doc Merge two Causal Contexts.
-spec merge(causal_context(), causal_context()) -> causal_context().
merge(CausalContextA, CausalContextB) ->
    ordsets:union(CausalContextA, CausalContextB).

-spec add_dot(dot_store:dot(), causal_context()) -> causal_context().
add_dot(Dot, CausalContext) ->
    ordsets:add_element(Dot, CausalContext).

%% @doc Get `dot_actor()''s max dot
-spec max_dot(dot_store:dot_actor(), causal_context()) -> dot_store:dot().
max_dot(DotActor, CausalContext) ->
    MaxValue = ordsets:fold(
        fun({Actor, Value}, CurrentMax) ->
            case Actor == DotActor andalso Value > CurrentMax of
                true ->
                    Value;
                false ->
                    CurrentMax
            end
        end,
        0,
        CausalContext
    ),
    {DotActor, MaxValue}.

%% @doc Get `dot_actor()''s next dot
-spec next_dot(dot_store:dot_actor(), causal_context()) -> dot_store:dot().
next_dot(DotActor, CausalContext) ->
    {DotActor, MaxValue} = max_dot(DotActor, CausalContext),
    {DotActor, MaxValue + 1}.

%% @doc Convert a CausalContext to a DotSet
-spec to_dot_set(causal_context()) -> dot_store:dot_set().
to_dot_set(CausalContext) ->
    ordsets:fold(
        fun(Dot, DotSet) ->
            dot_set:add_element(Dot, DotSet)
        end,
        dot_set:new(),
        CausalContext
    ).

%% @doc Given a DotStore, extract a Causal Context.
-spec to_causal_context(dot_store:dot_store()) -> causal_context().
to_causal_context({dot_set, DotSet}) ->
    ordsets:fold(
        fun(Dot, CausalContext) ->
            causal_context:add_dot(Dot, CausalContext)
        end,
        causal_context:new(),
        DotSet
    );

to_causal_context({{dot_fun, _CRDTType}, DotFun}) ->
    Dots = orddict:fetch_keys(DotFun),
    lists:foldl(
        fun(Dot, CausalContext) ->
            causal_context:add_dot(Dot, CausalContext)
        end,
        causal_context:new(),
        Dots
    );

to_causal_context({{dot_map, _DotStoreType}, DotMap}) ->
    orddict:fold(
        fun(_Key, SubDotStore, CausalContext) ->
            causal_context:merge(
                to_causal_context(SubDotStore),
                CausalContext
            )
        end,
        causal_context:new(),
        DotMap
    ).
