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

%% @doc DotFun.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(dot_fun).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-include("state_type.hrl").

-behaviour(dot_store).

-export([new/0,
         new/1,
         is_empty/1,
         to_causal_context/1]).

% DotFun related (following the same API as `orddict`)
-export([fetch/2,
         fetch_keys/1,
         store/3]).

%% @doc Create an empty DotFun.
-spec new() -> dot_store:dot_fun().
new() ->
    new(?MAX_INT_TYPE).

-spec new(term()) -> dot_store:dot_fun().
new(CRDTType) ->
    {{dot_fun, CRDTType}, orddict:new()}.

%% @doc Check if a DotFun is empty.
-spec is_empty(dot_store:dot_fun()) -> boolean().
is_empty({{dot_fun, _CRDTType}, DotFun}) ->
    orddict:is_empty(DotFun).

%% @doc Given a DotFun, extract a Causal Context.
-spec to_causal_context(dot_store:dot_fun()) -> causal_context:causal_context().
to_causal_context({{dot_fun, _CRDTType}, DotFun}) ->
    Dots = orddict:fetch_keys(DotFun),
    lists:foldl(
        fun(Dot, CausalContext) ->
            causal_context:add_dot(Dot, CausalContext)
        end,
        causal_context:new(),
        Dots
    ).


%% DotSet API
%% @doc Given a Dot and a DotSet, get the correspondent CRDT value.
-spec fetch(dot_store:dot(), dot_store:dot_fun()) -> state_type:crdt().
fetch(Dot, {{dot_fun, _CRDTType}, DotFun}) ->
    {ok, CRDTValue} = orddict:fetch(Dot, DotFun),
    CRDTValue.

%% @doc Get the list of dots used as keys in the DotFun.
-spec fetch_keys(dot_store:dot_fun()) -> [term()].
fetch_keys({{dot_fun, _CRDTType}, DotFun}) ->
    orddict:fetch_keys(DotFun).

%% @doc Stores a new {Dot, CRDTValue} pair in the DotFun.
-spec store(dot_store:dot(), state_type:crdt(), dot_store:dot_fun()) -> dot_store:dot_fun().
store(Dot, {CRDTType, _}=CRDT, {{dot_fun, CRDTType}, DotFun}) ->
    {{dot_fun, CRDTType}, orddict:store(Dot, CRDT, DotFun)}.
