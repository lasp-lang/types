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
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(causal_context).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-export([
         new/0,
         from_dot_set/1,
         dots/1,
         add_dot/2,
         next_dot/2,
         is_empty/1,
         is_element/2,
         union/2,
         to_dot/1
        ]).

-export_type([causal_context/0]).

-type dot_set() :: dot_store:dot_set().
-type compressed() :: orddict:orddict(dot_store:dot_actor(),
                                      dot_store:dot_sequence()).
-type causal_context() :: {compressed(), dot_set()}.

%% @doc Create an empty Causal Context.
-spec new() -> causal_context().
new() ->
    {orddict:new(), dot_set:new()}.

%% @doc Create a CausalContext from a DotSet.
-spec from_dot_set(dot_set()) -> causal_context().
from_dot_set(DotSet) ->
    dot_set:fold(
        fun(Dot, CausalContext) ->
            causal_context:add_dot(Dot, CausalContext)
        end,
        causal_context:new(),
        DotSet
    ).

%% @doc Return a list of dots from a CausalContext.
-spec dots(causal_context()) -> dot_set().
dots({Compressed, DotSet}) ->
    CompressedDots = orddict:fold(
        fun(Actor, Sequence, Acc0) ->
            lists:foldl(
                fun(I, Acc1) ->
                    dot_set:add_dot({Actor, I}, Acc1)
                end,
                Acc0,
                lists:seq(1, Sequence)
            )
        end,
        dot_set:new(),
        Compressed
    ),

    dot_set:union(CompressedDots, DotSet).

%% @doc Add a dot to the CausalContext.
-spec add_dot(dot_store:dot(), causal_context()) -> causal_context().
add_dot({Actor, Sequence}=Dot, {Compressed0, DotSet0}=CC) ->
    Current = orddict_ext:fetch(Actor, Compressed0, 0),

    case Sequence == Current + 1 of
        true ->
            %% update the compressed component
            Compressed1 = orddict:store(Actor, Sequence, Compressed0),
            {Compressed1, DotSet0};
        false ->
            case Sequence > Current + 1 of
                true ->
                    %% store in the DotSet if in the future
                    DotSet1 = dot_set:add_dot(Dot, DotSet0),
                    {Compressed0, DotSet1};
                false ->
                    %% dot already in the CausalContext.
                    CC
           end
    end.

%% @doc Get `dot_actor()''s next dot
-spec next_dot(dot_store:dot_actor(), causal_context()) ->
    dot_store:dot().
next_dot(Actor, {Compressed, _DotSet}) ->
    MaxValue = orddict_ext:fetch(Actor, Compressed, 0),
    {Actor, MaxValue + 1}.

%% @doc Check if a Causal Context is empty.
-spec is_empty(causal_context()) -> boolean().
is_empty({Compressed, DotSet}) ->
    orddict:is_empty(Compressed) andalso dot_set:is_empty(DotSet).

%% @doc Check if a dot belongs to the CausalContext.
-spec is_element(dot_store:dot(), causal_context()) -> boolean().
is_element({Actor, Sequence}=Dot, {Compressed, DotSet}) ->
    Current = orddict_ext:fetch(Actor, Compressed, 0),
    Sequence =< Current orelse dot_set:is_element(Dot, DotSet).

%% @doc Merge two Causal Contexts.
-spec union(causal_context(), causal_context()) -> causal_context().
union({CompressedA, DotSetA}, {CompressedB, DotSetB}) ->
    Compressed = orddict:merge(
        fun(_Actor, SequenceA, SequenceB) ->
            max(SequenceA, SequenceB)
        end,
        CompressedA,
        CompressedB
    ),
    DotSet = dot_set:union(DotSetA, DotSetB),
    compress({Compressed, DotSet}).

%% @private Try to add the dots in the DotSet to the compressed
%%          component.
-spec compress(causal_context()) -> causal_context().
compress({Compressed, DotSet}) ->
    dot_set:fold(
        fun(Dot, CausalContext) ->
            add_dot(Dot, CausalContext)
        end,
        {Compressed, dot_set:new()},
        DotSet
    ).

%% @private Convert a CausalContext with a single dot, to that dot.
-spec to_dot(causal_context()) -> dot_store:dot().
to_dot({[Dot], []}) -> Dot;
to_dot({[], [Dot]}) -> Dot.
