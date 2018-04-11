%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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
%%

-module(prop_causal_context).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ACTOR, oneof([a, b, c])).
-define(DOT, {?ACTOR, dot_store:dot_sequence()}).
-define(DOTL, list(?DOT)).

prop_from_dots() ->
    ?FORALL(
       L,
       ?DOTL,
       begin
            %% if we construct a cc from a list of dots,
            %% all of those dots should be there
            CC = cc(L),
            lists:foldl(
                fun(Dot, Acc) ->
                    Acc andalso causal_context:is_element(Dot, CC)
                end,
                true,
                L
            )
       end
    ).

prop_add_dot() ->
    ?FORALL(
        {Dot, L},
        {?DOT, ?DOTL},
        begin
            %% if we add a dot to a cc it should be there
            CC = cc(L),
            causal_context:is_element(
                Dot,
                causal_context:add_dot(Dot, CC)
            )
        end
    ).

prop_next_dot() ->
    ?FORALL(
        {Actor, L},
        {?ACTOR, ?DOTL},
        begin
            %% the next dot should not be part of the cc.
            CC = cc(L),
            Dot = causal_context:next_dot(Actor, CC),
            not causal_context:is_element(
                Dot,
                CC
            )
        end
    ).

prop_union() ->
    ?FORALL(
        {L1, L2},
        {?DOTL, ?DOTL},
        begin
            CC1 = cc(L1),
            CC2 = cc(L2),
            Union = causal_context:union(CC1, CC2),

            %% Dots from the cc's belong to the union.
            R1 = dot_set:fold(
                fun(Dot, Acc) ->
                    Acc andalso
                    causal_context:is_element(Dot, Union)
                end,
                true,
                dot_set:union(causal_context:dots(CC1),
                              causal_context:dots(CC2))
            ),

            %% Dots from the union belong to one of the cc's.
            R2 =  dot_set:fold(
                fun(Dot, Acc) ->
                    Acc andalso
                    (
                        causal_context:is_element(Dot, CC1) orelse
                        causal_context:is_element(Dot, CC2)
                    )
                end,
                true,
                causal_context:dots(Union)
            ),

            %% Dots in the DotSet don't belong in the compressed part
            {Compressed, DotSet} = Union,
            FakeUnion = {Compressed, dot_set:new()},
            R3 = dot_set:fold(
                fun(Dot, Acc) ->
                    Acc andalso
                    not causal_context:is_element(Dot, FakeUnion)
                end,
                true,
                DotSet
            ),

            R1 andalso R2 andalso R3
        end
    ).

%% @private
cc(L) ->
    lists:foldl(
        fun(Dot, CC) ->
            causal_context:add_dot(Dot, CC)
        end,
        causal_context:new(),
        shuffle(L)
    ).

%% @private
shuffle(L) ->
    rand:seed(exsplus, erlang:timestamp()),
    lists:map(
        fun({_, E}) -> E end,
        lists:sort(
            lists:map(
                fun(E) -> {rand:uniform(), E} end, L
            )
        )
    ).
