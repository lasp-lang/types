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

-module(prop_dot_set).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ACTOR, oneof([a, b, c])).
-define(DOT, {?ACTOR, dot_store:dot_sequence()}).
-define(DOTL, list(?DOT)).

prop_add_dot() ->
    ?FORALL(
        {Dot, L},
        {?DOT, ?DOTL},
        begin
            %% if we add a dot to a ds it should be there
            DS = ds(L),
            dot_set:is_element(
                Dot,
                dot_set:add_dot(Dot, DS)
            )
        end
    ).

prop_union() ->
    ?FORALL(
        {L1, L2},
        {?DOTL, ?DOTL},
        begin
            DS1 = ds(L1),
            DS2 = ds(L2),
            Union = dot_set:union(DS1, DS2),

            %% Dots from the ds's belong to the union.
            R1 = lists:foldl(
                fun(Dot, Acc) ->
                    Acc andalso
                    dot_set:is_element(Dot, Union)
                end,
                true,
                dot_set:to_list(DS1) ++ dot_set:to_list(DS2)
            ),

            %% Dots from the union belong to one of the ds's.
            R2 =  lists:foldl(
                fun(Dot, Acc) ->
                    Acc andalso
                    (
                        dot_set:is_element(Dot, DS1) orelse
                        dot_set:is_element(Dot, DS2)
                    )
                end,
                true,
                dot_set:to_list(Union)
            ),

            R1 andalso R2
        end
    ).

prop_intersection() ->
    ?FORALL(
        {L1, L2},
        {?DOTL, ?DOTL},
        begin
            DS1 = ds(L1),
            DS2 = ds(L2),
            Intersection = dot_set:intersection(DS1, DS2),

            %% Dots from the intersection belong to one of the ds's.
            lists:foldl(
                fun(Dot, Acc) ->
                    Acc andalso
                    (
                        dot_set:is_element(Dot, DS1) andalso
                        dot_set:is_element(Dot, DS2)
                    )
                end,
                true,
                dot_set:to_list(Intersection)
            )
        end
    ).

prop_subtract() ->
    ?FORALL(
        {L1, L2},
        {?DOTL, ?DOTL},
        begin
            DS1 = ds(L1),
            DS2 = ds(L2),
            Subtract = dot_set:subtract(DS1, DS2),
            lists:foldl(
                fun(Dot, Acc) ->
                    Acc andalso not dot_set:is_element(Dot, Subtract)
                end,
                true,
                dot_set:to_list(DS2)
            )
        end
    ).

prop_subtract_causal_context() ->
    ?FORALL(
        {L1, L2},
        {?DOTL, ?DOTL},
        begin
            DS = ds(L1),
            CC = cc(L2),
            Subtract = dot_set:subtract_causal_context(DS, CC),
            lists:foldl(
                fun(Dot, Acc) ->
                    Acc andalso not dot_set:is_element(Dot, Subtract)
                end,
                true,
                dot_set:to_list(causal_context:dots(CC))
            )
        end
    ).

prop_set_laws() ->
?FORALL(
        {L1, L2},
        {?DOTL, ?DOTL},
        begin
            DS1 = ds(L1),
            DS2 = ds(L2),

            %% idempotency
            R1 = dot_set:union(DS1, DS1) == DS1,
            R2 = dot_set:intersection(DS1, DS1) == DS1,
            %% domination
            R3 = dot_set:union(DS1, dot_set:new()) == DS1,
            R4 = dot_set:intersection(DS1, dot_set:new()) == dot_set:new(),
            %% absorption
            R5 = dot_set:union(DS1, dot_set:intersection(DS1, DS2)) == DS1,
            R6 = dot_set:intersection(DS1, dot_set:union(DS1, DS2)) == DS1,

            R1 andalso R2
               andalso R3
               andalso R4
               andalso R5
               andalso R6
        end
    ).

%% @private
ds(L) ->
    dot_set:from_dots(L).

%% @private
cc(L) ->
    causal_context:from_dot_set(ds(L)).
