% %
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

%% @doc Pure GCounter CRDT: pure op-based grow-only counter.
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_gcounter).
-author("Georges Younes <georges.r.younes@gmail.com>").

-behaviour(type).
-behaviour(pure_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, query/1, equal/2, reset/2]).

-export_type([pure_gcounter/0, pure_gcounter_op/0]).

-opaque pure_gcounter() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), integer()}.
-type pure_gcounter_op() :: increment | {increment, non_neg_integer()}.

%% @doc Create a new, empty `pure_gcounter()'
-spec new() -> pure_gcounter().
new() ->
    {?TYPE, {orddict:new(), 0}}.

%% @doc Create a new, empty `pure_gcounter()'
-spec new([term()]) -> pure_gcounter().
new([]) ->
    new().

%% @doc Update a `pure_gcounter()'.
-spec mutate(pure_gcounter_op(), pure_type:id(), pure_gcounter()) ->
    {ok, pure_gcounter()}.
mutate(increment, _VV, {?TYPE, {POLog, PureGCounter}}) ->
    PureGCounter1 = {?TYPE, {POLog, PureGCounter + 1}},
    {ok, PureGCounter1};
mutate({increment, Val}, _VV, {?TYPE, {POLog, PureGCounter}}) ->
    PureGCounter1 = {?TYPE, {POLog, PureGCounter + Val}},
    {ok, PureGCounter1}.

%% @doc Clear/reset the state to initial state.
-spec reset(pure_type:id(), pure_gcounter()) -> pure_gcounter().
reset(VV, {?TYPE, _}=CRDT) ->
    pure_type:reset(VV, CRDT).

%% @doc Return the value of the `pure_gcounter()'.
-spec query(pure_gcounter()) -> non_neg_integer().
query({?TYPE, {_POLog, PureGCounter}}) ->
    PureGCounter.

%% @doc Check if two `pure_gcounter()' instances have the same value.
-spec equal(pure_gcounter(), pure_gcounter()) -> boolean().
equal({?TYPE, {_POLog1, PureGCounter1}}, {?TYPE, {_POLog2, PureGCounter2}}) ->
    PureGCounter1 == PureGCounter2.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {[], 0}}, new()).

query_test() ->
    PureGCounter0 = new(),
    PureGCounter1 = {?TYPE, {[], 15}},
    ?assertEqual(0, query(PureGCounter0)),
    ?assertEqual(15, query(PureGCounter1)).

increment_test() ->
    PureGCounter0 = new(),
    {ok, PureGCounter1} = mutate(increment, [], PureGCounter0),
    {ok, PureGCounter2} = mutate(increment, [], PureGCounter1),
    {ok, PureGCounter3} = mutate({increment, 5}, [], PureGCounter2),
    ?assertEqual({?TYPE, {[], 1}}, PureGCounter1),
    ?assertEqual({?TYPE, {[], 2}}, PureGCounter2),
    ?assertEqual({?TYPE, {[], 7}}, PureGCounter3).

reset_test() ->
    PureGCounter1 = {?TYPE, {[], 15}},
    PureGCounter2 = reset([], PureGCounter1),
    ?assertEqual({?TYPE, {[], 0}}, PureGCounter2).

equal_test() ->
    PureGCounter1 = {?TYPE, {[], 1}},
    PureGCounter2 = {?TYPE, {[], 2}},
    PureGCounter3 = {?TYPE, {[], 3}},
    ?assert(equal(PureGCounter1, PureGCounter1)),
    ?assertNot(equal(PureGCounter2, PureGCounter1)),
    ?assertNot(equal(PureGCounter2, PureGCounter3)).

-endif.
