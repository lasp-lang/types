% -------------------------------------------------------------------
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

%% @doc Pure PNCounter CRDT: pure op-based grow-only counter.
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_pncounter).
-author("Georges Younes <georges.r.younes@gmail.com>").

-behaviour(type).
%-behaviour(pure_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, query/1, equal/2]).

-export_type([pure_pncounter/0, pure_pncounter_op/0]).

-opaque pure_pncounter() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), integer()}.
-type pure_pncounter_op() :: increment | decrement.

%% @doc Create a new, empty `pure_pncounter()'
-spec new() -> pure_pncounter().
new() ->
    {?TYPE, {orddict:new(), 0}}.

%% @doc Create a new, empty `pure_pncounter()'
-spec new([term()]) -> pure_pncounter().
new([]) ->
    new().

%% @doc Update a `pure_pncounter()'.
-spec mutate(pure_pncounter_op(), pure_type:id(), pure_pncounter()) ->
    {ok, pure_pncounter()}.
mutate(increment, _VV, {?TYPE, {_POLog, PurePNCounter}}) ->
    PurePNCounter1 = {?TYPE, {_POLog, PurePNCounter + 1}},
    {ok, PurePNCounter1};
mutate(decrement, _VV, {?TYPE, {_POLog, PurePNCounter}}) ->
    PurePNCounter1 = {?TYPE, {_POLog, PurePNCounter - 1}},
    {ok, PurePNCounter1}.

%% @doc Return the value of the `pure_pncounter()'.
-spec query(pure_pncounter()) -> integer().
query({?TYPE, {_POLog, PurePNCounter}}) ->
    PurePNCounter.

%% @doc Check if two `pure_pncounter()' instances have the same value.
-spec equal(pure_pncounter(), pure_pncounter()) -> boolean().
equal({?TYPE, {_POLog1, PurePNCounter1}}, {?TYPE, {_POLog2, PurePNCounter2}}) ->
    PurePNCounter1 == PurePNCounter2.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {[], 0}}, new()).

query_test() ->
    PurePNCounter0 = new(),
    PurePNCounter1 = {?TYPE, {[], 15}},
    ?assertEqual(0, query(PurePNCounter0)),
    ?assertEqual(15, query(PurePNCounter1)).

increment_test() ->
    PurePNCounter0 = new(),
    {ok, PurePNCounter1} = mutate(increment, [], PurePNCounter0),
    {ok, PurePNCounter2} = mutate(increment, [], PurePNCounter1),
    ?assertEqual({?TYPE, {[], 1}}, PurePNCounter1),
    ?assertEqual({?TYPE, {[], 2}}, PurePNCounter2).

decrement_test() ->
    PurePNCounter0 = {?TYPE, {[], 1}},
    {ok, PurePNCounter1} = mutate(decrement, [], PurePNCounter0),
    {ok, PurePNCounter2} = mutate(decrement, [], PurePNCounter1),
    ?assertEqual({?TYPE, {[], 0}}, PurePNCounter1),
    ?assertEqual({?TYPE, {[], -1}}, PurePNCounter2).

equal_test() ->
    PurePNCounter1 = {?TYPE, {[], 1}},
    PurePNCounter2 = {?TYPE, {[], 2}},
    PurePNCounter3 = {?TYPE, {[], 3}},
    ?assert(equal(PurePNCounter1, PurePNCounter1)),
    ?assertNot(equal(PurePNCounter2, PurePNCounter1)),
    ?assertNot(equal(PurePNCounter2, PurePNCounter3)).

-endif.
