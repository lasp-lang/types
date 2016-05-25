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

%% @doc Pure GSet CRDT: pure op-based grow-only set.
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_gset).
-author("Georges Younes <georges.r.younes@gmail.com>").

-behaviour(type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, query/1, equal/2]).

-export_type([pure_gset/0, pure_gset_op/0]).

-opaque pure_gset() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), ordsets:set()}.
-type pure_gset_op() :: {add, pure_type:element()}.

%% @doc Create a new, empty `pure_gset()'
-spec new() -> pure_gset().
new() ->
    {?TYPE, {orddict:new(), ordsets:new()}}.

%% @doc Create a new, empty `pure_gset()'
-spec new([term()]) -> pure_gset().
new([]) ->
    new().

%% @doc Update a `pure_gset()'.
-spec mutate(pure_gset_op(), pure_type:id(), pure_gset()) ->
    {ok, pure_gset()}.
mutate({add, Elem}, _VV, {?TYPE, {POLog, PureGSet}}) ->
    PureGSet1 = {?TYPE, {POLog, ordsets:add_element(Elem, PureGSet)}},
    {ok, PureGSet1}.

%% @doc Returns the value of the `pure_gset()'.
%%      This value is a list with all the elements in the `pure_gset()'.
-spec query(pure_gset()) -> [pure_type:element()].
query({?TYPE, {_, PureGSet}}) ->
    ordsets:to_list(PureGSet).

%% @doc Equality for `pure_gset()'.
%% @todo use ordsets_ext:equal instead
-spec equal(pure_gset(), pure_gset()) -> boolean().
equal({?TYPE, {_, PureGSet1}}, {?TYPE, {_, PureGSet2}}) ->
    ordsets:is_subset(PureGSet1, PureGSet2) andalso ordsets:is_subset(PureGSet2, PureGSet1).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {orddict:new(), ordsets:new()}}, new()).

query_test() ->
    Set0 = new(),
    Set1 = {?TYPE, {[], [<<"a">>]}},
    ?assertEqual([], query(Set0)),
    ?assertEqual([<<"a">>], query(Set1)).

add_test() ->
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"a">>}, [], Set0),
    {ok, Set2} = mutate({add, <<"b">>}, [], Set1),
    ?assertEqual({?TYPE, {[], [<<"a">>]}}, Set1),
    ?assertEqual({?TYPE, {[], [<<"a">>, <<"b">>]}}, Set2).

equal_test() ->
    Set1 = {?TYPE, {[], [<<"a">>]}},
    Set2 = {?TYPE, {[], [<<"a">>, <<"b">>]}},
    ?assert(equal(Set1, Set1)),
    ?assertNot(equal(Set1, Set2)).

-endif.