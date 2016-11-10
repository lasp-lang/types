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

%% @doc Pure twoPSet CRDT: pure op-based two-phase set.
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_twopset).
-author("Georges Younes <georges.r.younes@gmail.com>").

-behaviour(type).
-behaviour(pure_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, query/1, equal/2, reset/2]).

-export_type([pure_twopset/0, pure_twopset_op/0]).

-opaque pure_twopset() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), {ordsets:ordset(any()), ordsets:ordset(any())}}.
-type pure_twopset_op() :: {add, pure_type:element()} | {rmv, pure_type:element()}.

%% @doc Create a new, empty `pure_twopset()'
-spec new() -> pure_twopset().
new() ->
    {?TYPE, {orddict:new(), {ordsets:new(), ordsets:new()}}}.

%% @doc Create a new, empty `pure_twopset()'
-spec new([term()]) -> pure_twopset().
new([]) ->
    new().

%% @doc Update a `pure_twopset()'.
-spec mutate(pure_twopset_op(), pure_type:id(), pure_twopset()) ->
    {ok, pure_twopset()}.
mutate({add, Elem}, _VV, {?TYPE, {POLog, {Pure2PAddSet, Pure2PRmvSet}}}) ->
    AlreadyRemoved = ordsets:is_element(Elem, Pure2PRmvSet),
    case AlreadyRemoved of
        true ->
            {ok, {?TYPE, {POLog, Pure2PAddSet, Pure2PRmvSet}}};
        false ->
            PureTwoPSet = {?TYPE, {POLog, {ordsets:add_element(Elem, Pure2PAddSet), Pure2PRmvSet}}},
            {ok, PureTwoPSet}
    end;
mutate({rmv, Elem}, _VV, {?TYPE, {POLog, {Pure2PAddSet, Pure2PRmvSet}}}) ->
    PureTwoPSet = {?TYPE, {POLog, {ordsets:del_element(Elem, Pure2PAddSet), ordsets:add_element(Elem, Pure2PRmvSet)}}},
    {ok, PureTwoPSet}.

%% @doc Clear/reset the state to initial state.
-spec reset(pure_type:id(), pure_twopset()) -> pure_twopset().
reset(VV, {?TYPE, _}=CRDT) ->
    pure_type:reset(VV, CRDT).

%% @doc Returns the value of the `pure_twopset()'.
%%      This value is a set with all the elements in the `pure_twopset()'.
-spec query(pure_twopset()) -> sets:set(pure_type:element()).
query({?TYPE, {_POLog, {Pure2PAddSet, Pure2PRmvSet}}}) ->
    sets:from_list(ordsets:subtract(Pure2PAddSet, Pure2PRmvSet)).

%% @doc Equality for `pure_twopset()'.
%% @todo use ordsets_ext:equal instead
-spec equal(pure_twopset(), pure_twopset()) -> boolean().
equal({?TYPE, {_POLog1, {Pure2PAddSet1, Pure2PRmvSet1}}}, {?TYPE, {_POLog2, {Pure2PAddSet2, Pure2PRmvSet2}}}) ->
    ordsets_ext:equal(Pure2PAddSet1, Pure2PAddSet2) andalso
    ordsets_ext:equal(Pure2PRmvSet1, Pure2PRmvSet2).
%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {orddict:new(), {ordsets:new(), ordsets:new()}}}, new()).

query_test() ->
    Set0 = new(),
    Set1 = {?TYPE, {[], {[<<"a">>], []}}},
    Set2 = {?TYPE, {[], {[<<"b">>, <<"c">>], [<<"a">>, <<"c">>]}}},
    ?assertEqual(sets:new(), query(Set0)),
    ?assertEqual(sets:from_list([<<"a">>]), query(Set1)),
    ?assertEqual(sets:from_list([<<"b">>]), query(Set2)).

add_test() ->
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"a">>}, [], Set0),
    {ok, Set2} = mutate({add, <<"b">>}, [], Set1),
    ?assertEqual({?TYPE, {[], {[<<"a">>], []}}}, Set1),
    ?assertEqual({?TYPE, {[], {[<<"a">>, <<"b">>], []}}}, Set2).

rmv_test() ->
    Set0 = {?TYPE, {[], {[<<"a">>, <<"b">>, <<"c">>], []}}},
    Set1 = {?TYPE, {[], {[<<"b">>, <<"c">>], [<<"a">>, <<"c">>]}}},
    {ok, Set2} = mutate({rmv, <<"a">>}, [], Set0),
    {ok, Set3} = mutate({rmv, <<"c">>}, [], Set1),
    ?assertEqual({?TYPE, {[], {[<<"b">>, <<"c">>], [<<"a">>]}}}, Set2),
    ?assertEqual({?TYPE, {[], {[<<"b">>], [<<"a">>, <<"c">>]}}}, Set3).

reset_test() ->
    Set1 = {?TYPE, {[], {[<<"b">>, <<"c">>], [<<"a">>, <<"c">>]}}},
    Set2 = reset([], Set1),
    ?assertEqual({?TYPE, {[], {[], []}}}, Set2).

equal_test() ->
    Set0 = {?TYPE, {[], {[<<"a">>, <<"b">>, <<"c">>], []}}},
    Set1 = {?TYPE, {[], {[<<"b">>, <<"c">>], [<<"a">>, <<"c">>]}}},
    Set2 = {?TYPE, {[], {[<<"b">>, <<"c">>], [<<"a">>, <<"c">>]}}},
    Set3 = {?TYPE, {[], {[<<"a">>, <<"b">>, <<"c">>], [<<"a">>, <<"c">>]}}},
    ?assert(equal(Set1, Set2)),
    ?assertNot(equal(Set0, Set1)),
    ?assertNot(equal(Set2, Set3)).

-endif.
