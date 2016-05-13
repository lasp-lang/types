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

%% @doc GSet CRDT: grow only set.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(gset).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-behaviour(type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_inflation/2, is_strict_inflation/2]).
-export([join_decomposition/1]).

-export_type([gset/0, delta_gset/0, gset_op/0]).

-opaque gset() :: {?TYPE, payload()}.
-opaque delta_gset() :: {?TYPE, {delta, payload()}}.
-type payload() :: ordsets:set().
-type element() :: term().
-type gset_op() :: {add, element()}.

%% @doc Create a new, empty `gset()'
-spec new() -> gset().
new() ->
    {?TYPE, ordsets:new()}.

%% @doc Create a new, empty `gset()'
-spec new([term()]) -> gset().
new([]) ->
    new().

%% @doc Mutate a `gset()'.
-spec mutate(gset_op(), type:actor(), gset()) ->
    {ok, gset()}.
mutate(Op, Actor, {?TYPE, _GSet}=CRDT) ->
    type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `gset()'.
%%      The first argument can only be `{add, term()}'.
%%      The second argument is the replica id (unused).
%%      The third argument is the `gset()' to be inflated.
%%      Returns a `gset()' delta which is a new `gset()'
%%      with only one element - the element to be added to
%%      the set. If the element is already in the set
%%      the resulting delta will be an empty `gset()'.
-spec delta_mutate(gset_op(), type:actor(), gset()) ->
    {ok, delta_gset()}.
delta_mutate({add, Elem}, _Actor, {?TYPE, GSet}) ->
    Delta = case ordsets:is_element(Elem, GSet) of
        true ->
            ordsets:new();
        false ->
            ordsets:add_element(Elem, ordsets:new())
    end,
    {ok, {delta, {?TYPE, Delta}}}.

%% @doc Returns the value of the `gset()'.
%%      This value is a list with all the elements in the `gset()'.
-spec query(gset()) -> [element()].
query({?TYPE, GSet}) ->
    ordsets:to_list(GSet).

%% @doc Merge two `gset()'.
%%      The result is the set union of both sets in the
%%      `gset()' passed as argument.
-spec merge(gset(), gset()) -> gset().
merge({?TYPE, GSet1}, {?TYPE, GSet2}) ->
    GSet = ordsets:union(GSet1, GSet2),
    {?TYPE, GSet}.

%% @doc Equality for `gset()'.
%%      Two sets s1 and s2 are equal if:
%%          - s1 is subset of s2
%%          - s2 is subset of s1
-spec equal(gset(), gset()) -> boolean().
equal({?TYPE, GSet1}, {?TYPE, GSet2}) ->
    ordsets:is_subset(GSet1, GSet2) andalso ordsets:is_subset(GSet2, GSet1).

%% @doc Given two `gset()', check if the second is and inflation
%%      of the first.
%%      The second `gset()' is and inflation if the first set is
%%      a subset of the second.
-spec is_inflation(gset(), gset()) -> boolean().
is_inflation({?TYPE, GSet1}, {?TYPE, GSet2}) ->
    ordsets:is_subset(GSet1, GSet2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(gset(), gset()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Join decomposition for `gset()'.
%%      The join decompostion for a `gset()' is the unique set
%%      partition where each set of the partition has exactly one
%%      element.
-spec join_decomposition(gset()) -> [gset()].
join_decomposition({?TYPE, GSet}) ->
    ordsets:fold(
        fun(Elem, Acc) ->
            [{?TYPE, ordsets:add_element(Elem, ordsets:new())} | Acc]
        end,
        [],
        GSet
     ).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, ordsets:new()}, new()).

query_test() ->
    Set0 = new(),
    Set1 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:new())},
    ?assertEqual([], query(Set0)),
    ?assertEqual([<<"a">>], query(Set1)).

add_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, {delta, Delta1}} = delta_mutate({add, <<"a">>}, Actor, Set0),
    Set1 = merge(Delta1, Set0),
    {ok, {delta, Delta2}} = delta_mutate({add, <<"a">>}, Actor, Set1),
    Set2 = merge(Delta2, Set1),
    {ok, {delta, Delta3}} = delta_mutate({add, <<"b">>}, Actor, Set2),
    Set3 = merge(Delta3, Set2),
    ?assertEqual({?TYPE, ordsets:add_element(<<"a">>, ordsets:new())}, Delta1),
    ?assertEqual({?TYPE, ordsets:add_element(<<"a">>, ordsets:new())}, Set1),
    ?assertEqual({?TYPE, ordsets:new()}, Delta2),
    ?assertEqual({?TYPE, ordsets:add_element(<<"a">>, ordsets:new())}, Set2),
    ?assertEqual({?TYPE, ordsets:add_element(<<"b">>, ordsets:new())}, Delta3),
    ?assertEqual({?TYPE, ordsets:add_element(<<"a">>, ordsets:add_element(<<"b">>, ordsets:new()))}, Set3).

merge_idempontent_test() ->
    Set1 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:new())},
    Set2 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:add_element(<<"b">>, ordsets:new()))},
    Set3 = merge(Set1, Set1),
    Set4 = merge(Set2, Set2),
    ?assertEqual({?TYPE, ordsets:add_element(<<"a">>, ordsets:new())}, Set3),
    ?assertEqual({?TYPE, ordsets:add_element(<<"a">>, ordsets:add_element(<<"b">>, ordsets:new()))}, Set4).

merge_commutative_test() ->
    Set1 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:new())},
    Set2 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:add_element(<<"b">>, ordsets:new()))},
    Set3 = merge(Set1, Set2),
    Set4 = merge(Set2, Set1),
    ?assertEqual({?TYPE, ordsets:add_element(<<"a">>, ordsets:add_element(<<"b">>, ordsets:new()))}, Set3),
    ?assertEqual({?TYPE, ordsets:add_element(<<"a">>, ordsets:add_element(<<"b">>, ordsets:new()))}, Set4).

equal_test() ->
    Set1 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:new())},
    Set2 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:add_element(<<"b">>, ordsets:new()))},
    Set3 = {?TYPE, ordsets:add_element(<<"b">>, ordsets:add_element(<<"a">>, ordsets:new()))},
    ?assert(equal(Set1, Set1)),
    ?assertNot(equal(Set1, Set2)),
    ?assert(equal(Set2, Set3)).

is_inflation_test() ->
    Set1 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:new())},
    Set2 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:add_element(<<"b">>, ordsets:new()))},
    ?assert(is_inflation(Set1, Set1)),
    ?assert(is_inflation(Set1, Set2)),
    ?assertNot(is_inflation(Set2, Set1)).

is_strict_inflation_test() ->
    Set1 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:new())},
    Set2 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:add_element(<<"b">>, ordsets:new()))},
    ?assertNot(is_strict_inflation(Set1, Set1)),
    ?assert(is_strict_inflation(Set1, Set2)),
    ?assertNot(is_strict_inflation(Set2, Set1)).

join_decomposition_test() ->
    Set1 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:new())},
    Set2 = {?TYPE, ordsets:add_element(<<"a">>, ordsets:add_element(<<"b">>, ordsets:new()))},
    Decomp1 = join_decomposition(Set1),
    Decomp2 = join_decomposition(Set2),
    ?assertEqual([{?TYPE, ordsets:add_element(<<"a">>, ordsets:new())}], Decomp1),
    ?assertEqual(lists:sort([{?TYPE, ordsets:add_element(<<"a">>, ordsets:new())}, {?TYPE, ordsets:add_element(<<"b">>, ordsets:new())}]), lists:sort(Decomp2)).

-endif.
