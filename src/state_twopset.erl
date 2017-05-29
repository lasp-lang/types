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

%% @doc 2PSet CRDT: two-phased set.
%%      Once removed, elements cannot be added again.
%%      Also, this is not an observed removed variant.
%%      This means elements can be removed before being
%%      in the set.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]
%%
%% @reference Carlos Baquero
%%      delta-enabled-crdts C++ library
%%      [https://github.com/CBaquero/delta-enabled-crdts]

-module(state_twopset).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-behaviour(type).
-behaviour(state_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_bottom/1, is_inflation/2,
         is_strict_inflation/2,
         irreducible_is_strict_inflation/2]).
-export([join_decomposition/1, delta/2, digest/1]).
-export([encode/2, decode/2]).

-export_type([state_twopset/0, state_twopset_op/0]).

-opaque state_twopset() :: {?TYPE, payload()}.
-type payload() :: {ordsets:ordset(any()), ordsets:ordset(any())}.
-type element() :: term().
-type state_twopset_op() :: {add, element()} |
                            {rmv, element()}.

%% @doc Create a new, empty `state_twopset()'
-spec new() -> state_twopset().
new() ->
    {?TYPE, {ordsets:new(), ordsets:new()}}.

%% @doc Create a new, empty `state_twopset()'
-spec new([term()]) -> state_twopset().
new([]) ->
    new().

-spec mutate(state_twopset_op(), type:id(), state_twopset()) ->
    {ok, state_twopset()}.
mutate(Op, Actor, {?TYPE, _TwoPSet}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_twopset()'.
%%      The first argument can be `{add, element()}' or
%%      `{rmv, element()}'.
-spec delta_mutate(state_twopset_op(), type:id(), state_twopset()) ->
    {ok, state_twopset()}.
delta_mutate({add, Elem}, _Actor, {?TYPE, {Added, _Removed}}) ->
    Delta = minimum_delta(Elem, Added),
    {ok, {?TYPE, {Delta, ordsets:new()}}};

delta_mutate({rmv, Elem}, _Actor, {?TYPE, {_Added, Removed}}) ->
    Delta = minimum_delta(Elem, Removed),
    {ok, {?TYPE, {ordsets:new(), Delta}}}.

%% @doc When trying to add an element to a set
%%      a delta could be a set with only that element.
%%      But if the element is already in the set,
%%      the minimum delta would be the empty set.
minimum_delta(Elem, Set) ->
    case ordsets:is_element(Elem, Set) of
        true ->
            ordsets:new();
        false ->
            ordsets:add_element(Elem, ordsets:new())
    end.

%% @doc Returns the value of the `state_twopset()'.
%%      This value is a set with added elements minus
%%      the removed elements.
-spec query(state_twopset()) -> sets:set(element()).
query({?TYPE, {Added, Removed}}) ->
    sets:from_list(ordsets:subtract(Added, Removed)).

%% @doc Merge two `state_twopset()'.
%%      The result is the component wise set union.
-spec merge(state_twopset(), state_twopset()) -> state_twopset().
merge({?TYPE, {Added1, Removed1}}, {?TYPE, {Added2, Removed2}}) ->
    Added = ordsets:union(Added1, Added2),
    Removed = ordsets:union(Removed1, Removed2),
    {?TYPE, {Added, Removed}}.

%% @doc Equality for `state_twopset()'.
-spec equal(state_twopset(), state_twopset()) -> boolean().
equal({?TYPE, {Added1, Removed1}}, {?TYPE, {Added2, Removed2}}) ->
    ordsets_ext:equal(Added1, Added2) andalso
    ordsets_ext:equal(Removed1, Removed2).

%% @doc Check if a TwoPSet is bottom.
-spec is_bottom(state_twopset()) -> boolean().
is_bottom({?TYPE, {Added, Removed}}) ->
        orddict:is_empty(Added) andalso
        orddict:is_empty(Removed).

%% @doc Given two `state_twopset()', check if the second is an inflation
%%      of the first.
%%      The second `state_twopset()' is an inflation if the first set
%%      with adds is a subset of the second with adds.
%%      Vice versa for the sets with removes.
-spec is_inflation(state_twopset(), state_twopset()) -> boolean().
is_inflation({?TYPE, {Added1, Removed1}}, {?TYPE, {Added2, Removed2}}) ->
    ordsets:is_subset(Added1, Added2) andalso
    ordsets:is_subset(Removed1, Removed2).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_twopset(), state_twopset()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_twopset(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=A, B) ->
    state_type:irreducible_is_strict_inflation(A, B).

-spec digest(state_twopset()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_twopset()'.
-spec join_decomposition(state_twopset()) -> [state_twopset()].
join_decomposition({?TYPE, {Added, Removed}}) ->
    L1 = ordsets:fold(
        fun(Elem, Acc) ->
            [{?TYPE, {[Elem], []}} | Acc]
        end,
        [],
        Added
    ),
    L2 = ordsets:fold(
        fun(Elem, Acc) ->
            [{?TYPE, {[], [Elem]}} | Acc]
        end,
        [],
        Removed
    ),
    lists:append(L1, L2).

%% @doc Delta calculation for `state_twopset()'.
-spec delta(state_twopset(), state_type:digest()) -> state_twopset().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_twopset()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_twopset().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {ordsets:new(), ordsets:new()}}, new()).

query_test() ->
    Set0 = new(),
    Set1 = {?TYPE, {[<<"a">>], []}},
    Set2 = {?TYPE, {[<<"a">>, <<"c">>], [<<"a">>, <<"b">>]}},
    ?assertEqual(sets:new(), query(Set0)),
    ?assertEqual(sets:from_list([<<"a">>]), query(Set1)),
    ?assertEqual(sets:from_list([<<"c">>]), query(Set2)).

delta_add_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate({add, <<"a">>}, Actor, Set0),
    Set1 = merge({?TYPE, Delta1}, Set0),
    {ok, {?TYPE, Delta2}} = delta_mutate({add, <<"a">>}, Actor, Set1),
    Set2 = merge({?TYPE, Delta2}, Set1),
    {ok, {?TYPE, Delta3}} = delta_mutate({add, <<"b">>}, Actor, Set2),
    Set3 = merge({?TYPE, Delta3}, Set2),
    ?assertEqual({?TYPE, {[<<"a">>], []}}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {[<<"a">>], []}}, Set1),
    ?assertEqual({?TYPE, {[], []}}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {[<<"a">>], []}}, Set2),
    ?assertEqual({?TYPE, {[<<"b">>], []}}, {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {[<<"a">>, <<"b">>], []}}, Set3).

add_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"a">>}, Actor, Set0),
    {ok, Set2} = mutate({add, <<"b">>}, Actor, Set1),
    ?assertEqual({?TYPE, {[<<"a">>], []}}, Set1),
    ?assertEqual({?TYPE, {[<<"a">>, <<"b">>], []}}, Set2).

rmv_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"a">>}, Actor, Set0),
    {ok, Set2} = mutate({add, <<"b">>}, Actor, Set1),
    {ok, Set3} = mutate({rmv, <<"b">>}, Actor, Set2),
    ?assertEqual({?TYPE, {[<<"a">>], []}}, Set1),
    ?assertEqual({?TYPE, {[<<"a">>, <<"b">>], []}}, Set2),
    ?assertEqual({?TYPE, {[<<"a">>, <<"b">>], [<<"b">>]}}, Set3).

merge_deltas_test() ->
    Set1 = {?TYPE, {[<<"a">>], []}},
    Delta1 = {?TYPE, {[], [<<"a">>, <<"b">>]}},
    Delta2 = {?TYPE, {[<<"c">>], []}},
    Set2 = merge(Delta1, Set1),
    Set3 = merge(Set1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, {[<<"a">>], [<<"a">>, <<"b">>]}}, Set2),
    ?assertEqual({?TYPE, {[<<"a">>], [<<"a">>, <<"b">>]}}, Set3),
    ?assertEqual({?TYPE, {[<<"c">>], [<<"a">>, <<"b">>]}}, DeltaGroup).

equal_test() ->
    Set1 = {?TYPE, {[<<"a">>], []}},
    Set2 = {?TYPE, {[<<"a">>], [<<"a">>]}},
    ?assert(equal(Set1, Set1)),
    ?assert(equal(Set2, Set2)),
    ?assertNot(equal(Set1, Set2)).

is_bottom_test() ->
    Set0 = new(),
    Set1 = {?TYPE, {[<<"a">>], []}},
    ?assert(is_bottom(Set0)),
    ?assertNot(is_bottom(Set1)).

is_inflation_test() ->
    Set1 = {?TYPE, {[<<"a">>], []}},
    Set2 = {?TYPE, {[<<"a">>], [<<"b">>]}},
    Set3 = {?TYPE, {[<<"a">>, <<"b">>], []}},
    ?assert(is_inflation(Set1, Set1)),
    ?assert(is_inflation(Set1, Set2)),
    ?assertNot(is_inflation(Set2, Set1)),
    ?assert(is_inflation(Set1, Set3)),
    ?assertNot(is_inflation(Set2, Set3)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Set1, Set1)),
    ?assert(state_type:is_inflation(Set1, Set2)),
    ?assertNot(state_type:is_inflation(Set2, Set1)),
    ?assert(state_type:is_inflation(Set1, Set3)),
    ?assertNot(state_type:is_inflation(Set2, Set3)).

is_strict_inflation_test() ->
    Set1 = {?TYPE, {[<<"a">>], []}},
    Set2 = {?TYPE, {[<<"a">>], [<<"b">>]}},
    Set3 = {?TYPE, {[<<"a">>, <<"b">>], []}},
    ?assertNot(is_strict_inflation(Set1, Set1)),
    ?assert(is_strict_inflation(Set1, Set2)),
    ?assertNot(is_strict_inflation(Set2, Set1)),
    ?assert(is_strict_inflation(Set1, Set3)),
    ?assertNot(is_strict_inflation(Set2, Set3)).

join_decomposition_test() ->
    Set1 = {?TYPE, {[<<"a">>, <<"b">>], [<<"b">>, <<"c">>]}},
    Decomp1 = join_decomposition(Set1),
    List = [{?TYPE, {[<<"a">>], []}},
            {?TYPE, {[<<"b">>], []}},
            {?TYPE, {[], [<<"b">>]}},
            {?TYPE, {[], [<<"c">>]}}],
    ?assertEqual(lists:sort(List), lists:sort(Decomp1)).

encode_decode_test() ->
    Set = {?TYPE, {[<<"a">>, <<"b">>], [<<"b">>, <<"c">>]}},
    Binary = encode(erlang, Set),
    ESet = decode(erlang, Binary),
    ?assertEqual(Set, ESet).

-endif.
