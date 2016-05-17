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

%% @doc ORSet CRDT: observed-remove set with tombstones
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011)
%%      A comprehensive study of Convergent and Commutative
%%      Replicated Data Types.
%%      [http://hal.upmc.fr/inria-00555588/]

-module(orset).
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

-export_type([orset/0, delta_orset/0, orset_op/0]).

-opaque orset() :: {?TYPE, payload()}.
-opaque delta_orset() :: {?TYPE, {delta, payload()}}.
-type payload() :: orddict:set().
-type element() :: term().
-type token() :: term().
-type orset_op() :: {add, element()} |
                    {add_by_token, token(), element()} |
                    {add_all, [element()]} |
                    {rmv, element()} |
                    {rmv_all, [element()]}.

%% @doc Create a new, empty `orset()'
-spec new() -> orset().
new() ->
    {?TYPE, orddict:new()}.

%% @doc Create a new, empty `orset()'
-spec new([term()]) -> orset().
new([]) ->
    new().

%% @doc Mutate a `orset()'.
-spec mutate(orset_op(), type:actor(), orset()) ->
    {ok, orset()} | {error, {precondition, {not_present, [element()]}}}.
mutate(Op, Actor, {?TYPE, _ORSet}=CRDT) ->
    type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `orset()'.
%%      The first argument can be:
%%          - `{add, element()}'
%%          - `{add_by_token, token(), element()}'
%%          - `{rmv, element()}'
%%      The second argument is the replica id.
%%      The third argument is the `orset()' to be inflated.
-spec delta_mutate(orset_op(), type:actor(), orset()) ->
    {ok, delta_orset()} | {error, {precondition, {not_present, element()}}}.
delta_mutate({add, Elem}, Actor, {?TYPE, _}=ORSet) ->
    Token = unique(Actor),
    delta_mutate({add_by_token, Token, Elem}, Actor, ORSet);

%% @doc Returns a new `orset()' with only one element in
%%      the dictionary. This element maps to a dictionary with
%%      only one token tagged as true (token active)
delta_mutate({add_by_token, Token, Elem}, _Actor, {?TYPE, _ORSet}) ->
    Tokens = orddict:store(Token, true, orddict:new()),
    Delta = orddict:store(Elem, Tokens, orddict:new()),
    {ok, {?TYPE, {delta, Delta}}};

%% @doc Returns a new `orset()' with the elements passed as argument
%%      as keys in the dictionary.
delta_mutate({add_all, Elems}, Actor, {?TYPE, _ORSet}) ->
    Delta = lists:foldl(
        fun(Elem, Acc) ->
            Token = unique(Actor),
            Tokens = orddict:store(Token, true, orddict:new()),
            orddict:store(Elem, Tokens, Acc)
        end,
        orddict:new(),
        Elems
    ),
    {ok, {?TYPE, {delta, Delta}}};

%% @doc Returns a new `orset()' with only one element in
%%      the dictionary mapping all current tokens to false (inactive).
%%      If the element is not in the dictionary a precondition
%%      (observed-remove) error is returned.
delta_mutate({rmv, Elem}, _Actor, {?TYPE, ORSet}) ->
    case orddict:find(Elem, ORSet) of
        {ok, Tokens} ->
            InactiveTokens = [{Token, false} || {Token, _Active} <- orddict:to_list(Tokens)],
            Delta = orddict:store(Elem, InactiveTokens, orddict:new()),
            {ok, {?TYPE, {delta, Delta}}};
        error ->
            {error, {precondition, {not_present, [Elem]}}}
    end;

%% @doc Removes a list of elemenets passed as input.
delta_mutate({rmv_all, Elems}, Actor, {?TYPE, _}=ORSet) ->
    {{?TYPE, DeltaGroup}, NotRemoved} = lists:foldl(
        fun(Elem, {DeltaGroupAcc, NotRemovedAcc}) ->
            case delta_mutate({rmv, Elem}, Actor, ORSet) of
                {ok, {?TYPE, {delta, Delta}}} ->
                    {merge({?TYPE, Delta}, DeltaGroupAcc), NotRemovedAcc};
                {error, {precondition, {not_present, [ElemNotRemoved]}}} ->
                    {DeltaGroupAcc, [ElemNotRemoved | NotRemovedAcc]}
            end
        end,
        {new(), []},
        Elems
    ),
    case NotRemoved of
        [] ->
            {ok, {?TYPE, {delta, DeltaGroup}}};
        _ ->
            {error, {precondition, {not_present, NotRemoved}}}
    end.

%% @doc Returns the value of the `orset()'.
%%      This value is a list with all the elements in the `orset()'
%%      that have at least one token still marked as active (true).
-spec query(orset()) -> [element()].
query({?TYPE, ORSet}) ->
    lists:reverse(orddict:fold(
        fun(Elem, Tokens, Acc) ->
            ActiveTokens = [Token || {Token, true} <- orddict:to_list(Tokens)],
            case length(ActiveTokens) > 0 of
                true ->
                    [Elem | Acc];
                false ->
                    Acc
            end
        end,
        [],
        ORSet
     )).

%% @doc Merge two `orset()'.
%%      The keys (elements) of the resulting `orset()' are the union
%%      of keys of both `orset()' passed as input.
%%      When one of the elements is present in both `orset()',
%%      the respective tokens are merged respecting the following rule:
%%          - if a token is only present in on of the `orset()',
%%          its value is preserved
%%          - if a token is present in both `orset()' its value will be:
%%              * active (true) if both were active before
%%              * inactive (false) otherwise
-spec merge(orset(), orset()) -> orset().
merge({?TYPE, ORSet1}, {?TYPE, ORSet2}) ->
    ORSet = orddict:merge(
        fun(_Elem, Tokens1, Tokens2) ->
            orddict:merge(
                fun(_Token, Active1, Active2) ->
                    Active1 andalso Active2
                end,
                Tokens1,
                Tokens2
             )
        end,
        ORSet1,
        ORSet2
    ),
    {?TYPE, ORSet}.

%% @doc Equality for `orset()'.
%%      Since everything is ordered, == should work.
-spec equal(orset(), orset()) -> boolean().
equal({?TYPE, ORSet1}, {?TYPE, ORSet2}) ->
    ORSet1 == ORSet2.

%% @doc Given two `orset()', check if the second is and inflation
%%      of the first.
%%      The second is an inflation if, at least, has all the elements
%%      of the first.
%%      Also, for each element, we have an inflation if all the tokens
%%      present in the first `orset()' are present in the second and
%%      the value (activeness) is:
%%          - active on both
%%          - inactive on both
%%          - first active and second inactive
-spec is_inflation(orset(), orset()) -> boolean().
is_inflation({?TYPE, ORSet1}, {?TYPE, ORSet2}) ->
    orddict:fold(
        fun(Elem, Tokens1, Acc) ->
            case orddict:find(Elem, ORSet2) of
                %% if element is found, compare tokens
                {ok, Tokens2} ->
                    orddict:fold(
                        fun(Token, Active1, AccTokens) ->
                            case orddict:find(Token, Tokens2) of
                                %% if token is found, compare activeness
                                {ok, Active2} ->
                                    %% (both active or both inactive)
                                    %% orelse (first active and second inactive)
                                    Inflation = (Active1  == Active2)
                                         orelse (Active1 andalso (not Active2)),
                                    Inflation andalso AccTokens;

                                %% if not found, not an inflation
                                error ->
                                    AccTokens andalso false
                            end
                        end,
                        true,
                        Tokens1
                    ) andalso Acc;

                %% if not found, not an inflation
                error ->
                    Acc andalso false
            end
        end,
        true,
        ORSet1
    ).

%% @doc Check for strict inflation.
-spec is_strict_inflation(orset(), orset()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Join decomposition for `orset()'.
-spec join_decomposition(orset()) -> [orset()].
join_decomposition({?TYPE, ORSet}) ->
    orddict:fold(
        fun(Elem, Tokens, Acc) ->
            Decomp = [{?TYPE, [{Elem, orddict:store(Token, Active, orddict:new())}]} || {Token, Active} <- orddict:to_list(Tokens)],
            lists:append(Decomp, Acc)
        end,
        [],
        ORSet
     ).

%% private
unique(_Actor) ->
    crypto:strong_rand_bytes(20).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, orddict:new()}, new()).

query_test() ->
    Set0 = new(),
    Set1 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}, {<<"b">>, [{<<"token2">>, false}]}]},
    ?assertEqual([], query(Set0)),
    ?assertEqual([<<"a">>], query(Set1)).

delta_add_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate({add_by_token, <<"token1">>, <<"a">>}, Actor, Set0),
    Set1 = merge({?TYPE, Delta1}, Set0),
    {ok, {?TYPE, {delta, Delta2}}} = delta_mutate({add_by_token, <<"token2">>, <<"a">>}, Actor, Set1),
    Set2 = merge({?TYPE, Delta2}, Set1),
    {ok, {?TYPE, {delta, Delta3}}} = delta_mutate({add_by_token, <<"token3">>, <<"b">>}, Actor, Set2),
    Set3 = merge({?TYPE, Delta3}, Set2),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token1">>, true}]}]}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token1">>, true}]}]}, Set1),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token2">>, true}]}]}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token1">>, true}, {<<"token2">>, true}]}]}, Set2),
    ?assertEqual({?TYPE, [{<<"b">>, [{<<"token3">>, true}]}]}, {?TYPE, Delta3}),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token1">>, true}, {<<"token2">>, true}]}, {<<"b">>, [{<<"token3">>, true}]}]}, Set3).

add_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add_by_token, <<"token1">>, <<"a">>}, Actor, Set0),
    {ok, Set2} = mutate({add_by_token, <<"token2">>, <<"a">>}, Actor, Set1),
    {ok, Set3} = mutate({add_by_token, <<"token3">>, <<"b">>}, Actor, Set2),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token1">>, true}]}]}, Set1),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token1">>, true}, {<<"token2">>, true}]}]}, Set2),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token1">>, true}, {<<"token2">>, true}]}, {<<"b">>, [{<<"token3">>, true}]}]}, Set3).

rmv_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"a">>}, Actor, Set0),
    {error, _} = mutate({rmv, <<"b">>}, Actor, Set1),
    {ok, Set2} = mutate({rmv, <<"a">>}, Actor, Set1),
    ?assertEqual([], query(Set2)).

add_all_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add_all, []}, Actor, Set0),
    {ok, Set2} = mutate({add_all, [<<"a">>, <<"b">>]}, Actor, Set0),
    {ok, Set3} = mutate({add_all, [<<"b">>, <<"c">>]}, Actor, Set2),
    ?assertEqual([], query(Set1)),
    ?assertEqual(lists:sort([<<"a">>, <<"b">>]), lists:sort(query(Set2))),
    ?assertEqual(lists:sort([<<"a">>, <<"b">>, <<"c">>]), lists:sort(query(Set3))).

remove_all_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add_all, [<<"a">>, <<"b">>, <<"c">>]}, Actor, Set0),
    {ok, Set2} = mutate({rmv_all, [<<"a">>, <<"c">>]}, Actor, Set1),
    {error, _} = mutate({rmv_all, [<<"b">>, <<"d">>]}, Actor, Set2),
    {ok, Set3} = mutate({rmv_all, [<<"b">>, <<"a">>]}, Actor, Set2),
    ?assertEqual([<<"b">>], query(Set2)),
    ?assertEqual([], query(Set3)).

merge_idempontent_test() ->
    Set1 = {?TYPE, [{<<"a">>, [{<<"token1">>, false}]}]},
    Set2 = {?TYPE, [{<<"b">>, [{<<"token2">>, true}]}]},
    Set3 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}, {<<"b">>, [{<<"token2">>, false}]}]},
    Set4 = merge(Set1, Set1),
    Set5 = merge(Set2, Set2),
    Set6 = merge(Set3, Set3),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token1">>, false}]}]}, Set4),
    ?assertEqual({?TYPE, [{<<"b">>, [{<<"token2">>, true}]}]}, Set5),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token1">>, true}]}, {<<"b">>, [{<<"token2">>, false}]}]}, Set6).

merge_commutative_test() ->
    Set1 = {?TYPE, [{<<"a">>, [{<<"token1">>, false}]}]},
    Set2 = {?TYPE, [{<<"b">>, [{<<"token2">>, true}]}]},
    Set3 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}, {<<"b">>, [{<<"token2">>, false}]}]},
    Set4 = merge(Set1, Set2),
    Set5 = merge(Set2, Set1),
    Set6 = merge(Set1, Set3),
    Set7 = merge(Set3, Set1),
    Set8 = merge(Set2, Set3),
    Set9 = merge(Set3, Set2),
    Set10 = merge(Set1, merge(Set2, Set3)),
    Set1_2 = {?TYPE, [{<<"a">>, [{<<"token1">>, false}]}, {<<"b">>, [{<<"token2">>, true}]}]},
    Set1_3 = {?TYPE, [{<<"a">>, [{<<"token1">>, false}]}, {<<"b">>, [{<<"token2">>, false}]}]},
    Set2_3 = Set3,
    ?assertEqual(Set1_2, Set4),
    ?assertEqual(Set1_2, Set5),
    ?assertEqual(Set1_3, Set6),
    ?assertEqual(Set1_3, Set7),
    ?assertEqual(Set2_3, Set8),
    ?assertEqual(Set2_3, Set9),
    ?assertEqual(Set1_3, Set10).

equal_test() ->
    Set1 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}]},
    Set2 = {?TYPE, [{<<"a">>, [{<<"token1">>, false}]}]},
    Set3 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}, {<<"b">>, [{<<"token2">>, false}]}]},
    ?assert(equal(Set1, Set1)),
    ?assert(equal(Set2, Set2)),
    ?assert(equal(Set3, Set3)),
    ?assertNot(equal(Set1, Set2)),
    ?assertNot(equal(Set1, Set3)),
    ?assertNot(equal(Set2, Set3)).

is_inflation_test() ->
    Set1 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}]},
    Set2 = {?TYPE, [{<<"a">>, [{<<"token1">>, false}]}]},
    Set3 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}, {<<"b">>, [{<<"token2">>, false}]}]},
    ?assert(is_inflation(Set1, Set1)),
    ?assert(is_inflation(Set1, Set2)),
    ?assertNot(is_inflation(Set2, Set1)),
    ?assert(is_inflation(Set1, Set3)),
    ?assertNot(is_inflation(Set2, Set3)),
    ?assertNot(is_inflation(Set3, Set2)),
    %% check inflation with merge
    ?assert(type:is_inflation(Set1, Set1)),
    ?assert(type:is_inflation(Set1, Set2)),
    ?assertNot(type:is_inflation(Set2, Set1)),
    ?assert(type:is_inflation(Set1, Set3)),
    ?assertNot(type:is_inflation(Set2, Set3)),
    ?assertNot(type:is_inflation(Set3, Set2)).

is_strict_inflation_test() ->
    Set1 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}]},
    Set2 = {?TYPE, [{<<"a">>, [{<<"token1">>, false}]}]},
    Set3 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}, {<<"b">>, [{<<"token2">>, false}]}]},
    ?assertNot(is_strict_inflation(Set1, Set1)),
    ?assert(is_strict_inflation(Set1, Set2)),
    ?assertNot(is_strict_inflation(Set2, Set1)),
    ?assert(is_strict_inflation(Set1, Set3)),
    ?assertNot(is_strict_inflation(Set2, Set3)),
    ?assertNot(is_strict_inflation(Set3, Set2)).

join_decomposition_test() ->
    Set1 = {?TYPE, [{<<"a">>, [{<<"token1">>, false}]}]},
    Set2 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}, {<<"token3">>, false}]}, {<<"b">>, [{<<"token2">>, false}]}]},
    Decomp1 = join_decomposition(Set1),
    Decomp2 = join_decomposition(Set2),
    List = [{?TYPE, [{<<"a">>, [{<<"token1">>, true}]}]},
            {?TYPE, [{<<"a">>, [{<<"token3">>, false}]}]},
            {?TYPE, [{<<"b">>, [{<<"token2">>, false}]}]}],
    ?assertEqual([Set1], Decomp1),
    ?assertEqual(lists:sort(List), lists:sort(Decomp2)).

-endif.
