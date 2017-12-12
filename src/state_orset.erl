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

-module(state_orset).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-behaviour(type).
-behaviour(state_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_bottom/1,
         is_inflation/2, is_strict_inflation/2,
         irreducible_is_strict_inflation/2]).
-export([join_decomposition/1, delta/2, digest/1]).
-export([encode/2, decode/2]).

-export_type([state_orset/0, state_orset_op/0]).

-opaque state_orset() :: {?TYPE, payload()}.
-type payload() :: orddict:orddict().
-type element() :: term().
-type token() :: term().
-type state_orset_op() :: {add, element()} |
                          {add_by_token, token(), element()} |
                          {add_all, [element()]} |
                          {rmv, element()} |
                          {rmv_all, [element()]}.

%% @doc Create a new, empty `state_orset()'
-spec new() -> state_orset().
new() ->
    {?TYPE, orddict:new()}.

%% @doc Create a new, empty `state_orset()'
-spec new([term()]) -> state_orset().
new([]) ->
    new().

%% @doc Mutate a `state_orset()'.
-spec mutate(state_orset_op(), type:id(), state_orset()) ->
    {ok, state_orset()}.
mutate(Op, Actor, {?TYPE, _ORSet}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_orset()'.
%%      The first argument can be:
%%          - `{add, element()}'
%%          - `{add_by_token, token(), element()}'
%%          - `{rmv, element()}'
%%      The second argument is the replica id.
%%      The third argument is the `state_orset()' to be inflated.
-spec delta_mutate(state_orset_op(), type:id(), state_orset()) ->
    {ok, state_orset()}.
delta_mutate({add, Elem}, Actor, {?TYPE, _}=ORSet) ->
    Token = unique(Actor),
    delta_mutate({add_by_token, Token, Elem}, Actor, ORSet);

%% @doc Returns a new `state_orset()' with only one element in
%%      the dictionary. This element maps to a dictionary with
%%      only one token tagged as true (token active)
delta_mutate({add_by_token, Token, Elem}, _Actor, {?TYPE, _ORSet}) ->
    Tokens = orddict:store(Token, true, orddict:new()),
    Delta = orddict:store(Elem, Tokens, orddict:new()),
    {ok, {?TYPE, Delta}};

%% @doc Returns a new `state_orset()' with the elements passed as argument
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
    {ok, {?TYPE, Delta}};

%% @doc Returns a new `state_orset()' with only one element in
%%      the dictionary mapping all current tokens to false (inactive).
delta_mutate({rmv, Elem}, _Actor, {?TYPE, ORSet}) ->
    Delta = case orddict:find(Elem, ORSet) of
        {ok, Tokens} ->
            InactiveTokens = [{Token, false} || {Token, _Active} <- orddict:to_list(Tokens)],
            orddict:store(Elem, InactiveTokens, orddict:new());
        error ->
            orddict:new()
    end,

    {ok, {?TYPE, Delta}};

%% @doc Removes a list of elements passed as input.
delta_mutate({rmv_all, Elems}, Actor, {?TYPE, _}=ORSet) ->
    {?TYPE, DeltaGroup} = lists:foldl(
        fun(Elem, DeltaGroupAcc) ->
            case delta_mutate({rmv, Elem}, Actor, ORSet) of
                {ok, {?TYPE, Delta}} ->
                    merge({?TYPE, Delta}, DeltaGroupAcc)
            end
        end,
        new(),
        Elems
    ),
    {ok, {?TYPE, DeltaGroup}}.

%% @doc Returns the value of the `state_orset()'.
%%      This value is a set with all the elements in the `state_orset()'
%%      that have at least one token still marked as active (true).
-spec query(state_orset()) -> sets:set(element()).
query({?TYPE, ORSet}) ->
    orddict:fold(
        fun(Elem, Tokens, Acc) ->
            ActiveTokens = [Token || {Token, true} <- orddict:to_list(Tokens)],
            case length(ActiveTokens) > 0 of
                true ->
                    sets:add_element(Elem, Acc);
                false ->
                    Acc
            end
        end,
        sets:new(),
        ORSet
     ).

%% @doc Merge two `state_orset()'.
%%      The keys (elements) of the resulting `state_orset()' are the union
%%      of keys of both `state_orset()' passed as input.
%%      When one of the elements is present in both `state_orset()',
%%      the respective tokens are merged respecting the following rule:
%%          - if a token is only present in on of the `state_orset()',
%%          its value is preserved
%%          - if a token is present in both `state_orset()' its value will be:
%%              * active (true) if both were active before
%%              * inactive (false) otherwise
-spec merge(state_orset(), state_orset()) -> state_orset().
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

%% @doc Equality for `state_orset()'.
%%      Since everything is ordered, == should work.
-spec equal(state_orset(), state_orset()) -> boolean().
equal({?TYPE, ORSet1}, {?TYPE, ORSet2}) ->
    ORSet1 == ORSet2.

%% @doc Check if an ORSet is bottom.
-spec is_bottom(state_orset()) -> boolean().
is_bottom({?TYPE, ORSet}) ->
    orddict:is_empty(ORSet).

%% @doc Given two `state_orset()', check if the second is and inflation
%%      of the first.
%%      The second is an inflation if, at least, has all the elements
%%      of the first.
%%      Also, for each element, we have an inflation if all the tokens
%%      present in the first `state_orset()' are present in the second and
%%      the value (activeness) is:
%%          - active on both
%%          - inactive on both
%%          - first active and second inactive
-spec is_inflation(state_orset(), state_orset()) -> boolean().
is_inflation({?TYPE, ORSet1}, {?TYPE, ORSet2}) ->
    lists_ext:iterate_until(
        fun({Elem, Tokens1}) ->
            case orddict:find(Elem, ORSet2) of
                %% if element is found, compare tokens
                {ok, Tokens2} ->
                    lists_ext:iterate_until(
                        fun({Token, Active1}) ->
                            case orddict:find(Token, Tokens2) of
                                %% if token is found, compare activeness
                                {ok, Active2} ->
                                    %% (both active or both inactive)
                                    %% orelse (first active and second inactive)
                                    (Active1  == Active2)
                                    orelse (Active1 andalso (not Active2));

                                %% if not found, not an inflation
                                error ->
                                    false
                            end
                        end,
                        Tokens1
                    );

                %% if not found, not an inflation
                error ->
                    false
            end
        end,
        ORSet1
    );

%% @todo get back here later
is_inflation({cardinality, Value}, {?TYPE, _}=CRDT) ->
    sets:size(query(CRDT)) >= Value.

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_orset(), state_orset()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2);

%% @todo get back here later
is_strict_inflation({cardinality, Value}, {?TYPE, _}=CRDT) ->
    sets:size(query(CRDT)) > Value.

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_orset(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, [{Elem, [{Token, Active}]}]},
                                {state, {?TYPE, ORSet}}) ->
    case orddict:find(Elem, ORSet) of
        {ok, Tokens} ->
            case orddict:find(Token, Tokens) of
                {ok, IsActive} ->
                    %% It will inflate if the token is active in
                    %% the current state (`true')
                    %% and the irreducible state has the token
                    %% inactive (`false')
                    IsActive andalso not Active;
                error ->
                    %% If the token is not there, it will inflate
                    true
            end;
        error ->
            %% If the element is not there, it will inflate
            true
    end.

-spec digest(state_orset()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_orset()'.
-spec join_decomposition(state_orset()) -> [state_orset()].
join_decomposition({?TYPE, ORSet}) ->
    orddict:fold(
        fun(Elem, Tokens, Acc) ->
            Decomp = [{?TYPE, [{Elem, orddict:store(Token, Active, orddict:new())}]} || {Token, Active} <- orddict:to_list(Tokens)],
            lists:append(Decomp, Acc)
        end,
        [],
        ORSet
     ).

%% @doc Delta calculation for `state_orset()'.
-spec delta(state_orset(), state_type:digest()) -> state_orset().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_orset()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_orset().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.

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
    ?assertEqual(sets:new(), query(Set0)),
    ?assertEqual(sets:from_list([<<"a">>]), query(Set1)).

delta_add_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate({add_by_token, <<"token1">>, <<"a">>}, Actor, Set0),
    Set1 = merge({?TYPE, Delta1}, Set0),
    {ok, {?TYPE, Delta2}} = delta_mutate({add_by_token, <<"token2">>, <<"a">>}, Actor, Set1),
    Set2 = merge({?TYPE, Delta2}, Set1),
    {ok, {?TYPE, Delta3}} = delta_mutate({add_by_token, <<"token3">>, <<"b">>}, Actor, Set2),
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
    {ok, Set1} = mutate({rmv, <<"b">>}, Actor, Set1),
    {ok, Set2} = mutate({rmv, <<"a">>}, Actor, Set1),
    ?assertEqual(sets:new(), query(Set2)).

add_all_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add_all, []}, Actor, Set0),
    {ok, Set2} = mutate({add_all, [<<"a">>, <<"b">>]}, Actor, Set0),
    {ok, Set3} = mutate({add_all, [<<"b">>, <<"c">>]}, Actor, Set2),
    ?assertEqual(sets:new(), query(Set1)),
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>]), query(Set2)),
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>, <<"c">>]), query(Set3)).

remove_all_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add_all, [<<"a">>, <<"b">>, <<"c">>]}, Actor, Set0),
    {ok, Set2} = mutate({rmv_all, [<<"a">>, <<"c">>]}, Actor, Set1),
    {ok, Set3} = mutate({rmv_all, [<<"b">>, <<"d">>]}, Actor, Set2),
    {ok, Set4} = mutate({rmv_all, [<<"b">>, <<"a">>]}, Actor, Set2),
    ?assertEqual(sets:from_list([<<"b">>]), query(Set2)),
    ?assertEqual(sets:new(), query(Set3)),
    ?assertEqual(sets:new(), query(Set4)).

merge_idempotent_test() ->
    Set1 = {?TYPE, [{<<"a">>, [{<<"token1">>, false}]}]},
    Set2 = {?TYPE, [{<<"b">>, [{<<"token2">>, true}]}]},
    Set3 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}, {<<"b">>, [{<<"token2">>, false}]}]},
    Set4 = merge(Set1, Set1),
    Set5 = merge(Set2, Set2),
    Set6 = merge(Set3, Set3),
    ?assertEqual(Set1, Set4),
    ?assertEqual(Set2, Set5),
    ?assertEqual(Set3, Set6).

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

merge_delta_test() ->
    Set1 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}]},
    Delta1 = {?TYPE, [{<<"a">>, [{<<"token1">>, false}]}]},
    Delta2 = {?TYPE, [{<<"b">>, [{<<"token2">>, true}]}]},
    Set2 = merge(Delta1, Set1),
    Set3 = merge(Set1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token1">>, false}]}]}, Set2),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token1">>, false}]}]}, Set3),
    ?assertEqual({?TYPE, [{<<"a">>, [{<<"token1">>, false}]}, {<<"b">>, [{<<"token2">>, true}]}]}, DeltaGroup).

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

is_bottom_test() ->
    Set0 = new(),
    Set1 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}]},
    ?assert(is_bottom(Set0)),
    ?assertNot(is_bottom(Set1)).

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
    ?assert(state_type:is_inflation(Set1, Set1)),
    ?assert(state_type:is_inflation(Set1, Set2)),
    ?assertNot(state_type:is_inflation(Set2, Set1)),
    ?assert(state_type:is_inflation(Set1, Set3)),
    ?assertNot(state_type:is_inflation(Set2, Set3)),
    ?assertNot(state_type:is_inflation(Set3, Set2)).

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

irreducible_is_strict_inflation_test() ->
    Set1 = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}, {<<"b">>, [{<<"token2">>, false}]}]},
    Digest = digest(Set1),
    Irreducible1 = {?TYPE, [{<<"a">>, [{<<"token1">>, false}]}]},
    Irreducible2 = {?TYPE, [{<<"b">>, [{<<"token2">>, false}]}]},
    Irreducible3 = {?TYPE, [{<<"b">>, [{<<"token3">>, false}]}]},
    Irreducible4 = {?TYPE, [{<<"c">>, [{<<"token4">>, true}]}]},
    ?assert(irreducible_is_strict_inflation(Irreducible1, Digest)),
    ?assertNot(irreducible_is_strict_inflation(Irreducible2, Digest)),
    ?assert(irreducible_is_strict_inflation(Irreducible3, Digest)),
    ?assert(irreducible_is_strict_inflation(Irreducible4, Digest)).

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

encode_decode_test() ->
    Set = {?TYPE, [{<<"a">>, [{<<"token1">>, true}]}, {<<"b">>, [{<<"token2">>, false}]}]},
    Binary = encode(erlang, Set),
    ESet = decode(erlang, Binary),
    ?assertEqual(Set, ESet).

-endif.
