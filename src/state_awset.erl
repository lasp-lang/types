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

%% @doc Add-Wins ORSet CRDT: observed-remove set without tombstones.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(state_awset).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

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

-export_type([state_awset/0, state_awset_op/0]).

-opaque state_awset() :: {?TYPE, payload()}.
-type payload() :: state_causal_type:causal_crdt().
-type element() :: term().
-type state_awset_op() :: {add, element()} |
                          {add_all, [element()]} |
                          {rmv, element()} |
                          {rmv_all, [element()]} |
                          {filter, function()}.

%% @doc Create a new, empty `state_awset()'
%%      DotMap<Elem, DotSet>
-spec new() -> state_awset().
new() ->
    {?TYPE, state_causal_type:new(dot_map)}.

%% @doc Create a new, empty `state_awset()'
-spec new([term()]) -> state_awset().
new([]) ->
    new().

%% @doc Mutate a `state_awset()'.
-spec mutate(state_awset_op(), type:id(), state_awset()) ->
    {ok, state_awset()}.
mutate(Op, Actor, {?TYPE, _AWSet}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_awset()'.
%%      The first argument can be:
%%          - `{add, element()}'
%%          - `{rmv, element()}'
%%          - `{add_all, [element()]}'
%%          - `{rmv_all, [element()]}'
%%      The second argument is the replica id.
%%      The third argument is the `state_awset()' to be inflated.
-spec delta_mutate(state_awset_op(), type:id(), state_awset()) ->
    {ok, state_awset()}.

%% @doc Adds a single element to `state_awset()'.
delta_mutate({add, Elem}, Actor, {?TYPE, {DotStore, CausalContext}}) ->
    NextDot = causal_context:next_dot(Actor, CausalContext),

    DeltaDotSet = dot_set:add_dot(NextDot, dot_set:new()),
    DeltaDotStore = dot_map:store(Elem, DeltaDotSet, dot_map:new()),

    CurrentDotSet = dot_map:fetch(Elem, DotStore, dot_set:new()),
    DeltaCausalContext0 = causal_context:from_dot_set(CurrentDotSet),
    DeltaCausalContext1 = causal_context:add_dot(NextDot,
                                                 DeltaCausalContext0),

    Delta = {DeltaDotStore, DeltaCausalContext1},
    {ok, {?TYPE, Delta}};

%% @doc Adds a list of elements to `state_awset()'.
delta_mutate({add_all, Elems}, Actor, {?TYPE, _}=AWSet) ->
    {_, {?TYPE, DeltaGroup}} = lists:foldl(
        fun(Elem, {AWSet0, DeltaGroup0}) ->
            {ok, Delta} = delta_mutate({add, Elem}, Actor, AWSet0),
            AWSet1 = merge(Delta, AWSet0),
            DeltaGroup1 = merge(Delta, DeltaGroup0),
            {AWSet1, DeltaGroup1}
        end,
        {AWSet, new()},
        Elems
    ),

    {ok, {?TYPE, DeltaGroup}};

%% @doc Removes a single element in `state_awset()'.
delta_mutate({rmv, Elem}, _Actor, {?TYPE, {DotStore, _CausalContext}}) ->
    CurrentDotSet = dot_map:fetch(Elem, DotStore, dot_set:new()),
    DeltaDotStore = dot_map:new(),
    DeltaCausalContext = causal_context:from_dot_set(CurrentDotSet),

    Delta = {DeltaDotStore, DeltaCausalContext},
    {ok, {?TYPE, Delta}};

%% @doc Removes a list of elements in `state_awset()'.
delta_mutate({rmv_all, Elems}, Actor, {?TYPE, _}=AWSet) ->
    {_, {?TYPE, DeltaGroup}} = lists:foldl(
        fun(Elem, {AWSet0, DeltaGroup0}) ->
            case delta_mutate({rmv, Elem}, Actor, AWSet0) of
                {ok, Delta} ->
                    AWSet1 = merge(Delta, AWSet0),
                    DeltaGroup1 = merge(Delta, DeltaGroup0),
                    {AWSet1, DeltaGroup1}
            end
        end,
        {AWSet, new()},
        Elems
    ),
    {ok, {?TYPE, DeltaGroup}};

%% @doc The first argument is:
%%        - {filter, Pred}
%%      This mutator removes all elements `e' s.t. `not Pred(e)'.
delta_mutate({filter, Pred}, Actor, {?TYPE, _}=CRDT) ->
    NotPred = fun(E) -> not Pred(E) end,
    Elems = sets:filter(NotPred, query(CRDT)),
    delta_mutate({rmv_all, sets:to_list(Elems)}, Actor, CRDT).

%% @doc Returns the value of the `state_awset()'.
%%      This value is a set with all the keys (elements) in the dot map.
-spec query(state_awset()) -> sets:set(element()).
query({?TYPE, {DotStore, _CausalContext}}) ->
    Elements = dot_map:fetch_keys(DotStore),
    sets:from_list(Elements).

%% @doc Merge two `state_awset()'.
%%      Merging is handled by the `merge' function in
%%      `state_causal_type' common library.
-spec merge(state_awset(), state_awset()) -> state_awset().
merge({?TYPE, AWSet1}, {?TYPE, AWSet2}) ->
    AWSet = state_causal_type:merge({dot_map, dot_set},
                                    AWSet1,
                                    AWSet2),
    {?TYPE, AWSet}.

%% @doc Equality for `state_awset()'.
%%      Since everything is ordered, == should work.
-spec equal(state_awset(), state_awset()) -> boolean().
equal({?TYPE, AWSet1}, {?TYPE, AWSet2}) ->
    AWSet1 == AWSet2.

%% @doc Check if an `state_awset()' is bottom.
-spec is_bottom(state_awset()) -> boolean().
is_bottom({?TYPE, _}=CRDT) ->
    CRDT == new().

%% @doc Given two `state_awset()', check if the second is and inflation of the first.
%% @todo
-spec is_inflation(state_awset(), state_awset()) -> boolean().
is_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_inflation(CRDT1, CRDT2);

%% @todo get back here later
is_inflation({cardinality, Value}, {?TYPE, _}=CRDT) ->
    sets:size(query(CRDT)) >= Value.

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_awset(), state_awset()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2);

%% @todo get back here later
is_strict_inflation({cardinality, Value}, {?TYPE, _}=CRDT) ->
    sets:size(query(CRDT)) > Value.

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_awset(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, {DSA, CCA}},
                                {state, {?TYPE, {DSB, CCB}}}) ->
    [Dot] = dot_set:to_list(causal_context:dots(CCA)),
    %% will inflate if the dot does not belong to the other cc
    not causal_context:is_element(Dot, CCB) orelse
    %% or if it was a not observed removal
    (dot_map:is_empty(DSA) andalso
     state_causal_type:is_element({dot_map, dot_set}, Dot, DSB));
irreducible_is_strict_inflation({?TYPE, {DSA, CCA}},
                                {mdata, {ActiveDots, CCB}}) ->
    [Dot] = dot_set:to_list(causal_context:dots(CCA)),
    %% will inflate if the dot does not belong to the other cc
    not causal_context:is_element(Dot, CCB) orelse
    %% or if it was a not observed removal
    (dot_map:is_empty(DSA) andalso
     dot_set:is_element(Dot, ActiveDots)).

%% @doc Join decomposition for `state_awset()'.
-spec join_decomposition(state_awset()) -> [state_awset()].
join_decomposition({?TYPE, {DotStore, CausalContext}}) ->
    {DecompList, ActiveDots} = lists:foldl(
        fun({Elem, ElemDotSet}, {List0, ActiveDots0}) ->
            List1 = lists:foldl(
                fun(Dot, List2) ->
                    DotSet = dot_set:add_dot(
                        Dot,
                        dot_set:new()
                    ),
                    DS = dot_map:store(
                        Elem,
                        DotSet,
                        dot_map:new()
                    ),
                    CC = causal_context:add_dot(
                        Dot,
                        causal_context:new()
                    ),
                    Decomp = {?TYPE, {DS, CC}},
                    [Decomp | List2]
                end,
                List0,
                dot_set:to_list(ElemDotSet)
            ),

            ActiveDots1 = dot_set:union(ActiveDots0, ElemDotSet),

            {List1, ActiveDots1}
        end,
        {[], dot_set:new()},
        dot_map:to_list(DotStore)
    ),

    CCDotSet = causal_context:dots(CausalContext),
    InactiveDots = dot_set:subtract(CCDotSet, ActiveDots),

    lists:foldl(
        fun(InactiveDot, List) ->
            DS = dot_map:new(),
            CC = causal_context:add_dot(InactiveDot,
                                        causal_context:new()),
            Decomp = {?TYPE, {DS, CC}},
            [Decomp | List]
        end,
        DecompList,
        dot_set:to_list(InactiveDots)
    ).

%% @doc Delta calculation for `state_awset()'.
-spec delta(state_awset(), state_type:digest()) -> state_awset().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

%% @doc Return a CRDT digest.
-spec digest(state_awset()) -> state_type:digest().
digest({?TYPE, {DotStore, CausalContext}}) ->
    ActiveDots = state_causal_type:dots({dot_map, dot_set}, DotStore),
    Metadata = {ActiveDots, CausalContext},
    {mdata, Metadata}.

-spec encode(state_type:format(), state_awset()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_awset().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {[], causal_context:new()}},
                 new()).

query_test() ->
    Set0 = new(),
    Set1 = {?TYPE, {[{"a", [{a, 2}]}],
                    {[{a, 2}], []}}},
    ?assertEqual(sets:new(), query(Set0)),
    ?assertEqual(sets:from_list(["a"]), query(Set1)).

delta_add_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate({add, "a"}, Actor, Set0),
    Set1 = merge({?TYPE, Delta1}, Set0),
    {ok, {?TYPE, Delta2}} = delta_mutate({add, "a"}, Actor, Set1),
    Set2 = merge({?TYPE, Delta2}, Set1),
    {ok, {?TYPE, Delta3}} = delta_mutate({add, "b"}, Actor, Set2),
    Set3 = merge({?TYPE, Delta3}, Set2),

    ?assertEqual({?TYPE, {[{"a", [{Actor, 1}]}],
                          {[{Actor, 1}], []}}},
                 {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {[{"a", [{Actor, 1}]}],
                          {[{Actor, 1}], []}}},
                 Set1),
    ?assertEqual({?TYPE, {[{"a", [{Actor, 2}]}],
                          {[{Actor, 2}], []}}},
                 {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {[{"a", [{Actor, 2}]}],
                          {[{Actor, 2}], []}}},
                 Set2),
    ?assertEqual({?TYPE, {[{"b", [{Actor, 3}]}],
                          {[], [{Actor, 3}]}}},
                 {?TYPE, Delta3}),
    ?assertEqual({?TYPE, {[{"a", [{Actor, 2}]},
                           {"b", [{Actor, 3}]}],
                          {[{Actor, 3}], []}}},
                 Set3).

add_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add, "a"}, Actor, Set0),
    {ok, Set2} = mutate({add, "a"}, Actor, Set1),
    {ok, Set3} = mutate({add, "b"}, Actor, Set2),

    ?assertEqual({?TYPE, {[{"a", [{Actor, 1}]}],
                          {[{Actor, 1}], []}}},
                 Set1),
    ?assertEqual({?TYPE, {[{"a", [{Actor, 2}]}],
                          {[{Actor, 2}], []}}},
                 Set2),
    ?assertEqual({?TYPE, {[{"a", [{Actor, 2}]},
                           {"b", [{Actor, 3}]}],
                          {[{Actor, 3}], []}}},
                 Set3).

rmv_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add, "a"}, Actor, Set0),
    {ok, Set1} = mutate({rmv, "b"}, Actor, Set1),
    {ok, Set2} = mutate({rmv, "a"}, Actor, Set1),

    ?assertEqual(sets:new(), query(Set2)).

add_all_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add_all, []}, Actor, Set0),
    {ok, Set2} = mutate({add_all, ["a", "b"]}, Actor, Set0),
    {ok, Set3} = mutate({add_all, ["b", "c"]}, Actor, Set2),
    ?assertEqual(sets:new(), query(Set1)),
    ?assertEqual(sets:from_list(["a", "b"]), query(Set2)),
    ?assertEqual(sets:from_list(["a", "b", "c"]), query(Set3)).

remove_all_test() ->
    Actor = 1,
    Set0 = new(),
    {ok, Set1} = mutate({add_all, ["a", "b", "c"]}, Actor, Set0),
    {ok, Set2} = mutate({rmv_all, ["a", "c"]}, Actor, Set1),
    {ok, Set3} = mutate({rmv_all, ["b", "d"]}, Actor, Set2),
    {ok, Set3} = mutate({rmv_all, ["b"]}, Actor, Set2),
    ?assertEqual(sets:from_list(["b"]), query(Set2)),
    ?assertEqual(sets:new(), query(Set3)).

filter_test() ->
    Actor = 1,
    Set0 = new(),
    Pred1 = fun({N, _}) -> N == a end,
    Pred2 = fun({N, _}) -> N == b end,
    Pred3 = fun({N, _}) -> N == c end,
    {ok, Set1} = mutate({add_all, [{a, 1}, {a, 2}, {b, 1}]}, Actor, Set0),
    {ok, Set2} = mutate({filter, Pred1}, Actor, Set1),
    {ok, Set3} = mutate({filter, Pred2}, Actor, Set1),
    {ok, Set4} = mutate({filter, Pred3}, Actor, Set1),
    ?assertEqual(sets:from_list([{a, 1}, {a, 2}]), query(Set2)),
    ?assertEqual(sets:from_list([{b, 1}]), query(Set3)),
    ?assertEqual(sets:new(), query(Set4)).

merge_idempontent_test() ->
    Set1 = {?TYPE, {[], {[{1, 1}], []}}},
    Set2 = {?TYPE, {[{"b", [{2, 1}]}],
                    {[{2, 1}], []}}},
    Set3 = {?TYPE, {[{"a", [{1, 1}]}],
                    {[{1, 1}, {2, 1}], []}}},
    Set4 = merge(Set1, Set1),
    Set5 = merge(Set2, Set2),
    Set6 = merge(Set3, Set3),
    ?assertEqual(Set1, Set4),
    ?assertEqual(Set2, Set5),
    ?assertEqual(Set3, Set6).

merge_commutative_test() ->
    Set1 = {?TYPE, {[], {[{1, 1}], []}}},
    Set2 = {?TYPE, {[{"b", [{2, 1}]}],
                    {[{2, 1}], []}}},
    Set3 = {?TYPE, {[{"a", [{1, 1}]}],
                    {[{1, 1}, {2, 1}], []}}},
    Set4 = merge(Set1, Set2),
    Set5 = merge(Set2, Set1),
    Set6 = merge(Set1, Set3),
    Set7 = merge(Set3, Set1),
    Set8 = merge(Set2, Set3),
    Set9 = merge(Set3, Set2),
    Set10 = merge(Set1, merge(Set2, Set3)),
    Set1_2 = {?TYPE, {[{"b", [{2, 1}]}],
                      {[{1, 1}, {2, 1}], []}}},
    Set1_3 = {?TYPE, {[], {[{1, 1}, {2, 1}], []}}},
    Set2_3 = Set3,
    ?assertEqual(Set1_2, Set4),
    ?assertEqual(Set1_2, Set5),
    ?assertEqual(Set1_3, Set6),
    ?assertEqual(Set1_3, Set7),
    ?assertEqual(Set2_3, Set8),
    ?assertEqual(Set2_3, Set9),
    ?assertEqual(Set1_3, Set10).

merge_delta_test() ->
    Set1 = {?TYPE, {[{"a", [{1, 1}]}],
                    {[{1, 1}], []}}},
    Delta1 = {?TYPE, {[], {[{1, 1}], []}}},
    Delta2 = {?TYPE, {[{"b", [{2, 1}]}],
                      {[{2, 1}], []}}},
    Set2 = merge(Delta1, Set1),
    Set3 = merge(Set1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, {[], {[{1, 1}], []}}}, Set2),
    ?assertEqual({?TYPE, {[], {[{1, 1}], []}}}, Set3),
    ?assertEqual({?TYPE, {[{"b", [{2, 1}]}],
                          {[{1, 1}, {2, 1}], []}}},
                 DeltaGroup).

equal_test() ->
    Set1 = {?TYPE, {[{"a", [{1, 1}]}],
                    {[{1, 1}], []}}},
    Set2 = {?TYPE, {[], {[{1, 1}], []}}},
    Set3 = {?TYPE, {[{"a", [{1, 1}]}],
                    {[{1, 1}, {2, 1}], []}}},
    ?assert(equal(Set1, Set1)),
    ?assert(equal(Set2, Set2)),
    ?assert(equal(Set3, Set3)),
    ?assertNot(equal(Set1, Set2)),
    ?assertNot(equal(Set1, Set3)),
    ?assertNot(equal(Set2, Set3)).

is_bottom_test() ->
    Set0 = new(),
    Set1 = {?TYPE, {[{"a", [{1, 1}]}],
                    {[{1, 1}], []}}},
    ?assert(is_bottom(Set0)),
    ?assertNot(is_bottom(Set1)).

is_inflation_test() ->
    Set1 = {?TYPE, {[{"a", [{1, 1}]}],
                    {[{1, 1}], []}}},
    Set2 = {?TYPE, {[], {[{1, 1}], []}}},
    Set3 = {?TYPE, {[{"a", [{1, 1}]}],
                    {[{1, 1}, {2, 1}], []}}},
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
    Set1 = {?TYPE, {[{"a", [{1, 1}]}],
                    {[{1, 1}], []}}},
    Set2 = {?TYPE, {[], {[{1, 1}], []}}},
    Set3 = {?TYPE, {[{"a", [{1, 1}]}],
                    {[{1, 1}, {2, 1}], []}}},
    ?assertNot(is_strict_inflation(Set1, Set1)),
    ?assert(is_strict_inflation(Set1, Set2)),
    ?assertNot(is_strict_inflation(Set2, Set1)),
    ?assert(is_strict_inflation(Set1, Set3)),
    ?assertNot(is_strict_inflation(Set2, Set3)),
    ?assertNot(is_strict_inflation(Set3, Set2)).

join_decomposition_test() ->
    Set1 = {?TYPE, {[], {[{1, 1}], []}}},
    Set2 = {?TYPE, {[{"a", [{1, 1}]}],
                    {[{1, 1}, {2, 1}, {3, 1}], []}}},
    Decomp1 = join_decomposition(Set1),
    Decomp2 = join_decomposition(Set2),
    List = [{?TYPE, {[{"a", [{1, 1}]}], {[{1, 1}], []}}},
            {?TYPE, {[], {[{2, 1}], []}}},
            {?TYPE, {[], {[{3, 1}], []}}}],
    ?assertEqual([Set1], Decomp1),
    ?assertEqual(lists:sort(List), lists:sort(Decomp2)).

digest_test() ->
    CC = {[{a, 2}, {b, 2}], []},
    Set1 = {?TYPE, {[{"elem1", [{a, 1}]},
                     {"elem2", [{a, 2}, {b, 1}]}], CC}},
    Expected = {mdata, {[{a, 1}, {a, 2}, {b, 1}], CC}},
    ?assertEqual(Expected, digest(Set1)).

encode_decode_test() ->
    Set = {?TYPE, {[{"a", [{1, 1}]}],
                   {[{1, 1}, {2, 1}, {3, 1}], []}}},
    Binary = encode(erlang, Set),
    ESet = decode(erlang, Binary),
    ?assertEqual(Set, ESet).

-endif.
