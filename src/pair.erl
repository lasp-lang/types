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

%% @doc Pair.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%%      Delta State Replicated Data Types (2016)
%%      [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(pair).
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

-export_type([pair/0, delta_pair/0, pair_op/0]).

-opaque pair() :: {?TYPE, payload()}.
-opaque delta_pair() :: {?TYPE, {delta, payload()}}.
-type component() :: {type:type(), type:crdt()}.
-type payload() :: {component(), component()}.
-type pair_op() :: {fst, term()} | {snd, term()}.

%% @doc Create a new, empty `pair()'
%%      By default it creates a pair of `gcounter()'.
-spec new() -> pair().
new() ->
    new([gcounter, gcounter]).

%% @doc Create a new, empty `pair()'
-spec new([type:type()]) -> pair().
new([Fst, Snd]) ->
    {?TYPE, {Fst:new(), Snd:new()}}.

%% @doc Mutate a `pair()'.
-spec mutate(pair_op(), type:actor(), pair()) ->
    {ok, pair()} | {error, term()}.
mutate({fst, _}=Op, Actor, {?TYPE, {{FstType, _}=Fst, {_SndType, _}=Snd}}=Pair) ->
    case delta_mutate(Op, Actor, Pair) of
        {ok, {?TYPE, {delta, {Delta, _}}}} ->
            {ok, {?TYPE, {FstType:merge({FstType, Delta}, Fst), Snd}}};
        Error ->
            Error
    end;
mutate({snd, _}=Op, Actor, {?TYPE, {{_FstType, _}=Fst, {SndType, _}=Snd}}=Pair) ->
    case delta_mutate(Op, Actor, Pair) of
        {ok, {?TYPE, {delta, {_, Delta}}}} ->
            {ok, {?TYPE, {Fst, SndType:merge({SndType, Delta}, Snd)}}};
        Error ->
            Error
    end.

%% @doc Delta-mutate a `pair()'.
%%      Depending on the atom passed, `fst' or `snd', this function
%%      will delta mutate the pair in the first or second component
%%      respectively.
-spec delta_mutate(pair_op(), type:actor(), pair()) ->
    {ok, delta_pair()} | {error, term()}.
delta_mutate({fst, Op}, Actor, {?TYPE, {{FstType, _}=Fst, {SndType, _}}}) ->
    case FstType:delta_mutate(Op, Actor, Fst) of
        {ok, {FstType, {delta, Delta}}} ->
            DeltaPair = {Delta, {SndType, SndType:new()}},
            {ok, {?TYPE, {delta, DeltaPair}}};
        Error ->
            Error
    end;
delta_mutate({snd, Op}, Actor, {?TYPE, {{FstType, _}, {SndType, _}=Snd}}) ->
    case SndType:delta_mutate(Op, Actor, Snd) of
        {ok, {SndType, {delta, Delta}}} ->
            DeltaPair = {{FstType, FstType:new()}, Delta},
            {ok, {?TYPE, {delta, DeltaPair}}};
        Error ->
            Error
    end.

%% @doc Returns a `pair()' where each component has the value resultant
%%      from `query/1' of the correspondent data type.
-spec query(pair()) -> {term(), term()}.
query({?TYPE, {{FstType, _}=Fst, {SndType, _}=Snd}}) ->
    {FstType:query(Fst), SndType:query(Snd)}.

%% @doc Merge two `pair()'.
%%      The resulting `pair()' is the component-wise join of components.
-spec merge(pair(), pair()) -> pair().
merge({?TYPE, {{FstType, _}=Fst1, {SndType, _}=Snd1}},
      {?TYPE, {{FstType, _}=Fst2, {SndType, _}=Snd2}}) ->
    Fst = FstType:merge(Fst1, Fst2),
    Snd = SndType:merge(Snd1, Snd2),
    {?TYPE, {Fst, Snd}}.

%% @doc Equality for `pair()'.
-spec equal(pair(), pair()) -> boolean().
equal({?TYPE, {{FstType, _}=Fst1, {SndType, _}=Snd1}},
      {?TYPE, {{FstType, _}=Fst2, {SndType, _}=Snd2}}) ->
    FstType:equal(Fst1, Fst2) andalso
    SndType:equal(Snd1, Snd2).

%% @doc Check for `pair()' inflation.
%%      We have an inflation when both of the components are inflations.
-spec is_inflation(pair(), pair()) -> boolean().
is_inflation({?TYPE, {{FstType, _}=Fst1, {SndType, _}=Snd1}},
             {?TYPE, {{FstType, _}=Fst2, {SndType, _}=Snd2}}) ->
    FstType:is_inflation(Fst1, Fst2) andalso
    SndType:is_inflation(Snd1, Snd2).

%% @doc Check for `pair()' strict inflation.
%%      In pairs we have strict inflations if we have component wise
%%      inflations and at least one strict inflation in the composition.
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, Alcino Cunha and Carla Ferreira
%%      Composition of State-based CRDTs (2015)
%%      [http://haslab.uminho.pt/cbm/files/crdtcompositionreport.pdf]
%%
-spec is_strict_inflation(pair(), pair()) -> boolean().
is_strict_inflation({?TYPE, {{FstType, _}=Fst1, {SndType, _}=Snd1}},
                    {?TYPE, {{FstType, _}=Fst2, {SndType, _}=Snd2}}) ->
    (FstType:is_strict_inflation(Fst1, Fst2) andalso SndType:is_inflation(Snd1, Snd2))
    orelse
    (FstType:is_inflation(Fst1, Fst2) andalso SndType:is_strict_inflation(Snd1, Snd2)).

%% @doc Join decomposition for `pair()'.
%% @todo Check how to do this.
-spec join_decomposition(pair()) -> [pair()].
join_decomposition({?TYPE, _Pair}) -> [].

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    Pair0 = new(),
    Pair1 = new([gset, gset]),
    ?assertEqual({?TYPE, {{gcounter, []}, {gcounter, []}}}, Pair0),
    ?assertEqual({?TYPE, {{gset, []}, {gset, []}}}, Pair1).

query_test() ->
    GCounter = {gcounter, [{1, 5}, {2, 10}]},
    GSet = {gset, [<<"a">>]},
    Pair = {?TYPE, {GCounter, GSet}},
    ?assertEqual({15, [<<"a">>]}, query(Pair)).

mutate_test() ->
    Actor = 1,
    GCounter = {gcounter, [{1, 5}, {2, 10}]},
    GSet = {gset, [<<"a">>]},
    Pair0 = {?TYPE, {GCounter, GSet}},
    {ok, Pair1} = mutate({fst, increment}, Actor, Pair0),
    {ok, Pair2} = mutate({snd, {add, <<"b">>}}, Actor, Pair1),
    ?assertEqual({?TYPE, {{gcounter, [{1, 6}, {2, 10}]}, {gset, [<<"a">>]}}}, Pair1),
    ?assertEqual({?TYPE, {{gcounter, [{1, 6}, {2, 10}]}, {gset, [<<"a">>, <<"b">>]}}}, Pair2).

merge_test() ->
    GCounter1 = {gcounter, [{1, 5}, {2, 10}]},
    GCounter2 = {gcounter, [{1, 7}, {3, 8}]},
    GSet1 = {gset, [<<"a">>]},
    GSet2 = {gset, [<<"b">>]},
    Pair1 = {?TYPE, {GCounter1, GSet1}},
    Pair2 = {?TYPE, {GCounter2, GSet2}},
    Pair3 = merge(Pair1, Pair1),
    Pair4 = merge(Pair1, Pair2),
    Pair5 = merge(Pair2, Pair1),
    ?assertEqual({?TYPE, {{gcounter, [{1, 5}, {2, 10}]}, {gset, [<<"a">>]}}}, Pair3),
    ?assertEqual({?TYPE, {{gcounter, [{1, 7}, {2, 10}, {3, 8}]}, {gset, [<<"a">>, <<"b">>]}}}, Pair4),
    ?assertEqual({?TYPE, {{gcounter, [{1, 7}, {2, 10}, {3, 8}]}, {gset, [<<"a">>, <<"b">>]}}}, Pair5).

equal_test() ->
    GCounter1 = {gcounter, [{1, 5}, {2, 10}]},
    GCounter2 = {gcounter, [{1, 7}, {3, 8}]},
    GSet1 = {gset, [<<"a">>]},
    GSet2 = {gset, [<<"b">>]},
    Pair1 = {?TYPE, {GCounter1, GSet1}},
    Pair2 = {?TYPE, {GCounter1, GSet2}},
    Pair3 = {?TYPE, {GCounter1, GSet1}},
    Pair4 = {?TYPE, {GCounter2, GSet1}},
    ?assert(equal(Pair1, Pair1)),
    ?assertNot(equal(Pair1, Pair2)),
    ?assertNot(equal(Pair3, Pair4)).

is_inflation_test() ->
    GCounter1 = {gcounter, [{1, 5}, {2, 10}]},
    GCounter2 = {gcounter, [{1, 7}, {2, 10}]},
    GSet1 = {gset, [<<"a">>]},
    GSet2 = {gset, [<<"b">>]},
    Pair1 = {?TYPE, {GCounter1, GSet1}},
    Pair2 = {?TYPE, {GCounter1, GSet2}},
    Pair3 = {?TYPE, {GCounter1, GSet1}},
    Pair4 = {?TYPE, {GCounter2, GSet1}},
    ?assert(is_inflation(Pair1, Pair1)),
    ?assertNot(is_inflation(Pair1, Pair2)),
    ?assert(is_inflation(Pair3, Pair4)).

is_strict_inflation_test() ->
    GCounter1 = {gcounter, [{1, 5}, {2, 10}]},
    GCounter2 = {gcounter, [{1, 7}, {2, 10}]},
    GSet1 = {gset, [<<"a">>]},
    GSet2 = {gset, [<<"b">>]},
    Pair1 = {?TYPE, {GCounter1, GSet1}},
    Pair2 = {?TYPE, {GCounter1, GSet2}},
    Pair3 = {?TYPE, {GCounter1, GSet1}},
    Pair4 = {?TYPE, {GCounter2, GSet1}},
    ?assertNot(is_strict_inflation(Pair1, Pair1)),
    ?assertNot(is_strict_inflation(Pair1, Pair2)),
    ?assert(is_strict_inflation(Pair3, Pair4)),
    ?assertNot(is_strict_inflation(Pair2, Pair4)).

join_decomposition_test() ->
    %% @todo
    ok.

equivalent_with_pncounter_test() ->
    Actor = 1,
    Pair0 = new(),
    {ok, Pair1} = ?TYPE:mutate({fst, increment}, Actor, Pair0),
    {ok, Pair2} = ?TYPE:mutate({snd, increment}, Actor, Pair1),
    {V1, V2} = ?TYPE:query(Pair2),
    PNCounter0 = pncounter:new(),
    {ok, PNCounter1} = pncounter:mutate(increment, Actor, PNCounter0),
    {ok, PNCounter2} = pncounter:mutate(decrement, Actor, PNCounter1),
    ?assertEqual(V1 - V2, pncounter:query(PNCounter2)).

-endif.
