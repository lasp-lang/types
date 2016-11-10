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

%% @doc Pure RWORSet CRDT: pure op-based rmv-wins observed-remove set
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_rwset).
-author("Georges Younes <georges.r.younes@gmail.com>").

-behaviour(type).
-behaviour(pure_type).
-behaviour(pure_polog).

-define(TYPE, ?MODULE).

-include("pure_type.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, query/1, equal/2, reset/2]).
-export([redundant/2, remove_redundant_crystal/2, remove_redundant_polog/2, check_stability/2]).

-export_type([pure_rwset/0, pure_rwset_op/0]).

-opaque pure_rwset() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), ordsets:ordset(any())}.
-type pure_rwset_op() :: {add, pure_type:element()} | {rmv, pure_type:element()}.

%% @doc Create a new, empty `pure_rwset()'
-spec new() -> pure_rwset().
new() ->
    {?TYPE, {orddict:new(), ordsets:new()}}.

%% @doc Create a new, empty `pure_rwset()'
-spec new([term()]) -> pure_rwset().
new([]) ->
    new().

-spec redundant({pure_type:id(), pure_rwset_op()}, {pure_type:id(), pure_rwset_op()}) ->
    atom().
redundant({VV1, {add, Elem1}}, {VV2, {X, Elem2}}) ->
    case Elem1 =:= Elem2 of
        true ->
            case pure_trcb:happened_before(VV1, VV2) of
                true ->
                    ?RA;
                false ->
                    % Elem1 == Elem2 and VV1 and VV2 concurrent
                    % because happened_before(VV1, VV2) false
                    % and happened_before(VV2, VV1) is always false because causal delivery.
                    case X == rmv of
                        true ->
                            ?RA;
                        false ->
                            ?AA
                    end
            end;
        false ->
            ?AA
    end;
redundant({VV1, {rmv, Elem1}}, {VV2, {X, Elem2}}) ->
    case Elem1 =:= Elem2 of
        true ->
            case pure_trcb:happened_before(VV1, VV2) of
                true ->
                    ?RA;
                false ->
                    % Elem1 == Elem2 and VV1 and VV2 concurrent
                    % because happened_before(VV1, VV2) false
                    % and happened_before(VV2, VV1) is always false because causal delivery.
                    case X == add of
                        true ->
                            ?AR;
                        false ->
                            ?AA
                    end
            end;
        false ->
            ?AA
    end.

%% @doc Removes redundant operations from POLog of `pure_rwset()'
%% Called upon updating (add, rmv) the `pure_rwset()'
-spec remove_redundant_polog({pure_type:id(), pure_rwset_op()}, pure_rwset()) -> {boolean(), pure_rwset()}.
remove_redundant_polog({VV1, Op}, {?TYPE, {POLog0, ORSet}}) ->
    case orddict:is_empty(POLog0) of
        true ->
            {true, {?TYPE, {POLog0, ORSet}}};
        false ->
            {POLog1, Add1} = orddict:fold(
                fun(Key, Value, {Acc, Add}) ->
                    case redundant({Key, Value}, {VV1, Op}) of
                        ?AA ->
                            {orddict:store(Key, Value, Acc), Add};
                        ?RA ->
                            {Acc, Add};
                        ?AR ->
                            {orddict:store(Key, Value, Acc), Add andalso false}
                    end
                end,
                {orddict:new(), true},
                POLog0
            ),
            {Add1, {?TYPE, {POLog1, ORSet}}}
    end.

%% @doc Removes redundant operations from POLog of `pure_rwset()'
%% Called upon updating (add, rmv) the `pure_rwset()'
-spec remove_redundant_crystal({pure_type:id(), pure_rwset_op()}, pure_rwset()) -> {boolean(), pure_rwset()}.
remove_redundant_crystal({_VV1, {_X, Elem}}, {?TYPE, {POLog, RWORSet}}) ->
    case ordsets:is_element(Elem, RWORSet) of
        true ->
            {true, {?TYPE, {POLog, ordsets:del_element(Elem, RWORSet)}}};
        false ->
            {true, {?TYPE, {POLog, RWORSet}}}
    end.

%% @doc Checks stable operations and remove them from POLog of `pure_rwset()'
-spec check_stability(pure_type:id(), pure_rwset()) -> pure_rwset().
check_stability(StableVV, {?TYPE, {POLog0, RWORSet0}}) ->
    {POLog1, RWORSet1} = orddict:fold(
        fun(Key, {Op, Elem}=Value, {AccPOLog, AccORSet}) ->
            case pure_trcb:happened_before(Key, StableVV) of
                true ->
                    case Op of
                        add ->
                            {AccPOLog, ordsets:add_element(Elem, AccORSet)};
                        rmv ->
                            {AccPOLog, ordsets:del_element(Elem, AccORSet)}
                    end;
                false ->
                    {orddict:store(Key, Value, AccPOLog), AccORSet}
            end
        end,
        {orddict:new(), RWORSet0},
        POLog0
    ),
    {?TYPE, {POLog1, RWORSet1}}.

%% @doc Update a `pure_rwset()'.
-spec mutate(pure_rwset_op(), pure_type:id(), pure_rwset()) ->
    {ok, pure_rwset()}.
mutate({add, Elem}, VV, {?TYPE, {POLog, RWSet}}) ->
    {Add, {?TYPE, {POLog0, RWSet0}}} = pure_polog:remove_redundant({VV, {add, Elem}}, {?TYPE, {POLog, RWSet}}),
    case Add of
        false ->
            {ok, {?TYPE, {POLog0, RWSet0}}};
        true ->
            {ok, {?TYPE, {orddict:store(VV, {add, Elem}, POLog0), RWSet0}}}
    end;
mutate({rmv, Elem}, VV, {?TYPE, {POLog, RWSet}}) ->
    {_, {?TYPE, {POLog0, RWSet0}}} = pure_polog:remove_redundant({VV, {rmv, Elem}}, {?TYPE, {POLog, RWSet}}),
    {ok, {?TYPE, {orddict:store(VV, {rmv, Elem}, POLog0), RWSet0}}}.

%% @doc Clear/reset the state to initial state.
-spec reset(pure_type:id(), pure_rwset()) -> pure_rwset().
reset(VV, {?TYPE, _}=CRDT) ->
    pure_type:reset(VV, CRDT).

%% @doc Returns the value of the `pure_rwset()'.
%%      This value is a set with all the elements in the `pure_rwset()'.
-spec query(pure_rwset()) -> sets:set(pure_type:element()).
query({?TYPE, {POLog0, RWSet0}}) ->
    Elements0 = ordsets:to_list(RWSet0),
    Elements1 = [El || {_Key, {Op, El}} <- orddict:to_list(POLog0), Op == add],
    sets:from_list(lists:append(Elements0, Elements1)).


%% @doc Equality for `pure_rwset()'.
-spec equal(pure_rwset(), pure_rwset()) -> boolean().
equal({?TYPE, {POLog1, RWSet1}}, {?TYPE, {POLog2, RWSet2}}) ->
    Fun = fun(Value1, Value2) -> Value1 == Value2 end,
    ordsets_ext:equal(RWSet1, RWSet2) andalso orddict_ext:equal(POLog1, POLog2, Fun).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {orddict:new(), ordsets:new()}}, new()).

redundant_test() ->
    ?assertEqual(?AA, redundant({[{0, 0}, {1, 0}], {add, <<"a">>}}, {[{0, 1}, {1, 1}], {add, <<"b">>}})),
    ?assertEqual(?RA, redundant({[{0, 0}, {1, 0}], {add, <<"a">>}}, {[{0, 1}, {1, 1}], {add, <<"a">>}})),
    ?assertEqual(?AA, redundant({[{0, 0}, {1, 0}], {add, <<"a">>}}, {[{0, 0}, {1, 0}], {add, <<"a">>}})).

remove_redundant_crystal_test() ->
    {Redundant0, {?TYPE, {_POLog0, RWORSet0}}} = remove_redundant_crystal({[{0, 1}, {1, 2}, {2, 3}], {add, <<"a">>}}, {?TYPE, {[{0, 1}], [<<"a">>, <<"b">>, <<"c">>]}}),
    ?assertEqual(true, Redundant0),
    ?assertEqual([<<"b">>, <<"c">>], RWORSet0),
    {Redundant1, {?TYPE, {_POLog1, RWORSet1}}} = remove_redundant_crystal({[{0, 1}, {1, 2}, {2, 3}], {rmv, <<"a">>}}, {?TYPE, {[{0, 1}], [<<"a">>, <<"b">>, <<"c">>]}}),
    ?assertEqual(true, Redundant1),
    ?assertEqual([<<"b">>, <<"c">>], RWORSet1),
    {Redundant2, {?TYPE, {_POLog2, RWORSet2}}} = remove_redundant_crystal({[{0, 1}], {rmv, <<"d">>}}, {?TYPE, {[{0, 1}], [<<"a">>]}}),
    ?assertEqual(true, Redundant2),
    ?assertEqual([<<"a">>], RWORSet2).

query_test() ->
    Set0 = new(),
    Set1 = {?TYPE, {[], [<<"a">>]}},
    Set2 = {?TYPE, {[], [<<"a">>, <<"c">>]}},
    Set3 = {?TYPE, {[{[{1, 2}], {add, <<"b">>}}], [<<"a">>]}},
    Set4 = {?TYPE, {[{[{1, 3}], {add, <<"a">>}}], [<<"a">>]}},
    ?assertEqual(sets:new(), query(Set0)),
    ?assertEqual(sets:from_list([<<"a">>]), query(Set1)),
    ?assertEqual(sets:from_list([<<"a">>, <<"c">>]), query(Set2)),
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>]), query(Set3)),
    ?assertEqual(sets:from_list([<<"a">>]), query(Set4)).

add_test() ->
    Set0 = new(),
    {ok, Set1} = mutate({add, <<"a">>}, [{0, 1}], Set0),
    {ok, Set2} = mutate({add, <<"b">>}, [{0, 2}], Set1),
    {ok, Set3} = mutate({add, <<"b">>}, [{0, 2}], Set2),
    {ok, Set4} = mutate({add, <<"b">>}, [{0, 3}], Set3),
    Set5 = {?TYPE, {[], [<<"a">>, <<"b">>, <<"c">>]}},
    {ok, Set6} = mutate({add, <<"b">>}, [{0, 4}], Set5),
    ?assertEqual({?TYPE, {[{[{0, 1}], {add, <<"a">>}}], []}}, Set1),
    ?assertEqual({?TYPE, {[{[{0, 1}], {add, <<"a">>}}, {[{0, 2}], {add, <<"b">>}}], []}}, Set2),
    ?assertEqual({?TYPE, {[{[{0, 1}], {add, <<"a">>}}, {[{0, 2}], {add, <<"b">>}}], []}}, Set3),
    ?assertEqual({?TYPE, {[{[{0, 1}], {add, <<"a">>}}, {[{0, 3}], {add, <<"b">>}}], []}}, Set4),
    ?assertEqual({?TYPE, {[{[{0, 4}], {add, <<"b">>}}], [<<"a">>, <<"c">>]}}, Set6).

rmv_test() ->
    Set1 = {?TYPE, {[{[{0, 1}], {add, <<"a">>}}, {[{0, 2}], {add, <<"b">>}}], []}},
    {ok, Set2} = mutate({rmv, <<"b">>}, [{0, 3}], Set1),
    {ok, Set3} = mutate({rmv, <<"a">>}, [{0, 4}], Set2),
    {ok, Set4} = mutate({rmv, <<"c">>}, [{0, 5}], Set2),
    ?assertEqual({?TYPE, {[{[{0, 1}], {add, <<"a">>}}, {[{0, 3}], {rmv, <<"b">>}}], []}}, Set2),
    ?assertEqual({?TYPE, {[{[{0, 1}], {add, <<"a">>}}, {[{0, 3}], {rmv, <<"b">>}}, {[{0, 5}], {rmv, <<"c">>}}], []}}, Set4),
    ?assertEqual({?TYPE, {[{[{0, 3}], {rmv, <<"b">>}}, {[{0, 4}], {rmv, <<"a">>}}], []}}, Set3).

reset_test() ->
    Set1 = {?TYPE, {[{[{0, 1}, {1, 2}], {add, <<"b">>}}, {[{0, 3}, {1, 4}], {add, <<"c">>}}, {[{0, 6}, {1, 5}], {add, <<"d">>}}], [<<"a">>]}},
    Set2 = reset([{0, 5}, {1, 6}], Set1),
    ?assertEqual({?TYPE, {[{[{0, 6}, {1, 5}], {add, <<"d">>}}], []}}, Set2).

check_stability_test() ->
    Set0 = new(),
    Set1 = check_stability([], Set0),
    ?assertEqual(Set0, Set1).

equal_test() ->
    Set0 = {?TYPE, {[], [<<"a">>, <<"b">>, <<"c">>]}},
    Set1 = {?TYPE, {[{k1, "c"}], [<<"a">>, <<"b">>]}},
    Set2 = {?TYPE, {[{k1, "c"}], [<<"a">>, <<"b">>]}},
    Set3 = {?TYPE, {[], [<<"a">>, <<"b">>, <<"c">>]}},
    ?assert(equal(Set0, Set3)),
    ?assert(equal(Set1, Set2)),
    ?assertNot(equal(Set0, Set1)),
    ?assertNot(equal(Set2, Set3)).

-endif.
