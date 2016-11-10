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

%% @doc Pure AWSet CRDT: pure op-based add-wins observed-remove set
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_awset).
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

-export_type([pure_awset/0, pure_awset_op/0]).

-opaque pure_awset() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), ordsets:ordset(any())}.
-type pure_awset_op() :: {add, pure_type:element()} | {rmv, pure_type:element()}.

%% @doc Create a new, empty `pure_awset()'
-spec new() -> pure_awset().
new() ->
    {?TYPE, {orddict:new(), ordsets:new()}}.

%% @doc Create a new, empty `pure_awset()'
-spec new([term()]) -> pure_awset().
new([]) ->
    new().

%% @doc Check redundancy `pure_awset()'
%% For a given element, either an "add" or a "rmv" in the future of an "add" make it redundant.
%% Called in remove_redundant().
-spec redundant({pure_type:id(), pure_awset_op()}, {pure_type:id(), pure_awset_op()}) ->
    atom().
redundant({VV1, {add, Elem1}}, {VV2, {_X, Elem2}}) ->
    case Elem1 =:= Elem2 andalso pure_trcb:happened_before(VV1, VV2) of
        true ->
            ?RA;
        false ->
            ?AA
    end.

%% @doc Removes redundant operations from POLog of `pure_awset()'
%% Called upon updating (add, rmv) the `pure_awset()'
-spec remove_redundant_polog({pure_type:id(), pure_awset_op()}, pure_awset()) -> {boolean(), pure_awset()}.
remove_redundant_polog({VV1, Op}, {?TYPE, {POLog0, ORSet}}) ->
    POLog1 = orddict:fold(
        fun(Key, Value, Acc) ->
            case redundant({Key, Value}, {VV1, Op}) of
                ?AA ->
                    orddict:store(Key, Value, Acc);
                ?RA ->
                    Acc
            end
        end,
        orddict:new(),
        POLog0
    ),
    {true, {?TYPE, {POLog1, ORSet}}}.

%% @doc Removes redundant operations from Crystal of `pure_awset()'
%% If there is an "add" or "rmv" for some element, that element does not need to be in the sequential set.
%% Called upon updating (add, rmv) the `pure_awset()'
-spec remove_redundant_crystal({pure_type:id(), pure_awset_op()}, pure_awset()) -> {boolean(), pure_awset()}.
remove_redundant_crystal({_VV1, {_X, Elem}}, {?TYPE, {POLog, AWORSet}}) ->
    case ordsets:is_element(Elem, AWORSet) of
        true ->
            {true, {?TYPE, {POLog, ordsets:del_element(Elem, AWORSet)}}};
        false ->
            {true, {?TYPE, {POLog, AWORSet}}}
    end.

%% @doc Checks stable operations and remove them from POLog of `pure_awset()'
-spec check_stability(pure_type:id(), pure_awset()) -> pure_awset().
check_stability(StableVV, {?TYPE, {POLog0, AWORSet0}}) ->
    {POLog1, AWORSet1} = orddict:fold(
        fun(Key, {_Op, Elem}=Value, {AccPOLog, AccORSet}) ->
            case pure_trcb:happened_before(Key, StableVV) of
                true ->
                    {AccPOLog, ordsets:add_element(Elem, AccORSet)};
                false ->
                    {orddict:store(Key, Value, AccPOLog), AccORSet}
            end
        end,
        {orddict:new(), AWORSet0},
        POLog0
    ),
    {?TYPE, {POLog1, AWORSet1}}.

%% @doc Update a `pure_awset()'.
-spec mutate(pure_awset_op(), pure_type:id(), pure_awset()) ->
    {ok, pure_awset()}.
mutate({add, Elem}, VV, {?TYPE, {POLog, AWSet}}) ->
    {_, {?TYPE, {POLog0, AWSet0}}} = pure_polog:remove_redundant({VV, {add, Elem}}, {?TYPE, {POLog, AWSet}}),
    {ok, {?TYPE, {orddict:store(VV, {add, Elem}, POLog0), AWSet0}}};
mutate({rmv, Elem}, VV, {?TYPE, {POLog, AWSet}}) ->
    {_, {?TYPE, {POLog0, AWSet0}}} = pure_polog:remove_redundant({VV, {rmv, Elem}}, {?TYPE, {POLog, AWSet}}),
    {ok, {?TYPE, {POLog0, AWSet0}}}.

%% @doc Clear/reset the state to initial state.
-spec reset(pure_type:id(), pure_awset()) -> pure_awset().
reset(VV, {?TYPE, _}=CRDT) ->
    pure_type:reset(VV, CRDT).

%% @doc Returns the value of the `pure_awset()'.
%%      This value is a set with all the elements in the `pure_awset()'.
-spec query(pure_awset()) -> sets:set(pure_type:element()).
query({?TYPE, {POLog0, AWSet0}}) ->
    Elements0 = ordsets:to_list(AWSet0),
    Elements1 = [El || {_Key, {_Op, El}} <- orddict:to_list(POLog0)],
    sets:from_list(lists:append(Elements0, Elements1)).


%% @doc Equality for `pure_awset()'.
-spec equal(pure_awset(), pure_awset()) -> boolean().
equal({?TYPE, {POLog1, AWSet1}}, {?TYPE, {POLog2, AWSet2}}) ->
    Fun = fun(Value1, Value2) -> Value1 == Value2 end,
    ordsets_ext:equal(AWSet1, AWSet2) andalso orddict_ext:equal(POLog1, POLog2, Fun).


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
    {Redundant0, {?TYPE, {_POLog0, AWORSet0}}} = remove_redundant_crystal({[{0, 1}, {1, 2}, {2, 3}], {add, <<"a">>}}, {?TYPE, {[{0, 1}], [<<"a">>, <<"b">>, <<"c">>]}}),
    ?assertEqual(true, Redundant0),
    ?assertEqual([<<"b">>, <<"c">>], AWORSet0),
    {Redundant1, {?TYPE, {_POLog1, AWORSet1}}} = remove_redundant_crystal({[{0, 1}, {1, 2}, {2, 3}], {rmv, <<"a">>}}, {?TYPE, {[{0, 1}], [<<"a">>, <<"b">>, <<"c">>]}}),
    ?assertEqual(true, Redundant1),
    ?assertEqual([<<"b">>, <<"c">>], AWORSet1),
    {Redundant2, {?TYPE, {_POLog2, AWORSet2}}} = remove_redundant_crystal({[{0, 1}], {rmv, <<"d">>}}, {?TYPE, {[{0, 1}], [<<"a">>]}}),
    ?assertEqual(true, Redundant2),
    ?assertEqual([<<"a">>], AWORSet2).

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
    {ok, Set4} = mutate({rmv, <<"a">>}, [{0, 0}], Set2),
    {ok, Set5} = mutate({rmv, <<"c">>}, [{0, 5}], Set2),
    ?assertEqual({?TYPE, {[{[{0, 1}], {add, <<"a">>}}], []}}, Set2),
    ?assertEqual({?TYPE, {[{[{0, 1}], {add, <<"a">>}}], []}}, Set4),
    ?assertEqual({?TYPE, {[{[{0, 1}], {add, <<"a">>}}], []}}, Set5),
    ?assertEqual({?TYPE, {[], []}}, Set3).

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
