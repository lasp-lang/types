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

%% @doc Pure RWORSet CRDT: pure op-based rmv-wins observed-remove set
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_rworset).
-author("Georges Younes <georges.r.younes@gmail.com>").

-behaviour(type).
-behaviour(pure_polog).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, query/1, equal/2]).
-export([redundant/2, remove_redundant_crystal/2, remove_redundant_polog/2, check_stability/2]).

-export_type([pure_rworset/0, pure_rworset_op/0]).

-opaque pure_rworset() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), ordsets:set()}.
-type pure_rworset_op() :: {add, pure_type:element()} | {rmv, pure_type:element()}.

%% @doc Create a new, empty `pure_rworset()'
-spec new() -> pure_rworset().
new() ->
    {?TYPE, {orddict:new(), ordsets:new()}}.

%% @doc Create a new, empty `pure_rworset()'
-spec new([term()]) -> pure_rworset().
new([]) ->
    new().

-spec redundant({pure_type:id(), pure_rworset_op()}, {pure_type:id(), pure_rworset_op()}) ->
    integer().
redundant({VV1, {add, Elem1}}, {VV2, {X, Elem2}}) ->
    case Elem1 == Elem2 of
        true ->
            case pure_trcb:happened_before(VV1, VV2) of
                true ->
                    1;
                false ->
                    % Elem1 == Elem2 and VV1 and VV2 concurrent
                    % because happened_before(VV1, VV2) false
                    % and happened_before(VV2, VV1) is always false because causal delivery.
                    case X == rmv of
                        true ->
                            1;
                        false ->
                            0
                    end
            end;
        false ->
            0
    end;
redundant({VV1, {rmv, Elem1}}, {VV2, {X, Elem2}}) ->
    case Elem1 == Elem2 of
        true ->
            case pure_trcb:happened_before(VV1, VV2) of
                true ->
                    1;
                false ->
                    % Elem1 == Elem2 and VV1 and VV2 concurrent
                    % because happened_before(VV1, VV2) false
                    % and happened_before(VV2, VV1) is always false because causal delivery.
                    case X == add of
                        true ->
                            2;
                        false ->
                            0
                    end
            end;
        false ->
            0
    end.

%% @doc Removes redundant operations from POLog of `pure_rworset()'
%% Called upon updating (add, rmv) the `pure_rworset()'
-spec remove_redundant_polog({pure_type:id(), pure_rworset_op()}, pure_rworset()) -> {boolean(), pure_rworset()}.
remove_redundant_polog({VV1, Op}, {?TYPE, {POLog0, ORSet}}) ->
    case orddict:is_empty(POLog0) of
        true ->
            {false, {?TYPE, {POLog0, ORSet}}};
        false ->
            {POLog1, DoNotAdd1} = orddict:fold(
                fun(Key, Value, {Acc, DoNotAdd}) ->
                    case redundant({Key, Value}, {VV1, Op}) of
                        0 ->
                            {orddict:store(Key, Value, Acc), DoNotAdd andalso false};
                        1 ->
                            {Acc, DoNotAdd andalso false};
                        2 ->
                            {orddict:store(Key, Value, Acc), DoNotAdd}
                    end
                end,
                {orddict:new(), true},
                POLog0
            ),
            {DoNotAdd1, {?TYPE, {POLog1, ORSet}}}
    end.

%% @doc Removes redundant operations from POLog of `pure_rworset()'
%% Called upon updating (add, rmv) the `pure_rworset()'
-spec remove_redundant_crystal({pure_type:id(), pure_rworset_op()}, pure_rworset()) -> {boolean(), pure_rworset()}.
remove_redundant_crystal({_VV1, {_X, Elem}}, {?TYPE, {POLog, RWORSet}}) ->
    case ordsets:is_element(Elem, RWORSet) of
        true ->
            {true, {?TYPE, {POLog, ordsets:del_element(Elem, RWORSet)}}};
        false ->
            {false, {?TYPE, {POLog, RWORSet}}}
    end.

%% @doc Checks stable operations and remove them from POLog of `pure_rworset()'
-spec check_stability(pure_type:id(), pure_rworset()) -> pure_rworset().
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

%% @doc Update a `pure_rworset()'.
-spec mutate(pure_rworset_op(), pure_type:id(), pure_rworset()) ->
    {ok, pure_rworset()}.
mutate({add, Elem}, VV, {?TYPE, {POLog, PureRWORSet}}) ->
    {DoNotAdd, {?TYPE, {POLog0, PureRWORSet0}}} = pure_polog:remove_redundant({VV, {add, Elem}}, {?TYPE, {POLog, PureRWORSet}}),
    case DoNotAdd of
        true ->
            {ok, {?TYPE, {POLog0, PureRWORSet0}}};
        false ->
            {ok, {?TYPE, {orddict:store(VV, {add, Elem}, POLog0), PureRWORSet0}}}
    end;
mutate({rmv, Elem}, VV, {?TYPE, {POLog, PureRWORSet}}) ->
    {DoNotAdd, {?TYPE, {POLog0, PureRWORSet0}}} = pure_polog:remove_redundant({VV, {rmv, Elem}}, {?TYPE, {POLog, PureRWORSet}}),
    case DoNotAdd of
        true ->
            {ok, {?TYPE, {POLog0, PureRWORSet0}}};
        false ->
            {ok, {?TYPE, {orddict:store(VV, {rmv, Elem}, POLog0), PureRWORSet0}}}
    end.

%% @doc Returns the value of the `pure_rworset()'.
%%      This value is a set with all the elements in the `pure_rworset()'.
-spec query(pure_rworset()) -> sets:set(pure_type:element()).
query({?TYPE, {POLog0, PureRWORSet0}}) ->
    Elements0 = ordsets:to_list(PureRWORSet0),
    Elements1 = [El || {_Key, {_Op, El}} <- orddict:to_list(POLog0)],
    sets:from_list(lists:usort(lists:append(Elements0, Elements1))).


%% @doc Equality for `pure_rworset()'.
-spec equal(pure_rworset(), pure_rworset()) -> boolean().
equal({?TYPE, {POLog1, PureRWORSet1}}, {?TYPE, {POLog2, PureRWORSet2}}) ->
    Fun = fun(Value1, Value2) -> Value1 == Value2 end,
    ordsets_ext:equal(PureRWORSet1, PureRWORSet2) andalso orddict_ext:equal(POLog1, POLog2, Fun).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {orddict:new(), ordsets:new()}}, new()).

redundant_test() ->
    ?assertEqual(0, redundant({[{0, 0}, {1, 0}], {add, <<"a">>}}, {[{0, 1}, {1, 1}], {add, <<"b">>}})),
    ?assertEqual(1, redundant({[{0, 0}, {1, 0}], {add, <<"a">>}}, {[{0, 1}, {1, 1}], {add, <<"a">>}})),
    ?assertEqual(0, redundant({[{0, 0}, {1, 0}], {add, <<"a">>}}, {[{0, 0}, {1, 0}], {add, <<"a">>}})).

remove_redundant_crystal_test() ->
    {Redundant0, {?TYPE, {_POLog0, RWORSet0}}} = remove_redundant_crystal({[{0, 1}, {1, 2}, {2, 3}], {add, <<"a">>}}, {?TYPE, {[{0, 1}], [<<"a">>, <<"b">>, <<"c">>]}}),
    ?assertEqual(true, Redundant0),
    ?assertEqual([<<"b">>, <<"c">>], RWORSet0),
    {Redundant1, {?TYPE, {_POLog1, RWORSet1}}} = remove_redundant_crystal({[{0, 1}, {1, 2}, {2, 3}], {rmv, <<"a">>}}, {?TYPE, {[{0, 1}], [<<"a">>, <<"b">>, <<"c">>]}}),
    ?assertEqual(true, Redundant1),
    ?assertEqual([<<"b">>, <<"c">>], RWORSet1),
    {Redundant2, {?TYPE, {_POLog2, RWORSet2}}} = remove_redundant_crystal({[{0, 1}], {rmv, <<"d">>}}, {?TYPE, {[{0, 1}], [<<"a">>]}}),
    ?assertEqual(false, Redundant2),
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