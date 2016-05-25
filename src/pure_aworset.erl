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

%% @doc Pure AWORSet CRDT: pure op-based add-wins observed-remove set
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_aworset).
-author("Georges Younes <georges.r.younes@gmail.com>").

-behaviour(type).
%-behaviour(pure_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, query/1, equal/2]).

-export_type([pure_aworset/0, pure_aworset_op/0]).

-opaque pure_aworset() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), ordsets:set()}.
-type pure_aworset_op() :: {add, pure_type:element()} | {rmv, pure_type:element()}.

%% @doc Create a new, empty `pure_aworset()'
-spec new() -> pure_aworset().
new() ->
    {?TYPE, {orddict:new(), ordsets:new()}}.

%% @doc Create a new, empty `pure_aworset()'
-spec new([term()]) -> pure_aworset().
new([]) ->
    new().

%% @doc Check causal order of 2 version vector.
-spec happened_before(pure_type:id(), pure_type:id()) -> boolean().
happened_before(VV1, VV2) ->
    orddict:fold(
        fun(Key, Value1, Acc) ->
            case orddict:find(Key, VV2) of
                {ok, Value2} ->
                    Acc andalso Value1 =< Value2;
                error ->
                    Acc andalso false
            end
        end,
        true,
        VV1
     ).

%% @doc Check if 2 POLogs are equal.
-spec equal_polog(pure_type:polog(), pure_type:polog()) -> boolean().
equal_polog(POLog1, POLog2) ->
    orddict:size(POLog1) == orddict:size(POLog2) andalso
    orddict:fold(
        fun(Key, Value1, Acc) ->
            case orddict:find(Key, POLog2) of
                {ok, Value2} ->
                    Acc andalso Value1 == Value2;
                error ->
                    Acc andalso false
            end
        end,
        true,
        POLog1
     ).

%% @doc Check redundancy `pure_aworset()'
%% Called in remove_redundant().
-spec redundant({pure_type:id(), pure_aworset_op()}, {pure_type:id(), pure_aworset_op()}) ->
    integer().
redundant({VV1, {add, Elem1}}, {VV2, {_X, Elem2}}) ->
    case Elem1 == Elem2 andalso happened_before(VV1, VV2) of
        true ->
            1;
        false ->
            0
    end.

%% @doc Removes redundant operations from POLog of `pure_aworset()'
%% Called upon updating (add, rmv) the `pure_aworset()'
-spec remove_redundant_POLog({pure_type:id(), pure_aworset_op()}, pure_aworset()) -> {boolean(), pure_aworset()}.
remove_redundant_POLog({VV1, Op}, {?TYPE, {POLog0, ORSet}}) ->
    {POLog1, Same1} = orddict:fold(
        fun(Key, Value, {Acc, Same}) ->
            case redundant({Key, Value}, {VV1, Op}) of
                0 ->
                    {orddict:store(Key, Value, Acc), Same};
                1 ->
                    {Acc, Same andalso false}
            end
        end,
        {orddict:new(), true},
        POLog0
    ),
    {Same1, {?TYPE, {POLog1, ORSet}}}.

%% @doc Removes redundant operations from POLog of `pure_aworset()'
%% Called upon updating (add, rmv) the `pure_aworset()'
-spec remove_redundant_Crystal({pure_type:id(), pure_aworset_op()}, pure_aworset()) -> {boolean(), pure_aworset()}.
remove_redundant_Crystal({_VV1, {_X, Elem}}, {?TYPE, {POLog0, AWORSet}}) ->
    case ordsets:is_element(Elem, AWORSet) of
        true ->
            {true, {?TYPE, {POLog0, ordsets:del_element(Elem, AWORSet)}}};
        false ->
            {false, {?TYPE, {POLog0, AWORSet}}}
    end.


%% @doc Removes redundant operations from POLog of `pure_aworset()'
%% Called upon updating (add, rmv) the `pure_aworset()'
-spec remove_redundant({pure_type:id(), pure_aworset_op()}, pure_aworset()) -> pure_aworset().
remove_redundant({VV1, Op}, {?TYPE, {POLog, ORSet}}) ->
    {Crystal_Changed, {?TYPE, {POLog0, PureAWORSet0}}} = remove_redundant_Crystal({VV1, Op}, {?TYPE, {POLog, ORSet}}),
    case Crystal_Changed of
        true ->
            {?TYPE, {POLog0, PureAWORSet0}};
        false ->
            {_, {?TYPE, {POLog1, PureAWORSet1}}} = remove_redundant_POLog({VV1, Op}, {?TYPE, {POLog, ORSet}}),
            {?TYPE, {POLog1, PureAWORSet1}}
    end.

%% @doc Update a `pure_aworset()'.
-spec mutate(pure_aworset_op(), pure_type:id(), pure_aworset()) ->
    {ok, pure_aworset()}.
mutate({add, Elem}, VV, {?TYPE, {POLog, PureAWORSet}}) ->
    {?TYPE, {POLog0, PureAWORSet0}} = remove_redundant({VV, {add, Elem}}, {?TYPE, {POLog, PureAWORSet}}),
    {ok, {?TYPE, {orddict:store(VV, {add, Elem}, POLog0), PureAWORSet0}}};
mutate({rmv, Elem}, VV, {?TYPE, {POLog, PureAWORSet}}) ->
    {?TYPE, {POLog0, PureAWORSet0}} = remove_redundant({VV, {rmv, Elem}}, {?TYPE, {POLog, PureAWORSet}}),
    {ok, {?TYPE, {POLog0, PureAWORSet0}}}.

%% @doc Returns the value of the `pure_aworset()'.
%%      This value is a list with all the elements in the `pure_aworset()'
%%      that have at least one token still marked as active (true).
-spec query(pure_aworset()) -> [pure_type:element()].
query({?TYPE, {POLog0, PureAWORSet0}}) ->
    Elements0 = ordsets:to_list(PureAWORSet0),
    Elements1 = [El || {_Key, {_Op, El}} <- orddict:to_list(POLog0)],
    lists:usort(lists:append(Elements0, Elements1)).


%% @doc Equality for `pure_aworset()'.
%% @todo use ordsets_ext:equal instead
-spec equal(pure_aworset(), pure_aworset()) -> boolean().
equal({?TYPE, {POLog1, PureAWORSet1}}, {?TYPE, {POLog2, PureAWORSet2}}) ->
    ordsets:is_subset(PureAWORSet1, PureAWORSet2) andalso ordsets:is_subset(PureAWORSet2, PureAWORSet1) andalso equal_polog(POLog1, POLog2).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {orddict:new(), ordsets:new()}}, new()).

happened_before_test() ->
    ?assertEqual(true, happened_before([{0, 1}, {1, 2}, {2, 3}], [{0, 1}, {1, 2}, {2, 3}])),
    ?assertEqual(true, happened_before([{0, 1}, {1, 2}, {2, 3}], [{0, 2}, {1, 4}, {2, 5}])),
    ?assertEqual(false, happened_before([{0, 1}, {1, 2}, {2, 3}], [{0, 1}, {1, 4}, {2, 2}])),
    ?assertEqual(false, happened_before([{0, 2}, {1, 5}, {2, 3}], [{0, 1}, {1, 4}, {2, 2}])),
    ?assertEqual(true, happened_before([{0, 1}, {1, 2}, {2, 3}], [{0, 1}, {1, 4}, {2, 3}])).

equal_polog_test() ->
    ?assertEqual(false, equal_polog([{0, 1}], [{0, 1}, {1, 2}])),
    ?assertEqual(true, equal_polog([{0, 1}, {1, 2}], [{0, 1}, {1, 2}])),
    ?assertEqual(false, equal_polog([{0, 2}], [{0, 1}])),
    ?assertEqual(false, equal_polog([{0, 1}, {1, 2}, {2, 3}], [{0, 1}, {1, 2}])),
    ?assertEqual(true, equal_polog([], [])).

redundant_test() ->
    ?assertEqual(0, redundant({[{0, 0}, {1, 0}], {add, <<"a">>}}, {[{0, 1}, {1, 1}], {add, <<"b">>}})),
    ?assertEqual(1, redundant({[{0, 0}, {1, 0}], {add, <<"a">>}}, {[{0, 1}, {1, 1}], {add, <<"a">>}})),
    ?assertEqual(1, redundant({[{0, 0}, {1, 0}], {add, <<"a">>}}, {[{0, 0}, {1, 0}], {add, <<"a">>}})).

remove_redundant_Crystal_test() ->
    {Redundant0, {?TYPE, {_POLog0, AWORSet0}}} = remove_redundant_Crystal({[{0, 1}, {1, 2}, {2, 3}], {add, <<"a">>}}, {?TYPE, {[{0, 1}], [<<"a">>, <<"b">>, <<"c">>]}}),
    ?assertEqual(true, Redundant0),
    ?assertEqual([<<"b">>, <<"c">>], AWORSet0),
    {Redundant1, {?TYPE, {_POLog1, AWORSet1}}} = remove_redundant_Crystal({[{0, 1}, {1, 2}, {2, 3}], {rmv, <<"a">>}}, {?TYPE, {[{0, 1}], [<<"a">>, <<"b">>, <<"c">>]}}),
    ?assertEqual(true, Redundant1),
    ?assertEqual([<<"b">>, <<"c">>], AWORSet1),
    {Redundant2, {?TYPE, {_POLog2, AWORSet2}}} = remove_redundant_Crystal({[{0, 1}], {rmv, <<"d">>}}, {?TYPE, {[{0, 1}], [<<"a">>]}}),
    ?assertEqual(false, Redundant2),
    ?assertEqual([<<"a">>], AWORSet2).

%remove_redundant_POLog_test() ->

query_test() ->
    Set0 = new(),
    Set1 = {?TYPE, {[], [<<"a">>]}},
    Set2 = {?TYPE, {[], [<<"a">>, <<"c">>]}},
    Set3 = {?TYPE, {[{[{1, 2}], {add, <<"b">>}}], [<<"a">>]}},
    Set4 = {?TYPE, {[{[{1, 3}], {add, <<"a">>}}], [<<"a">>]}},
    ?assertEqual([], query(Set0)),
    ?assertEqual([<<"a">>], query(Set1)),
    ?assertEqual([<<"a">>, <<"c">>], query(Set2)),
    ?assertEqual([<<"a">>, <<"b">>], query(Set3)),
    ?assertEqual([<<"a">>], query(Set4)).

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