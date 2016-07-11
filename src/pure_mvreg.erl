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

%% @doc Pure MVReg CRDT: pure op-based multi-value register
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_mvreg).
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

-export_type([pure_mvreg/0, pure_mvreg_op/0]).

-opaque pure_mvreg() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), []}.
-type pure_mvreg_op() :: {set, string()}.

%% @doc Create a new, empty `pure_mvreg()'
-spec new() -> pure_mvreg().
new() ->
    {?TYPE, {orddict:new(), []}}.

%% @doc Create a new, empty `pure_mvreg()'
-spec new([term()]) -> pure_mvreg().
new([]) ->
    new().

%% @doc Check redundancy `pure_mvreg()'
%% Called in remove_redundant().
-spec redundant({pure_type:id(), pure_mvreg_op()}, {pure_type:id(), pure_mvreg_op()}) ->
    atom().
redundant({VV1, {_Op1, _Str1}}, {VV2, {_Op2, _Str2}}) ->
    case pure_trcb:happened_before(VV1, VV2) of
        true ->
            ?RA;
        false ->
            ?AA
    end.

%% @doc Removes redundant operations from POLog of `pure_mvreg()'
-spec remove_redundant_polog({pure_type:id(), pure_mvreg_op()}, pure_mvreg()) -> {boolean(), pure_mvreg()}.
remove_redundant_polog({VV1, Op}, {?TYPE, {POLog0, MVReg}}) ->
    case orddict:is_empty(POLog0) of
        true ->
            {true, {?TYPE, {POLog0, MVReg}}};
        false ->
            {POLog1, Add1} = orddict:fold(
                fun(Key, Value, {Acc, Add}) ->
                    case redundant({Key, {set, Value}}, {VV1, Op}) of
                        ?AA ->
                            {orddict:store(Key, Value, Acc), Add};
                        ?RA ->
                            {Acc, Add}
                    end
                end,
                {orddict:new(), true},
                POLog0
            ),
            {Add1, {?TYPE, {POLog1, MVReg}}}
    end.

%% @doc Removes redundant operations from crystal of `pure_mvreg()'
%% No crystalisation for this one.
-spec remove_redundant_crystal({pure_type:id(), pure_mvreg_op()}, pure_mvreg()) -> {boolean(), pure_mvreg()}.
remove_redundant_crystal({_VV1, _X}, {?TYPE, {POLog0, MVReg}}) ->
    {true, {?TYPE, {POLog0, MVReg}}}.

%% @doc Checks stable operations and remove them from POLog of `pure_mvreg()'
-spec check_stability(pure_type:id(), pure_mvreg()) -> pure_mvreg().
check_stability(_StableVV, {?TYPE, {POLog0, MVReg0}}) ->
    {?TYPE, {POLog0, MVReg0}}.

%% @doc Update a `pure_mvreg()'.
-spec mutate(pure_mvreg_op(), pure_type:id(), pure_mvreg()) ->
    {ok, pure_mvreg()}.
mutate({Op, Str}, VV, {?TYPE, {POLog, PureMVReg}}) ->
    {_Add, {?TYPE, {POLog0, PureMVReg0}}} = pure_polog:remove_redundant({VV, {Op, Str}}, {?TYPE, {POLog, PureMVReg}}),
    {ok, {?TYPE, {orddict:store(VV, Str, POLog0), PureMVReg0}}}.

%% @doc Clear/reset the state to initial state.
-spec reset(pure_type:id(), pure_mvreg()) -> pure_mvreg().
reset(VV, {?TYPE, _}=CRDT) ->
    pure_type:reset(VV, CRDT).

%% @doc Returns the value of the `pure_mvreg()'.
%%      This value is a a boolean value in the `pure_mvreg()'.
-spec query(pure_mvreg()) -> [string()].
query({?TYPE, {POLog0, _PureMVReg0}}) ->
    case orddict:is_empty(POLog0) of
        true ->
            [];
        false ->
            [Str || {_Key, Str} <- orddict:to_list(POLog0)]
    end.


%% @doc Equality for `pure_mvreg()'.
-spec equal(pure_mvreg(), pure_mvreg()) -> boolean().
equal({?TYPE, {POLog1, _PureMVReg1}}, {?TYPE, {POLog2, _PureMVReg2}}) ->
    Fun = fun(Value1, Value2) -> Value1 == Value2 end,
    orddict_ext:equal(POLog1, POLog2, Fun).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {orddict:new(), []}}, new()).

redundant_test() ->
    ?assertEqual(?RA, redundant({[{0, 0}, {1, 0}], {set, "foo"}}, {[{0, 1}, {1, 1}], {set, "bar"}})),
    ?assertEqual(?AA, redundant({[{0, 0}, {1, 1}], {set, "foo"}}, {[{0, 1}, {1, 0}], {set, "bar"}})).

query_test() ->
    MVReg0 = new(),
    MVReg1 = {?TYPE, {[], []}},
    MVReg2 = {?TYPE, {[], []}},
    MVReg3 = {?TYPE, {[{[{1, 2}], "foo"}], []}},
    MVReg4 = {?TYPE, {[{[{0, 1}, {1, 3}], "bar"}, {[{0, 2}, {1, 1}], "baz"}], []}},
    ?assertEqual([], query(MVReg0)),
    ?assertEqual([], query(MVReg1)),
    ?assertEqual([], query(MVReg2)),
    ?assertEqual(["foo"], query(MVReg3)),
    ?assertEqual(["bar", "baz"], query(MVReg4)).

mutate_test() ->
    MVReg0 = new(),
    {ok, MVReg1} = mutate({set, "foo"}, [{0, 1}, {1, 1}], MVReg0),
    {ok, MVReg2} = mutate({set, "bar"}, [{0, 2}, {1, 2}], MVReg1),
    ?assertEqual({?TYPE, {[{[{0, 1}, {1, 1}], "foo"}], []}}, MVReg1),
    ?assertEqual({?TYPE, {[{[{0, 2}, {1, 2}], "bar"}], []}}, MVReg2).

reset_test() ->
    MVReg1 = {?TYPE, {[{[{0, 1}, {1, 3}], "bar"}, {[{0, 2}, {1, 1}], "baz"}], []}},
    MVReg2 = reset([{0, 5}, {1, 6}], MVReg1),
    ?assertEqual({?TYPE, {[], []}}, MVReg2).

check_stability_test() ->
    MVReg0 = new(),
    MVReg1 = check_stability([], MVReg0),
    ?assertEqual(MVReg0, MVReg1).

equal_test() ->
    MVReg0 = {?TYPE, {[{[{0, 1}, {1, 1}], "foo"}], []}},
    MVReg1 = {?TYPE, {[{[{0, 1}, {1, 1}], "bar"}], []}},
    MVReg2 = {?TYPE, {[{[{0, 1}, {1, 1}], "bar"}], []}},
    MVReg3 = {?TYPE, {[{[{0, 1}, {1, 1}], "foo"}], []}},
    ?assert(equal(MVReg0, MVReg3)),
    ?assert(equal(MVReg1, MVReg2)),
    ?assertNot(equal(MVReg0, MVReg1)),
    ?assertNot(equal(MVReg2, MVReg3)).

-endif.
