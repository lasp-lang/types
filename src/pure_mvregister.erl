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

%% @doc Pure MVRegister CRDT: pure op-based multi-value register
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_mvregister).
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

-export_type([pure_mvregister/0, pure_mvregister_op/0]).

-opaque pure_mvregister() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), []}.
-type timestamp() :: non_neg_integer().
-type value() :: term().
-type pure_mvregister_op() :: {set, timestamp(), value()}.

%% @doc Create a new, empty `pure_mvregister()'
-spec new() -> pure_mvregister().
new() ->
    {?TYPE, {orddict:new(), []}}.

%% @doc Create a new, empty `pure_mvregister()'
-spec new([term()]) -> pure_mvregister().
new([]) ->
    new().

%% @doc Check redundancy `pure_mvregister()'
%% Called in remove_redundant().
-spec redundant({pure_type:id(), pure_mvregister_op()}, {pure_type:id(), pure_mvregister_op()}) ->
    atom().
redundant({VV1, _Op1}, {VV2, _Op2}) ->
    case pure_trcb:happened_before(VV1, VV2) of
        true ->
            ?RA;
        false ->
            ?AA
    end.

%% @doc Removes redundant operations from POLog of `pure_mvregister()'
-spec remove_redundant_polog({pure_type:id(), pure_mvregister_op()}, pure_mvregister()) -> {boolean(), pure_mvregister()}.
remove_redundant_polog({VV1, Op}, {?TYPE, {POLog0, MVReg}}) ->
    case orddict:is_empty(POLog0) of
        true ->
            {true, {?TYPE, {POLog0, MVReg}}};
        false ->
            {POLog1, Add1} = orddict:fold(
                fun(Key, Value, {Acc, Add}) ->
                    Timestamp = 0, %% won't be used
                    case redundant({Key, {set, Timestamp, Value}}, {VV1, Op}) of
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

%% @doc Removes redundant operations from crystal of `pure_mvregister()'
%% No crystalisation for this one.
-spec remove_redundant_crystal({pure_type:id(), pure_mvregister_op()}, pure_mvregister()) -> {boolean(), pure_mvregister()}.
remove_redundant_crystal({_VV1, _X}, {?TYPE, {POLog0, MVReg}}) ->
    {true, {?TYPE, {POLog0, MVReg}}}.

%% @doc Checks stable operations and remove them from POLog of `pure_mvregister()'
-spec check_stability(pure_type:id(), pure_mvregister()) -> pure_mvregister().
check_stability(_StableVV, {?TYPE, {POLog0, MVReg0}}) ->
    {?TYPE, {POLog0, MVReg0}}.

%% @doc Update a `pure_mvregister()'.
%%      The first argument is:
%%          - `{set, timestamp(), value()}'.
%%          - the second component in this triple will not be used.
%%          - in order to have an unified API for all registers
%%          (since LWWRegister needs to receive a timestamp),
%%          the timestamp is also supplied here
-spec mutate(pure_mvregister_op(), pure_type:id(), pure_mvregister()) ->
    {ok, pure_mvregister()}.
mutate({Op, _Timestamp, Value}, VV, {?TYPE, {POLog, PureMVReg}}) ->
    {_Add, {?TYPE, {POLog0, PureMVReg0}}} = pure_polog:remove_redundant({VV, {Op, Value}}, {?TYPE, {POLog, PureMVReg}}),
    {ok, {?TYPE, {orddict:store(VV, Value, POLog0), PureMVReg0}}}.

%% @doc Clear/reset the state to initial state.
-spec reset(pure_type:id(), pure_mvregister()) -> pure_mvregister().
reset(VV, {?TYPE, _}=CRDT) ->
    pure_type:reset(VV, CRDT).

%% @doc Returns the value of the `pure_mvregister()'.
-spec query(pure_mvregister()) -> sets:set(value()).
query({?TYPE, {POLog0, _PureMVReg0}}) ->
    sets:from_list([Value || {_Key, Value} <- orddict:to_list(POLog0)]).

%% @doc Equality for `pure_mvregister()'.
-spec equal(pure_mvregister(), pure_mvregister()) -> boolean().
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
    Timestamp = 0, %% won't be used
    ?assertEqual(?RA, redundant({[{0, 0}, {1, 0}], {set, Timestamp, "foo"}}, {[{0, 1}, {1, 1}], {set, Timestamp, "bar"}})),
    ?assertEqual(?AA, redundant({[{0, 0}, {1, 1}], {set, Timestamp, "foo"}}, {[{0, 1}, {1, 0}], {set, Timestamp, "bar"}})).

query_test() ->
    MVReg0 = new(),
    MVReg1 = {?TYPE, {[{[{1, 2}], "foo"}], []}},
    MVReg2 = {?TYPE, {[{[{0, 1}, {1, 3}], "bar"}, {[{0, 2}, {1, 1}], "baz"}], []}},
    ?assertEqual(sets:from_list([]), query(MVReg0)),
    ?assertEqual(sets:from_list(["foo"]), query(MVReg1)),
    ?assertEqual(sets:from_list(["bar", "baz"]), query(MVReg2)).

mutate_test() ->
    Timestamp = 0, %% won't be used
    MVReg0 = new(),
    {ok, MVReg1} = mutate({set, Timestamp, "foo"}, [{0, 1}, {1, 1}], MVReg0),
    {ok, MVReg2} = mutate({set, Timestamp, "bar"}, [{0, 2}, {1, 2}], MVReg1),
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
