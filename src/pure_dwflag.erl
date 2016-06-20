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

%% @doc Pure DWFlag CRDT: pure op-based disable-wins flag
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_dwflag).
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

-export_type([pure_dwflag/0, pure_dwflag_op/0]).

-opaque pure_dwflag() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), boolean()}.
-type pure_dwflag_op() :: enable | disable.

%% @doc Create a new, empty `pure_dwflag()'
-spec new() -> pure_dwflag().
new() ->
    {?TYPE, {orddict:new(), true}}.

%% @doc Create a new, empty `pure_dwflag()'
-spec new([term()]) -> pure_dwflag().
new([]) ->
    new().

%% @doc Check redundancy `pure_dwflag()'
%% Called in remove_redundant().
-spec redundant({pure_type:id(), pure_dwflag_op()}, {pure_type:id(), pure_dwflag_op()}) ->
    integer().
redundant({VV1, Op1}, {VV2, Op2}) ->
    case pure_trcb:happened_before(VV1, VV2) of
        true ->
            1;
        false ->
            case Op1 == Op2 of
                true ->
                    0;
                false ->
                    case Op2 of
                        disable ->
                            1;
                        enable ->
                            2
                    end
            end
    end.

%% @doc Removes redundant operations from POLog of `pure_dwflag()'
%% Called upon updating (enable, disable) the `pure_dwflag()'
-spec remove_redundant_polog({pure_type:id(), pure_dwflag_op()}, pure_dwflag()) -> {boolean(), pure_dwflag()}.
remove_redundant_polog({VV1, Op}, {?TYPE, {POLog0, Flag}}) ->
    case orddict:is_empty(POLog0) of
        true ->
            {true, {?TYPE, {POLog0, Flag}}};
        false ->
            {POLog1, Add1} = orddict:fold(
                fun(Key, Value, {Acc, Add}) ->
                    case redundant({Key, Value}, {VV1, Op}) of
                        0 ->
                            {orddict:store(Key, Value, Acc), Add};
                        1 ->
                            {Acc, Add};
                        2 ->
                            {orddict:store(Key, Value, Acc), Add andalso false}
                    end
                end,
                {orddict:new(), true},
                POLog0
            ),
            {Add1, {?TYPE, {POLog1, Flag}}}
    end.

%% @doc Removes redundant operations from POLog of `pure_dwflag()'
%% Called upon updating (enable, disable) the `pure_dwflag()'
-spec remove_redundant_crystal({pure_type:id(), pure_dwflag_op()}, pure_dwflag()) -> {boolean(), pure_dwflag()}.
remove_redundant_crystal({_VV1, _X}, {?TYPE, {POLog0, DWFlag}}) ->
    {true, {?TYPE, {POLog0, DWFlag}}}.

%% @doc Checks stable operations and remove them from POLog of `pure_dwflag()'
-spec check_stability(pure_type:id(), pure_dwflag()) -> pure_dwflag().
check_stability(StableVV, {?TYPE, {POLog0, DWFlag0}}) ->
    {POLog1, DWFlag1} = orddict:fold(
        fun(Key, Value, {AccPOLog, AccFlag}) ->
            case pure_trcb:happened_before(Key, StableVV) of
                true ->
                    case Value of
                        enable ->
                            {AccPOLog, true};
                        disable ->
                            {AccPOLog, false}
                    end;
                false ->
                    {orddict:store(Key, Value, AccPOLog), AccFlag}
            end
        end,
        {orddict:new(), DWFlag0},
        POLog0
    ),
    {?TYPE, {POLog1, DWFlag1}}.

%% @doc Update a `pure_dwflag()'.
-spec mutate(pure_dwflag_op(), pure_type:id(), pure_dwflag()) ->
    {ok, pure_dwflag()}.
mutate(Op, VV, {?TYPE, {POLog, PureDWFlag}}) ->
    {Add, {?TYPE, {POLog0, PureDWFlag0}}} = pure_polog:remove_redundant({VV, Op}, {?TYPE, {POLog, PureDWFlag}}),
    case Add of
        false ->
            {ok, {?TYPE, {POLog0, PureDWFlag0}}};
        true ->
            {ok, {?TYPE, {orddict:store(VV, Op, POLog0), PureDWFlag0}}}
    end.

%% @doc Returns the value of the `pure_dwflag()'.
%%      This value is a a boolean value in the `pure_dwflag()'.
-spec query(pure_dwflag()) -> boolean().
query({?TYPE, {POLog0, PureDWFlag0}}) ->
    case orddict:is_empty(POLog0) of
        true ->
            PureDWFlag0;
        false ->
            Op1 = lists:last([Op || {_Key, Op} <- orddict:to_list(POLog0)]),
            case Op1 of
                enable ->
                    true;
                disable ->
                    false
            end
    end.


%% @doc Equality for `pure_dwflag()'.
-spec equal(pure_dwflag(), pure_dwflag()) -> boolean().
equal({?TYPE, {POLog1, PureDWFlag1}}, {?TYPE, {POLog2, PureDWFlag2}}) ->
    Fun = fun(Value1, Value2) -> Value1 == Value2 end,
    PureDWFlag1 == PureDWFlag2 andalso orddict_ext:equal(POLog1, POLog2, Fun).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {orddict:new(), true}}, new()).

redundant_test() ->
    ?assertEqual(1, redundant({[{0, 0}, {1, 0}], enable}, {[{0, 1}, {1, 1}], enable})),
    ?assertEqual(1, redundant({[{0, 0}, {1, 0}], enable}, {[{0, 1}, {1, 1}], disable})),
    ?assertEqual(1, redundant({[{0, 0}, {1, 0}], disable}, {[{0, 1}, {1, 1}], enable})),
    ?assertEqual(1, redundant({[{0, 0}, {1, 0}], disable}, {[{0, 1}, {1, 1}], disable})),
    ?assertEqual(0, redundant({[{0, 1}, {1, 0}], enable}, {[{0, 0}, {1, 1}], enable})),
    ?assertEqual(0, redundant({[{0, 1}, {1, 0}], disable}, {[{0, 0}, {1, 1}], disable})),
    ?assertEqual(2, redundant({[{0, 1}, {1, 0}], disable}, {[{0, 0}, {1, 1}], enable})),
    ?assertEqual(1, redundant({[{0, 1}, {1, 0}], enable}, {[{0, 0}, {1, 1}], disable})).

query_test() ->
    Flag0 = new(),
    Flag1 = {?TYPE, {[], false}},
    Flag2 = {?TYPE, {[], true}},
    Flag3 = {?TYPE, {[{[{1, 2}], enable}], false}},
    Flag4 = {?TYPE, {[{[{1, 3}], disable}], true}},
    ?assertEqual(true, query(Flag0)),
    ?assertEqual(false, query(Flag1)),
    ?assertEqual(true, query(Flag2)),
    ?assertEqual(true, query(Flag3)),
    ?assertEqual(false, query(Flag4)).

mutate_test() ->
    Flag0 = new(),
    {ok, Flag1} = mutate(enable, [{0, 1}, {1, 1}], Flag0),
    {ok, Flag2} = mutate(enable, [{0, 2}, {1, 2}], Flag1),
    {ok, Flag3} = mutate(enable, [{0, 4}, {1, 1}], Flag2),
    {ok, Flag4} = mutate(disable, [{0, 3}, {1, 2}], Flag3),
    ?assertEqual({?TYPE, {[{[{0, 1}, {1, 1}], enable}], true}}, Flag1),
    ?assertEqual({?TYPE, {[{[{0, 2}, {1, 2}], enable}], true}}, Flag2),
    ?assertEqual({?TYPE, {[{[{0, 2}, {1, 2}], enable}, {[{0, 4}, {1, 1}], enable}], true}}, Flag3),
    ?assertEqual({?TYPE, {[{[{0, 3}, {1, 2}], disable}], true}}, Flag4).

check_stability_test() ->
    Flag0 = new(),
    Flag1 = check_stability([], Flag0),
    ?assertEqual(Flag0, Flag1).

equal_test() ->
    Flag0 = {?TYPE, {[{[{0, 1}, {1, 1}], enable}], false}},
    Flag1 = {?TYPE, {[{[{0, 1}, {1, 1}], disable}], true}},
    Flag2 = {?TYPE, {[{[{0, 1}, {1, 1}], disable}], true}},
    Flag3 = {?TYPE, {[{[{0, 1}, {1, 1}], enable}], false}},
    ?assert(equal(Flag0, Flag3)),
    ?assert(equal(Flag1, Flag2)),
    ?assertNot(equal(Flag0, Flag1)),
    ?assertNot(equal(Flag2, Flag3)).

-endif.