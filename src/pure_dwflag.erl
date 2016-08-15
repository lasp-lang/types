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

%% @doc Pure DWFlag CRDT: pure op-based disable-wins flag
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_dwflag).
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

-export_type([pure_dwflag/0, pure_dwflag_op/0]).

-opaque pure_dwflag() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), boolean()}.
-type pure_dwflag_op() :: enable | disable.

%% @doc Create a new, empty `pure_dwflag()'
%% @doc Disable-wins flags start at True state
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
    atom().
redundant({VV1, Op1}, {VV2, Op2}) ->
    case pure_trcb:happened_before(VV1, VV2) of
        true ->
            ?RA; %% Op1 removed, Op2 added
        false -> %% VV1 and VV2 are concurrent
            case Op1 == Op2 of
                true ->
                    ?AA; %% Op1 stays, Op2 added
                false ->
                    case Op2 of
                        disable ->
                            ?RA; %% Op1 removed, Op2 added. Since disable wins
                        enable ->
                            ?AR %% Op1 stays, Op2 non added. Enable loses
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
            {Add1, {?TYPE, {POLog1, Flag}}}
    end.

%% @doc Removes redundant operations from POLog of `pure_dwflag()'
%% Called upon updating (enable, disable) the `pure_dwflag()'
-spec remove_redundant_crystal({pure_type:id(), pure_dwflag_op()}, pure_dwflag()) -> {boolean(), pure_dwflag()}.
remove_redundant_crystal({_VV1, _X}, {?TYPE, {POLog0, DWFlag}}) ->
    {true, {?TYPE, {POLog0, DWFlag}}}.

%% @doc Checks stable operations and remove them from POLog of `pure_dwflag()'
-spec check_stability(pure_type:id(), pure_dwflag()) -> pure_dwflag().
check_stability(_StableVV, {?TYPE, {POLog, DWFlag}}) ->
    {?TYPE, {POLog, DWFlag}}.

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

%% @doc Clear/reset the state to initial state.
-spec reset(pure_type:id(), pure_dwflag()) -> pure_dwflag().
reset(VV, {?TYPE, _}=CRDT) ->
    pure_type:reset(VV, CRDT).

%% @doc Returns the value of the `pure_dwflag()'.
%%      This value is a a boolean value in the `pure_dwflag()'.
-spec query(pure_dwflag()) -> boolean().
query({?TYPE, {POLog0, _PureDWFlag0}}) ->
    (orddict:is_empty(POLog0) orelse lists:member(enable, [Op || {_Key, Op} <- orddict:to_list(POLog0)])).



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
    ?assertEqual(?RA, redundant({[{0, 0}, {1, 0}], enable}, {[{0, 1}, {1, 1}], enable})),
    ?assertEqual(?RA, redundant({[{0, 0}, {1, 0}], enable}, {[{0, 1}, {1, 1}], disable})),
    ?assertEqual(?RA, redundant({[{0, 0}, {1, 0}], disable}, {[{0, 1}, {1, 1}], enable})),
    ?assertEqual(?RA, redundant({[{0, 0}, {1, 0}], disable}, {[{0, 1}, {1, 1}], disable})),
    ?assertEqual(?AA, redundant({[{0, 1}, {1, 0}], enable}, {[{0, 0}, {1, 1}], enable})),
    ?assertEqual(?AA, redundant({[{0, 1}, {1, 0}], disable}, {[{0, 0}, {1, 1}], disable})),
    ?assertEqual(?AR, redundant({[{0, 1}, {1, 0}], disable}, {[{0, 0}, {1, 1}], enable})),
    ?assertEqual(?RA, redundant({[{0, 1}, {1, 0}], enable}, {[{0, 0}, {1, 1}], disable})).

query_test() ->
    Flag0 = new(),
    Flag2 = {?TYPE, {[], true}},
    Flag3 = {?TYPE, {[{[{1, 2}], enable}], false}},
    Flag4 = {?TYPE, {[{[{1, 3}], disable}], true}},
    ?assertEqual(true, query(Flag0)),
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

reset_test() ->
    Flag1 = {?TYPE, {[{[{0, 2}, {1, 2}], enable}, {[{0, 4}, {1, 1}], enable}], false}},
    Flag2 = reset([{0, 5}, {1, 6}], Flag1),
    ?assertEqual({?TYPE, {[], true}}, Flag2).

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
