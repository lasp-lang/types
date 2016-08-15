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

%% @doc Pure EWFlag CRDT: pure op-based enable-wins flag
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_ewflag).
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

-export_type([pure_ewflag/0, pure_ewflag_op/0]).

-opaque pure_ewflag() :: {?TYPE, payload()}.
-type payload() :: {pure_type:polog(), boolean()}.
-type pure_ewflag_op() :: enable | disable.

%% @doc Create a new, empty `pure_ewflag()'
-spec new() -> pure_ewflag().
new() ->
    {?TYPE, {orddict:new(), false}}.

%% @doc Create a new, empty `pure_ewflag()'
-spec new([term()]) -> pure_ewflag().
new([]) ->
    new().

%% @doc Check redundancy `pure_ewflag()'
%% Called in remove_redundant().
-spec redundant({pure_type:id(), pure_ewflag_op()}, {pure_type:id(), pure_ewflag_op()}) ->
    atom().
redundant({VV1, Op1}, {VV2, Op2}) ->
    case pure_trcb:happened_before(VV1, VV2) of
        true ->
            ?RA;
        false ->
            case Op1 == Op2 of
                true ->
                    ?AA;
                false ->
                    case Op2 of
                        enable ->
                            ?RA;
                        disable ->
                            ?AR
                    end
            end
    end.

%% @doc Removes redundant operations from POLog of `pure_ewflag()'
%% Called upon updating (enable, disable) the `pure_ewflag()'
-spec remove_redundant_polog({pure_type:id(), pure_ewflag_op()}, pure_ewflag()) -> {boolean(), pure_ewflag()}.
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

%% @doc Removes redundant operations from crystal of `pure_ewflag()'
%% No crystalisation for this one.
-spec remove_redundant_crystal({pure_type:id(), pure_ewflag_op()}, pure_ewflag()) -> {boolean(), pure_ewflag()}.
remove_redundant_crystal({_VV1, _X}, {?TYPE, {POLog0, EWFlag}}) ->
    {true, {?TYPE, {POLog0, EWFlag}}}.

%% @doc Checks stable operations and remove them from POLog of `pure_ewflag()'
-spec check_stability(pure_type:id(), pure_ewflag()) -> pure_ewflag().
check_stability(_StableVV, {?TYPE, {POLog, EWFlag}}) ->
    {?TYPE, {POLog, EWFlag}}.

%% @doc Update a `pure_ewflag()'.
-spec mutate(pure_ewflag_op(), pure_type:id(), pure_ewflag()) ->
    {ok, pure_ewflag()}.
mutate(Op, VV, {?TYPE, {POLog, PureEWFlag}}) ->
    {Add, {?TYPE, {POLog0, PureEWFlag0}}} = pure_polog:remove_redundant({VV, Op}, {?TYPE, {POLog, PureEWFlag}}),
    case Add of
        false ->
            {ok, {?TYPE, {POLog0, PureEWFlag0}}};
        true ->
            {ok, {?TYPE, {orddict:store(VV, Op, POLog0), PureEWFlag0}}}
    end.

%% @doc Clear/reset the state to initial state.
-spec reset(pure_type:id(), pure_ewflag()) -> pure_ewflag().
reset(VV, {?TYPE, _}=CRDT) ->
    pure_type:reset(VV, CRDT).

%% @doc Returns the value of the `pure_ewflag()'.
%%      This value is a a boolean value in the `pure_ewflag()'.
-spec query(pure_ewflag()) -> boolean().
query({?TYPE, {POLog0, _PureEWFlag0}}) ->
    not (orddict:is_empty(POLog0) orelse lists:member(disable, [Op || {_Key, Op} <- orddict:to_list(POLog0)])).

%% @doc Equality for `pure_ewflag()'.
-spec equal(pure_ewflag(), pure_ewflag()) -> boolean().
equal({?TYPE, {POLog1, PureEWFlag1}}, {?TYPE, {POLog2, PureEWFlag2}}) ->
    Fun = fun(Value1, Value2) -> Value1 == Value2 end,
    PureEWFlag1 == PureEWFlag2 andalso orddict_ext:equal(POLog1, POLog2, Fun).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, {orddict:new(), false}}, new()).

redundant_test() ->
    ?assertEqual(?RA, redundant({[{0, 0}, {1, 0}], enable}, {[{0, 1}, {1, 1}], enable})),
    ?assertEqual(?RA, redundant({[{0, 0}, {1, 0}], enable}, {[{0, 1}, {1, 1}], disable})),
    ?assertEqual(?RA, redundant({[{0, 0}, {1, 0}], disable}, {[{0, 1}, {1, 1}], enable})),
    ?assertEqual(?RA, redundant({[{0, 0}, {1, 0}], disable}, {[{0, 1}, {1, 1}], disable})),
    ?assertEqual(?AA, redundant({[{0, 1}, {1, 0}], enable}, {[{0, 0}, {1, 1}], enable})),
    ?assertEqual(?AA, redundant({[{0, 1}, {1, 0}], disable}, {[{0, 0}, {1, 1}], disable})),
    ?assertEqual(?RA, redundant({[{0, 1}, {1, 0}], disable}, {[{0, 0}, {1, 1}], enable})),
    ?assertEqual(?AR, redundant({[{0, 1}, {1, 0}], enable}, {[{0, 0}, {1, 1}], disable})).

query_test() ->
    Flag0 = new(),
    Flag1 = {?TYPE, {[], false}},
    Flag3 = {?TYPE, {[{[{1, 2}], enable}], false}},
    Flag4 = {?TYPE, {[{[{1, 3}], disable}], true}},
    ?assertEqual(false, query(Flag0)),
    ?assertEqual(false, query(Flag1)),
    ?assertEqual(true, query(Flag3)),
    ?assertEqual(false, query(Flag4)).

mutate_test() ->
    Flag0 = new(),
    {ok, Flag1} = mutate(enable, [{0, 1}, {1, 1}], Flag0),
    {ok, Flag2} = mutate(enable, [{0, 2}, {1, 2}], Flag1),
    {ok, Flag3} = mutate(enable, [{0, 4}, {1, 1}], Flag2),
    {ok, Flag4} = mutate(disable, [{0, 3}, {1, 2}], Flag3),
    ?assertEqual({?TYPE, {[{[{0, 1}, {1, 1}], enable}], false}}, Flag1),
    ?assertEqual({?TYPE, {[{[{0, 2}, {1, 2}], enable}], false}}, Flag2),
    ?assertEqual({?TYPE, {[{[{0, 2}, {1, 2}], enable}, {[{0, 4}, {1, 1}], enable}], false}}, Flag3),
    ?assertEqual({?TYPE, {[{[{0, 4}, {1, 1}], enable}], false}}, Flag4).

reset_test() ->
    Flag1 = {?TYPE, {[{[{0, 2}, {1, 2}], enable}, {[{0, 4}, {1, 1}], enable}], true}},
    Flag2 = reset([{0, 5}, {1, 6}], Flag1),
    ?assertEqual({?TYPE, {[], false}}, Flag2).

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
