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

%% @doc Pure No Compaction CRDT: pure op-based implementation
%% of mutate and query without POLog compaction.
%%

-module(pure_no_compaction).
-author("Georges Younes <georges.r.younes@gmail.com>").


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([mutate/3, query/1]).

-type pure_op() :: term().

%% @doc Update with no polog compaction.
-spec mutate(pure_op(), pure_type:id(), pure_type:crdt()) ->
    {ok, pure_type:crdt()}.
mutate(Op, VV, {Type, {POLog, Crystal}}) ->
    {ok, {Type, {orddict:store(VV, Op, POLog), Crystal}}}.

%% @doc Specific query for no polog compaction pure data types.
-spec query(pure_type:crdt()) -> term().
query({pure_gcounter, {POLog0, _Crystal}}) ->
    orddict:fold(
        fun(_Key, Value, Acc) ->
            case Value of
                increment ->
                    Acc + 1;
                {increment, Val} ->
                    Acc + Val
            end
        end,
        0,
        POLog0
    );
query({pure_pncounter, {POLog0, _Crystal}}) ->
    orddict:fold(
        fun(_Key, Value, Acc) ->
            case Value of
                increment ->
                    Acc + 1;
                {increment, Val} ->
                    Acc + Val;
                decrement ->
                    Acc - 1;
                {decrement, Val} ->
                    Acc - Val
            end
        end,
        0,
        POLog0
    );
query({pure_gset, {POLog0, _Crystal}}) ->
    sets:from_list(orddict:fold(
        fun(_Key, {add, El}, Acc) ->
                ordsets:add_element(El, Acc)
        end,
        ordsets:new(),
        POLog0
    ));
query({pure_twopset, {POLog0, _Crystal}}) ->
    {Add, Rmv} = orddict:fold(
        fun(_Key, {Op, El}, {Acc1, Acc2}) ->
            case Op of
                add ->
                    {ordsets:add_element(El, Acc1), Acc2};
                rmv ->
                    {Acc1, ordsets:add_element(El, Acc2)}
            end
        end,
        {ordsets:new(), ordsets:new()},
        POLog0
    ),
    sets:from_list(ordsets:subtract(Add, Rmv));
query({pure_aworset, {POLog0, _Crystal}}) ->
    POLog1 = orddict:fold(
        fun(Key0, {Op0, El0}, Acc0) ->
            orddict:append(El0, {Key0, Op0}, Acc0)
        end,
        orddict:new(),
        POLog0
    ),
    sets:from_list(orddict:fold(
        fun(El1, [_|_], Acc1) ->
            ElOps = lists:last([L || {El, L} <- orddict:to_list(POLog1), El =:= El1]),
            case [VV || {VV, Op} <- ElOps, Op =:= add] of
                [] ->
                    Acc1;
                [_|_] ->
                    case [VV || {VV, Op} <- ElOps, Op =:= rmv] of
                        [] ->
                            ordsets:add_element(El1, Acc1);
                        [_|_] ->
                            Add = lists:last(lists:usort([VV || {VV, Op} <- ElOps, Op =:= add])),
                            Rmv = lists:last(lists:usort([VV || {VV, Op} <- ElOps, Op =:= rmv])),
                            case pure_trcb:happened_before(Add, Rmv) of
                                true ->
                                    Acc1;
                                false ->
                                    ordsets:add_element(El1, Acc1)
                            end
                    end
            end
        end,
        ordsets:new(),
        POLog1
    ));
query({pure_rworset, {POLog0, _Crystal}}) ->
    POLog1 = orddict:fold(
            fun(Key0, {Op0, El0}, Acc0) ->
                orddict:append(El0, {Key0, Op0}, Acc0)
            end,
            orddict:new(),
            POLog0
        ),
    sets:from_list(orddict:fold(
        fun(El1, [_|_], Acc1) ->
            ElOps = lists:last([L || {El, L} <- orddict:to_list(POLog1), El =:= El1]),
            case [VV || {VV, Op} <- ElOps, Op =:= add] of
                [] ->
                    Acc1;
                [_|_] ->
                    case [VV || {VV, Op} <- ElOps, Op =:= rmv] of
                        [] ->
                            ordsets:add_element(El1, Acc1);
                        [_|_] ->
                            Add = lists:last(lists:usort([VV || {VV, Op} <- ElOps, Op =:= add])),
                            Rmv = lists:last(lists:usort([VV || {VV, Op} <- ElOps, Op =:= rmv])),
                            case pure_trcb:happened_before(Rmv, Add) of
                                true ->
                                    ordsets:add_element(El1, Acc1);
                                false ->
                                    Acc1
                            end
                    end
            end
        end,
        ordsets:new(),
        POLog1
    ));
query({pure_ewflag, {POLog0, _Crystal}}) ->
    case [VV || {VV, Op} <- orddict:to_list(POLog0), Op =:= enable] of
        [] ->
            false;
        [_|_] ->
            case [VV || {VV, Op} <- orddict:to_list(POLog0), Op =:= disable] of
                [] ->
                    true;
                [_|_] ->
                    Enable = lists:last(lists:usort([VV || {VV, Op} <- orddict:to_list(POLog0), Op =:= enable])),
                    Disable = lists:last(lists:usort([VV || {VV, Op} <- orddict:to_list(POLog0), Op =:= disable])),
                    not pure_trcb:happened_before(Enable, Disable)
            end
    end;
query({pure_dwflag, {POLog0, _Crystal}}) ->
    case [VV || {VV, Op} <- orddict:to_list(POLog0), Op =:= disable] of
        [] ->
            true;
        [_|_] ->
            case [VV || {VV, Op} <- orddict:to_list(POLog0), Op =:= enable] of
                [] ->
                    false;
                [_|_] ->
                    Enable = lists:last(lists:usort([VV || {VV, Op} <- orddict:to_list(POLog0), Op =:= enable])),
                    Disable = lists:last(lists:usort([VV || {VV, Op} <- orddict:to_list(POLog0), Op =:= disable])),
                    pure_trcb:happened_before(Disable, Enable)
            end
    end;
query({pure_mvreg, {POLog0, _Crystal}}) ->
    case orddict:is_empty(POLog0) of
        true ->
            [];
        false ->
            LastVV = lists:last(lists:usort([VV || {VV, _Reg} <- orddict:to_list(POLog0)])),
            [Str || {VV, Str} <- orddict:to_list(POLog0), not pure_trcb:happened_before(VV, LastVV)]
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

query_test() ->
    AWORSet1 = {pure_aworset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 2}], {add, <<"b">>}}, {[{0, 2}, {1, 3}], {add, <<"c">>}}], []}},
    AWORSet2 = {pure_aworset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 2}], {add, <<"b">>}}, {[{0, 2}, {1, 3}], {add, <<"a">>}}], []}},
    AWORSet3 = {pure_aworset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 2}], {rmv, <<"a">>}}, {[{0, 2}, {1, 3}], {add, <<"b">>}}], []}},
    AWORSet4 = {pure_aworset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 0}], {rmv, <<"a">>}}, {[{0, 2}, {1, 3}], {rmv, <<"b">>}}], []}},
    AWORSet5 = {pure_aworset, {[], []}},
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>, <<"c">>]), query(AWORSet1)),
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>]), query(AWORSet2)),
    ?assertEqual(sets:from_list([<<"b">>]), query(AWORSet3)),
    ?assertEqual(sets:from_list([<<"a">>]), query(AWORSet4)),
    ?assertEqual(sets:from_list([]), query(AWORSet5)),
    RWORSet1 = {pure_rworset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 2}], {add, <<"b">>}}, {[{0, 2}, {1, 3}], {add, <<"c">>}}], []}},
    RWORSet2 = {pure_rworset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 2}], {add, <<"b">>}}, {[{0, 2}, {1, 3}], {add, <<"a">>}}], []}},
    RWORSet3 = {pure_rworset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 2}], {rmv, <<"a">>}}, {[{0, 2}, {1, 3}], {add, <<"b">>}}], []}},
    RWORSet4 = {pure_rworset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 0}], {rmv, <<"a">>}}, {[{0, 2}, {1, 3}], {rmv, <<"b">>}}], []}},
    RWORSet5 = {pure_rworset, {[], []}},
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>, <<"c">>]), query(RWORSet1)),
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>]), query(RWORSet2)),
    ?assertEqual(sets:from_list([<<"b">>]), query(RWORSet3)),
    ?assertEqual(sets:from_list([]), query(RWORSet4)),
    ?assertEqual(sets:from_list([]), query(RWORSet5)),
    EWFlag0 = {pure_ewflag, {[{[{0, 0}, {1, 1}], disable}, {[{0, 1}, {1, 2}], disable}, {[{0, 2}, {1, 3}], disable}], []}},
    EWFlag1 = {pure_ewflag, {[{[{0, 0}, {1, 1}], enable}, {[{0, 1}, {1, 2}], enable}, {[{0, 2}, {1, 3}], enable}], []}},
    EWFlag2 = {pure_ewflag, {[{[{0, 0}, {1, 1}], enable}, {[{0, 1}, {1, 0}], disable}], []}},
    EWFlag3 = {pure_ewflag, {[{[{0, 0}, {1, 1}], enable}, {[{0, 1}, {1, 2}], disable}, {[{0, 2}, {1, 3}], enable}], []}},
    EWFlag4 = {pure_ewflag, {[{[{0, 0}, {1, 1}], enable}, {[{0, 1}, {1, 0}], disable}, {[{0, 2}, {1, 3}], disable}], []}},
    EWFlag5 = {pure_ewflag, {[], []}},
    ?assertEqual(false, query(EWFlag0)),
    ?assertEqual(true, query(EWFlag1)),
    ?assertEqual(true, query(EWFlag2)),
    ?assertEqual(true, query(EWFlag3)),
    ?assertEqual(false, query(EWFlag4)),
    ?assertEqual(false, query(EWFlag5)),
    DWFlag0 = {pure_dwflag, {[{[{0, 0}, {1, 1}], disable}, {[{0, 1}, {1, 2}], disable}, {[{0, 2}, {1, 3}], disable}], []}},
    DWFlag1 = {pure_dwflag, {[{[{0, 0}, {1, 1}], enable}, {[{0, 1}, {1, 2}], enable}, {[{0, 2}, {1, 3}], enable}], []}},
    DWFlag2 = {pure_dwflag, {[{[{0, 0}, {1, 1}], enable}, {[{0, 1}, {1, 0}], disable}], []}},
    DWFlag3 = {pure_dwflag, {[{[{0, 0}, {1, 1}], enable}, {[{0, 1}, {1, 2}], disable}, {[{0, 2}, {1, 3}], enable}], []}},
    DWFlag4 = {pure_dwflag, {[{[{0, 0}, {1, 1}], enable}, {[{0, 1}, {1, 0}], disable}, {[{0, 2}, {1, 3}], disable}], []}},
    DWFlag5 = {pure_dwflag, {[], []}},
    ?assertEqual(false, query(DWFlag0)),
    ?assertEqual(true, query(DWFlag1)),
    ?assertEqual(false, query(DWFlag2)),
    ?assertEqual(true, query(DWFlag3)),
    ?assertEqual(false, query(DWFlag4)),
    ?assertEqual(true, query(DWFlag5)),
    MVReg0 = {pure_mvreg, {[{[{0, 1}, {1, 1}], "foo"}], []}},
    MVReg1 = {pure_mvreg, {[{[{0, 1}, {1, 1}], "foo"}, {[{0, 1}, {1, 2}], "bar"}], []}},
    MVReg2 = {pure_mvreg, {[{[{0, 2}, {1, 1}], "foo"}, {[{0, 1}, {1, 2}], "bar"}], []}},
    MVReg3 = {pure_mvreg, {[{[{0, 3}, {1, 4}], "foo"}, {[{0, 1}, {1, 1}], "foo"}, {[{0, 2}, {1, 3}], "bar"}], []}},
    MVReg4 = {pure_mvreg, {[], []}},
    ?assertEqual(["foo"], query(MVReg0)),
    ?assertEqual(["bar"], query(MVReg1)),
    ?assertEqual(["foo", "bar"], query(MVReg2)),
    ?assertEqual(["foo"], query(MVReg3)),
    ?assertEqual([], query(MVReg4)).

-endif.
