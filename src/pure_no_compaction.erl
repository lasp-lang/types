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
query({pure_awset, {POLog0, _Crystal}}) ->
    POLog1 = orddict:fold(
        fun(Key0, {Op0, El0}, Acc0) ->
            case orddict:fold(
                fun(Key1, {Op1, El1}, Acc1) ->
                    case El0 =:= El1 andalso Op0 =:= add andalso Op1 =:= rmv of
                        true ->
                            Acc1 andalso not pure_trcb:happened_before(Key0, Key1);
                        false ->
                            Acc1
                    end
                end,
                true,
                POLog0
            ) of
                true ->
                    orddict:store(Key0, {Op0, El0}, Acc0);
                false ->
                    Acc0
            end
        end,
        orddict:new(),
        POLog0
    ),
    sets:from_list([El || {_Key, {Op, El}} <- orddict:to_list(POLog1), Op =:= add]);
query({pure_rwset, {POLog0, _Crystal}}) ->
    POLog1 = orddict:fold(
        fun(Key0, {Op0, El0}, Acc0) ->
            case orddict:fold(
                fun(Key1, {Op1, El1}, Acc1) ->
                    case El0 =:= El1 andalso Op0 =:= add andalso Op1 =:= rmv of
                        true ->
                            Acc1 andalso pure_trcb:happened_before(Key1, Key0);
                        false ->
                            Acc1
                    end
                end,
                true,
                POLog0
            ) of
                true ->
                    orddict:store(Key0, {Op0, El0}, Acc0);
                false ->
                    Acc0
            end
        end,
        orddict:new(),
        POLog0
    ),
    sets:from_list([El || {_Key, {Op, El}} <- orddict:to_list(POLog1), Op =:= add]);
query({pure_ewflag, {POLog0, _Crystal}}) ->
    POLog1 = orddict:fold(
        fun(Key0, Op0, Acc0) ->
            case orddict:fold(
                fun(Key1, _Op1, Acc1) ->
                    Acc1 andalso not pure_trcb:happened_before(Key0, Key1)
                end,
                true,
                POLog0
            ) of
                true ->
                    orddict:store(Key0, Op0, Acc0);
                false ->
                    Acc0
            end
        end,
        orddict:new(),
        POLog0
    ),
    case [Op || {_Key, Op} <- orddict:to_list(POLog1), Op =:= enable] of
        [] ->
            false;
        [_|_] ->
            true
    end;
query({pure_dwflag, {POLog0, _Crystal}}) ->
    case orddict:is_empty(POLog0) of
        true ->
            true;
        false ->
            POLog1 = orddict:fold(
                fun(Key0, Op0, Acc0) ->
                    case orddict:fold(
                        fun(Key1, Op1, Acc1) ->
                            case Op0 =:= enable andalso Op1 =:= disable of
                                true ->
                                    Acc1 andalso pure_trcb:happened_before(Key1, Key0);
                                false ->
                                    Acc1
                            end
                        end,
                        true,
                        POLog0
                    ) of
                        true ->
                            orddict:store(Key0, Op0, Acc0);
                        false ->
                            Acc0
                    end
                end,
                orddict:new(),
                POLog0
            ),
            case [Op || {_Key, Op} <- orddict:to_list(POLog1), Op =:= enable] of
                [] ->
                    false;
                [_|_] ->
                    true
            end
    end;
query({pure_mvregister, {POLog0, _Crystal}}) ->
    POLog1 = orddict:fold(
        fun(Key0, Op0, Acc0) ->
            case orddict:fold(
                fun(Key1, _Op1, Acc1) ->
                    Acc1 andalso not pure_trcb:happened_before(Key0, Key1)
                end,
                true,
                POLog0
            ) of
                true ->
                    orddict:store(Key0, Op0, Acc0);
                false ->
                    Acc0
            end
        end,
        orddict:new(),
        POLog0
    ),
    sets:from_list([Reg || {_Key, Reg} <- orddict:to_list(POLog1)]).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

query_test() ->
    AWSet1 = {pure_awset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 2}], {add, <<"b">>}}, {[{0, 2}, {1, 3}], {add, <<"c">>}}], []}},
    AWSet2 = {pure_awset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 2}], {add, <<"b">>}}, {[{0, 2}, {1, 3}], {add, <<"a">>}}], []}},
    AWSet3 = {pure_awset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 2}], {rmv, <<"a">>}}, {[{0, 2}, {1, 3}], {add, <<"b">>}}], []}},
    AWSet4 = {pure_awset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 0}], {rmv, <<"a">>}}, {[{0, 2}, {1, 3}], {rmv, <<"b">>}}], []}},
    AWSet5 = {pure_awset, {[], []}},
    AWSet6 = {pure_awset, {[{[{0, 5}, {1, 1}], {add, <<"a">>}}, {[{0, 6}, {1, 1}], {rmv, <<"a">>}}, {[{0, 1}, {1, 5}], {add, <<"a">>}}], []}},
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>, <<"c">>]), query(AWSet1)),
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>]), query(AWSet2)),
    ?assertEqual(sets:from_list([<<"b">>]), query(AWSet3)),
    ?assertEqual(sets:from_list([<<"a">>]), query(AWSet4)),
    ?assertEqual(sets:from_list([]), query(AWSet5)),
    ?assertEqual(sets:from_list([<<"a">>]), query(AWSet6)),
    RWSet1 = {pure_rwset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 2}], {add, <<"b">>}}, {[{0, 2}, {1, 3}], {add, <<"c">>}}], []}},
    RWSet2 = {pure_rwset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 2}], {add, <<"b">>}}, {[{0, 2}, {1, 3}], {add, <<"a">>}}], []}},
    RWSet3 = {pure_rwset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 2}], {rmv, <<"a">>}}, {[{0, 2}, {1, 3}], {add, <<"b">>}}], []}},
    RWSet4 = {pure_rwset, {[{[{0, 0}, {1, 1}], {add, <<"a">>}}, {[{0, 1}, {1, 0}], {rmv, <<"a">>}}, {[{0, 2}, {1, 3}], {rmv, <<"b">>}}], []}},
    RWSet5 = {pure_rwset, {[], []}},
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>, <<"c">>]), query(RWSet1)),
    ?assertEqual(sets:from_list([<<"a">>, <<"b">>]), query(RWSet2)),
    ?assertEqual(sets:from_list([<<"b">>]), query(RWSet3)),
    ?assertEqual(sets:from_list([]), query(RWSet4)),
    ?assertEqual(sets:from_list([]), query(RWSet5)),
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
    MVReg0 = {pure_mvregister, {[{[{0, 1}, {1, 1}], "foo"}], []}},
    MVReg1 = {pure_mvregister, {[{[{0, 1}, {1, 1}], "foo"}, {[{0, 1}, {1, 2}], "bar"}], []}},
    MVReg2 = {pure_mvregister, {[{[{0, 2}, {1, 1}], "foo"}, {[{0, 1}, {1, 2}], "bar"}], []}},
    MVReg3 = {pure_mvregister, {[{[{0, 3}, {1, 4}], "foo"}, {[{0, 1}, {1, 1}], "foo"}, {[{0, 2}, {1, 3}], "bar"}], []}},
    MVReg4 = {pure_mvregister, {[], []}},
    ?assertEqual(sets:from_list(["foo"]), query(MVReg0)),
    ?assertEqual(sets:from_list(["bar"]), query(MVReg1)),
    ?assertEqual(sets:from_list(["foo", "bar"]), query(MVReg2)),
    ?assertEqual(sets:from_list(["foo"]), query(MVReg3)),
    ?assertEqual(sets:from_list([]), query(MVReg4)).

-endif.
