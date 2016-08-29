%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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
%%

-module(state_type_semantics_SUITE).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

%% common_test callbacks
-export([init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-compile([export_all]).

-include("state_type.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.
init_per_testcase(_Case, Config) -> Config.
end_per_testcase(_Case, Config) -> Config.

all() ->
    [
        flag_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

flag_test(_Config) ->
    List = [
        {state_ewflag, true},
        {state_dwflag, false}
    ],

    lists:foreach(
        fun({FlagType, ExpectedValue}) ->
            {Value1, Value2} = flag_mutate_concurrently(FlagType),
            ?assert(Value1 == Value2),
            ?assertEqual(ExpectedValue, Value1)
        end,
        List
    ).

%% ===================================================================
%% Internal functions
%% ===================================================================

flag_mutate_concurrently(FlagType) ->
    ActorA = "A",
    ActorB = "B",
    ActorC = "C",
    Flag0 = FlagType:new(),
    {ok, EFlag} = FlagType:mutate(enable, ActorC, Flag0),
    {ok, DFlag} = FlagType:mutate(disable, ActorC, Flag0),
    ?assertEqual(true, FlagType:query(EFlag)),
    ?assertEqual(false, FlagType:query(DFlag)),

    %% concurrent mutations in enabled flag
    {ok, EFlagA} = FlagType:mutate(enable, ActorA, EFlag),
    {ok, EFlagB} = FlagType:mutate(disable, ActorB, EFlag),
    ?assertEqual(true, FlagType:query(EFlagA)),
    ?assertEqual(false, FlagType:query(EFlagB)),
    %% merge them
    EFlagMerged = FlagType:merge(EFlagA, EFlagB),

    %% concurrent mutations in disabled flag
    {ok, DFlagA} = FlagType:mutate(enable, ActorA, DFlag),
    {ok, DFlagB} = FlagType:mutate(disable, ActorB, DFlag),
    ?assertEqual(true, FlagType:query(DFlagA)),
    ?assertEqual(false, FlagType:query(DFlagB)),
    %% merge them
    DFlagMerged = FlagType:merge(DFlagA, DFlagB),

    %% return query values
    {
        FlagType:query(EFlagMerged),
        FlagType:query(DFlagMerged)
    }.
