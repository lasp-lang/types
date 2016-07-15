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

-module(state_type_composability_SUITE).
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
		 pair_with_gcounters_test,
     pair_with_gcounter_and_another_pair_test,
     pair_with_pairs_with_gcounters_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

pair_with_gcounters_test(_Config) ->
    Actor = "A",
    Pair0 = ?PAIR_TYPE:new([?GCOUNTER_TYPE, ?GCOUNTER_TYPE]),
    {ok, Pair1} = ?PAIR_TYPE:mutate({fst, increment}, Actor, Pair0),
    {ok, Pair2} = ?PAIR_TYPE:mutate({snd, increment}, Actor, Pair1),
    {ok, Pair3} = ?PAIR_TYPE:mutate({fst, increment}, Actor, Pair2),
    Query = ?PAIR_TYPE:query(Pair3),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, []}, {?GCOUNTER_TYPE, []}}}, Pair0),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{"A", 1}]}, {?GCOUNTER_TYPE, []}}}, Pair1),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{"A", 1}]}, {?GCOUNTER_TYPE, [{"A", 1}]}}}, Pair2),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{"A", 2}]}, {?GCOUNTER_TYPE, [{"A", 1}]}}}, Pair3),
    ?assertEqual({2, 1}, Query).

pair_with_gcounter_and_another_pair_test(_Config) ->
    Actor = "A",
    Pair0 = ?PAIR_TYPE:new([?GCOUNTER_TYPE, {?PAIR_TYPE, [?GCOUNTER_TYPE, ?GCOUNTER_TYPE]}]),
    {ok, Pair1} = ?PAIR_TYPE:mutate({fst, increment}, Actor, Pair0),
    {ok, Pair2} = ?PAIR_TYPE:mutate({snd, {fst, increment}}, Actor, Pair1),
    {ok, Pair3} = ?PAIR_TYPE:mutate({snd, {snd, increment}}, Actor, Pair2),
    ?assertEqual({?PAIR_TYPE, {
        {?GCOUNTER_TYPE, []},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair0),
    ?assertEqual({?PAIR_TYPE, {
        {?GCOUNTER_TYPE, [{"A", 1}]},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair1),
    ?assertEqual({?PAIR_TYPE, {
        {?GCOUNTER_TYPE, [{"A", 1}]},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{"A", 1}]},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair2),
    ?assertEqual({?PAIR_TYPE, {
        {?GCOUNTER_TYPE, [{"A", 1}]},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{"A", 1}]},
            {?GCOUNTER_TYPE, [{"A", 1}]}
        }}
    }}, Pair3).

pair_with_pairs_with_gcounters_test(_Config) ->
    Actor = "A",
    Pair0 = ?PAIR_TYPE:new([
        {?PAIR_TYPE, [?GCOUNTER_TYPE, ?GCOUNTER_TYPE]},
        {?PAIR_TYPE, [?GCOUNTER_TYPE, ?GCOUNTER_TYPE]}
    ]),
    {ok, Pair1} = ?PAIR_TYPE:mutate({fst, {fst, increment}}, Actor, Pair0),
    {ok, Pair2} = ?PAIR_TYPE:mutate({fst, {snd, increment}}, Actor, Pair1),
    {ok, Pair3} = ?PAIR_TYPE:mutate({snd, {fst, increment}}, Actor, Pair2),
    {ok, Pair4} = ?PAIR_TYPE:mutate({snd, {snd, increment}}, Actor, Pair3),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair0),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{"A", 1}]},
            {?GCOUNTER_TYPE, []}
        }},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair1),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{"A", 1}]},
            {?GCOUNTER_TYPE, [{"A", 1}]}
        }},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair2),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{"A", 1}]},
            {?GCOUNTER_TYPE, [{"A", 1}]}
        }},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{"A", 1}]},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair3),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{"A", 1}]},
            {?GCOUNTER_TYPE, [{"A", 1}]}
        }},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{"A", 1}]},
            {?GCOUNTER_TYPE, [{"A", 1}]}
        }}
    }}, Pair4).

