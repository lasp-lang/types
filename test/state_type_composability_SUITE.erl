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
        pair_with_pairs_with_gcounters_test,
        pair_with_gcounter_and_gset_test,
        pair_with_pairs_with_gcounter_and_gset_test,
        pair_with_gcounter_and_gmap_test,
        pair_with_gmap_and_pair_with_gcounter_and_gmap_test,
        pair_with_pair_with_gcounter_and_gmap_and_gmap_test,
        gmap_with_pair_test,
        gmap_with_awset_test,
        gmap_with_gmap_with_awset_test
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
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{Actor, 1}]}, {?GCOUNTER_TYPE, []}}}, Pair1),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{Actor, 1}]}, {?GCOUNTER_TYPE, [{Actor, 1}]}}}, Pair2),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{Actor, 2}]}, {?GCOUNTER_TYPE, [{Actor, 1}]}}}, Pair3),
    ?assertEqual({2, 1}, Query).

pair_with_gcounter_and_another_pair_test(_Config) ->
    Actor = "A",
    Pair0 = ?PAIR_TYPE:new([?GCOUNTER_TYPE, {?PAIR_TYPE, [?GCOUNTER_TYPE, ?GCOUNTER_TYPE]}]),
    {ok, Pair1} = ?PAIR_TYPE:mutate({fst, increment}, Actor, Pair0),
    {ok, Pair2} = ?PAIR_TYPE:mutate({snd, {fst, increment}}, Actor, Pair1),
    {ok, Pair3} = ?PAIR_TYPE:mutate({snd, {snd, increment}}, Actor, Pair2),
    Query = ?PAIR_TYPE:query(Pair3),

    ?assertEqual({?PAIR_TYPE, {
        {?GCOUNTER_TYPE, []},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair0),
    ?assertEqual({?PAIR_TYPE, {
        {?GCOUNTER_TYPE, [{Actor, 1}]},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair1),
    ?assertEqual({?PAIR_TYPE, {
        {?GCOUNTER_TYPE, [{Actor, 1}]},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair2),
    ?assertEqual({?PAIR_TYPE, {
        {?GCOUNTER_TYPE, [{Actor, 1}]},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GCOUNTER_TYPE, [{Actor, 1}]}
        }}
    }}, Pair3),
    ?assertEqual({1, {1, 1}}, Query).

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
    Query = ?PAIR_TYPE:query(Pair4),

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
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GCOUNTER_TYPE, []}
        }},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair1),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GCOUNTER_TYPE, [{Actor, 1}]}
        }},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair2),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GCOUNTER_TYPE, [{Actor, 1}]}
        }},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair3),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GCOUNTER_TYPE, [{Actor, 1}]}
        }},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GCOUNTER_TYPE, [{Actor, 1}]}
        }}
    }}, Pair4),
    ?assertEqual({{1, 1}, {1, 1}}, Query).

pair_with_gcounter_and_gset_test(_Config) ->
    Actor = "A",
    Pair0 = ?PAIR_TYPE:new([?GCOUNTER_TYPE, ?GSET_TYPE]),
    {ok, Pair1} = ?PAIR_TYPE:mutate({fst, increment}, Actor, Pair0),
    {ok, Pair2} = ?PAIR_TYPE:mutate({snd, {add, 17}}, Actor, Pair1),
    {ok, Pair3} = ?PAIR_TYPE:mutate({fst, increment}, Actor, Pair2),
    Query = ?PAIR_TYPE:query(Pair3),

    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, []}, {?GSET_TYPE, []}}}, Pair0),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{Actor, 1}]}, {?GSET_TYPE, []}}}, Pair1),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{Actor, 1}]}, {?GSET_TYPE, [17]}}}, Pair2),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{Actor, 2}]}, {?GSET_TYPE, [17]}}}, Pair3),
    ?assertEqual({2, sets:from_list([17])}, Query).

pair_with_pairs_with_gcounter_and_gset_test(_Config) ->
    Actor = "A",
    Pair0 = ?PAIR_TYPE:new([
        {?PAIR_TYPE, [?GCOUNTER_TYPE, ?GSET_TYPE]},
        {?PAIR_TYPE, [?GSET_TYPE, ?GCOUNTER_TYPE]}
    ]),
    {ok, Pair1} = ?PAIR_TYPE:mutate({fst, {fst, increment}}, Actor, Pair0),
    {ok, Pair2} = ?PAIR_TYPE:mutate({fst, {snd, {add, 17}}}, Actor, Pair1),
    {ok, Pair3} = ?PAIR_TYPE:mutate({snd, {fst, {add, 17}}}, Actor, Pair2),
    {ok, Pair4} = ?PAIR_TYPE:mutate({snd, {snd, increment}}, Actor, Pair3),
    Query = ?PAIR_TYPE:query(Pair4),

    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GSET_TYPE, []}
        }},
        {?PAIR_TYPE, {
            {?GSET_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair0),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GSET_TYPE, []}
        }},
        {?PAIR_TYPE, {
            {?GSET_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair1),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GSET_TYPE, [17]}
        }},
        {?PAIR_TYPE, {
            {?GSET_TYPE, []},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair2),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GSET_TYPE, [17]}
        }},
        {?PAIR_TYPE, {
            {?GSET_TYPE, [17]},
            {?GCOUNTER_TYPE, []}
        }}
    }}, Pair3),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GSET_TYPE, [17]}
        }},
        {?PAIR_TYPE, {
            {?GSET_TYPE, [17]},
            {?GCOUNTER_TYPE, [{Actor, 1}]}
        }}
    }}, Pair4),
    ?assertEqual({{1, sets:from_list([17])}, {sets:from_list([17]), 1}}, Query).

pair_with_gcounter_and_gmap_test(_Config) ->
    Actor = "A",
    Pair0 = ?PAIR_TYPE:new([?GCOUNTER_TYPE, {?GMAP_TYPE, [?BOOLEAN_TYPE]}]),
    {ok, Pair1} = ?PAIR_TYPE:mutate({fst, increment}, Actor, Pair0),
    {ok, Pair2} = ?PAIR_TYPE:mutate({snd, {Actor, true}}, Actor, Pair1),
    {ok, Pair3} = ?PAIR_TYPE:mutate({fst, increment}, Actor, Pair2),
    Query = ?PAIR_TYPE:query(Pair3),

    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, []}, {?GMAP_TYPE, {?BOOLEAN_TYPE, []}}}}, Pair0),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{Actor, 1}]}, {?GMAP_TYPE, {?BOOLEAN_TYPE, []}}}}, Pair1),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{Actor, 1}]}, {?GMAP_TYPE, {?BOOLEAN_TYPE, [{Actor, {?BOOLEAN_TYPE, true}}]}}}}, Pair2),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{Actor, 2}]}, {?GMAP_TYPE, {?BOOLEAN_TYPE, [{Actor, {?BOOLEAN_TYPE, true}}]}}}}, Pair3),
    ?assertEqual({2, [{Actor, true}]}, Query).

pair_with_gmap_and_pair_with_gcounter_and_gmap_test(_Config) ->
    Actor = "A",
    Pair0 = ?PAIR_TYPE:new([
        {?GMAP_TYPE, [?BOOLEAN_TYPE]},
        {?PAIR_TYPE, [
            ?GCOUNTER_TYPE, 
            {?GMAP_TYPE, [?BOOLEAN_TYPE]}
        ]}
    ]),
    {ok, Pair1} = ?PAIR_TYPE:mutate({fst, {Actor, true}}, Actor, Pair0),
    {ok, Pair2} = ?PAIR_TYPE:mutate({snd, {fst, increment}}, Actor, Pair1),
    {ok, Pair3} = ?PAIR_TYPE:mutate({snd, {snd, {Actor, true}}}, Actor, Pair2),
    Query = ?PAIR_TYPE:query(Pair3),

    ?assertEqual({?PAIR_TYPE, {
        {?GMAP_TYPE, {?BOOLEAN_TYPE, []}},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, []}}
        }}
    }}, Pair0),
    ?assertEqual({?PAIR_TYPE, {
        {?GMAP_TYPE, {?BOOLEAN_TYPE, [{Actor, {?BOOLEAN_TYPE, true}}]}},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, []}}
        }}
    }}, Pair1),
    ?assertEqual({?PAIR_TYPE, {
        {?GMAP_TYPE, {?BOOLEAN_TYPE, [{Actor, {?BOOLEAN_TYPE, true}}]}},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, []}}
        }}
    }}, Pair2),
    ?assertEqual({?PAIR_TYPE, {
        {?GMAP_TYPE, {?BOOLEAN_TYPE, [{Actor, {?BOOLEAN_TYPE, true}}]}},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, [{Actor, {?BOOLEAN_TYPE, true}}]}}
        }}
    }}, Pair3),
    ?assertEqual({
        [{Actor, true}],
        {1, [{Actor, true}]}
    }, Query).

pair_with_pair_with_gcounter_and_gmap_and_gmap_test(_Config) ->
    Actor = "A",
    Pair0 = ?PAIR_TYPE:new([
        {?PAIR_TYPE, [
            ?GCOUNTER_TYPE,
            {?GMAP_TYPE, [?BOOLEAN_TYPE]}
        ]},
        {?GMAP_TYPE, [?BOOLEAN_TYPE]}
    ]),
    {ok, Pair1} = ?PAIR_TYPE:mutate({snd, {Actor, true}}, Actor, Pair0),
    {ok, Pair2} = ?PAIR_TYPE:mutate({fst, {fst, increment}}, Actor, Pair1),
    {ok, Pair3} = ?PAIR_TYPE:mutate({fst, {snd, {Actor, true}}}, Actor, Pair2),
    Query = ?PAIR_TYPE:query(Pair3),

    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, []}}
        }},
        {?GMAP_TYPE, {?BOOLEAN_TYPE, []}}
    }}, Pair0),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, []}}
        }},
        {?GMAP_TYPE, {?BOOLEAN_TYPE, [{Actor, {?BOOLEAN_TYPE, true}}]}}
    }}, Pair1),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, []}}
        }},
        {?GMAP_TYPE, {?BOOLEAN_TYPE, [{Actor, {?BOOLEAN_TYPE, true}}]}}
    }}, Pair2),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, [{Actor, {?BOOLEAN_TYPE, true}}]}}
        }},
        {?GMAP_TYPE, {?BOOLEAN_TYPE, [{Actor, {?BOOLEAN_TYPE, true}}]}}
    }}, Pair3),
    ?assertEqual({
        {1, [{Actor, true}]},
        [{Actor, true}]
    }, Query).

gmap_with_pair_test(_Config) ->
    Actor = "A",
    CType = {?PAIR_TYPE, [?BOOLEAN_TYPE, ?BOOLEAN_TYPE]},
    GMap0 = ?GMAP_TYPE:new([CType]),
    {ok, GMap1} = ?GMAP_TYPE:mutate({Actor, {fst, true}}, Actor, GMap0),
    {ok, GMap2} = ?GMAP_TYPE:mutate({Actor, {snd, true}}, Actor, GMap0),
    {ok, GMap3} = ?GMAP_TYPE:mutate({Actor, {snd, true}}, Actor, GMap1),
    Query = ?GMAP_TYPE:query(GMap3),

    ?assertEqual({?GMAP_TYPE, {CType, []}}, GMap0),
    ?assertEqual({?GMAP_TYPE, {CType, [{Actor, {?PAIR_TYPE, {{?BOOLEAN_TYPE, true}, {?BOOLEAN_TYPE, false}}}}]}}, GMap1),
    ?assertEqual({?GMAP_TYPE, {CType, [{Actor, {?PAIR_TYPE, {{?BOOLEAN_TYPE, false}, {?BOOLEAN_TYPE, true}}}}]}}, GMap2),
    ?assertEqual({?GMAP_TYPE, {CType, [{Actor, {?PAIR_TYPE, {{?BOOLEAN_TYPE, true}, {?BOOLEAN_TYPE, true}}}}]}}, GMap3),
    ?assertEqual([{Actor, {true, true}}], Query).

gmap_with_awset_test(_Config) ->
    Actor = "A",
    CType = ?AWSET_TYPE,
    GMap0 = ?GMAP_TYPE:new([CType]),
    {ok, GMap1} = ?GMAP_TYPE:mutate({"hello", {add, 3}}, Actor, GMap0),
    {ok, GMap2} = ?GMAP_TYPE:mutate({"world", {add, 17}}, Actor, GMap1),
    {ok, GMap3} = ?GMAP_TYPE:mutate({"hello", {add, 13}}, Actor, GMap2),
    Query1 = ?GMAP_TYPE:query(GMap1),
    Query2 = ?GMAP_TYPE:query(GMap2),
    Query3 = ?GMAP_TYPE:query(GMap3),

    ?assertEqual([{"hello", sets:from_list([3])}], Query1),
    ?assertEqual([{"hello", sets:from_list([3])}, {"world", sets:from_list([17])}], Query2),
    ?assertEqual([{"hello", sets:from_list([3, 13])}, {"world", sets:from_list([17])}], Query3).

gmap_with_gmap_with_awset_test(_Config) ->
    Actor = "A",
    CType = {?GMAP_TYPE, [?AWSET_TYPE]},
    GMap0 = ?GMAP_TYPE:new([CType]),
    {ok, GMap1} = ?GMAP_TYPE:mutate({"hello", {"world_one", {add, 3}}}, Actor, GMap0),
    {ok, GMap2} = ?GMAP_TYPE:mutate({"hello", {"world_two", {add, 7}}}, Actor, GMap1),
    {ok, GMap3} = ?GMAP_TYPE:mutate({"world", {"hello", {add, 17}}}, Actor, GMap2),
    {ok, GMap4} = ?GMAP_TYPE:mutate({"hello", {"world_one", {add, 13}}}, Actor, GMap3),
    Query1 = ?GMAP_TYPE:query(GMap1),
    Query2 = ?GMAP_TYPE:query(GMap2),
    Query3 = ?GMAP_TYPE:query(GMap3),
    Query4 = ?GMAP_TYPE:query(GMap4),

    ?assertEqual([{"hello", [{"world_one", sets:from_list([3])}]}], Query1),
    ?assertEqual([{"hello", [{"world_one", sets:from_list([3])}, {"world_two", sets:from_list([7])}]}], Query2),
    ?assertEqual([{"hello", [{"world_one", sets:from_list([3])}, {"world_two", sets:from_list([7])}]}, {"world", [{"hello", sets:from_list([17])}]}], Query3),
    ?assertEqual([{"hello", [{"world_one", sets:from_list([3, 13])}, {"world_two", sets:from_list([7])}]}, {"world", [{"hello", sets:from_list([17])}]}], Query4).
