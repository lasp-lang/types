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
-compile([nowarn_export_all, export_all]).

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
        maps_within_maps_test,
        awmap_nested_rmv_test,
        gmap_with_arbitrary_nested_values_test
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
    {ok, Pair2} = ?PAIR_TYPE:mutate({snd, {apply, Actor, true}}, Actor, Pair1),
    {ok, Pair3} = ?PAIR_TYPE:mutate({fst, increment}, Actor, Pair2),
    Query = ?PAIR_TYPE:query(Pair3),

    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, []},
                               {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([])}}}}, Pair0),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{Actor, 1}]},
                               {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([])}}}}, Pair1),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{Actor, 1}]},
                               {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([{Actor, {?BOOLEAN_TYPE, 1}}])}}}}, Pair2),
    ?assertEqual({?PAIR_TYPE, {{?GCOUNTER_TYPE, [{Actor, 2}]},
                               {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([{Actor, {?BOOLEAN_TYPE, 1}}])}}}}, Pair3),
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
    {ok, Pair1} = ?PAIR_TYPE:mutate({fst, {apply, Actor, true}}, Actor, Pair0),
    {ok, Pair2} = ?PAIR_TYPE:mutate({snd, {fst, increment}}, Actor, Pair1),
    {ok, Pair3} = ?PAIR_TYPE:mutate({snd, {snd, {apply, Actor, true}}}, Actor, Pair2),
    Query = ?PAIR_TYPE:query(Pair3),

    ?assertEqual({?PAIR_TYPE, {
        {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([])}},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([])}}
        }}
    }}, Pair0),
    ?assertEqual({?PAIR_TYPE, {
        {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([{Actor, {?BOOLEAN_TYPE, 1}}])}},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([])}}
        }}
    }}, Pair1),
    ?assertEqual({?PAIR_TYPE, {
        {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([{Actor, {?BOOLEAN_TYPE, 1}}])}},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([])}}
        }}
    }}, Pair2),
    ?assertEqual({?PAIR_TYPE, {
        {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([{Actor, {?BOOLEAN_TYPE, 1}}])}},
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([{Actor, {?BOOLEAN_TYPE, 1}}])}}
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
    {ok, Pair1} = ?PAIR_TYPE:mutate({snd, {apply, Actor, true}}, Actor, Pair0),
    {ok, Pair2} = ?PAIR_TYPE:mutate({fst, {fst, increment}}, Actor, Pair1),
    {ok, Pair3} = ?PAIR_TYPE:mutate({fst, {snd, {apply, Actor, true}}}, Actor, Pair2),
    Query = ?PAIR_TYPE:query(Pair3),

    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([])}}
        }},
        {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([])}}
    }}, Pair0),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, []},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([])}}
        }},
        {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([{Actor, {?BOOLEAN_TYPE, 1}}])}}
    }}, Pair1),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([])}}
        }},
        {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([{Actor, {?BOOLEAN_TYPE, 1}}])}}
    }}, Pair2),
    ?assertEqual({?PAIR_TYPE, {
        {?PAIR_TYPE, {
            {?GCOUNTER_TYPE, [{Actor, 1}]},
            {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([{Actor, {?BOOLEAN_TYPE, 1}}])}}
        }},
        {?GMAP_TYPE, {?BOOLEAN_TYPE, dict:from_list([{Actor, {?BOOLEAN_TYPE, 1}}])}}
    }}, Pair3),
    ?assertEqual({
        {1, [{Actor, true}]},
        [{Actor, true}]
    }, Query).

gmap_with_pair_test(_Config) ->
    Actor = "A",
    CType = {?PAIR_TYPE, [?BOOLEAN_TYPE, ?BOOLEAN_TYPE]},
    GMap0 = ?GMAP_TYPE:new([CType]),
    {ok, GMap1} = ?GMAP_TYPE:mutate({apply, Actor, {fst, true}}, Actor, GMap0),
    {ok, GMap2} = ?GMAP_TYPE:mutate({apply, Actor, {snd, true}}, Actor, GMap0),
    {ok, GMap3} = ?GMAP_TYPE:mutate({apply, Actor, {snd, true}}, Actor, GMap1),
    Query = ?GMAP_TYPE:query(GMap3),

    ?assertEqual({?GMAP_TYPE, {CType, dict:from_list([])}}, GMap0),
    ?assertEqual({?GMAP_TYPE, {CType, dict:from_list([{Actor, {?PAIR_TYPE, {{?BOOLEAN_TYPE, 1}, {?BOOLEAN_TYPE, 0}}}}])}}, GMap1),
    ?assertEqual({?GMAP_TYPE, {CType, dict:from_list([{Actor, {?PAIR_TYPE, {{?BOOLEAN_TYPE, 0}, {?BOOLEAN_TYPE, 1}}}}])}}, GMap2),
    ?assertEqual({?GMAP_TYPE, {CType, dict:from_list([{Actor, {?PAIR_TYPE, {{?BOOLEAN_TYPE, 1}, {?BOOLEAN_TYPE, 1}}}}])}}, GMap3),
    ?assertEqual([{Actor, {true, true}}], Query).

maps_within_maps_test(_Config) ->
    lists:foreach(
        fun(MapType) ->
            map_with_awset(MapType),
            map_with_map_with_awset(MapType)
        end,
        [?GMAP_TYPE, ?AWMAP_TYPE]
    ).

awmap_nested_rmv_test(_Config) ->
    Actor = "A",
    CType = {?AWMAP_TYPE, [?AWSET_TYPE]},
    Map0 = ?AWMAP_TYPE:new([CType]),
    {ok, Map1} = ?AWMAP_TYPE:mutate({apply, "hello", {apply, "world_one", {add, 3}}}, Actor, Map0),
    {ok, Map2} = ?AWMAP_TYPE:mutate({apply, "hello", {apply, "world_two", {add, 7}}}, Actor, Map1),
    {ok, Map3} = ?AWMAP_TYPE:mutate({apply, "world", {apply, "hello", {add, 17}}}, Actor, Map2),
    {ok, Map4} = ?AWMAP_TYPE:mutate({apply, "hello", {rmv, "world_one"}}, Actor, Map3),
    {ok, Map5} = ?AWMAP_TYPE:mutate({rmv, "world"}, Actor, Map4),
    {ok, Map6} = ?AWMAP_TYPE:mutate({apply, "hello", {apply, "world_z", {add, 23}}}, Actor, Map5),
    {ok, Map7} = ?AWMAP_TYPE:mutate({apply, "hello", {rmv, "world_two"}}, Actor, Map6),
    {ok, Map8} = ?AWMAP_TYPE:mutate({apply, "hello", {rmv, "world_z"}}, Actor, Map7),
    Query3 = ?AWMAP_TYPE:query(Map3),
    Query4 = ?AWMAP_TYPE:query(Map4),
    Query5 = ?AWMAP_TYPE:query(Map5),
    Query6 = ?AWMAP_TYPE:query(Map6),
    Query7 = ?AWMAP_TYPE:query(Map7),
    Query8 = ?AWMAP_TYPE:query(Map8),

    ?assertEqual([{"hello", [{"world_one", sets:from_list([3])}, {"world_two", sets:from_list([7])}]}, {"world", [{"hello", sets:from_list([17])}]}], Query3),
    ?assertEqual([{"hello", [{"world_two", sets:from_list([7])}]}, {"world", [{"hello", sets:from_list([17])}]}], Query4),
    ?assertEqual([{"hello", [{"world_two", sets:from_list([7])}]}], Query5),
    ?assertEqual([{"hello", [{"world_two", sets:from_list([7])}, {"world_z", sets:from_list([23])}]}], Query6),
    ?assertEqual([{"hello", [{"world_z", sets:from_list([23])}]}], Query7),
    ?assertEqual([], Query8).

gmap_with_arbitrary_nested_values_test(_Config) ->
    Actor = "A",
    Map0 = ?GMAP_TYPE:new([?LWWREGISTER_TYPE]),
    {ok, Map1} = ?GMAP_TYPE:mutate({apply, "set", {?GMAP_TYPE, [?AWSET_TYPE]}, {apply, "key", {add, 3}}}, Actor, Map0),
    {ok, Map2} = ?GMAP_TYPE:mutate({apply, "reg", ?LWWREGISTER_TYPE, {set, 1, "hello"}}, Actor, Map1),
    {ok, Map3} = ?GMAP_TYPE:mutate({apply, "reg", {set, 2, "world"}}, Actor, Map2),

    Query1 = ?GMAP_TYPE:query(Map1),
    Query2 = ?GMAP_TYPE:query(Map2),
    Query3 = ?GMAP_TYPE:query(Map3),

    ?assertEqual([{"set", [{"key", sets:from_list([3])}]}], Query1),
    ?assertEqual([{"reg", "hello"}, {"set", [{"key", sets:from_list([3])}]}], Query2),
    ?assertEqual([{"reg", "world"}, {"set", [{"key", sets:from_list([3])}]}], Query3),

    {ok, Map4} = ?GMAP_TYPE:mutate({apply_all, [{"set", {?GMAP_TYPE, [?AWSET_TYPE]}, {apply, "key", {add, 3}}},
                                                {"reg", ?LWWREGISTER_TYPE, {set, 1, "hello"}},
                                                {"reg", {set, 2, "world"}}]}, Actor, Map0),

    ?assert(?GMAP_TYPE:equal(Map3, Map4)).


%% ===================================================================
%% Internal functions
%% ===================================================================

map_with_awset(MapType) ->
    Actor = "A",
    CType = ?AWSET_TYPE,
    Map0 = MapType:new([CType]),
    {ok, Map1} = MapType:mutate({apply, "hello", {add, 3}}, Actor, Map0),
    {ok, Map2} = MapType:mutate({apply, "world", {add, 17}}, Actor, Map1),
    {ok, Map3} = MapType:mutate({apply, "hello", {add, 13}}, Actor, Map2),
    Query1 = MapType:query(Map1),
    Query2 = MapType:query(Map2),
    Query3 = MapType:query(Map3),

    ?assertEqual([{"hello", sets:from_list([3])}], Query1),
    ?assertEqual([{"hello", sets:from_list([3])}, {"world", sets:from_list([17])}], Query2),
    ?assertEqual([{"hello", sets:from_list([3, 13])}, {"world", sets:from_list([17])}], Query3).

map_with_map_with_awset(MapType) ->
    Actor = "A",
    CType = {MapType, [?AWSET_TYPE]},
    Map0 = MapType:new([CType]),
    {ok, Map1} = MapType:mutate({apply, "hello", {apply, "world_one", {add, 3}}}, Actor, Map0),
    {ok, Map2} = MapType:mutate({apply, "hello", {apply, "world_two", {add, 7}}}, Actor, Map1),
    {ok, Map3} = MapType:mutate({apply, "world", {apply, "hello", {add, 17}}}, Actor, Map2),
    {ok, Map4} = MapType:mutate({apply, "hello", {apply, "world_one", {add, 13}}}, Actor, Map3),
    Query1 = MapType:query(Map1),
    Query2 = MapType:query(Map2),
    Query3 = MapType:query(Map3),
    Query4 = MapType:query(Map4),

    ?assertEqual([{"hello", [{"world_one", sets:from_list([3])}]}], Query1),
    ?assertEqual([{"hello", [{"world_one", sets:from_list([3])}, {"world_two", sets:from_list([7])}]}], Query2),
    ?assertEqual([{"hello", [{"world_one", sets:from_list([3])}, {"world_two", sets:from_list([7])}]}, {"world", [{"hello", sets:from_list([17])}]}], Query3),
    ?assertEqual([{"hello", [{"world_one", sets:from_list([3, 13])}, {"world_two", sets:from_list([7])}]}, {"world", [{"hello", sets:from_list([17])}]}], Query4).
