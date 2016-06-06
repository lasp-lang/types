%% -------------------------------------------------------------------
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

%% @doc Common library for causal CRDTs.
%%
%% @reference Paulo Sérgio Almeida, Ali Shoker, and Carlos Baquero
%% Delta State Replicated Data Types (2016)
%% [http://arxiv.org/pdf/1603.01529v1.pdf]

-module(state_causal_type).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

%% causal_crdt() related.
-export([new_causal_crdt/1,
         causal_join/2,
         is_lattice_inflation/2,
         is_lattice_strict_inflation/2]).

%% data_store() related.
-export([new_data_store/1,
         get_dot_cloud/1,
         is_bottom_data_store/1]).
%% dot_cloud() related.
-export([get_next_dot_context/2,
         insert_dot_context/2,
         merge_dot_clouds/2]).
%% {{dot_map, data_store_type()}, dot_map()} related.
-export([get_sub_data_store/2,
         insert_object/3,
         remove_object/2,
         get_all_objects/1,
         get_objects_count/1]).

-export_type([dot_context/0,
              dot_cloud/0,
              data_store/0,
              data_store_type/0,
              causal_crdt/0]).

-type dot_actor() :: term().
-type dot_context() :: {dot_actor(), pos_integer()}.
-type dot_cloud() :: ordsets:ordsets(dot_context()).
-type dot_set() :: dot_cloud().
-type dot_fun() :: orddict:orddict(dot_context(), term()).
-type dot_map() :: orddict:orddict(term(), data_store()).
-type data_store_type() :: dot_set | dot_fun | {dot_map, data_store_type()}.
-type data_store() :: {dot_set, dot_set()}
                    | {dot_fun, dot_fun()}
                    | {{dot_map, data_store_type()}, dot_map()}.
-type causal_crdt() :: {data_store(), dot_cloud()}.

%% @doc Create an empty CausalCRDT.
-spec new_causal_crdt(data_store_type()) -> causal_crdt().
new_causal_crdt(dot_set) ->
    {{dot_set, ordsets:new()}, ordsets:new()};
new_causal_crdt(dot_fun) ->
    {{dot_fun, orddict:new()}, ordsets:new()};
new_causal_crdt({dot_map, ValueDataStoreType}) ->
    {{{dot_map, ValueDataStoreType}, orddict:new()}, ordsets:new()}.

%% @doc Universal causal join for a two CausalCRDTs.
-spec causal_join(causal_crdt(), causal_crdt()) -> causal_crdt().
causal_join({DataStore, DotCloud}, {DataStore, DotCloud}) ->
    {DataStore, DotCloud};
causal_join({{dot_set, DataStoreA}, DotCloudA},
            {{dot_set, DataStoreB}, DotCloudB}) ->
    {{dot_set, ordsets:union([ordsets:intersection(DataStoreA, DataStoreB)] ++
                             [ordsets:subtract(DataStoreA, DotCloudB)] ++
                             [ordsets:subtract(DataStoreB, DotCloudA)])},
     ordsets:union(DotCloudA, DotCloudB)};
causal_join({{dot_fun, DataStoreA}=_Fst, DotCloudA},
            {{dot_fun, DataStoreB}=Snd, DotCloudB}) ->
    DomainSnd = get_dot_cloud(Snd),
    InterDataStore =
        orddict:fold(fun(DotContext, Object, {dot_fun, DataStore}) ->
                             case lists:member(DotContext, DomainSnd) of
                                 true ->
                                     {dot_fun, orddict:store(DotContext,
                                                             Object,
                                                             DataStore)};
                                 false ->
                                     {dot_fun, DataStore}
                             end
                     end, new_data_store(dot_fun), DataStoreA),
    MergedDataStore0 =
        orddict:fold(fun(DotContext, Object, {dot_fun, DataStore}) ->
                             case ordsets:is_element(DotContext, DotCloudB) of
                                 true ->
                                     {dot_fun, DataStore};
                                 false ->
                                     {dot_fun, orddict:store(DotContext,
                                                             Object,
                                                             DataStore)}
                             end
                     end, InterDataStore, DataStoreA),
    MergedDataStore1 =
        orddict:fold(fun(DotContext, Object, {dot_fun, DataStore}) ->
                             case ordsets:is_element(DotContext, DotCloudA) of
                                 true ->
                                     {dot_fun, DataStore};
                                 false ->
                                     {dot_fun, orddict:store(DotContext,
                                                             Object,
                                                             DataStore)}
                             end
                     end, MergedDataStore0, DataStoreB),
    {MergedDataStore1, ordsets:union(DotCloudA, DotCloudB)};
causal_join({{{dot_map, ValueDataStoreType}, DataStoreA}=Fst, DotCloudA},
            {{{dot_map, ValueDataStoreType}, DataStoreB}=Snd, DotCloudB}) ->
    UnionObjects = ordsets:union(ordsets:from_list(orddict:fetch_keys(DataStoreA)),
                                 ordsets:from_list(orddict:fetch_keys(DataStoreB))),
    MergedDataStore =
        ordsets:fold(fun(Object, MergedDataStore0) ->
                             {ok, SubDataStoreA} = get_sub_data_store(Object, Fst),
                             {ok, SubDataStoreB} = get_sub_data_store(Object, Snd),
                             {MergedSubDataStore, _} =
                                 causal_join({SubDataStoreA, DotCloudA},
                                             {SubDataStoreB, DotCloudB}),
                             case is_bottom_data_store(MergedSubDataStore) of
                                 true ->
                                     MergedDataStore0;
                                 false ->
                                     insert_object(Object,
                                                   MergedSubDataStore,
                                                   MergedDataStore0)
                             end
                     end, new_data_store({dot_map, ValueDataStoreType}), UnionObjects),
    {MergedDataStore, ordsets:union(DotCloudA, DotCloudB)}.

%% @doc Determine if a change for a given causal crdt is an inflation or not.
%%
%% Given a particular causal crdt and two instances of that causal crdt,
%% determine if `B(second)' is an inflation of `A(first)'.
-spec is_lattice_inflation(causal_crdt(), causal_crdt()) -> boolean().
is_lattice_inflation({_DataStoreA, DotCloudA}, {_DataStoreB, DotCloudB}) ->
    DotCloudAList = get_maxs_for_all(DotCloudA),
    DotCloudBList = get_maxs_for_all(DotCloudB),
    lists:foldl(fun({DotActor, Count}, Acc) ->
                        case lists:keyfind(DotActor, 1, DotCloudBList) of
                            false ->
                                Acc andalso false;
                            {_DotActor1, Count1} ->
                                Acc andalso (Count =< Count1)
                        end
                end, true, DotCloudAList).

%% @doc Determine if a change for a given causal crdt is a strict inflation or not.
%%
%% Given a particular causal crdt and two instances of that causal crdt,
%% determine if `B(second)' is a strict inflation of `A(first)'.
-spec is_lattice_strict_inflation(causal_crdt(), causal_crdt()) -> boolean().
is_lattice_strict_inflation(CausalCRDTA, CausalCRDTB) ->
    is_lattice_inflation(CausalCRDTA, CausalCRDTB) andalso
        not is_lattice_inflation(CausalCRDTB, CausalCRDTA).

%% @doc Create an empty DataStore.
new_data_store(dot_set) ->
    {dot_set, ordsets:new()};
new_data_store(dot_fun) ->
    {dot_fun, orddict:new()};
new_data_store({dot_map, ValueDataStoreType}) ->
    {{dot_map, ValueDataStoreType}, orddict:new()}.

%% @doc Get DotCloud from DataStore.
-spec get_dot_cloud(data_store()) -> dot_cloud().
get_dot_cloud({dot_set, DataStore}) ->
    DataStore;
get_dot_cloud({dot_fun, DataStore}) ->
    ordsets:from_list(orddict:fetch_keys(DataStore));
get_dot_cloud({{dot_map, _ValueDataStoreType}, DataStore}) ->
    orddict:fold(fun(_Object, SubDataStore, DotCloud) ->
                         ordsets:union(DotCloud, get_dot_cloud(SubDataStore))
                 end, ordsets:new(), DataStore).

%% @doc Check whether the DataStore is empty.
-spec is_bottom_data_store(data_store()) -> boolean().
is_bottom_data_store({dot_set, DataStore}) ->
    ordsets:size(DataStore) == 0;
is_bottom_data_store({dot_fun, DataStore}) ->
    orddict:is_empty(DataStore);
is_bottom_data_store({{dot_map, _ValueDataStoreType}, DataStore}) ->
    orddict:is_empty(DataStore).

%% @doc Get the Actor's next DotContext from DotCloud.
-spec get_next_dot_context(dot_actor(), dot_cloud()) -> dot_context().
get_next_dot_context(DotActor, DotCloud) ->
    MaxCounter = ordsets:fold(fun({DotActor0, DotCounter0}, MaxValue0) ->
                                      case (DotActor0 == DotActor) andalso
                                              (DotCounter0 > MaxValue0) of
                                          true ->
                                              DotCounter0;
                                          false ->
                                              MaxValue0
                                      end
                              end, 0, DotCloud),
    {DotActor, MaxCounter + 1}.

%% @doc Insert a dot to DotCloud.
-spec insert_dot_context(dot_context(), dot_cloud()) -> dot_cloud().
insert_dot_context(DotContext, DotCloud) ->
    ordsets:add_element(DotContext, DotCloud).

%% @doc Merge two DotClouds.
-spec merge_dot_clouds(dot_cloud(), dot_cloud()) -> dot_cloud().
merge_dot_clouds(DotCloudA, DotCloudB) ->
    ordsets:union(DotCloudA, DotCloudB).

%% @doc Get SubDataStore pointed by the object from {dot_map, dot_map()}.
-spec get_sub_data_store(term(), {{dot_map, data_store_type()}, dot_map()}) ->
          {ok, data_store()}.
get_sub_data_store(Object, {{dot_map, ValueDataStoreType}, DataStore}) ->
    case orddict:find(Object, DataStore) of
        {ok, SubDataStore} ->
            {ok, SubDataStore};
        error ->
            {ok, new_data_store(ValueDataStoreType)}
    end.

%% @doc Insert (key: Object, value: SubDataStore) into {dot_map, dot_map()}.
-spec insert_object(term(), data_store(), {{dot_map, data_store_type()}, dot_map()}) ->
          {{dot_map, data_store_type()}, dot_map()}.
insert_object(Object, SubDataStore, {{dot_map, ValueDataStoreType}, DataStore}) ->
    {{dot_map, ValueDataStoreType}, orddict:store(Object, SubDataStore, DataStore)}.

%% @doc Remove the Object from DataStore.
%% The Object has to be in the DataStore. This can be checked by
%% get_data_store().
-spec remove_object(term(), {{dot_map, data_store_type()}, dot_map()}) ->
          {{dot_map, data_store_type()}, dot_map()}.
remove_object(Object, {{dot_map, ValueDataStoreType}, DataStore}) ->
    {{dot_map, ValueDataStoreType}, orddict:erase(Object, DataStore)}.

%% @doc Get all Objects from DataStore.
-spec get_all_objects({{dot_map, data_store_type()}, dot_map()}) -> [term()].
get_all_objects({{dot_map, _ValueDataStoreType}, DataStore}) ->
    orddict:fetch_keys(DataStore).

%% @doc Get the number of Objects from DataStore.
-spec get_objects_count({{dot_map, data_store_type()}, dot_map()}) -> non_neg_integer().
get_objects_count({{dot_map, _ValueDataStoreType}, DataStore}) ->
    orddict:size(DataStore).

%% @private
%% @todo This function can be used for the compressing functionality.
get_maxs_for_all(DotCloud) ->
    ordsets:fold(fun({DotActor0, DotCounter0}, MaxList) ->
                         {Counter, NewList} =
                             case lists:keytake(DotActor0, 1, MaxList) of
                                 false ->
                                     {DotCounter0, MaxList};
                                 {value, {_DotActor, C}, ModList} ->
                                     {max(DotCounter0, C), ModList}
                             end,
                         [{DotActor0,Counter}|NewList]
                 end, [], DotCloud).
