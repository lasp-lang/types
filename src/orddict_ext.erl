% -------------------------------------------------------------------
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

-module(orddict_ext).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-export([equal/3]).

-spec equal(orddict:orddict(), orddict:orddict(), function()) ->
    boolean().
equal(Dict1, Dict2, Fun) ->
    orddict:size(Dict1) == orddict:size(Dict2) andalso
    orddict:fold(
        fun(Key, Value1, Acc) ->
            case orddict:find(Key, Dict2) of
                {ok, Value2} ->
                    Acc andalso Fun(Value1, Value2);
                error ->
                    Acc andalso false
            end
        end,
        true,
        Dict1
     ).

