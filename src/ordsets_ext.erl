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

-module(ordsets_ext).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-export([equal/2]).

-spec equal(ordsets:ordset(any()), ordsets:ordset(any())) ->
    boolean().
%% @doc Two sets s1 and s2 are equal if:
%%      - s1 is subset of s2
%%      - s2 is subset of s1
equal(Set1, Set2) ->
    ordsets:is_subset(Set1, Set2) andalso
    ordsets:is_subset(Set2, Set1).
