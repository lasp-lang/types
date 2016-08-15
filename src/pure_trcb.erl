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

%% @doc initial TRCB lib
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, and Ali Shoker
%%      Making Operation-based CRDTs Operation-based (2014)
%%      [http://haslab.uminho.pt/ashoker/files/opbaseddais14.pdf]

-module(pure_trcb).
-author("Georges Younes <georges.r.younes@gmail.com>").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([happened_before/2]).

%% @doc Check causal order of 2 version vector.
-spec happened_before(orddict:orddict(), orddict:orddict()) -> boolean().
happened_before(VV1, VV2) ->
Fun = fun(Value1, Value2) -> Value1 == Value2 end,
orddict:size(VV1) == orddict:size(VV2) andalso
    orddict:fold(
        fun(Key, Value1, Acc) ->
            case orddict:find(Key, VV2) of
                {ok, Value2} ->
                    Acc andalso Value1 =< Value2;
                error ->
                    Acc andalso false
            end
        end,
        true,
        VV1
     ) andalso not (orddict_ext:equal(VV1, VV2, Fun)).
% to-do: replace happend before wth this
% Fun = fun(Value1, Value2) -> Value1 == Value2 end,
% orddict:size(VV1) == orddict:size(VV2) andalso
%     lists_ext:iterate_until(
%         fun({Key, Value1}) ->
%             case orddict:find(Key, VV2) of
%                 {ok, Value2} ->
%                     Value1 =< Value2;
%                 error ->
%                     false
%             end
%         end,
%         VV1
%      ) andalso not (orddict_ext:equal(VV1, VV2, Fun)).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

happened_before_test() ->
    ?assertEqual(false, happened_before([{0, 1}, {1, 2}, {2, 3}], [{0, 1}, {1, 2}, {2, 3}])),
    ?assertEqual(true, happened_before([{0, 1}, {1, 2}, {2, 3}], [{0, 2}, {1, 4}, {2, 5}])),
    ?assertEqual(false, happened_before([{0, 1}, {1, 2}, {2, 3}], [{0, 1}, {1, 4}, {2, 2}])),
    ?assertEqual(false, happened_before([{0, 2}, {1, 5}, {2, 3}], [{0, 1}, {1, 4}, {2, 2}])),
    ?assertEqual(true, happened_before([{0, 1}, {1, 2}, {2, 3}], [{0, 1}, {1, 4}, {2, 3}])).

-endif.
