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

%% @doc Boolean primitive CRDT.
%%
%% @reference Carlos Baquero, Paulo Sérgio Almeida, Alcino Cunha and Carla Ferreira
%%      Composition of State-based CRDTs (2015)
%%      [http://haslab.uminho.pt/cbm/files/crdtcompositionreport.pdf]

-module(state_boolean).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-behaviour(type).
-behaviour(state_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0, new/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_bottom/1,
         is_inflation/2, is_strict_inflation/2,
         irreducible_is_strict_inflation/2]).
-export([join_decomposition/1, delta/2, digest/1]).
-export([encode/2, decode/2]).

-export_type([state_boolean/0, state_boolean_op/0]).

-opaque state_boolean() :: {?TYPE, payload()}.
-type payload() :: 0 | 1.
-type state_boolean_op() :: true.

%% @doc Create a new `state_boolean()'
-spec new() -> state_boolean().
new() ->
    {?TYPE, 0}.

%% @doc Create a new `state_boolean()'
-spec new([term()]) -> state_boolean().
new([]) ->
    new().

%% @doc Mutate a `state_boolean()'.
-spec mutate(state_boolean_op(), type:id(), state_boolean()) ->
    {ok, state_boolean()}.
mutate(Op, Actor, {?TYPE, _Boolean}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_boolean()'.
%%      The first argument can only be `true'.
%%      The second argument is the replica id.
%%      The third argument is the `state_boolean()' to be inflated.
-spec delta_mutate(state_boolean_op(), type:id(), state_boolean()) ->
    {ok, state_boolean()}.
delta_mutate(true, _Actor, {?TYPE, _Boolean}) ->
    {ok, {?TYPE, 1}}.

%% @doc Returns the value of the `state_boolean()'.
-spec query(state_boolean()) -> boolean().
query({?TYPE, Boolean}) ->
    Boolean == 1.

%% @doc Merge two `state_boolean()'.
%%      Join is the logical or.
-spec merge(state_boolean(), state_boolean()) -> state_boolean().
merge({?TYPE, Boolean1}, {?TYPE, Boolean2}) ->
    {?TYPE, max(Boolean1, Boolean2)}.

%% @doc Equality for `state_boolean()'.
-spec equal(state_boolean(), state_boolean()) -> boolean().
equal({?TYPE, Boolean1}, {?TYPE, Boolean2}) ->
    Boolean1 == Boolean2.

%% @doc Check if a Boolean is bottom.
-spec is_bottom(state_boolean()) -> boolean().
is_bottom({?TYPE, Boolean}) ->
    Boolean == 0.

%% @doc Given two `state_boolean()', check if the second is an inflation
%%      of the first.
-spec is_inflation(state_boolean(), state_boolean()) -> boolean().
is_inflation({?TYPE, Boolean1}, {?TYPE, Boolean2}) ->
    Boolean1 == Boolean2 orelse
    (Boolean1 == 0 andalso Boolean2 == 1).

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_boolean(), state_boolean()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_boolean(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=A, B) ->
    state_type:irreducible_is_strict_inflation(A, B).

-spec digest(state_boolean()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_boolean()'.
-spec join_decomposition(state_boolean()) -> [state_boolean()].
join_decomposition({?TYPE, _}=Boolean) ->
    [Boolean].

%% @doc Delta calculation for `state_boolean()'.
-spec delta(state_boolean(), state_type:digest()) -> state_boolean().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_boolean()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_boolean().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, 0}, new()).

query_test() ->
    Boolean0 = new(),
    Boolean1 = {?TYPE, 1},
    ?assertEqual(false, query(Boolean0)),
    ?assertEqual(true, query(Boolean1)).

delta_true_test() ->
    Boolean0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate(true, 1, Boolean0),
    ?assertEqual({?TYPE, 1}, {?TYPE, Delta1}).

true_test() ->
    Boolean0 = new(),
    {ok, Boolean1} = mutate(true, 1, Boolean0),
    ?assertEqual({?TYPE, 1}, Boolean1).

merge_test() ->
    Boolean1 = {?TYPE, 0},
    Boolean2 = {?TYPE, 1},
    Boolean3 = merge(Boolean1, Boolean1),
    Boolean4 = merge(Boolean1, Boolean2),
    Boolean5 = merge(Boolean2, Boolean1),
    Boolean6 = merge(Boolean2, Boolean2),
    ?assertEqual({?TYPE, 0}, Boolean3),
    ?assertEqual({?TYPE, 1}, Boolean4),
    ?assertEqual({?TYPE, 1}, Boolean5),
    ?assertEqual({?TYPE, 1}, Boolean6).

merge_deltas_test() ->
    Boolean1 = {?TYPE, 0},
    Delta1 = {?TYPE, 0},
    Delta2 = {?TYPE, 1},
    Boolean3 = merge(Delta1, Boolean1),
    Boolean4 = merge(Boolean1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, 0}, Boolean3),
    ?assertEqual({?TYPE, 0}, Boolean4),
    ?assertEqual({?TYPE, 1}, DeltaGroup).

equal_test() ->
    Boolean1 = {?TYPE, 0},
    Boolean2 = {?TYPE, 1},
    ?assert(equal(Boolean1, Boolean1)),
    ?assertNot(equal(Boolean1, Boolean2)).

is_bottom_test() ->
    Boolean0 = new(),
    Boolean1 = {?TYPE, 1},
    ?assert(is_bottom(Boolean0)),
    ?assertNot(is_bottom(Boolean1)).

is_inflation_test() ->
    Boolean1 = {?TYPE, 0},
    Boolean2 = {?TYPE, 1},
    ?assert(is_inflation(Boolean1, Boolean1)),
    ?assert(is_inflation(Boolean1, Boolean2)),
    ?assertNot(is_inflation(Boolean2, Boolean1)),
    ?assert(is_inflation(Boolean2, Boolean2)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Boolean1, Boolean1)),
    ?assert(state_type:is_inflation(Boolean1, Boolean2)),
    ?assertNot(state_type:is_inflation(Boolean2, Boolean1)),
    ?assert(state_type:is_inflation(Boolean2, Boolean2)).

is_strict_inflation_test() ->
    Boolean1 = {?TYPE, 0},
    Boolean2 = {?TYPE, 1},
    ?assertNot(is_strict_inflation(Boolean1, Boolean1)),
    ?assert(is_strict_inflation(Boolean1, Boolean2)),
    ?assertNot(is_strict_inflation(Boolean2, Boolean1)),
    ?assertNot(is_strict_inflation(Boolean2, Boolean2)).

join_decomposition_test() ->
    Boolean1 = {?TYPE, 1},
    Decomp1 = join_decomposition(Boolean1),
    ?assertEqual([Boolean1], Decomp1).

encode_decode_test() ->
    Boolean = {?TYPE, 1},
    Binary = encode(erlang, Boolean),
    EBoolean = decode(erlang, Binary),
    ?assertEqual(Boolean, EBoolean).
-endif.
