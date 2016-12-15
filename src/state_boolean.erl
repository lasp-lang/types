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

-export([new/0, new/1, new_delta/0, new_delta/1, is_delta/1]).
-export([mutate/3, delta_mutate/3, merge/2]).
-export([query/1, equal/2, is_bottom/1, is_inflation/2, is_strict_inflation/2, irreducible_is_strict_inflation/2]).
-export([join_decomposition/1, delta/3]).
-export([encode/2, decode/2]).

-export_type([state_boolean/0, delta_state_boolean/0, state_boolean_op/0]).

-opaque state_boolean() :: {?TYPE, payload()}.
-opaque delta_state_boolean() :: {?TYPE, {delta, payload()}}.
-type delta_or_state() :: state_boolean() | delta_state_boolean().
-type payload() :: true | false.
-type state_boolean_op() :: true.

%% @doc Create a new `state_boolean()'
-spec new() -> state_boolean().
new() ->
    {?TYPE, false}.

%% @doc Create a new `state_boolean()'
-spec new([term()]) -> state_boolean().
new([]) ->
    new().

-spec new_delta() -> delta_state_boolean().
new_delta() ->
    state_type:new_delta(?TYPE).

-spec new_delta([term()]) -> delta_state_boolean().
new_delta([]) ->
    new_delta().

-spec is_delta(delta_or_state()) -> boolean().
is_delta({?TYPE, _}=CRDT) ->
    state_type:is_delta(CRDT).

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
    {ok, delta_state_boolean()}.
delta_mutate(true, _Actor, {?TYPE, _Boolean}) ->
    {ok, {?TYPE, {delta, true}}}.

%% @doc Returns the value of the `state_boolean()'.
-spec query(state_boolean()) -> non_neg_integer().
query({?TYPE, Boolean}) ->
    Boolean.

%% @doc Merge two `state_boolean()'.
%%      Join is the logical or.
-spec merge(delta_or_state(), delta_or_state()) -> delta_or_state().
merge({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    MergeFun = fun({?TYPE, Boolean1}, {?TYPE, Boolean2}) ->
        {?TYPE, Boolean1 orelse Boolean2}
    end,
    state_type:merge(CRDT1, CRDT2, MergeFun).

%% @doc Equality for `state_boolean()'.
-spec equal(state_boolean(), state_boolean()) -> boolean().
equal({?TYPE, Boolean1}, {?TYPE, Boolean2}) ->
    Boolean1 == Boolean2.

%% @doc Check if a Boolean is bottom.
-spec is_bottom(delta_or_state()) -> boolean().
is_bottom({?TYPE, {delta, Boolean}}) ->
    is_bottom({?TYPE, Boolean});
is_bottom({?TYPE, Boolean}) ->
    Boolean == false.

%% @doc Given two `state_boolean()', check if the second is an inflation
%%      of the first.
-spec is_inflation(delta_or_state(), state_boolean()) -> boolean().
is_inflation({?TYPE, {delta, Boolean1}}, {?TYPE, Boolean2}) ->
    is_inflation({?TYPE, Boolean1}, {?TYPE, Boolean2});
is_inflation({?TYPE, Boolean1}, {?TYPE, Boolean2}) ->
    Boolean1 == Boolean2 orelse
    (Boolean1 == false andalso Boolean2 == true).

%% @doc Check for strict inflation.
-spec is_strict_inflation(delta_or_state(), state_boolean()) -> boolean().
is_strict_inflation({?TYPE, {delta, Boolean1}}, {?TYPE, Boolean2}) ->
    is_strict_inflation({?TYPE, Boolean1}, {?TYPE, Boolean2});
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_boolean(), state_boolean()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=Irreducible, {?TYPE, _}=CRDT) ->
    state_type:irreducible_is_strict_inflation(Irreducible, CRDT).

%% @doc Join decomposition for `state_boolean()'.
-spec join_decomposition(delta_or_state()) -> [state_boolean()].
join_decomposition({?TYPE, {delta, Payload}}) ->
    join_decomposition({?TYPE, Payload});
join_decomposition({?TYPE, _}=Boolean) ->
    [Boolean].

%% @doc Delta calculation for `state_boolean()'.
-spec delta(state_type:delta_method(), delta_or_state(), delta_or_state()) ->
    state_boolean().
delta(Method, {?TYPE, _}=A, {?TYPE, _}=B) ->
    state_type:delta(Method, A, B).

-spec encode(state_type:format(), delta_or_state()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> delta_or_state().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({?TYPE, false}, new()),
    ?assertEqual({?TYPE, {delta, false}}, new_delta()).

query_test() ->
    Boolean0 = new(),
    Boolean1 = {?TYPE, true},
    ?assertEqual(false, query(Boolean0)),
    ?assertEqual(true, query(Boolean1)).

delta_true_test() ->
    Boolean0 = new(),
    {ok, {?TYPE, {delta, Delta1}}} = delta_mutate(true, 1, Boolean0),
    ?assertEqual({?TYPE, true}, {?TYPE, Delta1}).

true_test() ->
    Boolean0 = new(),
    {ok, Boolean1} = mutate(true, 1, Boolean0),
    ?assertEqual({?TYPE, true}, Boolean1).

merge_test() ->
    Boolean1 = {?TYPE, false},
    Boolean2 = {?TYPE, true},
    Boolean3 = merge(Boolean1, Boolean1),
    Boolean4 = merge(Boolean1, Boolean2),
    Boolean5 = merge(Boolean2, Boolean1),
    Boolean6 = merge(Boolean2, Boolean2),
    ?assertEqual({?TYPE, false}, Boolean3),
    ?assertEqual({?TYPE, true}, Boolean4),
    ?assertEqual({?TYPE, true}, Boolean5),
    ?assertEqual({?TYPE, true}, Boolean6).

merge_deltas_test() ->
    Boolean1 = {?TYPE, false},
    Delta1 = {?TYPE, {delta, false}},
    Delta2 = {?TYPE, {delta, true}},
    Boolean3 = merge(Delta1, Boolean1),
    Boolean4 = merge(Boolean1, Delta1),
    DeltaGroup = merge(Delta1, Delta2),
    ?assertEqual({?TYPE, false}, Boolean3),
    ?assertEqual({?TYPE, false}, Boolean4),
    ?assertEqual({?TYPE, {delta, true}}, DeltaGroup).

equal_test() ->
    Boolean1 = {?TYPE, false},
    Boolean2 = {?TYPE, true},
    ?assert(equal(Boolean1, Boolean1)),
    ?assertNot(equal(Boolean1, Boolean2)).

is_bottom_test() ->
    Boolean0 = new(),
    Boolean1 = {?TYPE, true},
    ?assert(is_bottom(Boolean0)),
    ?assertNot(is_bottom(Boolean1)).

is_inflation_test() ->
    Boolean1 = {?TYPE, false},
    Boolean2 = {?TYPE, true},
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
    Boolean1 = {?TYPE, false},
    Boolean2 = {?TYPE, true},
    ?assertNot(is_strict_inflation(Boolean1, Boolean1)),
    ?assert(is_strict_inflation(Boolean1, Boolean2)),
    ?assertNot(is_strict_inflation(Boolean2, Boolean1)),
    ?assertNot(is_strict_inflation(Boolean2, Boolean2)).

join_decomposition_test() ->
    Boolean1 = {?TYPE, true},
    Decomp1 = join_decomposition(Boolean1),
    ?assertEqual([Boolean1], Decomp1).

encode_decode_test() ->
    Boolean = {?TYPE, true},
    Binary = encode(erlang, Boolean),
    EBoolean = decode(erlang, Binary),
    ?assertEqual(Boolean, EBoolean).
-endif.
