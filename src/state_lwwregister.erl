% %
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

%% @doc LWWRegister.
%%      We assume timestamp are unique, totally ordered and consistent
%%      with causal order. We use integers as timestamps.
%%      When using this, make sure you provide globally unique
%%      timestamps.
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero and Marek Zawirsk
%%      A comprehensive study of Convergent and Commutative Replicated Data Types (2011)
%%      [http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf]
%%
%% @reference Carlos Baquero
%%      delta-enabled-crdts C++ library
%%      [https://github.com/CBaquero/delta-enabled-crdts]

-module(state_lwwregister).
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

-export_type([state_lwwregister/0, state_lwwregister_op/0]).

-opaque state_lwwregister() :: {?TYPE, payload()}.
-type payload() :: {timestamp(), value()}.
-type timestamp() :: non_neg_integer().
-type value() :: term().
-type state_lwwregister_op() :: {set, timestamp(), value()}.

%% @doc Create a new, empty `state_lwwregister()'
-spec new() -> state_lwwregister().
new() ->
    {?TYPE, {0, undefined}}.

%% @doc Create a new, empty `state_lwwregister()'
-spec new([term()]) -> state_lwwregister().
new([]) ->
    new().

%% @doc Mutate a `state_lwwregister()'.
-spec mutate(state_lwwregister_op(), type:id(), state_lwwregister()) ->
    {ok, state_lwwregister()}.
mutate(Op, Actor, {?TYPE, _Register}=CRDT) ->
    state_type:mutate(Op, Actor, CRDT).

%% @doc Delta-mutate a `state_lwwregister()'.
%%      The first argument can only be `{set, timestamp(), value()}'.
%%      The second argument is the replica id (unused).
%%      The third argument is the `state_lwwregister()' to be inflated.
-spec delta_mutate(state_lwwregister_op(), type:id(), state_lwwregister()) ->
    {ok, state_lwwregister()}.
delta_mutate({set, Timestamp, Value}, _Actor, {?TYPE, _Register}) when is_integer(Timestamp) ->
    {ok, {?TYPE, {Timestamp, Value}}}.

%% @doc Returns the value of the `state_lwwregister()'.
-spec query(state_lwwregister()) -> value().
query({?TYPE, {_Timestamp, Value}}) ->
    Value.

%% @doc Merge two `state_lwwregister()'.
%%      The result is the set union of both sets in the
%%      `state_lwwregister()' passed as argument.
-spec merge(state_lwwregister(), state_lwwregister()) -> state_lwwregister().
merge({?TYPE, {Timestamp1, Value1}}, {?TYPE, {Timestamp2, Value2}}) ->
    Register = case Timestamp1 > Timestamp2 of
        true ->
            {Timestamp1, Value1};
        false ->
            {Timestamp2, Value2}
    end,
    {?TYPE, Register}.

%% @doc Equality for `state_lwwregister()'.
-spec equal(state_lwwregister(), state_lwwregister()) -> boolean().
equal({?TYPE, Register1}, {?TYPE, Register2}) ->
    Register1 == Register2.

%% @doc Check if a Register is bottom.
-spec is_bottom(state_lwwregister()) -> boolean().
is_bottom({?TYPE, _}=CRDT) ->
    CRDT == new().

%% @doc Given two `state_lwwregister()', check if the second is an inflation
%%      of the first.
%%      The second `state_lwwregister()' it has a higher timestamp or the same
%%      timstamp (and in this case, they are equal).
-spec is_inflation(state_lwwregister(), state_lwwregister()) -> boolean().
is_inflation({?TYPE, {Timestamp1, _}}, {?TYPE, {Timestamp2, _}}) ->
    Timestamp2 >= Timestamp1.

%% @doc Check for strict inflation.
-spec is_strict_inflation(state_lwwregister(), state_lwwregister()) -> boolean().
is_strict_inflation({?TYPE, _}=CRDT1, {?TYPE, _}=CRDT2) ->
    state_type:is_strict_inflation(CRDT1, CRDT2).

%% @doc Check for irreducible strict inflation.
-spec irreducible_is_strict_inflation(state_lwwregister(),
                                      state_type:digest()) ->
    boolean().
irreducible_is_strict_inflation({?TYPE, _}=A, B) ->
    state_type:irreducible_is_strict_inflation(A, B).

-spec digest(state_lwwregister()) -> state_type:digest().
digest({?TYPE, _}=CRDT) ->
    {state, CRDT}.

%% @doc Join decomposition for `state_lwwregister()'.
-spec join_decomposition(state_lwwregister()) -> [state_lwwregister()].
join_decomposition({?TYPE, _}=CRDT) ->
    [CRDT].

%% @doc Delta calculation for `state_lwwregister()'.
-spec delta(state_lwwregister(), state_type:digest()) -> state_lwwregister().
delta({?TYPE, _}=A, B) ->
    state_type:delta(A, B).

-spec encode(state_type:format(), state_lwwregister()) -> binary().
encode(erlang, {?TYPE, _}=CRDT) ->
    erlang:term_to_binary(CRDT).

-spec decode(state_type:format(), binary()) -> state_lwwregister().
decode(erlang, Binary) ->
    {?TYPE, _} = CRDT = erlang:binary_to_term(Binary),
    CRDT.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    Bottom = {0, undefined},
    ?assertEqual({?TYPE, Bottom}, new()).

query_test() ->
    Register0 = new(),
    Register1 = {?TYPE, {1234, "a"}},
    ?assertEqual(undefined, query(Register0)),
    ?assertEqual("a", query(Register1)).

delta_set_test() ->
    Actor = 1,
    Register0 = new(),
    {ok, {?TYPE, Delta1}} = delta_mutate({set, 1234, "a"}, Actor, Register0),
    Register1 = merge({?TYPE, Delta1}, Register0),
    {ok, {?TYPE, Delta2}} = delta_mutate({set, 1235, "b"}, Actor, Register1),
    Register2 = merge({?TYPE, Delta2}, Register1),
    ?assertEqual({?TYPE, {1234, "a"}}, {?TYPE, Delta1}),
    ?assertEqual({?TYPE, {1234, "a"}}, Register1),
    ?assertEqual({?TYPE, {1235, "b"}}, {?TYPE, Delta2}),
    ?assertEqual({?TYPE, {1235, "b"}}, Register2).

set_test() ->
    Actor = 1,
    Register0 = new(),
    {ok, Register1} = mutate({set, 1234, "a"}, Actor, Register0),
    {ok, Register2} = mutate({set, 1235, "b"}, Actor, Register1),
    ?assertEqual({?TYPE, {1234, "a"}}, Register1),
    ?assertEqual({?TYPE, {1235, "b"}}, Register2).

merge_idempotent_test() ->
    Register1 = {?TYPE, {1234, "a"}},
    Register2 = {?TYPE, {1235, "b"}},
    Register3 = merge(Register1, Register1),
    Register4 = merge(Register2, Register2),
    ?assertEqual(Register1, Register3),
    ?assertEqual(Register2, Register4).

merge_commutative_test() ->
    Register1 = {?TYPE, {1234, "a"}},
    Register2 = {?TYPE, {1235, "b"}},
    Register3 = merge(Register1, Register2),
    Register4 = merge(Register2, Register1),
    ?assertEqual(Register2, Register3),
    ?assertEqual(Register2, Register4).

equal_test() ->
    Register1 = {?TYPE, {1234, "a"}},
    Register2 = {?TYPE, {1235, "b"}},
    ?assert(equal(Register1, Register1)),
    ?assertNot(equal(Register1, Register2)).

is_bottom_test() ->
    Register0 = new(),
    Register1 = {?TYPE, {1234, "a"}},
    ?assert(is_bottom(Register0)),
    ?assertNot(is_bottom(Register1)).

is_inflation_test() ->
    Register1 = {?TYPE, {1234, "a"}},
    Register2 =  {?TYPE, {1235, "b"}},
    ?assert(is_inflation(Register1, Register1)),
    ?assert(is_inflation(Register1, Register2)),
    ?assertNot(is_inflation(Register2, Register1)),
    %% check inflation with merge
    ?assert(state_type:is_inflation(Register1, Register1)),
    ?assert(state_type:is_inflation(Register1, Register2)),
    ?assertNot(state_type:is_inflation(Register2, Register1)).

is_strict_inflation_test() ->
    Register1 =  {?TYPE, {1234, "a"}},
    Register2 =  {?TYPE, {1235, "b"}},
    ?assertNot(is_strict_inflation(Register1, Register1)),
    ?assert(is_strict_inflation(Register1, Register2)),
    ?assertNot(is_strict_inflation(Register2, Register1)).

join_decomposition_test() ->
    Register =  {?TYPE, {1234, "a"}},
    Decomp = join_decomposition(Register),
    ?assertEqual([Register], Decomp).

encode_decode_test() ->
    Register =  {?TYPE, {1234, "a"}},
    Binary = encode(erlang, Register),
    ERegister = decode(erlang, Binary),
    ?assertEqual(Register, ERegister).

-endif.
