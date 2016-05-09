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

-module(async_iterate).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/1,
         iterator/0,
         next/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {tid, function, next_position}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link(function()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Function) ->
    gen_server:start_link(?MODULE, [Function], []).

%% @doc Return an iterator.
iterator() ->
    gen_server:call(?MODULE, iterator, infinity).

%% @doc Get the next value.
next(Continuation) ->
    gen_server:call(?MODULE, {next, Continuation}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init(Function) ->
    %% Generate unique identifier for the request.
    Id = mk_unique(),

    %% Generate an ets table to store results.
    Tid = ets:new(Id, []),

    %% Trap exits.
    process_flag(trap_exit, true),

    %% Spawn function to populate ets table and pass in our process
    %% identifier.
    spawn_link(Function, [self()]),

    {ok, #state{function=Function, tid=Tid}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.
handle_call(iterator, _From, #state{tid=Tid}=State) ->
    {[Match], Continuation} = ets:select(Tid, [{{'$1','$2'},[],['$2']}], 1),
    {reply, {ok, {Match, Continuation}}, State};
handle_call({next, Continuation}, _From, State) ->
    Result = case ets:select(Continuation) of
        '$end_of_table' ->
            ok;
        {[Match], Continuation} ->
            {ok, {Match, Continuation}}
    end,
    {reply, {ok, Result}, State};
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info({'EXIT', _From, normal}, State) ->
    %% Population function quit normally; ignore as it most likely means
    %% that the results are fully populated.
    {noreply, State};
handle_info({'EXIT', _From, Reason}, State) ->
    %% Abnormal exit from population function; quit.
    {stop, {error, {function_terminated, Reason}}, State};
handle_info({value, Value}, #state{tid=Tid, next_position=NextPosition}=State) ->
    %% Received a value from the function; populate next position in the
    %% ets table.
    true = ets:insert(Tid, [{NextPosition, Value}]),
    {noreply, State#state{next_position=NextPosition+1}};
handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
mk_unique() ->
    Node = atom_to_list(node()),
    Unique = time_compat:unique_integer([monotonic, positive]),
    TS = integer_to_list(Unique),
    Term = Node ++ TS,
    crypto:hash(sha, Term).
