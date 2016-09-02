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

-module(async_iterate).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/2,
         iterator/0,
         next/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {tid :: ets:tid(),
                function :: function(),
                next_position :: non_neg_integer(),
                finished :: boolean()}).

%% Sleep interval when waiting for more data.
-define(INTERVAL, 300).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link(atom(), function()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Unique, Function) ->
    gen_server:start_link(?MODULE, [Unique, Function], []).

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
-spec init([term()]) -> {ok, #state{}}.
init([Unique, Function]) ->
    %% Generate an ets table to store results.
    Tid = ets:new(Unique, []),

    %% Trap exits.
    process_flag(trap_exit, true),

    %% Spawn function to populate ets table and pass in our process
    %% identifier.
    spawn_link(fun() ->
                       Function(self())
               end),

    {ok, #state{function=Function,
                tid=Tid,
                next_position=0,
                finished=false}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.
handle_call(iterator, _From, #state{tid=Tid}=State) ->
    {[Match], Continuation} = ets:select(Tid, [{{'$1', '$2'}, [], ['$2']}], 1),
    {reply, {ok, {Match, Continuation}}, State};
handle_call({next, Continuation}, From, #state{finished=Finished}=State) ->
    read(From, Finished, Continuation),
    {noreply, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info({retry, From, Continuation}, #state{finished=Finished}=State) ->
    read(From, Finished, Continuation),
    {noreply, State};
handle_info({'EXIT', _From, normal}, State) ->
    %% Population function quit normally; ignore as it most likely means
    %% that the results are fully populated.
    {noreply, State#state{finished=true}};
handle_info({'EXIT', _From, Reason}, State) ->
    %% Abnormal exit from population function; quit.
    {stop, {error, {function_terminated, Reason}}, State};
handle_info({value, Value}, #state{tid=Tid, next_position=NextPosition}=State) ->
    %% Received a value from the function; populate next position in the
    %% ets table.
    true = ets:insert(Tid, [{NextPosition, Value}]),
    {noreply, State#state{next_position=NextPosition+1}};
handle_info(_Msg, State) ->
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
read(From, Finished, Continuation) ->
    case ets:select(Continuation) of
        '$end_of_table' ->
            case Finished of
                true ->
                    %% We've reached the end of the results and know
                    %% that population is complete, reply immediately
                    %% with ok.
                    gen_server:reply(From, ok);
                false ->
                    %% We aren't done yet, therefore, schedule response
                    %% for the future.
                    erlang:send_after(?INTERVAL, self(), {retry, From, Continuation})
            end;
        {[Match], Continuation} ->
            %% Got result; reply immediately.
            gen_server:reply(From, {ok, {Match, Continuation}})
    end.
