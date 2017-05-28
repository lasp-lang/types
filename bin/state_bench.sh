#!/usr/bin/env escript

%%! -pa _build/default/lib/types/ebin/

main(_) ->
    benchmark(state_gset),
    benchmark(state_awset),
    ok.

%% @private
benchmark(Type) ->
    io:format("Running ~p~n", [Type]),
    R0 = create(Type),
    R1 = adds(Type, part(1), true, R0),
    R2 = adds(Type, part(2), false, R1),
    R3 = adds(Type, part(3), true, R2),
    analyze(R3).

%% @private
analyze({_, Metrics}) ->
    lists:foreach(
        fun({MetricType, Values}) ->
            Min = lists:min(Values),
            Max = lists:max(Values),
            Average = lists:sum(Values) / length(Values),

            io:format("- ~p:~n", [MetricType]),
            io:format("  > min: ~p~n", [Min]),
            io:format("  > max: ~p~n", [Max]),
            io:format("  > avg: ~p~n", [Average])
        end,
        Metrics
    ).

%% @private
create(Type) ->
    lists:foldl(
        fun(Replica, {StateIn, MetricsIn}) ->
            {Time, Result} = timer:tc(fun() -> Type:new() end),
            StateOut = orddict:store(Replica, Result, StateIn),
            MetricsOut = orddict:append(new,
                                        Time,
                                        MetricsIn),
            {StateOut, MetricsOut}
        end,
        {orddict:new(), orddict:new()},
        replicas()
    ).

%% @private
adds(Type, Seq, WithSync, R0) ->
    lists:foldl(
        fun(I, R1) ->
            %% each replica adds an element to the set
            R2 = lists:foldl(
                fun(Replica, {StateIn, MetricsIn}) ->
                    Value = orddict:fetch(Replica, StateIn),
                    Element = create_element(Replica, I),
                    
                    {Time, Result} = timer:tc(
                        fun() ->
                            {ok, NewValue} = Type:mutate({add, Element}, Replica, Value),
                            NewValue
                        end
                    ),

                    StateOut = orddict:store(Replica, Result, StateIn),
                    MetricsOut = orddict:append(mutate, Time, MetricsIn),
                    {StateOut, MetricsOut}
                end,
                R1,
                replicas()
            ),

            {StateBeforeMerge, _} = R2,

            R3 = case WithSync of
                true ->
                    %% each replica synchronizes with its neighbors
                    lists:foldl(
                        fun(Replica, In) ->
                            lists:foldl(
                                fun(Neighbor, {StateIn, MetricsIn}) ->

                                    %% get the state that would be received in a message
                                    RemoteState = orddict:fetch(Neighbor, StateBeforeMerge),
                                    %& get the current local state
                                    LocalState = orddict:fetch(Replica, StateIn),

                                    {TimeDelta, Delta} = timer:tc(
                                        fun() ->
                                            %% which part of the remote state inflates the local state
                                            Type:delta(RemoteState, {state, LocalState})
                                        end
                                    ),

                                    {TimeBottom, false} = timer:tc(
                                        fun() ->
                                            %% check if the delta is bottom
                                            Type:is_bottom(Delta)
                                        end
                                    ),

                                    {TimeMerge, Merged} = timer:tc(
                                        fun() ->
                                            %% merge the received state
                                            Type:merge(LocalState, RemoteState)
                                        end
                                    ),

                                    {TimeInflation, true} = timer:tc(
                                        fun() ->
                                            %% check if there was a strict inflation
                                            Type:is_strict_inflation(LocalState, Merged)
                                        end
                                    ),

                                    StateOut = orddict:store(Replica, Merged, StateIn),
                                    MetricsOut0 = orddict:append(delta,
                                                                 TimeDelta,
                                                                 MetricsIn),
                                    MetricsOut1 = orddict:append(is_bottom,
                                                                 TimeBottom,
                                                                 MetricsOut0),
                                    MetricsOut2 = orddict:append(is_strict_inflation,
                                                                 TimeInflation,
                                                                 MetricsOut1),
                                    MetricsOut3 = orddict:append(merge,
                                                                 TimeMerge,
                                                                 MetricsOut2),
                                    {StateOut, MetricsOut3}
                                end,
                                In,
                                neighbors(config(topology), Replica)
                            )
                        end,
                        R2,
                        replicas()
                    );
                false ->
                    R2
            end,

            %% perform a read
            lists:foldl(
                fun(Replica, {StateIn, MetricsIn}) ->
                    Value = orddict:fetch(Replica, StateIn),

                    {Time, _} = timer:tc(
                        fun() ->
                            Type:query(Value)
                        end
                    ),

                    MetricsOut = orddict:append(query,
                                                Time,
                                                MetricsIn),
                    {StateIn, MetricsOut}
                end,
                R3,
                replicas()
            )
        end,
        R0,
        Seq
    ).

%% @private
replicas() ->
    lists:seq(1, config(replica_number)).

%% @private Get the neighbors of a replica in a ring topology.
neighbors(ring, Replica) ->
    ReplicaNumber = config(replica_number),

    Left = case Replica of
        1 ->
            ReplicaNumber;
        _ ->
            Replica - 1
    end,
    Right = case Replica of
        ReplicaNumber ->
            1;
        _ ->
            Replica + 1
    end,
    [Left, Right].

%% @private
part(Number) ->
    AddNumber = config(add_number),
    D2 = AddNumber div 2,
    D4 = AddNumber div 4,
    case Number of
        1 ->
            lists:seq(1, D4); %% from 0% to 25%
        2 ->
            lists:seq(D4 + 1, D2 + D4); %% from 25% to 75%
        3 ->
            lists:seq(D2 + D4 + 1, AddNumber) %% from 75% to 100%
    end.

%% @private
create_element(Replica, I) ->
    integer_to_list(Replica) ++ "#####" ++ integer_to_list(I).


%% @private config
config(topology) -> ring;
config(replica_number) -> 6;
config(add_number) -> 100.
