%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2018 redrock
%%% @doc rabbitmq pool
%%%   -callback start({Channel::pid(), PoolName::atom()}) -> {ok, pid()} | {error, any()}.
%%% @end
%%%-------------------------------------------------------------------
-module(rmp_bind).

-export([get_deads/1,
         get_connects/1,
         get_channels/1,
         get_processes/1]).

%% callbacks
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%------------------------------------------------------------------------------
%% @doc define
%%------------------------------------------------------------------------------
-behaviour(gen_server).

-include("rabbitmq_pool.hrl").

-define(TIMEOUT, 1000).

-record(state, {process_ets  = 0,
                connects     = #{},
                bind_channel = #{},
                deads        = []}).

%%------------------------------------------------------------------------------
%% @doc interface
%%------------------------------------------------------------------------------
get_deads(Proc) ->
    element(#state.deads, sys:get_state(Proc)).

get_connects(Proc) ->
    [X || {X, _} <- maps:to_list(element(#state.connects, sys:get_state(Proc)))].

get_channels(Proc) ->
    maps:keys(element(#state.bind_channel, sys:get_state(Proc))).

get_processes(ETS) ->
    [X || {X, _} <- ets:tab2list(ETS)].

%%------------------------------------------------------------------------------
%% @doc gen_server
%%------------------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?MODULE, [Args], []).

%%------------------------------------------------------------------------------
init([{Pool, Prop}]) ->
    Processes = ets:new(channel, [public, {read_concurrency, true}]),
    case do_reborn_deads(rmp_util:parse_pool(Pool, Prop),
                         #state{process_ets = Processes, deads = []}) of
        #state{deads = []} = State ->
            ?INFO("bind start:~p", [Pool]),
            rabbitmq_pool:add_bind(Pool, Prop, Processes),
            {ok, State, 0};
        #state{deads = [Dead | _]} ->
            {error, {fail_connect, {Dead, get(reborn_fail)}}}
    end.

handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(timeout, State) ->
    State1 = do_reborn_deads(State#state.deads, State#state{deads = []}),
    erlang:send_after(?TIMEOUT, self(), timeout),
    {noreply, State1};

handle_info({'DOWN', _MRef, process, PID, Reason}, State) ->
    State1 = do_find_deads(PID, Reason, State),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

do_reborn_deads([{connect, Prop} = H | T],
               #state{connects = ConnectSet, deads = Deads} = State) ->
    case rmp_util:connect(Prop) of
        {ok, Connect} ->
            ?INFO("conenct start:~p,~p", [Connect, Prop]),
            erlang:monitor(process, Connect),
            List = lists:duplicate(proplists:get_value(channel_size, Prop, 1), {channel, Connect, Prop}),
            State1 = State#state{connects = ConnectSet#{Connect => #{prop => Prop, childs => []}}},
            do_reborn_deads(T ++ List, State1);
        Reason ->
            ?WARN("conenct start fail:~p,~p", [Reason, Prop]),
            put(reborn_fail, Reason),
            do_reborn_deads(T, State#state{deads = [H | Deads]})
    end;
do_reborn_deads([{channel, Connect, Prop} = H | T],
               #state{bind_channel = BChannelSet, deads = Deads} = State) ->
    case is_process_alive(Connect) of
        true ->
            case catch amqp_connection:open_channel(Connect) of
                {ok, Channel} ->
                    ?INFO("bind channel start:~p,~p", [Channel, Prop]),
                    erlang:monitor(process, Channel),
                    BChannelSet1 = BChannelSet#{Channel => #{connect => Connect, prop => Prop}},
                    do_reborn_deads(T ++ [{process, Connect, Channel, Prop}], State#state{bind_channel = BChannelSet1});
                Reason ->
                    ?WARN("bind channel start fail:~p,~p", [Reason, Prop]),
                    put(reborn_fail, Reason),
                    do_reborn_deads(T, State#state{deads = [H | Deads]})
            end;
        false ->
            do_reborn_deads(T, State)
    end;
do_reborn_deads([{process, Connect, Channel, Prop} = H | T],
               #state{bind_channel = BChannelSet,
                      connects = ConnectSet,
                      deads = Deads} = State) ->
    case is_process_alive(Connect)
         andalso is_process_alive(Channel) of
        true ->
            Mod = proplists:get_value(module, Prop),
            process_flag(trap_exit, true),
            case catch Mod:start({Channel, proplists:get_value(pool, Prop)}) of
                {ok, Process}  ->
                    ?INFO("bind process start:~p,~p", [Channel, Prop]),
                    process_flag(trap_exit, false),
                    erlang:monitor(process, Process),
                    BChannelSet1 = rmp_util:map_set(BChannelSet, Channel, process, Process),
                    ConnectSet1 = rmp_util:map_add(ConnectSet, Connect, childs, Process),
                    ets:insert(State#state.process_ets, {Process, #{connect => Connect, channel => Channel, prop => Prop}}),
                    do_reborn_deads(T, State#state{bind_channel = BChannelSet1, connects = ConnectSet1});
                Reason ->
                    ?WARN("bind process start fail:~p,~p", [Reason, Prop]),
                    put(reborn_fail, Reason),
                    process_flag(trap_exit, false),
                    do_reborn_deads(T, State#state{deads = [H | Deads]})
            end;
        _ ->
            do_reborn_deads(T, State)
    end;
do_reborn_deads([], State) -> State.

%%------------------------------------------------------------------------------
do_find_deads(PID, Reason,
              #state{deads = Deads, connects = ConnectSet, bind_channel = BChannelSet} = State) ->
    try
        maps:is_key(PID, ConnectSet) andalso throw(connect_dead),
        ets:lookup(State#state.process_ets, PID) =/= [] andalso throw(bind_process_dead),
        maps:is_key(PID, BChannelSet) andalso throw(bind_channel_dead),
        State
    catch
        throw:connect_dead ->
            {ok, #{childs := Childs, prop := Prop}} = maps:find(PID, ConnectSet),
            ?INFO("connect stop try stop connect consumer:~p, ~p, ~p, ~p", [PID, Reason, Prop, Childs]),
            [exit(Process, shutdown) || Process <- Childs],
            case lists:member(consume, proplists:get_value(connect_method, Prop, []))
                 andalso (proplists:get_value(nodes, Prop) > []
                          orelse lists:member(proxy_rr, proplists:get_value(connect_method, Prop, []))) of
                true ->
                    ?INFO("connect stop try refresh pool:~p, ~p, ~p", [maps:keys(ConnectSet), Reason, Prop]),
                    [catch amqp_connection:close(X) || X <- maps:keys(ConnectSet) -- [PID]],
                    State#state{connects = #{}, deads = [{connect, Y} || #{prop := Y} <- maps:values(ConnectSet)] ++ Deads};
                _ ->
                    State#state{connects = maps:remove(PID, ConnectSet), deads = [{connect, Prop} | Deads]}
            end;
        throw:bind_process_dead ->
            [{PID, #{connect := Connect, channel := Channel, prop := Prop}}] = ets:lookup(State#state.process_ets, PID),
            ets:delete(State#state.process_ets, PID),
            ?INFO("bind process stop:~p, ~p, ~p", [PID, Reason, Prop]),
            State#state{connects = rmp_util:map_del(ConnectSet, Connect, childs, PID),
                        deads = [{process, Connect, Channel, Prop} | Deads]};
        throw:bind_channel_dead ->
            {ok, #{connect := Connect, prop := Prop} = M} = maps:find(PID, BChannelSet),
            ?INFO("bind channel stop:~p, ~p, ~p", [PID, Reason, Prop]),
            catch gen_server:stop(maps:get(process, M, undefined)),
            State#state{bind_channel = maps:remove(PID, BChannelSet),
                        deads = [{channel, Connect, Prop} | Deads]}
    end.

