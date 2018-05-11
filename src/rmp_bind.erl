%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2018 redrock
%%% @doc rabbitmq pool
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

-define(TIMEOUT, 5000).

-record(state, {connects     = #{},
                bind_channel = #{},
                bind_process = #{},
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

get_processes(Proc) ->
    maps:keys(element(#state.bind_process, sys:get_state(Proc))).

%%------------------------------------------------------------------------------
%% @doc gen_server
%%------------------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?MODULE, [Args], []).

%%------------------------------------------------------------------------------
init([{Pool, Prop}]) ->
    rabbitmq_pool:add_bind(Pool),
    case do_reborn_deads(rmp_util:parse_pool(Pool, Prop), #state{deads = []}) of
        #state{deads = []} = State -> {ok, State, 0};
        #state{deads = [Dead | _]} -> {error, {fail_connect, Dead}}
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
    case catch amqp_connection:start(?AMQP_NETWORK(Prop)) of
        {ok, Connect} ->
            ?INFO("conenct start:~p,~p", [Connect, Prop]),
            erlang:monitor(process, Connect),
            List = lists:duplicate(proplists:get_value(channel_size, Prop, 1), {channel, Connect, Prop}),
            State1 = State#state{connects = ConnectSet#{Connect => #{prop => Prop, childs => []}}},
            do_reborn_deads(T ++ List, State1);
        _ ->
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
                _ ->
                    do_reborn_deads(T, State#state{deads = [H | Deads]})
            end;
        false ->
            do_reborn_deads(T, State)
    end;
do_reborn_deads([{process, Connect, Channel, Prop} = H | T],
               #state{bind_channel = BChannelSet,
                      bind_process = BProcessSet,
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
                    BProcessSet1 = BProcessSet#{Process => #{connect => Connect, channel => Channel, prop => Prop}},
                    ConnectSet1 = rmp_util:map_add(ConnectSet, Connect, childs, Process),
                    do_reborn_deads(T, State#state{bind_channel = BChannelSet1,
                                                   bind_process = BProcessSet1,
                                                   connects = ConnectSet1});
                _ ->
                    process_flag(trap_exit, false),
                    do_reborn_deads(T, State#state{deads = [H | Deads]})
            end;
        _ ->
            do_reborn_deads(T, State)
    end;
do_reborn_deads([], State) -> State.

%%------------------------------------------------------------------------------
do_find_deads(PID, Reason, #state{deads = Deads,
                                  connects = ConnectSet,
                                  bind_channel = BChannelSet,
                                  bind_process = BProcessSet} = State) ->
    try
        maps:is_key(PID, ConnectSet) andalso throw(connect_dead),
        maps:is_key(PID, BProcessSet) andalso throw(bind_process_dead),
        maps:is_key(PID, BChannelSet) andalso throw(bind_channel_dead)
    catch
        throw:connect_dead ->
            {ok, #{childs := Childs, prop := Prop}} = maps:find(PID, ConnectSet),
            ?INFO("connect stop:~p, ~p, ~p", [PID, Reason, Prop]),
            ConnectSet1 = maps:remove(PID, ConnectSet),
            [exit(Process, shutdown) || Process <- Childs],
            State#state{connects = ConnectSet1, deads = [{connect, Prop} | Deads]};
        throw:bind_process_dead ->
            {ok, #{connect := Connect, channel := Channel, prop := Prop}} = maps:find(PID, BProcessSet),
            ?INFO("bind process stop:~p, ~p, ~p", [PID, Reason, Prop]),
            State#state{bind_process = maps:remove(PID, BProcessSet),
                        connects = rmp_util:map_del(ConnectSet, Connect, childs, PID),
                        deads = [{process, Connect, Channel, Prop} | Deads]};
        throw:bind_channel_dead ->
            {ok, #{connect := Connect, prop := Prop} = M} = maps:find(PID, BChannelSet),
            ?INFO("bind channel stop:~p, ~p, ~p", [PID, Reason, Prop]),
            catch exit(maps:get(process, M, undefined), shutdown),
            State#state{bind_channel = maps:remove(PID, BChannelSet),
                        deads = [{channel, Connect, Prop} | Deads]}
    end.

