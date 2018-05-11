%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2018 redrock
%%% @doc rabbitmq pool
%%% @end
%%%-------------------------------------------------------------------
-module(rmp_lone).

-export([get_deads/1,
         get_connects/1,
         get_channels/3]).

%% callbacks
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%------------------------------------------------------------------------------
%% @doc define
%%------------------------------------------------------------------------------
-behaviour(gen_server).

-include("rabbitmq_pool.hrl").

-define(TIMEOUT, 5000).

-record(state, {total_ets    = undefined,
                busy_ets     = undefined,
                connects     = #{},
                lone_free    = [],
                lone_wait    = queue:new(),
                deads        = []}).

%%------------------------------------------------------------------------------
%% @doc interface
%%------------------------------------------------------------------------------
get_deads(Proc) ->
    element(#state.deads, sys:get_state(Proc)).

get_connects(Proc) ->
    [X || {X, _} <- maps:to_list(element(#state.connects, sys:get_state(Proc)))].

get_channels(Proc, Channels, Busy) ->
    State = sys:get_state(Proc),
    [{all, [X || {X, _} <- ets:tab2list(Channels)]},
     {wait, element(#state.lone_wait, State)},
     {free, element(#state.lone_free, State)},
     {busy, ets:tab2list(Busy)}].

%%------------------------------------------------------------------------------
%% @doc gen_server
%%------------------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?MODULE, [Args], []).

%%------------------------------------------------------------------------------
init([{Pool, Prop}]) ->
    Channels = ets:new(channel, [public, {read_concurrency, true}]),
    Busy = ets:new(busy, [public]),
    rabbitmq_pool:add_lone(Pool, Channels, Busy),
    case do_reborn_deads(rmp_util:parse_pool(Pool, Prop),
                         #state{total_ets = Channels, busy_ets = Busy, deads = []}) of
        #state{deads = []} = State -> {ok, State, 0};
        #state{deads = [Dead | _]} -> {error, {fail_connect, Dead}}
    end.

handle_call({checkout_lone, CRef}, From, State) ->
    {noreply, do_checkout_lone(CRef, From, State)};
handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast({checkin_lone, Channel}, State) ->
    {noreply, do_checkin_lone(Channel, State)};
handle_cast({cancel_wait, CRef}, State) ->
    {noreply, do_cancel_wait(CRef, State)};
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

handle_info({'DOWN', MRef, process, PID, Reason}, State) ->
    State1 = do_find_deads(PID, Reason, MRef, State),
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
               #state{deads = Deads} = State) ->
    case is_process_alive(Connect) of
        true ->
            case catch amqp_connection:open_channel(Connect) of
                {ok, Channel} ->
                    ?INFO("lone channel start:~p,~p", [Channel, Prop]),
                    do_check_confirm(Channel),
                    erlang:monitor(process, Channel),
                    ets:insert(State#state.total_ets, {Channel, #{connect => Connect, prop => Prop}}),
                    do_reborn_deads(T, State#state{lone_free = [Channel | State#state.lone_free]});
                _ ->
                    do_reborn_deads(T, State#state{deads = [H | Deads]})
            end;
        false ->
            do_reborn_deads(T, State)
    end;
do_reborn_deads([], State) -> State.

do_check_confirm(Channel) ->
    case application:get_env(rabbitmq_pool, publish_method, undefined) of
        undefined-> skip;
        confirm -> #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{})
    end.

%%------------------------------------------------------------------------------
do_find_deads(PID, Reason, MRef, #state{deads = Deads,
                                        connects = ConnectSet,
                                        lone_free = LoneFree,
                                        lone_wait = LoneWait} = State) ->
    try
        maps:is_key(PID, ConnectSet) andalso throw(connect_dead),
        queue:member(PID, LoneWait) andalso throw(lone_wait_dead),
        ets:lookup(State#state.total_ets, PID) =/= [] andalso throw(lone_channel_dead),
        throw(lone_user_dead)
    catch
        throw:connect_dead ->
            {ok, #{childs := Childs, prop := Prop}} = maps:find(PID, ConnectSet),
            ?INFO("connect stop:~p, ~p, ~p", [PID, Reason, Prop]),
            ConnectSet1 = maps:remove(PID, ConnectSet),
            [exit(Process, shutdown) || Process <- Childs],
            State#state{connects = ConnectSet1, deads = [{connect, Prop} | Deads]};
        throw:lone_channel_dead ->
            [{PID, #{connect := Connect, prop := Prop}}] = ets:lookup(State#state.total_ets, PID),
            ?INFO("lone channel stop:~p, ~p, ~p", [PID, Reason, Prop]),
            ets:delete(State#state.total_ets, PID),
            ets:delete(State#state.busy_ets, PID),
            State#state{deads = [{channel, Connect, Prop} | Deads],
                        lone_free = lists:delete(PID, LoneFree)};
        throw:lone_wait_dead ->
            ?INFO("lone wait stop:~p, ~p, ~p", [PID, Reason, MRef]),
            State#state{lone_wait = queue:filter(fun ({_, _, R}) -> R =/= MRef end, State#state.lone_wait)};
        throw:lone_user_dead ->
            ?INFO("lone user stop:~p, ~p, ~p", [PID, Reason, MRef]),
            case ets:match(State#state.busy_ets, {'$1', '_', MRef}) of
                [[Channel]] ->
                    ets:delete(State#state.busy_ets, Channel),
                    do_checkin_lone(Channel, State);
                [] ->
                    State
            end
    end.

%%------------------------------------------------------------------------------
do_checkout_lone(CRef, {FromPID, _} = From, State) ->
    case State#state.lone_free of
        [Channel | T] ->
            MRef = erlang:monitor(process, FromPID),
            true = ets:insert(State#state.busy_ets, {Channel, CRef, MRef}),
            gen_server:reply(From, Channel),
            State#state{lone_free = T};
        [] ->
            MRef = erlang:monitor(process, FromPID),
            Wait = queue:in({From, CRef, MRef}, State#state.lone_wait),
            State#state{lone_wait = Wait}
    end.

do_checkin_lone(Channel, State) ->
    case ets:lookup(State#state.busy_ets, Channel) of
        [{Channel, _, MRef}] ->
            demonitor(MRef, [flush]),
            true = ets:delete(State#state.busy_ets, Channel),
            do_checkin_lone1(Channel, State);
        [] -> State
    end.

do_checkin_lone1(Channel, #state{lone_free = Free} = State) ->
    case queue:out(State#state.lone_wait) of
        {{value, {From, CRef, MRef}}, Left} ->
            true = ets:insert(State#state.busy_ets, {Channel, CRef, MRef}),
            gen_server:reply(From, Channel),
            State#state{lone_wait = Left};
        {empty, Empty} ->
            State#state{lone_free = [Channel | Free], lone_wait = Empty}
    end.

do_cancel_wait(CRef, #state{lone_wait = Wait} = State) ->
    case ets:match(State#state.busy_ets, {'$1', CRef, '$2'}) of
        [[Channel, MRef]] ->
            demonitor(MRef, [flush]),
            true = ets:delete(State#state.busy_ets, Channel),
            do_checkin_lone1(Channel, State);
        [] ->
            Cancel = fun({_, Ref, MRef}) when Ref =:= CRef ->
                             demonitor(MRef, [flush]),
                             false;
                        (_) ->
                             true
                     end,
            State#state{lone_wait = queue:filter(Cancel, Wait)}
    end.

