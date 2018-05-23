%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2018 redrock
%%% @doc rabbitmq pool
%%% @end
%%%-------------------------------------------------------------------
-module(rmp_confirm).

-export([publish/5]).

%% callbacks
-export([start/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%------------------------------------------------------------------------------
%% @doc define
%%------------------------------------------------------------------------------
-behaviour(gen_server).

-include("rabbitmq_pool.hrl").

-define(LOOP_TIME,   1000).
-define(ACK_TIMEOUT, 5000).

-record(waiter, {ref = undefined, start = 0,
                 method = undefined, content = <<>>,
                 ack = undefined, args = []}).

-record(state, {pool = undefined, channel = undefined,
                seqno = 0, waiters = #{}, retrys = []}).

%%------------------------------------------------------------------------------
%% @doc interface
%%------------------------------------------------------------------------------
-spec publish(pid(), any(), any(), function() | {Mod::atom(), Fun::atom()}, any()) -> {ok, any()} | {error, any()}.
publish(PID, Method, Connect, AckCall, Args) ->
    Ref = erlang:make_ref(),
    case rmp_util:safe_cast(PID, {publish, Method, Connect, AckCall, Args, Ref}) of
        ok -> {ok, Ref};
        {error, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc gen_server
%%------------------------------------------------------------------------------
start(Args) ->
    gen_server:start(?MODULE, [Args], []).

%%------------------------------------------------------------------------------
init([{Channel, Pool}]) ->
    amqp_channel:register_confirm_handler(Channel, self()),
    SeqNo = amqp_channel:next_publish_seqno(Channel),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    case rmp_bind:take_legacy(Pool) of
        undefined ->
            {ok, #state{pool = Pool, channel = Channel, seqno = SeqNo}, 0};
        List ->
            {ok, #state{pool = Pool, channel = Channel, seqno = SeqNo, retrys = List}, 0}
    end.

handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast({publish, Method, Content, AckCall, Args, Ref}, State) ->
    {noreply, handle_publish(Method, Content, AckCall, Args, Ref, State)};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(#'basic.ack'{delivery_tag = SeqNo, multiple = Multiple}, State) ->
    {noreply, handle_ack(SeqNo, Multiple, State)};

handle_info(timeout, State) ->
    State1 = handle_timeout(State),
    erlang:send_after(?LOOP_TIME, self(), timeout),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    case [Waiter || {_, Waiter} <- maps:to_list(State#state.waiters)] ++ State#state.retrys of
        [] -> ok;
        Retrys -> ok = rmp_bind:add_legacy(State#state.pool, Retrys)
    end.

%%------------------------------------------------------------------------------
%% handle
%%------------------------------------------------------------------------------
handle_publish(Method, Content, AckCall, Args, Ref, State) ->
    Waiter = #waiter{ref = Ref, start = erlang:system_time(seconds),
                     method = Method, content = Content,
                     ack = AckCall, args = Args},
    try_publish(Waiter, State).

try_publish(#waiter{method = Method, content = Content} = Waiter,
            #state{channel = Channel, waiters = Waiters, seqno = SeqNo} = State) ->
    SeqNo1 = SeqNo + 1,
    case rmp_util:safe_eval(Channel, fun(X) -> amqp_channel:cast(X, Method, Content) end) of
        ok ->
            State#state{seqno = SeqNo1, waiters = Waiters#{SeqNo1 => Waiter}};
        {error, Reason} ->
            ?WARN("publish channel msg fail:~p, ~p, ~p, ~p", [State#state.pool, self(), Channel, Reason]),
            case amqp_channel:next_publish_seqno(Channel) of
                SeqNo ->
                    State#state{retrys = [Waiter | State#state.retrys]};
                SeqNo1 ->
                    State#state{seqno = SeqNo1, waiters = Waiters#{SeqNo1 => Waiter}}
            end
    end.

%%------------------------------------------------------------------------------
handle_ack(SeqNo, false, #state{waiters = Waiters} = State) ->
    case maps:find(SeqNo, Waiters) of
        error ->
            ?WARN("confirm ack msg miss:~p, ~p, ~p, ~p", [SeqNo, Waiters]),
            State;
        {ok, #waiter{ref = Ref, ack = AckFun, args = Args}} ->
            catch run_fun(AckFun, Ref, Args),
            State#state{waiters = maps:remove(SeqNo, Waiters)}
    end;
handle_ack(SeqNo, true, #state{waiters = Waiters} = State) ->
    Pred = fun(K, _) when K > SeqNo -> true;
              (_, #waiter{ref = Ref, ack = AckFun, args = Args}) -> catch run_fun(AckFun, Ref, Args), false
           end,
    State#state{waiters = maps:filter(Pred, Waiters)}.

run_fun({Mod, Fun}, Ref, Args) -> Mod:Fun(Ref, Args);
run_fun(Fun, Ref, Args) when is_function(Fun) -> Fun(Ref, Args).

%%------------------------------------------------------------------------------
handle_timeout(State) ->
    State1 = check_timeout(State),
    _State2 = retry_publish(State1).

check_timeout(#state{waiters = Waiters} = State) ->
    Now = erlang:system_time(seconds),
    Fun = fun(_, #waiter{start = Time}, Acc) when Now < Time + ?ACK_TIMEOUT -> Acc;
              (K, Waiter, #state{waiters = M} = Acc) ->
                   Acc#state{retrys = [Waiter | State#state.retrys], waiters = maps:remove(K, M)}
           end,
    maps:fold(Fun, State, Waiters).

retry_publish(#state{retrys = Retrys} = State) ->
    lists:foldl(fun(Water, Acc) -> try_publish(Water, Acc) end, State, Retrys).

