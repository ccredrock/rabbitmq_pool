%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <free>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(rabbitmq_pool).

-export([start/0, stop/0]).

-export([start_link/0,
         channel_call/1,
         channel_call/2,
         channel_cast/1,
         channel_cast/2,
         checkout_lone/0,
         ack/2,
         nack/2,
         publish/2,
         safe_publish/2,
         safe_publish/3]).

-export([get_connects/0,
         get_channels/0,
         get_bind_channels/0,
         get_lone_channels/0,
         get_processes/0,
         get_deads/0,
         get_lone_free/0,
         get_lone_wait/0,
         get_lone_busy/0,
         add_pools/1]).

%% callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-define(TIMEOUT, 5000).

-define(ETS_LONE_CHANNELS, '$rabbitmq_pool_lone_channels').
-define(ETS_LONE_BUSY,     '$rabbitmq_pool_lone_busy').

-define(RECONNECT_REFRESH, refresh_all).
-define(RECONNECT_SINGLE,  single_one).

-define(PUBLISH_CONFIRM,  confirm).

-define(INFO(F, A), error_logger:info_msg("rabbitmq_pool " ++ F, A)).
-define(WARN(F, A), error_logger:warning_msg("rabbitmq_pool " ++ F, A)).

-define(AMQP_NETWORK(List),
        #amqp_params_network{host         = proplists:get_value(host, List),
                             port         = proplists:get_value(port, List),
                             username     = proplists:get_value(username, List),
                             password     = proplists:get_value(password, List),
                             virtual_host = proplists:get_value(virtual_host, List)}).

-record(state, {connects     = #{},
                bind_channel = #{},
                bind_process = #{},
                lone_free = [],
                lone_wait = queue:new(),
                deads = []}).

%%------------------------------------------------------------------------------
start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

channel_call(Method) -> channel_call(Method, none).
channel_call(Method, Content) ->
    case ets:tab2list(?ETS_LONE_CHANNELS) of
        [] -> {error, empty_channel};
        List ->
            {Channel, _} = lists:nth(rand:uniform(length(List)), List),
            channel_call(Channel, Method, Content)
    end.
channel_call(Channel, Method, Content) ->
    try
        case amqp_channel:call(Channel, Method, Content) of
            blocked -> {error, blocked};
            closing -> {error, closing};
            Result -> Result
        end
    catch
        E:R -> {error, {E, R}}
    end.

channel_cast(Method) -> channel_cast(Method, none).
channel_cast(Method, Content) ->
    case ets:tab2list(?ETS_LONE_CHANNELS) of
        [] ->
            {error, empty_channel};
        List ->
            {Channel, _} = lists:nth(rand:uniform(length(List)), List),
            amqp_channel:cast(Channel, Method, Content)
    end.

checkout_lone() ->
    CRef = make_ref(),
    try
        gen_server:call(?MODULE, {checkout_lone, CRef})
    catch
        Class:Reason ->
            gen_server:cast(?MODULE, {cancel_wait, CRef}),
            erlang:raise(Class, Reason, erlang:get_stacktrace())
    end.

checkin_lone(Channel) ->
    gen_server:cast(?MODULE, {checkin_lone, Channel}).

ack(Channel, Tag) ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}, none).

nack(Channel, Tag) ->
    amqp_channel:cast(Channel, #'basic.nack'{delivery_tag = Tag, requeue = true}).

publish(Exchange, Payload) ->
    Content = #amqp_msg{props = #'P_basic'{delivery_mode = 2}, payload = Payload},
    channel_cast(#'basic.publish'{exchange = Exchange}, Content).

safe_publish(Exchange, Payload) -> safe_publish(Exchange, Payload, <<>>).
safe_publish(Exchange, Payload, RoutingKey) ->
    case catch checkout_lone() of
        {'EXIT', Reason} -> {error, Reason};
        Channel ->
            try
                Content = #amqp_msg{props = #'P_basic'{delivery_mode = 2}, payload = Payload},
                amqp_channel:cast(Channel, #'basic.publish'{exchange = Exchange, routing_key = RoutingKey}, Content),
                true = amqp_channel:wait_for_confirms(Channel, ?TIMEOUT), ok
            catch
                E:R -> {error, {E, R}}
            after
                checkin_lone(Channel)
            end
    end.

%%------------------------------------------------------------------------------
get_connects() ->
    State = sys:get_state(?MODULE),
    [PID || {PID, _} <- maps:to_list(State#state.connects)].

get_channels() ->
    State = sys:get_state(?MODULE),
    [Channel || {Channel, _} <- ets:tab2list(?ETS_LONE_CHANNELS)], maps:keys(State#state.bind_channel).

get_lone_channels() ->
    case get_channels() of
        {Lone, _Bind} -> Lone;
        Result -> Result
    end.

get_bind_channels() ->
    case get_channels() of
        {_Lone, Bind} -> Bind;
        Result -> Result
    end.

get_processes() -> maps:keys(element(#state.bind_process, sys:get_state(?MODULE))).
get_deads() -> element(#state.deads, sys:get_state(?MODULE)).
get_lone_wait() -> element(#state.lone_wait, sys:get_state(?MODULE)).
get_lone_free() -> element(#state.lone_free, sys:get_state(?MODULE)).
get_lone_busy() -> ets:tab2list(?ETS_LONE_BUSY).

add_pools(Pools) ->
    gen_server:call(?MODULE, {add_pools, Pools}).

%%------------------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
init([]) ->
    ets:new(?ETS_LONE_CHANNELS, [named_table, public, {read_concurrency, true}]),
    ets:new(?ETS_LONE_BUSY, [named_table, private]),
    Pools = do_forward_compatible_pools(),
    List = lists:flatten([lists:duplicate(proplists:get_value(connect_size, Prop), {connect, Prop})
                          || {_Name, Prop}  <- Pools]),
    case do_reborn_deads(List, #state{deads = []}) of
        #state{deads = []} = State -> {ok, State, 0};
        #state{deads = [Dead | _]} -> {error, {fail_connect, Dead}}
    end.

handle_call({add_pools, Pools}, _From, State) ->
    List = [lists:duplicate(proplists:get_value(connect_size, Prop), {connect, Prop}) || {_Name, Prop}  <- Pools],
    {reply, ok, State#state{deads = [State#state.deads | List]}};
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

handle_info({'DOWN', MRef, process, PID, _Reason}, State) ->
    State1 = do_find_deads(PID, MRef, State),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
do_forward_compatible_pools() ->
    case application:get_env(rabbitmq_pool, pools) of
        undefined ->
            case proplists:delete(included_applications, application:get_all_env(rabbitmq_pool)) of
                List when is_list(List) -> List;
                _ -> []
            end;
        {ok, List} -> List
    end.

do_reborn_deads([{connect, Prop} = H | T],
               #state{connects = ConnectSet, deads = Deads} = State) ->
    case catch amqp_connection:start(?AMQP_NETWORK(Prop)) of
        {ok, Connect} ->
            ?INFO("conenct start:~p,~p", [Connect, Prop]),
            erlang:monitor(process, Connect),
            List = lists:duplicate(proplists:get_value(channel_size, Prop), {channel, Connect, Prop}),
            State1 = State#state{connects = ConnectSet#{Connect => #{prop => Prop, childs => []}}},
            do_reborn_deads(T ++ List, State1);
        _ ->
            do_reborn_deads(T, State#state{deads = [H | Deads]})
    end;
do_reborn_deads([{channel, Connect, Prop} = H | T],
               #state{bind_channel = BChannelSet, deads = Deads} = State) ->
    case is_process_alive(Connect) of
        true ->
            Mod = proplists:get_value(module, Prop),
            case catch amqp_connection:open_channel(Connect) of
                {ok, Channel} when Mod =:= undefined ->
                    ?INFO("lone channel start:~p,~p", [Channel, Prop]),
                    do_check_confirm(Channel),
                    erlang:monitor(process, Channel),
                    ets:insert(?ETS_LONE_CHANNELS, {Channel, #{connect => Connect, prop => Prop}}),
                    do_reborn_deads(T, State#state{lone_free = [Channel | State#state.lone_free]});
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
            case catch Mod:start(Channel) of
                {ok, Process}  ->
                    ?INFO("bind process start:~p,~p", [Channel, Prop]),
                    process_flag(trap_exit, false),
                    erlang:monitor(process, Process),
                    BChannelSet1 = do_child_set(BChannelSet, Channel, process, Process),
                    BProcessSet1 = BProcessSet#{Process => #{connect => Connect, channel => Channel, prop => Prop}},
                    ConnectSet1 = do_child_add(ConnectSet, Connect, childs, Process),
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

do_check_confirm(Channel) ->
    case application:get_env(rabbitmq_pool, publish_method, undefined) of
        undefined-> skip;
        confirm -> #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{})
    end.

%%------------------------------------------------------------------------------
do_find_deads(PID, MRef, #state{deads = Deads,
                                connects = ConnectSet,
                                bind_channel = BChannelSet,
                                bind_process = BProcessSet,
                                lone_free = LoneFree,
                                lone_wait = LoneWait} = State) ->
    try
        maps:is_key(PID, ConnectSet) andalso throw(connect_dead),
        maps:is_key(PID, BProcessSet) andalso throw(bind_process_dead),
        maps:is_key(PID, BChannelSet) andalso throw(bind_channel_dead),
        queue:member(PID, LoneWait) andalso throw(lone_wait_dead),
        ets:lookup(?ETS_LONE_CHANNELS, PID) =/= [] andalso throw(lone_channel_dead),
        throw(lone_user_dead)
    catch
        throw:connect_dead ->
            {ok, #{childs := Childs, prop := Prop}} = maps:find(PID, ConnectSet),
            ?INFO("connect stop:~p,~p", [PID, Prop]),
            ConnectSet1 = maps:remove(PID, ConnectSet),
            do_check_refresh(PID, Prop),
            [exit(Process, shutdown) || Process <- Childs],
            State#state{connects = ConnectSet1, deads = [{connect, Prop} | Deads]};
        throw:bind_process_dead ->
            {ok, #{connect := Connect, channel := Channel, prop := Prop}} = maps:find(PID, BProcessSet),
            ?INFO("bind process stop:~p,~p", [PID, Prop]),
            State#state{bind_process = maps:remove(PID, BProcessSet),
                        connects = do_child_del(ConnectSet, Connect, childs, PID),
                        deads = [{process, Connect, Channel, Prop} | Deads]};
        throw:bind_channel_dead ->
            {ok, #{connect := Connect, process := Process, prop := Prop}} = maps:find(PID, BChannelSet),
            ?INFO("bind channel stop:~p,~p", [PID, Prop]),
            exit(Process, shutdown),
            State#state{bind_channel = maps:remove(PID, BChannelSet),
                        deads = [{channel, Connect, Prop} | Deads]};
        throw:lone_channel_dead ->
            [{PID, #{connect := Connect, prop := Prop}}] = ets:lookup(?ETS_LONE_CHANNELS, PID),
            ?INFO("lone channel stop:~p,~p", [PID, Prop]),
            ets:delete(?ETS_LONE_CHANNELS, PID),
            ets:delete(?ETS_LONE_BUSY, PID),
            State#state{deads = [{channel, Connect, Prop} | Deads],
                        lone_free = lists:delete(PID, LoneFree)};
        throw:lone_wait_dead ->
            ?INFO("lone wait stop:~p,~p", [PID, MRef]),
            State#state{lone_wait = queue:filter(fun ({_, _, R}) -> R =/= MRef end, State#state.lone_wait)};
        throw:lone_user_dead ->
            ?INFO("lone user stop:~p,~p", [PID, MRef]),
            case ets:match(?ETS_LONE_BUSY, {'$1', '_', MRef}) of
                [[Channel]] ->
                    ets:delete(?ETS_LONE_BUSY, Channel),
                    do_checkin_lone(Channel, State);
                [] ->
                    State
            end
    end.

do_check_refresh(PID, Prop) ->
    case application:get_env(rabbitmq_pool, reconnect_method, ?RECONNECT_SINGLE) of
        ?RECONNECT_SINGLE -> skip;
        ?RECONNECT_REFRESH ->
            case application:stop(amqp_client) of
                {error, _} -> skip;
                ok -> ?WARN("stop amqp_client becase connect stop:~p,~p", [PID, Prop])
            end
    end.

%%------------------------------------------------------------------------------
do_child_set(Map, PKey, CKey, CVal) ->
    #{PKey := PVal} = Map,
    Map#{PKey => PVal#{CKey => CVal}}.

do_child_add(Map, PKey, CKey, CVal) ->
    #{PKey := #{CKey := List} = PVal} = Map,
    Map#{PKey => PVal#{CKey => [CVal | List]}}.

do_child_del(Map, PKey, CKey, CVal) ->
    case maps:find(PKey, Map) of
        {ok,  #{CKey := List} = PVal} ->
            Map#{PKey => PVal#{CKey => lists:delete(CVal, List)}};
        error ->
            Map
    end.

%%------------------------------------------------------------------------------
do_checkout_lone(CRef, {FromPID, _} = From, State) ->
    case State#state.lone_free of
        [Channel | T] ->
            MRef = erlang:monitor(process, FromPID),
            true = ets:insert(?ETS_LONE_BUSY, {Channel, CRef, MRef}),
            gen_server:reply(From, Channel),
            State#state{lone_free = T};
        [] ->
            MRef = erlang:monitor(process, FromPID),
            Wait = queue:in({From, CRef, MRef}, State#state.lone_wait),
            State#state{lone_wait = Wait}
    end.

do_checkin_lone(Channel, State) ->
    case ets:lookup(?ETS_LONE_BUSY, Channel) of
        [{Channel, _, MRef}] ->
            demonitor(MRef, [flush]),
            true = ets:delete(?ETS_LONE_BUSY, Channel),
            do_checkin_lone1(Channel, State);
        [] -> State
    end.

do_checkin_lone1(Channel, #state{lone_free = Free} = State) ->
    case queue:out(State#state.lone_wait) of
        {{value, {From, CRef, MRef}}, Left} ->
            true = ets:insert(?ETS_LONE_BUSY, {Channel, CRef, MRef}),
            gen_server:reply(From, Channel),
            State#state{lone_wait = Left};
        {empty, Empty} ->
            State#state{lone_free = [Channel | Free], lone_wait = Empty}
    end.

do_cancel_wait(CRef, #state{lone_wait = Wait} = State) ->
    case ets:match(?ETS_LONE_BUSY, {'$1', CRef, '$2'}) of
        [[Channel, MRef]] ->
            demonitor(MRef, [flush]),
            true = ets:delete(?ETS_LONE_BUSY, Channel),
            do_checkin_lone(Channel, State);
        [] ->
            Cancel = fun({_, Ref, MRef}) when Ref =:= CRef ->
                             demonitor(MRef, [flush]),
                             false;
                        (_) ->
                             true
                     end,
            State#state{lone_wait = queue:filter(Cancel, Wait)}
    end.

