%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <meituan>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(rabbitmq_pool).

-export([start/0, stop/0]).

-export([start_link/1,
         channel_call/1,
         channel_call/2]).

-export([get_connects/0,
         get_channels/0,
         get_bind_channels/0,
         get_lone_channels/0,
         get_processes/0,
         get_deads/0,
         add_pools/1]).

%% callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-define(TIMEOUT, 5000).

-define(ETS_LONE_CHANNELS, '$rabbitmq_pool_normals').

-define(AMQP_NETWORK(List),
        #amqp_params_network{host         = proplists:get_value(host, List),
                             port         = proplists:get_value(port, List),
                             username     = proplists:get_value(username, List),
                             password     = proplists:get_value(password, List),
                             virtual_host = proplists:get_value(virtual_host, List)}).

-record(state, {connects     = maps:new(),
                bind_channel = maps:new(),
                bind_process = maps:new(),
                deads = []}).

%%------------------------------------------------------------------------------
start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

channel_call(Method) ->
    channel_call(Method, none).

channel_call(Method, Content) ->
    List = ets:tab2list(?ETS_LONE_CHANNELS),
    {Channel, _} = lists:nth(rand:uniform(length(List)), List),
    try
        case amqp_channel:call(Channel, Method, Content) of
            blocked -> {error, blocked};
            closing -> {error, closing};
            Result -> Result
        end
    catch
        E:R -> {error, {E, R}}
    end.

%%------------------------------------------------------------------------------
get_connects() ->
    gen_server:call(?MODULE, get_connects).

get_channels() ->
    gen_server:call(?MODULE, get_channels).

get_lone_channels() ->
    case gen_server:call(?MODULE, get_channels) of
        {Lone, _Bind} -> Lone;
        Result -> Result
    end.

get_bind_channels() ->
    case gen_server:call(?MODULE, get_channels) of
        {_Lone, Bind} -> Bind;
        Result -> Result
    end.

get_processes() ->
    gen_server:call(?MODULE, get_processes).

get_deads() ->
    gen_server:call(?MODULE, get_deads).

add_pools(Pools) ->
    gen_server:call(?MODULE, {add_pools, Pools}).

%%------------------------------------------------------------------------------
start_link(Pools) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Pools], []).

%%------------------------------------------------------------------------------
init([Pools]) ->
    ets:new(?ETS_LONE_CHANNELS, [named_table, public, {read_concurrency, true}]),
    List = lists:flatten([lists:duplicate(proplists:get_value(connect_size, Prop), {connect, Prop})
                          || {_Name, Prop}  <- Pools]),
    case do_check_deads(List, #state{deads = []}) of
        #state{deads = []} = State -> {ok, State, 0};
        #state{deads = Deads} -> {error, Deads}
    end.

handle_call(get_channels, _From, State) ->
    {reply, {[Channel || {Channel, _} <- ets:tab2list(?ETS_LONE_CHANNELS)],
             maps:keys(State#state.bind_channel)}, State};
handle_call(get_processes, _From, State) ->
    {reply, maps:keys(State#state.bind_process), State};
handle_call({add_pools, Pools}, _From, State) ->
    List = [lists:duplicate(proplists:get_value(connect_size, Prop), {connect, Prop}) || {_Name, Prop}  <- Pools],
    {reply, ok, State#state{deads = [State#state.deads | List]}};
handle_call(get_deads, _From, State) ->
    {reply, State#state.deads, State};
handle_call(get_connects, _From, State) ->
    {reply, [PID || {PID, _} <- maps:to_list(State#state.connects)], State};
handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(timeout, State) ->
    State1 = do_check_deads(State#state.deads, State#state{deads = []}),
    erlang:send_after(?TIMEOUT, self(), timeout),
    {noreply, State1};

handle_info({'DOWN', _Ref, process, PID, _Reason}, State) ->
    State1 = do_input_deads(PID, State),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
do_check_deads([{connect, Prop} = H | T],
               #state{connects = ConnectSet, deads = Deads} = State) ->
    case catch amqp_connection:start(?AMQP_NETWORK(Prop)) of
        {ok, Connect} ->
            erlang:monitor(process, Connect),
            List = lists:duplicate(proplists:get_value(channel_size, Prop), {channel, Connect, Prop}),
            State1 = State#state{connects = ConnectSet#{Connect => #{prop => Prop, childs => []}}},
            do_check_deads(T ++ List, State1);
        _ ->
            do_check_deads(T, State#state{deads = [H | Deads]})
    end;
do_check_deads([{channel, Connect, Prop} = H | T],
               #state{bind_channel = BChannelSet, deads = Deads} = State) ->
    case is_process_alive(Connect) of
        true ->
            Mod = proplists:get_value(module, Prop),
            case catch amqp_connection:open_channel(Connect) of
                {ok, Channel} when Mod =:= undefined ->
                    erlang:monitor(process, Channel),
                    ets:insert(?ETS_LONE_CHANNELS, {Channel, #{connect => Connect, prop => Prop}}),
                    do_check_deads(T, State);
                {ok, Channel} ->
                    erlang:monitor(process, Channel),
                    BChannelSet1 = BChannelSet#{Channel => #{connect => Connect, prop => Prop}},
                    do_check_deads(T ++ [{process, Connect, Channel, Prop}], State#state{bind_channel = BChannelSet1});
                _ ->
                    do_check_deads(T, State#state{deads = [H | Deads]})
            end;
        false ->
            do_check_deads(T, State)
    end;
do_check_deads([{process, Connect, Channel, Prop} = H | T],
               #state{bind_channel = BChannelSet,
                      bind_process = BProcessSet,
                      connects = ConnectSet,
                      deads = Deads} = State) ->
    case is_process_alive(Connect)
         andalso is_process_alive(Channel) of
        true ->
            Mod = proplists:get_value(module, Prop),
            case catch Mod:start(Channel) of
                {ok, Process}  ->
                    erlang:monitor(process, Process),
                    BChannelSet1 = do_child_set(BChannelSet, Channel, process, Process),
                    BProcessSet1 = BProcessSet#{Process => #{connect => Connect, channel => Channel, prop => Prop}},
                    ConnectSet1 = do_child_add(ConnectSet, Connect, childs, Process),
                    do_check_deads(T, State#state{bind_channel = BChannelSet1,
                                                  bind_process = BProcessSet1,
                                                  connects = ConnectSet1});
                _ ->
                    do_check_deads(T, State#state{deads = [H | Deads]})
            end;
        _ ->
            do_check_deads(T, State)
    end;
do_check_deads([], State) -> State.

do_input_deads(PID, #state{deads = Deads, connects = ConnectSet} = State) ->
    case maps:find(PID, ConnectSet) of
        {ok, #{childs := Childs, prop := Prop}} ->
            ConnectSet1 = maps:remove(PID, ConnectSet),
            [true = exit(Process, shutdown) || Process <- Childs],
            State#state{connects = ConnectSet1, deads = [{connect, Prop} | Deads]};
        error ->
            case ets:lookup(?ETS_LONE_CHANNELS, PID) of
                [] ->
                    do_input_deads_bind(PID, State);
                [{PID, #{connect := Connect, prop := Prop}}] ->
                    ets:delete(?ETS_LONE_CHANNELS, PID),
                    State#state{deads = [{channel, Connect, Prop} | Deads]}
            end
    end.

do_input_deads_bind(PID,
                    #state{bind_channel = BChannelSet,
                           bind_process = BProcessSet,
                           connects = ConnectSet,
                           deads = Deads} = State) ->
    case maps:find(PID, BProcessSet) of
        {ok, #{connect := Connect, channel := Channel, prop := Prop}} ->
            State#state{bind_process = maps:remove(PID, BProcessSet),
                        connects = do_child_del(ConnectSet, Connect, childs, PID),
                        deads = [{process, Connect, Channel, Prop} | Deads]};
        error ->
            case maps:find(PID, BChannelSet) of
                {ok, #{connect := Connect, process := Process, prop := Prop}} ->
                    true = exit(Process, shutdown),
                    State#state{bind_channel = maps:remove(PID, BChannelSet),
                                deads = [{channel, Connect, Prop} | Deads]};
                error ->
                    State
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

