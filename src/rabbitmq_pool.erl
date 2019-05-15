%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2018 redrock
%%% @doc rabbitmq pool
%%% @end
%%%-------------------------------------------------------------------
-module(rabbitmq_pool).

-export([channel_call/3,
         channel_cast/3]).

-export([ack/2,
         nack/2,
         publish/2,
         safe_publish/4]).

-export([channel_call/1,
         channel_call/2,
         channel_cast/1,
         channel_cast/2,
         safe_publish/2,
         safe_publish/3]).

-export([rand_confirm/5,
         hash_confirm/6]).

-export([get_pools/0,
         get_props/1,
         get_deads/1,
         get_connects/1,
         get_channels/1,
         get_processes/1]).

-export([add_pools/1,
         add_pools/2,
         add_lone/4,
         add_bind/3]).

-export([pool_process/1,
         checkout_lone/1,
         checkin_lone/2]).

-export([start/0,
         init/0]).

%%------------------------------------------------------------------------------
%% @doc define
%%------------------------------------------------------------------------------
-include("rabbitmq_pool.hrl").

-define(TIMEOUT, 5000).

-record(pool, {name, type, prop, process, args}).

%%------------------------------------------------------------------------------
%% @doc interface
%%------------------------------------------------------------------------------
-spec start() -> {ok, []} | {error, any()}.
start() ->
    application:ensure_all_started(?MODULE).

init() ->
    rmp_util:init(),
    ets:new(?MODULE, [named_table, public, {read_concurrency, true}, {keypos, #pool.name}]),
    ets:new(rmp_confirm, [named_table, public, {read_concurrency, true}, {keypos, #pool.name}]),
    add_pools(get_pools_props()).

%%------------------------------------------------------------------------------
add_pools(Pools) -> add_pools(Pools, infinity).
add_pools(Pools, _Timeout) ->
    [begin
         lists:duplicate(proplists:get_value(connect_size, Prop, 1), {connect, [{pool, Pool} | Prop]}),
         case proplists:get_value(module, Prop) of
             undefined -> {ok, _} = rabbitmq_pool_sup:start_lone(Pool, Prop);
             _ -> {ok, _} = rabbitmq_pool_sup:start_bind(Pool, Prop)
         end
     end || {Pool, Prop} <- Pools].

add_lone(Name, Prop, Channels, Busy) ->
    ets:insert(?MODULE, #pool{name = Name, type = lone, prop = Prop,
                              process = self(), args = #{channel_ets => Channels, busy_ets => Busy}}).

add_bind(Name, Prop, Processes) ->
    ets:insert(?MODULE, #pool{name = Name, type = bind, prop = Prop,
                              process = self(), args = #{process_ets => Processes}}).

%%------------------------------------------------------------------------------
channel_call(Method) -> channel_call(Method, none).
channel_call(Method, Content) ->
    channel_call(get_default_bind(), Method, Content).

channel_call(Pool, Method, Content) ->
    [#pool{args = #{channel_ets := ETS}}] = ets:lookup(?MODULE, Pool),
    case ets:tab2list(ETS) of
        [] -> {error, empty_channel};
        List ->
            {Channel, _} = lists:nth(rand:uniform(length(List)), List),
            channel_call1(Channel, Method, Content)
    end.

channel_call1(Channel, Method, Content) ->
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
    channel_call(get_default_bind(), Method, Content).

channel_cast(Pool, Method, Content) ->
    [#pool{args = #{channel_ets := ETS}}] = ets:lookup(?MODULE, Pool),
    case ets:tab2list(ETS) of
        [] ->
            {error, empty_channel};
        List ->
            {Channel, _} = lists:nth(rand:uniform(length(List)), List),
            amqp_channel:cast(Channel, Method, Content)
    end.

%%------------------------------------------------------------------------------
safe_publish(Exchange, Payload) -> safe_publish(Exchange, Payload, <<>>).
safe_publish(Exchange, Payload, RoutingKey) ->
    safe_publish(get_default_bind(), Exchange, Payload, RoutingKey).

safe_publish(Pool, Exchange, Payload, RoutingKey) ->
    Process = pool_process(Pool),
    case catch checkout_lone(Process) of
        {'EXIT', Reason} ->
            {error, Reason};
        Channel ->
            try
                Content = #amqp_msg{props = #'P_basic'{delivery_mode = 2}, payload = Payload},
                amqp_channel:cast(Channel, #'basic.publish'{exchange = Exchange, routing_key = RoutingKey}, Content),
                true = amqp_channel:wait_for_confirms(Channel, ?TIMEOUT), ok
            catch
                E:R -> {error, {E, R}}
            after
                checkin_lone(Process, Channel)
            end
    end.

rand_confirm(Pool, Method, Content, AckCall, Args) ->
    [#pool{args = #{process_ets := ETS}}] = ets:lookup(?MODULE, Pool),
    case rmp_bind:get_processes(ETS) of
        [] -> {error, empty_channel};
        List ->
            PID = lists:nth(rand:uniform(length(List)), List),
            rmp_confirm:publish(PID, Method, Content, AckCall, Args)
    end.

hash_confirm(Pool, Method, Content, AckCall, Args, Key) ->
    [#pool{args = #{process_ets := ETS}}] = ets:lookup(?MODULE, Pool),
    case rmp_bind:get_processes(ETS) of
        [] -> {error, empty_channel};
        List ->
            PID = lists:nth(erlang:phash2(Key, length(List)) + 1, List),
            rmp_confirm:publish(PID, Method, Content, AckCall, Args)
    end.

%%------------------------------------------------------------------------------
pool_process(Pool) ->
    ets:lookup_element(?MODULE, Pool, #pool.process).

checkout_lone(Process) ->
    CRef = make_ref(),
    try
        gen_server:call(Process, {checkout_lone, CRef})
    catch
        Class:Reason ->
            gen_server:cast(Process, {cancel_wait, CRef}),
            erlang:raise(Class, Reason, erlang:get_stacktrace())
    end.

checkin_lone(Process, Channel) ->
    gen_server:cast(Process, {checkin_lone, Channel}).

%%------------------------------------------------------------------------------
get_pools() ->
    [Name || #pool{name = Name} <- ets:tab2list(?MODULE)].

get_props(Pool) ->
    element(#pool.prop, hd(ets:lookup(?MODULE, Pool))).

get_deads(Pool) ->
    Proc = pool_process(Pool),
    case ets:lookup(?MODULE, Pool) of
        [#pool{type = lone, process = Proc}] -> rmp_lone:get_deads(Proc);
        [#pool{process = Proc}] -> rmp_bind:get_deads(Proc)
    end.

get_connects(Pool) ->
    Proc = pool_process(Pool),
    case ets:lookup(?MODULE, Pool) of
        [#pool{type = lone, process = Proc}] -> rmp_lone:get_connects(Proc);
        [#pool{process = Proc}] -> rmp_bind:get_connects(Proc)
    end.

get_channels(Pool) ->
    case ets:lookup(?MODULE, Pool) of
        [#pool{type = lone, args = #{channel_ets := Channels}}] -> rmp_lone:get_channels(Channels);
        [#pool{process = Proc}] -> rmp_bind:get_channels(Proc)
    end.

get_processes(Pool) ->
    case ets:lookup(?MODULE, Pool) of
        [#pool{args = #{process_ets := ETS}}] -> rmp_bind:get_processes(ETS);
        [] -> []
    end.

%%------------------------------------------------------------------------------
get_default_bind() ->
    case get_pools_props() of
        [] -> undefined;
        List ->
            case [Pool || {Pool, Prop} <- List, proplists:get_value(module, Prop) =:= undefined] of
                [Pool | _] -> Pool;
                _ -> undefined
            end
    end.

get_pools_props() ->
    case application:get_env(rabbitmq_pool, pools) of
        undefined ->
            case proplists:delete(included_applications, application:get_all_env(rabbitmq_pool)) of
                List when is_list(List) -> List;
                _ -> []
            end;
        {ok, List} -> List
    end.

%%------------------------------------------------------------------------------
%% common
%%------------------------------------------------------------------------------
ack(Channel, Tag) ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}, none).

nack(Channel, Tag) ->
    amqp_channel:cast(Channel, #'basic.nack'{delivery_tag = Tag, requeue = true}).

publish(Exchange, Payload) ->
    Content = #amqp_msg{props = #'P_basic'{delivery_mode = 2}, payload = Payload},
    channel_cast(#'basic.publish'{exchange = Exchange}, Content).

