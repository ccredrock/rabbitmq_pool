%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2018 redrock
%%% @doc rabbitmq pool
%%% @end
%%%-------------------------------------------------------------------
-module(rmp_util).

-export([init/0]).
-export([parse_pool/2]).
-export([map_set/4, map_add/4, map_del/4]).
-export([safe_cast/2, safe_eval/2]).
-export([connect/1]).

%%------------------------------------------------------------------------------
%% define
%%------------------------------------------------------------------------------
-include("rabbitmq_pool.hrl").

%%------------------------------------------------------------------------------
%% interface
%%------------------------------------------------------------------------------
init() ->
    ets:new(?MODULE, [public, named_table]).

%%------------------------------------------------------------------------------
parse_pool(Pool, Prop) ->
    lists:duplicate(proplists:get_value(connect_size, Prop, 1), {connect, [{pool, Pool} | Prop]}).

%%------------------------------------------------------------------------------
map_set(Map, PKey, CKey, CVal) ->
    case maps:find(PKey, Map) of
        {ok, PVal} ->
            Map#{PKey => PVal#{CKey => CVal}};
        error ->
            Map#{PKey => #{CKey => CVal}}
    end.

map_add(Map, PKey, CKey, CVal) ->
    case maps:find(PKey, Map) of
        {ok,  #{CKey := List} = PVal} ->
            Map#{PKey => PVal#{CKey => [CVal | List]}};
        error ->
            Map#{PKey => #{CKey => [CVal]}}
    end.

map_del(Map, PKey, CKey, CVal) ->
    case maps:find(PKey, Map) of
        {ok,  #{CKey := List} = PVal} ->
            Map#{PKey => PVal#{CKey => lists:delete(CVal, List)}};
        error ->
            Map
    end.

safe_cast(PID, Msg) ->
    safe_eval(PID, fun(X) -> gen_server:cast(X, Msg) end).

safe_eval(PID, Fun) ->
    case safe_eval(PID, Fun, 0) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

safe_eval(PID, Fun, Cnt) ->
    safe_eval(PID, Fun, -1, Cnt).

safe_eval(_PID, _Fun, Cnt, MaxCnt) when Cnt >= MaxCnt -> {error, max_retry};
safe_eval(PID, Fun, RetryCnt, Cnt) ->
    Ref = erlang:monitor(process, PID),
    case erlang:is_process_alive(PID) of
        false ->
            safe_eval(PID, Fun, RetryCnt + 1, Cnt);
        true ->
            case catch Fun(PID) of
                {'EXIT', Reason} -> {error, Reason};
                _ ->
                    case receive {'DOWN', Ref, process, _, _} -> false after 0 -> true end of
                        false ->
                            safe_eval(PID, Fun, RetryCnt + 1, Cnt);
                        true ->
                            erlang:demonitor(Ref), {ok, RetryCnt}
                    end
            end
    end.

%%------------------------------------------------------------------------------
connect(Prop) ->
    Prop1 = form_connect_prop(Prop),
    case lists:member(proxy_rr, proplists:get_value(connect_method, Prop, [])) of
        true -> connect_proxy_rr(Prop1);
        _ -> connect_ips(Prop1, undefined)
    end.

form_connect_prop(Prop) ->
    case proplists:get_value(nodes, Prop) of
        undefined ->
            [Prop];
        List ->
            Prop1 = proplists:delete(port, proplists:delete(host, Prop)),
            lists:sort([Prop1 ++ [{host, Host}, {port, Port}] || {Host, Port} <- List])
    end.

connect_proxy_rr([Prop]) ->
    case catch amqp_connection:start(?AMQP_NETWORK(Prop)) of
        {ok, Connect} ->
            {ok, Connect};
        _ ->
            case catch amqp_connection:start(?AMQP_NETWORK(Prop)) of
                {ok, Connect} ->
                    {ok, Connect};
                Result ->
                    Result
            end
    end.

connect_ips([Prop | T], _) ->
    case is_available_node(Prop) of
        false ->
            connect_ips(T, {error, disable_node});
        _ ->
            case catch amqp_connection:start(?AMQP_NETWORK(Prop)) of
                {ok, Connect} ->
                    {ok, Connect};
                Result ->
                    ?WARN("conenct fail disable node:~p,~p", [Prop, Result]),
                    disable_node(Prop),
                    connect_ips(T, Result)
            end
    end;
connect_ips([], Result) ->
    Result.

disable_node(Prop) ->
    Host = proplists:get_value(host, Prop),
    Port = proplists:get_value(port, Prop),
    ets:insert(?MODULE, {{disable_node, Host, Port}, erlang:system_time(milli_seconds)}).

is_available_node(Prop) ->
    Host = proplists:get_value(host, Prop),
    Port = proplists:get_value(port, Prop),
    Now = erlang:system_time(milli_seconds),
    Timeout = application:get_env(rabbitmq_pool, disable_timeout, 1000),
    case ets:lookup(?MODULE, {disable_node, Host, Port}) of
        [{_, Last}] when Now - Last =< Timeout -> false;
        _ -> true
    end.

