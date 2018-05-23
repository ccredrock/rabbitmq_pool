%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2018 redrock
%%% @doc rabbitmq pool
%%% @end
%%%-------------------------------------------------------------------
-module(rmp_util).

-export([parse_pool/2]).
-export([map_set/4, map_add/4, map_del/4]).
-export([safe_cast/2, safe_eval/2]).

%%------------------------------------------------------------------------------
parse_pool(Pool, Prop) ->
    lists:duplicate(proplists:get_value(connect_size, Prop, 1), {connect, [{pool, Pool} | Prop]}).

%%------------------------------------------------------------------------------
map_set(Map, PKey, CKey, CVal) ->
    #{PKey := PVal} = Map,
    Map#{PKey => PVal#{CKey => CVal}}.

map_add(Map, PKey, CKey, CVal) ->
    #{PKey := #{CKey := List} = PVal} = Map,
    Map#{PKey => PVal#{CKey => [CVal | List]}}.

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

safe_eval(_PID, _Fun, Cnt, Cnt) -> {error, max_retry};
safe_eval(PID, Fun, RetryCnt, _Cnt) ->
    Ref = erlang:monitor(process, PID),
    case erlang:is_process_alive(PID) of
        false ->
            safe_eval(PID, Fun, RetryCnt + 1);
        true ->
            case catch Fun(PID) of
                {'EXIT', Reason} -> {error, Reason};
                _ ->
                    case receive {'DOWN', Ref, process, _, _} -> false after 0 -> true end of
                        false ->
                            safe_eval(PID, Fun, RetryCnt + 1);
                        true ->
                            erlang:demonitor(Ref), {ok, RetryCnt}
                    end
            end
    end.

