%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2018 redrock
%%% @doc rabbitmq pool
%%% @end
%%%-------------------------------------------------------------------
-module(rmp_util).

-export([parse_pool/2]).
-export([map_set/4, map_add/4, map_del/4]).

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

