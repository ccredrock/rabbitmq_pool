%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <free>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(rabbitmq_pool_app).

-export([start/2, stop/1]).

%%------------------------------------------------------------------------------
-behaviour(application).

%%------------------------------------------------------------------------------
start(_StartType, _StartArgs) ->
    Result = rabbitmq_pool_sup:start_link(),
    rabbitmq_pool:init(),
    Result.

stop(_State) ->
    ok.
