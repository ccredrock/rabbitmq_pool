%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <meituan>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(rabbitmq_pool_sup).

-export([start_link/0, start_link/1]).
-export([init/1]).

%%------------------------------------------------------------------------------
-behaviour(supervisor).

%%------------------------------------------------------------------------------
start_link() ->
    Pools = proplists:delete(included_applications, application:get_all_env(rabbitmq_pool)),
    start_link(Pools) .

start_link(Pools) ->
    {ok, Sup} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    {ok, _} = supervisor:start_child(?MODULE, {rabbitmq_pool,
                                               {rabbitmq_pool, start_link, [Pools]},
                                               transient, infinity, worker,
                                               [rabbitmq_pool]}),
    {ok, Sup}.

init([]) ->
    {ok, {{one_for_one, 1, 60}, []}}.
