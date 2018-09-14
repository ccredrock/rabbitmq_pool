%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <free>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(rabbitmq_pool_sup).

-export([start_link/0, init/1]).

-export([start_bind/2, start_lone/2]).

%%------------------------------------------------------------------------------
-behaviour(supervisor).

%%------------------------------------------------------------------------------
start_link() ->
    {ok, Sup} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    {ok, _} = supervisor:start_child(?MODULE, {rmp_bank,
                                               {rmp_bank, start_link, []},
                                               transient, infinity, worker,
                                               [rmp_bank]}),
    {ok, Sup}.

init([]) ->
    {ok, {{one_for_one, 1, 60}, []}}.

start_bind(Pool, Props) ->
    supervisor:start_child(?MODULE, {Pool,
                                     {rmp_bind, start_link, [{Pool, Props}]},
                                     transient, infinity, worker,
                                     []}).

start_lone(Pool, Props) ->
    supervisor:start_child(?MODULE, {Pool,
                                     {rmp_lone, start_link, [{Pool, Props}]},
                                     transient, infinity, worker,
                                     []}).

