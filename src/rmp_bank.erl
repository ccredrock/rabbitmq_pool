%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2018 redrock
%%% @doc rabbitmq pool
%%% @end
%%%-------------------------------------------------------------------
-module(rmp_bank).

-export([add_legacy/2,
         take_legacy/1]).

%% callbacks
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%------------------------------------------------------------------------------
%% @doc define
%%------------------------------------------------------------------------------
-behaviour(gen_server).

-include("rabbitmq_pool.hrl").

-record(state, {legacy = #{}}).

%%------------------------------------------------------------------------------
%% @doc interface
%%------------------------------------------------------------------------------
add_legacy(Pool, V) ->
    X = gen_server:call(?MODULE, {add_legacy, Pool, V}),
    ?INFO("~p add_legacy:~p, ~p, ~p", [self(), Pool, V, X]),
    X.


take_legacy(Pool) ->
    X =
    case catch gen_server:call(?MODULE, {take_legacy, Pool}) of
        {'EXIT', _} -> undefined;
        Result -> Result
    end,
    ?INFO("~p take_legacy:~p, ~p", [self(), Pool, X]),
    X.

%%------------------------------------------------------------------------------
%% @doc gen_server
%%------------------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [[]], []).

%%------------------------------------------------------------------------------
init(_) ->
    {ok, #state{}, 0}.

handle_call({add_legacy, Pool, Val}, _From, #state{legacy = M} = State) ->
    {reply, ok, State#state{legacy = M#{Pool => [Val | maps:get(Pool, M, [])]}}};
handle_call({take_legacy, Pool}, _From, #state{legacy = M} = State) ->
    case maps:get(Pool, M, []) of
        [] -> {reply, undefined, State};
        [H | T] -> {reply, H, State#state{legacy = M#{Pool => T}}}
    end;
handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(_Info, State) ->
    {noreply, State}.

