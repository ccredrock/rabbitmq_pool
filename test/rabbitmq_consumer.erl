-module(rabbitmq_consumer).

-export([start/1]).

start(_) ->
    {ok, spawn(fun() -> timer:sleep(500000) end)}.

