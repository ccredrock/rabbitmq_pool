%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2018 redrock
%%% @doc rabbitmq pool
%%% @end
%%%-------------------------------------------------------------------
-module(rmp_test).

-export([test_publish/2, test_publish/1]).

-export([start/1]).

%%------------------------------------------------------------------------------
-include_lib("amqp_client/include/amqp_client.hrl").

%%------------------------------------------------------------------------------
test_publish(Num, Count) ->
    [spawn(fun() -> test_publish(Count) end) || _ <- lists:seq(1, Num)].

test_publish(Count) ->
    Ack = fun(_, _) -> ok end,
    Data = <<0:400>>,
    [begin rabbitmq_pool:rand_confirm(publish, #'basic.publish'{exchange = <<"rmp">>,
                                                                routing_key = integer_to_binary(rand:uniform(100))},
                                      #amqp_msg{payload = Data}, Ack, null),
           timer:sleep(1000) end
     || _ <- lists:seq(1, Count)].

%%------------------------------------------------------------------------------
start({Channel, _}) ->
    {ok, spawn(fun() -> init(Channel) end)}.

init(Channel) ->
    Nth = rand:uniform(20),
    Queue = <<"rmp", (integer_to_binary(Nth))/binary>>,
    RoutingKeys = [integer_to_binary(Nth * 5 + X) || X <- lists:seq(1, 5)],
    #'exchange.declare_ok'{} =
    amqp_channel:call(Channel, #'exchange.declare'{exchange = <<"rmp">>, durable = true}),
    #'queue.declare_ok'{} =
    amqp_channel:call(Channel, #'queue.declare'{queue = Queue, durable = true,
                                                arguments = [{<<"x-message-ttl">>, long, 30 * 1000}]}),
    [#'queue.bind_ok'{} =
     amqp_channel:call(Channel, #'queue.bind'{queue = Queue, exchange = <<"rmp">>, routing_key = X})
     || X <- RoutingKeys],
    #'basic.consume_ok'{consumer_tag = Tag} =
    amqp_channel:call(Channel, #'basic.consume'{queue = Queue, no_ack = false}),
    receive #'basic.consume_ok'{consumer_tag = Tag} -> ok end,
    #'basic.qos_ok'{} = amqp_channel:call(Channel, #'basic.qos'{prefetch_count = 5000, global = true}),
    recv(Channel).

recv(Channel) ->
    receive
        {#'basic.deliver'{delivery_tag = Tag},
         #amqp_msg{payload = _}} ->
            erlang:garbage_collect(),
            amqp_channel:call(Channel, #'basic.ack'{delivery_tag = Tag}),
            recv(Channel);
        _ ->
            recv(Channel)
    end.

