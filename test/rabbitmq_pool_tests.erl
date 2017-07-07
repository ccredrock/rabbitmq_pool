-module(rabbitmq_pool_tests).

-include_lib("eunit/include/eunit.hrl").

-define(Setup, fun() -> application:start(rabbitmq_pool)  end).
-define(Clearnup, fun(_) -> application:stop(rabbitmq_pool)  end).
-define(DEFAULT, dbsrv).

basic_test_() ->
    {inorder,
     {setup, ?Setup, ?Clearnup,
      [{"pools_size",
         fun() ->
                 ?assertEqual(0, length(rabbitmq_pool:get_deads())),
                 ?assertEqual(6, length(rabbitmq_pool:get_processes())),
                 ?assertEqual(6, length(rabbitmq_pool:get_bind_channels())),
                 ?assertEqual(4, length(rabbitmq_pool:get_lone_channels()))
         end},
       {timeout, 20,
        [{"connect_dead",
         fun() ->
                 List = lists:sort(rabbitmq_pool:get_connects()),
                 exit(lists:nth(3, List), kill),
                 timer:sleep(1000),
                 ?assertEqual(3, length(rabbitmq_pool:get_deads())),
                 ?assertEqual(3, length(rabbitmq_pool:get_connects())),
                 timer:sleep(4500),
                 ?assertEqual(0, length(rabbitmq_pool:get_deads())),
                 ?assertEqual(6, length(rabbitmq_pool:get_processes())),
                 ?assertEqual(6, length(rabbitmq_pool:get_bind_channels())),
                 ?assertEqual(4, length(rabbitmq_pool:get_lone_channels())),
                 exit(lists:nth(2, List), kill),
                 timer:sleep(1000),
                 ?assertEqual(7, length(rabbitmq_pool:get_deads())),
                 ?assertEqual(3, length(rabbitmq_pool:get_connects())),
                 timer:sleep(4500),
                 ?assertEqual(0, length(rabbitmq_pool:get_deads())),
                 ?assertEqual(6, length(rabbitmq_pool:get_processes())),
                 ?assertEqual(6, length(rabbitmq_pool:get_bind_channels())),
                 ?assertEqual(4, length(rabbitmq_pool:get_lone_channels()))
         end}
        ]},
       {timeout, 20,
        [{"channel_dead",
         fun() ->
                 exit(lists:nth(1, rabbitmq_pool:get_lone_channels()), kill),
                 timer:sleep(1000),
                 ?assertEqual(3, length(rabbitmq_pool:get_deads())),
                 ?assertEqual(2, length(rabbitmq_pool:get_lone_channels())),
                 timer:sleep(4500),
                 ?assertEqual(0, length(rabbitmq_pool:get_deads())),
                 ?assertEqual(6, length(rabbitmq_pool:get_processes())),
                 ?assertEqual(6, length(rabbitmq_pool:get_bind_channels())),
                 ?assertEqual(4, length(rabbitmq_pool:get_lone_channels())),
                 exit(lists:nth(1, rabbitmq_pool:get_bind_channels()), kill),
                 timer:sleep(1000),
                 ?assertEqual(7, length(rabbitmq_pool:get_deads())),
                 ?assertEqual(3, length(rabbitmq_pool:get_bind_channels())),
                 timer:sleep(4500),
                 ?assertEqual(0, length(rabbitmq_pool:get_deads())),
                 ?assertEqual(6, length(rabbitmq_pool:get_processes())),
                 ?assertEqual(6, length(rabbitmq_pool:get_bind_channels())),
                 ?assertEqual(4, length(rabbitmq_pool:get_lone_channels()))
         end}
        ]},
       {timeout, 20,
        [{"process_dead",
         fun() ->
                 exit(lists:nth(1, rabbitmq_pool:get_processes()), kill),
                 timer:sleep(1000),
                 ?assertEqual(1, length(rabbitmq_pool:get_deads())),
                 ?assertEqual(5, length(rabbitmq_pool:get_processes())),
                 timer:sleep(4500),
                 ?assertEqual(0, length(rabbitmq_pool:get_deads())),
                 ?assertEqual(6, length(rabbitmq_pool:get_processes())),
                 ?assertEqual(6, length(rabbitmq_pool:get_bind_channels())),
                 ?assertEqual(4, length(rabbitmq_pool:get_lone_channels()))
         end}
        ]}
      ]}
    }.

