-module(rabbitmq_pool_tests).

-include_lib("eunit/include/eunit.hrl").

-define(Setup, fun() -> rabbitmq_pool:start()  end).
-define(Clearnup, fun(_) -> skip end).

basic_test_() ->
    {inorder,
     {setup, ?Setup, ?Clearnup,
      [{"pools_size",
         fun() ->
                 error_logger:info_msg("connects ~p", [{get_deads()}]),
                 ?assertEqual(0, length(get_deads())),
                 ?assertEqual(6, length(get_processes())),
                 ?assertEqual(6, length(get_bind_channels())),
                 ?assertEqual(4, length(get_lone_channels()))
         end},
       {timeout, 20,
        [{"connect_dead",
         fun() ->
                 List = get_connects(),
                 exit(lists:nth(3, List), kill),
                 timer:sleep(1000),
                 ?assertEqual(3, length(get_deads())),
                 ?assertEqual(3, length(get_connects())),
                 timer:sleep(4500),
                 ?assertEqual(0, length(get_deads())),
                 ?assertEqual(6, length(get_processes())),
                 ?assertEqual(6, length(get_bind_channels())),
                 ?assertEqual(4, length(get_lone_channels())),
                 exit(lists:nth(2, List), kill),
                 timer:sleep(1000),
                 ?assertEqual(7, length(get_deads())),
                 ?assertEqual(3, length(get_connects())),
                 timer:sleep(4500),
                 ?assertEqual(0, length(get_deads())),
                 ?assertEqual(6, length(get_processes())),
                 ?assertEqual(6, length(get_bind_channels())),
                 ?assertEqual(4, length(get_lone_channels()))
         end}
        ]},
       {timeout, 20,
        [{"channel_dead",
         fun() ->
                 exit(lists:nth(1, get_lone_channels()), kill),
                 timer:sleep(1000),
                 ?assertEqual(3, length(get_deads())),
                 ?assertEqual(2, length(get_lone_channels())),
                 timer:sleep(4500),
                 ?assertEqual(0, length(get_deads())),
                 ?assertEqual(6, length(get_processes())),
                 ?assertEqual(6, length(get_bind_channels())),
                 ?assertEqual(4, length(get_lone_channels())),
                 exit(lists:nth(1, get_bind_channels()), kill),
                 timer:sleep(1000),
                 ?assertEqual(7, length(get_deads())),
                 ?assertEqual(3, length(get_bind_channels())),
                 timer:sleep(4500),
                 ?assertEqual(0, length(get_deads())),
                 ?assertEqual(6, length(get_processes())),
                 ?assertEqual(6, length(get_bind_channels())),
                 ?assertEqual(4, length(get_lone_channels()))
         end}
        ]},
       {timeout, 20,
        [{"process_dead",
         fun() ->
                 exit(lists:nth(1, get_processes()), kill),
                 timer:sleep(1000),
                 ?assertEqual(1, length(get_deads())),
                 ?assertEqual(5, length(get_processes())),
                 timer:sleep(4500),
                 ?assertEqual(0, length(get_deads())),
                 ?assertEqual(6, length(get_processes())),
                 ?assertEqual(6, length(get_bind_channels())),
                 ?assertEqual(4, length(get_lone_channels()))
         end}
        ]}
      ]}
    }.

get_deads() ->
    rabbitmq_pool:get_deads(rabbitmq_2331) ++ rabbitmq_pool:get_deads(rabbitmq_2332).

get_processes() ->
    rabbitmq_pool:get_processes(rabbitmq_2332).

get_bind_channels() ->
    rabbitmq_pool:get_channels(rabbitmq_2332).

get_lone_channels() ->
    rabbitmq_pool:get_channels(rabbitmq_2331).

get_connects() ->
    rabbitmq_pool:get_connects(rabbitmq_2332)
    ++ rabbitmq_pool:get_connects(rabbitmq_2331).

