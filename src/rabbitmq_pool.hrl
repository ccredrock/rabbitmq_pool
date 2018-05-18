%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2018 redrock
%%% @doc rabbitmq pool
%%% @end
%%%-------------------------------------------------------------------
-ifndef(RABBITMQ_POOL_HRL).
-define(RABBITMQ_POOL_HRL, true).

-include_lib("amqp_client/include/amqp_client.hrl").

-define(INFO(F, A), error_logger:info_msg("rabbitmq_pool " ++ F, A)).
-define(WARN(F, A), error_logger:warning_msg("rabbitmq_pool " ++ F, A)).

-define(AMQP_NETWORK(List),
        #amqp_params_network{host         = proplists:get_value(host, List),
                             port         = proplists:get_value(port, List, 5672),
                             username     = proplists:get_value(username, List, <<"guest">>),
                             password     = proplists:get_value(password, List, <<"guest">>),
                             virtual_host = proplists:get_value(virtual_host, List, <<"/">>)}).

-endif.

