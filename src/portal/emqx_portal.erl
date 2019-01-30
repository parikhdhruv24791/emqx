%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%% @doc This is the API module for EMQX portal

-module(emqx_portal).

-export([start_link_export/2,
         import_batch/2,
         handle_ack/2
        ]).

-export_type([export_opts/0,
              batch/0,
              ref/0]).

-type export_opts() :: emqx_portal_export:opts().
-type batch() :: [emqx_portal_msg:msg()].
-type ref() :: reference().

%% @see emqx_portal_export:start_link/2.
-spec start_link_export(atom(), export_opts()) -> {ok, pid()}.
start_link_export(Name, Options) ->
    emqx_portal_export:start_link(Name, Options).

%% @doc This function is to be evaluated on message/batch receiver side.
-spec import_batch(batch(), fun(() -> ok)) -> ok.
import_batch(Batch, AckFun) ->
    lists:foreach(fun emqx_broker:publish/1, emqx_portal_msg:to_broker_msgs(Batch)),
    AckFun().

%% @doc This function is to be evaluated on exporter side
%% when message/batch is accepted by remote node.
-spec handle_ack(ExportPid :: pid(), ref()) -> ok.
handle_ack(ExportPid, Ref) when node() =:= node(ExportPid) ->
    ok = emqx_portal_export:ack(ExportPid, Ref).

