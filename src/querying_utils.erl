%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc An Antidote module that contains some common and utility
%%%      functions for, but not exclusively to, the indexing and
%%%      query_optimizer modules.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(querying_utils).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(LOG_UTIL, mock_partition).
-else.
-define(LOG_UTIL, log_utilities).
-endif.

-include("antidote.hrl").

-define(CRDT_INDEX, antidote_crdt_index).
-define(CRDT_INDEX_P, antidote_crdt_index_p).
-define(CRDT_MAP, antidote_crdt_map_go).
-define(CRDT_SET, antidote_crdt_set_aw).
-define(INVALID_OP_MSG(Operation, CRDT), io_lib:format("The operation ~p is not part of the ~p specification", [Operation, CRDT])).

%% API
-export([build_keys/3,
    read_keys/3, read_keys/2,
    read_function/3, read_function/2,
    write_keys/2, write_keys/1,
    start_transaction/0, commit_transaction/1,
    to_atom/1,
    to_list/1,
    remove_duplicates/1,
    create_crdt_update/3,
    is_list_of_lists/1,
    replace/3,
    first_occurrence/2]).

build_keys([], _Types, _Bucket) -> [];
build_keys(Keys, Types, Bucket) when is_list(Keys) and is_list(Types) ->
    build_keys(Keys, Types, Bucket, []);
build_keys(Keys, Type, Bucket) when is_list(Keys) ->
    Len = length(Keys),
    build_keys(Keys, lists:duplicate(Len, Type), Bucket);
build_keys(Key, Type, Bucket) ->
    build_keys([Key], [Type], Bucket).

build_keys([Key | Tail1], [Type | Tail2], Bucket, Acc) ->
    BucketAtom = to_atom(Bucket),
    TypeAtom = to_atom(Type),
    ObjKey = {Key, TypeAtom, BucketAtom},
    build_keys(Tail1, Tail2, Bucket, lists:append(Acc, [ObjKey]));
build_keys([], [], _Bucket, Acc) ->
    Acc.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%     Read Values or States    %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

read_keys(_StateOrValue, [], _TxId) -> [[]];
read_keys(StateOrValue, ObjKeys, ignore) ->
    read_keys(StateOrValue, ObjKeys);
read_keys(StateOrValue, ObjKeys, TxId) when is_list(ObjKeys) ->
    read_crdts(StateOrValue, ObjKeys, TxId);
read_keys(StateOrValue, ObjKey, TxId) ->
    read_keys(StateOrValue, [ObjKey], TxId).

read_keys(_StateOrValue, []) -> [[]];
read_keys(StateOrValue, ObjKeys) when is_list(ObjKeys) ->
    read_crdts(StateOrValue, ObjKeys);
read_keys(StateOrValue, ObjKey) ->
    read_keys(StateOrValue, [ObjKey]).

%% Applying a function on a set of keys implies returning the values
%% of the CRDTs mapped by those keys.
%-spec read_function(bound_object() | [bound_object()], {atom(), [term()]}, txid() | {txid(), [term()], [term()]}) ->
%    term() | {error, reason()} | [term() | {error, reason()}].
read_function([], _Func, _TxId) -> [[]];
read_function(ObjKeys, Function, ignore) ->
    read_function(ObjKeys, Function);

read_function(ObjKeys, {Function, Args}, TxId)
    when is_list(ObjKeys) andalso is_record(TxId, tx_id) ->

    Reads = lists:map(fun(Key) -> {Key, {Function, Args}} end, ObjKeys),
    read_crdts(value, Reads, TxId);
read_function(ObjKeys, {Function, Args}, {TxId, ReadSet, UpdatedPartitions})
    when is_list(ObjKeys) andalso is_record(TxId, transaction)  ->

    lists:map(fun({Key, Type, Bucket}) ->
        Partition = ?LOG_UTIL:get_key_partition({Key, Bucket}),
        WriteSet = get_write_set(Partition, UpdatedPartitions),

        case clocksi_object_function:sync_execute_object_function(
            TxId, Partition, Key, Type, {Function, Args}, WriteSet, ReadSet) of
            {ok, {Key, Type, _, _, Value}} ->
                Value;
            {error, Reason} ->
                {error, Reason}
        end
    end, ObjKeys);
read_function(ObjKey, Range, TxId) ->
    read_function([ObjKey], Range, TxId).

%-spec read_function(bound_object() | [bound_object()], {atom(), [term()]}) ->
%    term() | {error, reason()} | [term() | {error, reason()}].
read_function([], _Func) -> [[]];
read_function(ObjKeys, {Function, Args}) when is_list(ObjKeys) ->
    Reads = lists:map(fun(Key) -> {Key, Function, Args} end, ObjKeys),
    read_crdts(value, Reads).

write_keys(Updates, TxId) when is_list(Updates) ->
    cure:update_objects(Updates, TxId);
write_keys(Update, TxId) when is_tuple(Update) ->
    write_keys([Update], TxId).

write_keys(Updates) when is_list(Updates) ->
    cure:update_objects(ignore, [], Updates);
write_keys(Update) when is_tuple(Update) ->
    write_keys([Update]).

start_transaction() ->
    cure:start_transaction(ignore, []).

commit_transaction(TxId) ->
    cure:commit_transaction(TxId).

to_atom(Term) when is_list(Term) ->
    list_to_atom(Term);
to_atom(Term) when is_integer(Term) ->
    List = integer_to_list(Term),
    list_to_atom(List);
to_atom(Term) when is_atom(Term) ->
    Term.

to_list(Term) when is_list(Term) ->
    Term;
to_list(Term) when is_integer(Term) ->
    integer_to_list(Term);
to_list(Term) when is_atom(Term) ->
    atom_to_list(Term).

remove_duplicates(List) when is_list(List) ->
    Aux = sets:from_list(List),
    sets:to_list(Aux);
remove_duplicates(Other) ->
    case sets:is_set(Other) of
        true -> Other;
        false ->
            ErrorMsg = io_lib:format("Cannot remove duplicates in this object: ~p", [Other]),
            throw(lists:flatten(ErrorMsg))
    end.

%% TODO pass this function to crdt_utils
create_crdt_update({_Key, ?CRDT_MAP, _Bucket} = ObjKey, UpdateOp, Value) ->
    Update = map_update(Value),
    {ObjKey, UpdateOp, Update};
create_crdt_update({_Key, ?CRDT_INDEX, _Bucket} = ObjKey, UpdateOp, Value) ->
    Update = index_update(Value),
    {ObjKey, UpdateOp, Update};
create_crdt_update({_Key, ?CRDT_INDEX_P, _Bucket} = ObjKey, UpdateOp, Value) ->
    Update = index_p_update(UpdateOp, Value),
    {ObjKey, UpdateOp, Update};
create_crdt_update(ObjKey, UpdateOp, Value) ->
    set_update(ObjKey, UpdateOp, Value).

is_list_of_lists(List) when is_list(List) ->
    NotDropped = lists:dropwhile(fun(Elem) -> is_list(Elem) end, List),
    NotDropped =:= [];
is_list_of_lists(_) -> false.

replace(N, Element, List) when N >= 0 andalso N < length(List)->
    {First, [_H | Second]} = lists:split(N, List),
    lists:append(First, [Element | Second]).

first_occurrence(Predicate, [Elem | List]) ->
    case Predicate(Elem) of
        true -> Elem;
        false -> first_occurrence(Predicate, List)
    end;
first_occurrence(_Predicate, []) -> undefined.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% TODO read objects from Cure or Materializer?
read_crdts(StateOrValue, ObjKeys, {TxId, _ReadSet, _UpdatedPartitions} = Transaction)
    when is_list(ObjKeys) andalso is_record(TxId, transaction) ->
    {ok, Objs} = read_data_items(StateOrValue, ObjKeys, Transaction),
    Objs;
read_crdts(StateOrValue, ObjKey, {TxId, _ReadSet, _UpdatedPartitions} = Transaction)
    when is_record(TxId, transaction) ->
    read_crdts(StateOrValue, [ObjKey], Transaction);

read_crdts(value, ObjKeys, TxId)
    when is_list(ObjKeys) andalso is_record(TxId, tx_id) ->
    {ok, Objs} = cure:read_objects(ObjKeys, TxId),
    Objs;
read_crdts(state, ObjKeys, TxId)
    when is_list(ObjKeys) andalso is_record(TxId, tx_id) ->
    {ok, Objs} = cure:get_objects(ObjKeys, TxId),
    Objs;
read_crdts(StateOrValue, ObjKey, TxId) ->
    read_crdts(StateOrValue, [ObjKey], TxId).

read_crdts(value, ObjKeys) when is_list(ObjKeys) ->
    {ok, Objs, _} = cure:read_objects(ignore, [], ObjKeys),
    Objs;
read_crdts(state, ObjKeys) when is_list(ObjKeys) ->
    {ok, Objs, _} = cure:get_objects(ignore, [], ObjKeys),
    Objs;
read_crdts(StateOrValue, ObjKey) ->
    read_crdts(StateOrValue, [ObjKey]).

read_data_items(StateOrValue, ObjKeys, Transaction) when is_list(ObjKeys) ->
    ReadObjects = lists:map(fun({_Key, Type, _Bucket} = ObjKey) ->
        {ok, Snapshot} = read_data_item(ObjKey, Transaction),
        case StateOrValue of
            value -> Type:value(Snapshot);
            state -> Snapshot
        end
    end, ObjKeys),
    {ok, ReadObjects}.

read_data_item({Key, Type, Bucket}, {Transaction, ReadSet, UpdatedPartitions}) ->
    SendKey = {Key, Bucket},
    case orddict:find(SendKey, ReadSet) of
        error ->
            Partition = ?LOG_UTIL:get_key_partition(SendKey),
            WriteSet = get_write_set(Partition, UpdatedPartitions),

            {ok, Snapshot} = clocksi_vnode:read_data_item(Partition, Transaction, SendKey, Type, WriteSet),
            {ok, Snapshot};
        {ok, State} ->
            {ok, State}
    end.

get_write_set(Partition, Partitions) ->
    case lists:keyfind(Partition, 1, Partitions) of
        false -> [];
        {Partition, WS} -> WS
    end.

map_update({{Key, CRDT}, {Op, Value} = Operation}) ->
    case CRDT:is_operation(Operation) of
        true -> [{{Key, CRDT}, {Op, Value}}];
        false -> throw(lists:flatten(?INVALID_OP_MSG(Operation, CRDT)))
    end;
map_update(Values) when is_list(Values) ->
    lists:foldl(fun(Update, Acc) ->
        lists:append(Acc, map_update(Update))
                end, [], Values).

index_update({CRDT, Key, {Op, Value} = Operation}) ->
    case CRDT:is_operation(Operation) of
        true -> [{CRDT, Key, {Op, Value}}];
        false -> throw(lists:flatten(?INVALID_OP_MSG(Operation, CRDT)))
    end;
index_update({CRDT, Key, Operations}) when is_list(Operations) ->
    lists:foldl(fun(Op, Acc) ->
        lists:append(Acc, index_update({CRDT, Key, Op}))
                end, [], Operations);
index_update(Values) when is_list(Values) ->
    lists:foldl(fun(Update, Acc) ->
        lists:append(Acc, index_update(Update))
                end, [], Values).

index_p_update(UpdateOp, {_Key, {_Op, _Value}} = Operation) ->
    case ?CRDT_INDEX_P:is_operation({UpdateOp, Operation}) of
        true -> [Operation];
        false -> throw(lists:flatten(?INVALID_OP_MSG(Operation, ?CRDT_INDEX_P)))
    end;
index_p_update(UpdateOp, {Key, Operations}) when is_list(Operations) ->
    lists:foldl(fun(Op, Acc) ->
        lists:append(Acc, index_p_update(UpdateOp, {Key, Op}))
                end, [], Operations);
index_p_update(UpdateOp, Values) when is_list(Values) ->
    lists:foldl(fun(Update, Acc) ->
        lists:append(Acc, index_p_update(UpdateOp, Update))
                end, [], Values).

set_update({_Key, ?CRDT_SET, _Bucket} = ObjKey, UpdateOp, Value) ->
    case ?CRDT_SET:is_operation(UpdateOp) of
        true -> {ObjKey, UpdateOp, Value};
        false -> throw(lists:flatten(?INVALID_OP_MSG(UpdateOp, ?CRDT_SET)))
    end.
