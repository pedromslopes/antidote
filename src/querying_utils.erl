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

-define(CRDT_INDEX, antidote_crdt_gindex).
-define(CRDT_MAP, antidote_crdt_map_go).
-define(CRDT_SET, antidote_crdt_set_aw).
-define(INVALID_OP_MSG(Operation, CRDT), ["The operation ", Operation, " is not part of the ", CRDT, " specification"]).

%% API
-export([build_keys/3,
         read_keys/2,
         read_keys/1,
         read_function/3,
         read_function/2,
         write_keys/3,
         write_keys/1,
         to_atom/1,
         to_list/1,
         remove_duplicates/1,
         create_crdt_update/3,
         is_list_of_lists/1,
         is_subquery/1,
         replace/3]).

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

read_keys([], _TxId) -> [[]];
read_keys(ObjKeys, ignore) ->
    read_keys(ObjKeys);
read_keys(ObjKeys, TxId) when is_list(ObjKeys) ->
    %% TODO read objects from Cure or Materializer?
    {ok, Objs} = cure:read_objects(ObjKeys, TxId),
    Objs;
read_keys(ObjKey, TxId) ->
    read_keys([ObjKey], TxId).

read_keys([]) -> [[]];
read_keys(ObjKeys) when is_list(ObjKeys) ->
    {ok, Objs, _} = cure:read_objects(ignore, [], ObjKeys),
    Objs;
read_keys(ObjKey) ->
    read_keys([ObjKey]).

read_function([], _Func, _TxId) -> [[]];
read_function(ObjKeys, Function, ignore) ->
    read_keys(ObjKeys, Function);
read_function(ObjKeys, {Function, Args}, TxId) when is_list(ObjKeys) ->
    %% TODO read objects from Cure or Materializer?
    Reads = lists:map(fun(Key) -> {Key, Function, Args} end, ObjKeys),
    {ok, Objs} = cure:read_objects(Reads, TxId),
    Objs;
read_function(ObjKey, Range, TxId) ->
    read_function([ObjKey], Range, TxId).

read_function([], _Func) -> [[]];
read_function(ObjKeys, {Function, Args}) when is_list(ObjKeys) ->
    Reads = lists:map(fun(Key) -> {Key, Function, Args} end, ObjKeys),
    {ok, Objs, _} = cure:read_objects(ignore, [], Reads),
    Objs.

write_keys(_ObjKeys, Updates, TxId) ->
    cure:update_objects(Updates, TxId).

write_keys(Updates) ->
    cure:update_objects(ignore, [], Updates).

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
        false -> throw(lists:concat(["Cannot remove duplicates in this object: ", Other]))
    end.

create_crdt_update({_Key, ?CRDT_MAP, _Bucket} = ObjKey, UpdateOp, Value) ->
    Update = map_update(Value),
    {ObjKey, UpdateOp, Update};
create_crdt_update({_Key, ?CRDT_INDEX, _Bucket} = ObjKey, UpdateOp, Value) ->
    Update = index_update(Value),
    {ObjKey, UpdateOp, Update};
create_crdt_update(ObjKey, UpdateOp, Value) ->
    set_update(ObjKey, UpdateOp, Value).

map_update({{Key, CRDT}, {Op, Value} = Operation}) ->
    case CRDT:is_operation(Operation) of
        true -> [{{Key, CRDT}, {Op, Value}}];
        false -> throw(lists:concat(?INVALID_OP_MSG(Operation, CRDT)))
    end.
index_update({CRDT, Key, {Op, Value} = Operation}) ->
    case CRDT:is_operation(Operation) of
        true -> [{CRDT, Key, {Op, Value}}];
        false -> throw(lists:concat(?INVALID_OP_MSG(Operation, CRDT)))
    end;
index_update({CRDT, Key, Operations}) when is_list(Operations) ->
    lists:foldl(fun(Op, Acc) ->
        lists:append(Acc, index_update({CRDT, Key, Op}))
    end, [], Operations);
index_update(Values) when is_list(Values) ->
    lists:foldl(fun(Update, Acc) ->
        lists:append(Acc, index_update(Update))
    end, [], Values).

set_update({_Key, ?CRDT_SET, _Bucket} = ObjKey, UpdateOp, Value) ->
    case ?CRDT_SET:is_operation(UpdateOp) of
        true -> {ObjKey, UpdateOp, Value};
        false -> throw(lists:concat(?INVALID_OP_MSG(UpdateOp, ?CRDT_SET)))
    end.

is_list_of_lists(List) when is_list(List) ->
    NotDropped = lists:dropwhile(fun(Elem) -> is_list(Elem) end, List),
    NotDropped =:= [];
is_list_of_lists(_) -> false.

is_subquery({sub, Conditions}) when is_list(Conditions) -> true;
is_subquery(_) -> false.

replace(N, Element, List) when N >= 0 andalso N < length(List)->
    {First, [_H | Second]} = lists:split(N, List),
    lists:append(First, [Element | Second]).
