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
%%% @doc An Antidote module to manage triggers that modify indexes.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(index_triggers).

-include("querying.hrl").

-define(MAP_UPD(ColName, ColType, Operation, Value), {{ColName, ColType}, {Operation, Value}}).
-define(INDEX_UPD(TableName, IndexName, EntryKey, EntryValue), {TableName, IndexName, EntryKey, EntryValue}).
-define(MAP_OPERATION, update).

%% API
-export([create_index_hooks/2,
         index_update_hook/2,
         index_update_hook/1,
         build_index_updates/2]).

create_index_hooks(Updates, _TxId) when is_list(Updates) ->
    lists:foldl(fun(ObjUpdate, UpdAcc) ->
        ?OBJECT_UPDATE(Key, Type, Bucket, _Op, _Param) = ObjUpdate,
        case update_type({Key, Type, Bucket}) of
            ?TABLE_UPD_TYPE ->
                case antidote_hooks:has_hook(pre_commit, Bucket) of
                    true -> ok;
                    false -> antidote_hooks:register_pre_hook(Bucket, ?MODULE, index_update_hook)
                end,

                lists:append(UpdAcc, []);
            ?RECORD_UPD_TYPE ->
                case antidote_hooks:has_hook(pre_commit, Bucket) of
                    true -> ok;
                    false -> antidote_hooks:register_pre_hook(Bucket, ?MODULE, index_update_hook)
                end,

                lists:append(UpdAcc, []);
            _ ->
                lists:append(UpdAcc, [])
        end
    end, [], Updates).

% Uncomment to use with following 3 hooks
%%fill_pindex(Key, Bucket) ->
%%    TableName = querying_utils:to_atom(Bucket),
%%    PIdxName = generate_pindex_key(TableName),
%%    [PIdxKey] = querying_utils:build_keys(PIdxName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
%%    PIdxUpdate = {PIdxKey, add, Key},
%%    [PIdxUpdate].
%%
%%empty_hook({{Key, Bucket}, Type, Param}) ->
%%    {ok, {{Key, Bucket}, Type, Param}}.
%%
%%transaction_hook({{Key, Bucket}, Type, Param}) ->
%%    {UpdateOp, Updates} = Param,
%%    ObjUpdate = ?OBJECT_UPDATE(Key, Type, Bucket, UpdateOp, Updates),
%%
%%    {ok, TxId} = cure:start_transaction(ignore, [], false),
%%    {ok, _} = cure:commit_transaction(TxId),
%%
%%    {ok, ObjUpdate}.
%%
%%rwtransaction_hook({{Key, Bucket}, Type, Param}) ->
%%    {UpdateOp, Updates} = Param,
%%    ObjUpdate = ?OBJECT_UPDATE(Key, Type, Bucket, UpdateOp, Updates),
%%
%%    {ok, TxId} = cure:start_transaction(ignore, [], false),
%%
%%    %% write a key
%%    [ObjKey] = querying_utils:build_keys(key1, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
%%    Upd = querying_utils:create_crdt_update(ObjKey, ?MAP_OPERATION, {antidote_crdt_register_lww, pk1, {assign, val1}}),
%%    {ok, _} = querying_utils:write_keys(Upd, TxId),
%%
%%    %% read a key
%%    [ObjKey] = querying_utils:build_keys(key2, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
%%    [_Obj] = querying_utils:read_keys(value, ObjKey, TxId),
%%
%%    {ok, _} = cure:commit_transaction(TxId),
%%
%%    {ok, ObjUpdate}.

%% The 'Transaction' object passed here is a tuple on the form
%% {TxId, ReadSet, WriteSet} that represents a transaction id,
%% a transaction read set, and a transaction write set, respectively.
index_update_hook(Update, Transaction) when is_tuple(Update) ->
    %lager:info("Begin hook: ~p", [Update]),
    {{Key, Bucket}, Type, Param} = Update,
    case generate_index_updates(Key, Type, Bucket, Param, Transaction) of
        {NewObjUpdate, []} ->
            %lager:info("End hook: ~p", [NewObjUpdate]),
            {ok, NewObjUpdate};
        {NewObjUpd, IdxUpds} ->
            SendUpds = lists:append([NewObjUpd], IdxUpds),
            %lager:info("End hook: ~p", [SendUpds]),
            {ok, SendUpds}
    end.

index_update_hook(Update) when is_tuple(Update) ->
    {ok, TxId} = querying_utils:start_transaction(),
    {ok, Updates} = index_update_hook(Update, TxId),
    {ok, _} = querying_utils:commit_transaction(TxId),

    {ok, Updates}.

%% ====================================================================
%% Internal functions
%% ====================================================================

update_type({?TABLE_METADATA_KEY, ?TABLE_METADATA_DT, ?AQL_METADATA_BUCKET}) ->
    ?TABLE_UPD_TYPE;
update_type({_Key, ?TABLE_METADATA_DT, ?AQL_METADATA_BUCKET}) ->
    ?METADATA_UPD_TYPE;
update_type({_Key, ?TABLE_DT, _Bucket}) ->
    ?RECORD_UPD_TYPE;
update_type(_) ->
    ?OTHER_UPD_TYPE.

generate_index_updates(Key, Type, Bucket, Param, Transaction) ->
    {UpdateOp, Updates} = Param,
    ObjUpdate = ?OBJECT_UPDATE(Key, Type, Bucket, UpdateOp, Updates),
    ObjBoundKey = {Key, Type, Bucket},

    case update_type({Key, Type, Bucket}) of
        ?TABLE_UPD_TYPE ->
            % Is a table update
            %lager:info("Is a table update: ~p", [ObjUpdate]),

            [{{TableName, _}, {_Op, TableOps}}] = Updates,

            Table = table_utils:table_metadata(TableName, Transaction),
            SIdxUpdates = fill_index(ObjUpdate, Table, Transaction),
            Upds = build_index_updates(SIdxUpdates, Transaction),

            %% TODO set policies of primary index
            PIdxUpdates = set_policies(TableName, TableOps),

            %% Remove table metadata entry from cache -- mark as 'dirty'
            ok = metadata_caching:remove_key(TableName),

            {{{Key, Bucket}, Type, Param}, lists:flatten([PIdxUpdates, Upds])};
        ?RECORD_UPD_TYPE ->
            % Is a record update
            %lager:info("Is a record update: ~p", [ObjUpdate]),

            Table = table_utils:table_metadata(Bucket, Transaction),
            case Table of
                [] -> {{{Key, Bucket}, Type, Param}, []};
                _Else ->
                    % A table exists
                    TableName = table_utils:table(Table),

                    PIdxName = indexing:generate_pindex_key(TableName),
                    [PIdxKey] = querying_utils:build_keys(PIdxName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),

                    {NewRecordUpdates, PIdxUpdate} =
                        create_pindex_update(ObjBoundKey, Updates, Table, PIdxKey, Transaction),

                    Indexes = table_utils:indexes(Table),
                    SIdxUpdates = lists:foldl(fun(Operation, IdxUpdates2) ->
                        ?MAP_UPD(Col, CRDT, CRDTOper, Val) = Operation,
                        case indexing:lookup_index(Col, Indexes) of
                            [] -> IdxUpdates2;
                            Idxs ->
                                AuxUpdates = lists:map(fun(Idx) ->
                                    ?INDEX_UPD(TableName, indexing:index_name(Idx), {Val, CRDT}, {ObjBoundKey, {CRDTOper, Val}})
                                end, Idxs),
                                lists:append(IdxUpdates2, AuxUpdates)
                        end
                    end, [], NewRecordUpdates),

                    %lager:info("SIdxUpdates: ~p", [SIdxUpdates]),

                    NewRecordUpd = {{Key, Bucket}, Type, {UpdateOp, NewRecordUpdates}},
                    {NewRecordUpd, lists:append([PIdxUpdate], build_index_updates(SIdxUpdates, Transaction))}
            end;
        _ -> {{{Key, Bucket}, Type, Param}, []}
    end.

fill_index(ObjUpdate, Table, Transaction) ->
    case retrieve_index(ObjUpdate, Table) of
        {_, []} ->
            % No index was created
            %lager:info("No index was created"),
            [];
        {NewTable, Indexes} when is_list(Indexes) ->
            lists:foldl(fun(Index, Acc) ->
                % A new index was created
                %lager:info("A new index was created"),

                ?INDEX(IndexName, TableName, [IndexedColumn]) = Index, %% TODO support more than one column
                [PrimaryKey] = table_utils:primary_key_name(NewTable),
                {_, _, PIndexObject} = indexing:read_index(primary, TableName, Transaction),
                SIndexObject = indexing:read_index(secondary, {TableName, IndexName}, Transaction),

                IdxUpds = lists:map(fun(Entry) ->
                    ObjKey = indexing:entry_bobj(Entry),
                    case record_utils:record_data(ObjKey, Transaction) of
                        [] -> [];
                        [Record] ->
                            PkValue = querying_utils:to_atom(record_utils:lookup_value(PrimaryKey, Record)),
                            ?ATTRIBUTE(_ColName, Type, Value) = record_utils:get_column(IndexedColumn, Record),
                            case is_element(Value, PkValue, SIndexObject) of
                                true -> [];
                                false ->
                                    Op = crdt_utils:to_insert_op(Type, Value), %% generate an op according to Type
                                    ?INDEX_UPD(TableName, IndexName, {Value, Type}, {ObjKey, Op})
                            end
                    end
                end, PIndexObject),
                lists:append(Acc, lists:flatten(IdxUpds))
            end, [], Indexes)
    end.

%% Given a list of index updates on the form {TableName, IndexName, {EntryKey, EntryValue}},
%% build the database updates given that it may be necessary to delete old entries and
%% insert the new ones.
build_index_updates([], _TxId) -> [];
build_index_updates(Updates, _TxId) when is_list(Updates) ->
    %lager:info("List of updates: ~p", [Updates]),

    lists:foldl(fun(Update, AccList) ->
        ?INDEX_UPD(TableName, IndexName, {_Value, Type}, {PkValue, Op}) = Update,

        DBIndexName = indexing:generate_sindex_key(TableName, IndexName),
        [IndexKey] = querying_utils:build_keys(DBIndexName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),

        IdxUpdate = case is_list(Op) of
                        true ->
                            lists:map(fun(Op2) ->
                                {{K, T, B}, UpdateOp, Upd} = querying_utils:create_crdt_update(IndexKey, ?MAP_OPERATION, {Type, PkValue, Op2}),
                                {{K, B}, T, {UpdateOp, Upd}}
                                      end, Op);
                        false ->
                            {{K, T, B}, UpdateOp, Upd} = querying_utils:create_crdt_update(IndexKey, ?MAP_OPERATION, {Type, PkValue, Op}),
                            [{{K, B}, T, {UpdateOp, Upd}}]
                    end,

        lists:append([AccList, IdxUpdate])
                end, [], Updates);
build_index_updates(Update, TxId) when ?is_index_upd(Update) ->
    build_index_updates([Update], TxId).

retrieve_index(ObjUpdate, Table) ->
    ?OBJECT_UPDATE(_Key, _Type, _Bucket, _UpdOp, [Assign]) = ObjUpdate,
    ?MAP_UPD(_TableName, _CRDT, _CRDTOp, NewTableMeta) = Assign,

    CurrentIdx =
        case Table of
            [] -> [];
            _ -> table_utils:indexes(Table)
        end,
    NIdx = table_utils:indexes(NewTableMeta),
    NewIndexes = lists:filter(fun(?INDEX(IndexName, _, _)) ->
        not lists:keymember(IndexName, 1, CurrentIdx)
    end, NIdx),
    {NewTableMeta, NewIndexes}.

create_pindex_update(ObjBoundKey, Updates, Table, PIndexKey, Transaction) ->
    {RecordUpds, IdxUpds} = split_updates(Updates, Table, [], []),

    BoundObjUpd = {bound_obj, crdt_utils:to_insert_op(?CRDT_VARCHAR, ObjBoundKey)},
    FullIdxUpd = [BoundObjUpd | IdxUpds],

    %lager:info("FullIdxUpd: ~p", [FullIdxUpd]),

    ConvPKey = get_pk_value(ObjBoundKey, Table, Updates, Transaction),
    {{IdxKey, IdxType, IdxBucket}, UpdateType, IndexOp} =
        querying_utils:create_crdt_update(PIndexKey, ?MAP_OPERATION, {ConvPKey, FullIdxUpd}),
    RetIdxUpd = {{IdxKey, IdxBucket}, IdxType, {UpdateType, IndexOp}},
    {RecordUpds, RetIdxUpd}.

set_policies(TName, Table) ->
    PIdxName = indexing:generate_pindex_key(TName),
    [PIdxKey] = querying_utils:build_keys(PIdxName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
    {IdxKey, IdxType, IdxBucket} = PIdxKey,

    {TPolicy, DepPolicy, _} = table_utils:policy(Table),
    TPUpd = {set, {index_policy, crdt_utils:to_insert_op(?CRDT_VARCHAR, TPolicy)}},
    DPUpd = {set, {dep_policy, crdt_utils:to_insert_op(?CRDT_VARCHAR, DepPolicy)}},
    [{{IdxKey, IdxBucket}, IdxType, TPUpd}, {{IdxKey, IdxBucket}, IdxType, DPUpd}].

split_updates([?MAP_UPD(?STATE_COL, _, Op, Val) | Updates], Table, RecordUpds, IdxUpds) ->
    FormatUpd = {state, {Op, Val}},
    split_updates(Updates, Table, RecordUpds, lists:append(IdxUpds, [FormatUpd]));
split_updates([?MAP_UPD(?VERSION_COL, _, Op, Val) | Updates], Table, RecordUpds, IdxUpds) ->
    FormatUpd = {version, {Op, Val}},
    split_updates(Updates, Table, RecordUpds, lists:append(IdxUpds, [FormatUpd]));
split_updates([Update | Updates], Table, RecordUpds, IdxUpds) ->
    ?MAP_UPD(ColName, _, Op, Val) = Update,
    case table_utils:get_foreign_key(ColName, Table) of
        ?FK(ColName, _, _, _, _) = FkSpec ->
            RefsList = case lists:keyfind(refs, 1, IdxUpds) of
                           false -> [];
                           {refs, List} -> List
                       end,
            NewRefsList = lists:append(RefsList, [{ColName, {Op, {FkSpec, Val}}}]),
            NewIdxUpds = lists:keystore(refs, 1, IdxUpds, {refs, NewRefsList}),
            split_updates(Updates, Table, RecordUpds, NewIdxUpds);
        undefined ->
            split_updates(Updates, Table, lists:append(RecordUpds, [Update]), IdxUpds)
    end;
split_updates([], _Table, RecordUpds, IdxUpds) ->
    {RecordUpds, IdxUpds}.

get_pk_value(ObjBoundKey, Table, Updates, Transaction) ->
    TableCols = table_utils:columns(Table),
    [PKName] = table_utils:primary_key_name(Table),
    {PKName, PKType, _Constraint} = maps:get(PKName, TableCols),
    PKCRDT = crdt_utils:type_to_crdt(PKType, ignore),

    case proplists:get_value({PKName, PKCRDT}, Updates) of
        {_Op, Value} ->
            Value;
        undefined ->
            [Record] = record_utils:record_data(ObjBoundKey, Transaction),
            record_utils:lookup_value({PKName, PKCRDT}, Record)
    end.

is_element(IndexedVal, Pk, IndexObj) ->
    case orddict:find(IndexedVal, IndexObj) of
        {ok, Pks} ->
            querying_utils:first_occurrence(fun({K, _T, _B}) -> K == Pk end, Pks) =/= undefined;
        error -> false
    end.

