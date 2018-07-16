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
%%% @doc An Antidote module that manages the indexing data structures
%%%      of the database, including the primary and secondary indexes.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(indexing).

-include("querying.hrl").

-define(PINDEX_ENTRY_DT, antidote_crdt_register_lww).
-define(INDEX_OPERATION, update).

%% API
-export([index_name/1,
         table_name/1,
         attributes/1,
         read_index/3,
         read_index_function/4,
         create_index/2,
         generate_pindex_key/1,
         generate_sindex_key/2,
         apply_updates/2,
         get_indexed_values/1,
         get_primary_keys/2,
         lookup_index/2]).
-export([entry_key/1,
         entry_bobj/1,
         entry_state/1,
         entry_version/1,
         entry_refs/1,
         get_field/2,
         get_ref_by_name/2,
         %%get_ref_by_spec/2,
         format_refs/1,
         delete_entry/3]).

index_name(?INDEX(IndexName, _TableName, _Attributes)) -> IndexName.

table_name(?INDEX(_IndexName, TableName, _Attributes)) -> TableName.

attributes(?INDEX(_IndexName, _TableName, Attributes)) -> Attributes.

read_index(primary, TableName, TxId) ->
    IndexName = generate_pindex_key(TableName),
    ObjKeys = querying_utils:build_keys(IndexName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_keys(value, ObjKeys, TxId),
    IdxObj;
read_index(secondary, {TableName, IndexName}, TxId) ->
    %% The secondary index is identified by the notation #2i_<IndexName>, where
    %% <IndexName> = <table_name>.<index_name>

    FullIndexName = generate_sindex_key(TableName, IndexName),
    ObjKeys = querying_utils:build_keys(FullIndexName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_keys(value, ObjKeys, TxId),
    IdxObj.

read_index_function(primary, TableName, {Function, Args}, TxId) ->
    IndexName = generate_pindex_key(TableName),
    ObjKeys = querying_utils:build_keys(IndexName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_function(ObjKeys, {Function, Args}, TxId),
    case IdxObj of
        {error, _} -> [];
        IdxObj -> IdxObj
    end;
read_index_function(secondary, {TableName, IndexName}, {Function, Args}, TxId) ->
    FullIndexName = generate_sindex_key(TableName, IndexName),
    ObjKeys = querying_utils:build_keys(FullIndexName, ?SINDEX_DT, ?AQL_METADATA_BUCKET),
    [IdxObj] = querying_utils:read_function(ObjKeys, {Function, Args}, TxId),
    case IdxObj of
        {error, _} -> [];
        IdxObj -> IdxObj
    end.

%% TODO
create_index(_IndexName, _TxId) -> {error, not_implemented}.

generate_pindex_key(TableName) ->
    IndexName = lists:concat([?PINDEX_PREFIX, TableName]),
    querying_utils:to_atom(IndexName).

generate_sindex_key(TableName, IndexName) ->
    FullIndexName = lists:concat([?SINDEX_PREFIX, TableName, '.', IndexName]),
    querying_utils:to_atom(FullIndexName).

apply_updates(Update, TxId) when ?is_index_upd(Update) ->
    apply_updates([Update], TxId);
apply_updates([Update | Tail], TxId) when ?is_index_upd(Update) ->
    DatabaseUpdates = index_triggers:build_index_updates([Update], TxId),
    ok = querying_utils:write_keys(DatabaseUpdates, TxId),
    apply_updates(Tail, TxId);
apply_updates([], _TxId) ->
    ok.

get_indexed_values([]) -> [];
get_indexed_values(IndexObj) ->
    orddict:fetch_keys(IndexObj).

get_primary_keys(_IndexedValues, []) -> [];
get_primary_keys(IndexedValues, IndexObj) when is_list(IndexedValues) ->
    get_primary_keys(IndexedValues, IndexObj, []);
get_primary_keys(IndexedValue, IndexObj) ->
    get_primary_keys([IndexedValue], IndexObj).

get_primary_keys([IdxValue | Tail], IndexObj, Acc) ->
    case orddict:find(IdxValue, IndexObj) of
        error -> get_primary_keys(Tail, IndexObj, Acc);
        {ok, PKs} -> get_primary_keys(Tail, IndexObj, lists:append(Acc, ordsets:to_list(PKs)))
    end;
get_primary_keys([], _Index, Acc) ->
    Acc.

lookup_index(ColumnName, Indexes) ->
    lookup_index(ColumnName, Indexes, []).
lookup_index(ColumnName, [Index | Tail], Acc) ->
    Attributes = attributes(Index),
    case lists:member(ColumnName, Attributes) of
        true -> lookup_index(ColumnName, Tail, lists:append(Acc, [Index]));
        false -> lookup_index(ColumnName, Tail, Acc)
    end;
lookup_index(_ColumnName, [], Acc) ->
    Acc.

%% ===================================================================
%% Index entry functions
%% ===================================================================

entry_key(?INDEX_ENTRY(Key, _, _, _, _)) -> Key.

entry_bobj(?INDEX_ENTRY(_, BObj, _, _, _)) -> BObj.

entry_state(?INDEX_ENTRY(_, _, State, _, _)) -> State.

entry_version(?INDEX_ENTRY(_ ,_, _, Version, _)) -> Version.

entry_refs(?INDEX_ENTRY(_, _, _, _, Refs)) -> Refs.

get_field(key, IndexEntry) -> entry_key(IndexEntry);
get_field(bound_obj, IndexEntry) -> entry_bobj(IndexEntry);
get_field('#st', IndexEntry) -> entry_state(IndexEntry);
get_field('state', IndexEntry) -> entry_state(IndexEntry);
get_field('#version', IndexEntry) -> entry_version(IndexEntry);
get_field('version', IndexEntry) -> entry_version(IndexEntry);
get_field(refs, IndexEntry) -> entry_refs(IndexEntry);
get_field(RefName, IndexEntry) ->
    case get_ref_by_name(RefName, IndexEntry) of
        {_, Value} -> Value;
        undefined -> undefined
    end.

get_ref_by_name(RefName, ?INDEX_ENTRY(_, _, _, _, Refs)) ->
    querying_utils:first_occurrence(
        fun({FkName, _}) ->
            FkName == RefName
        end,
        Refs).

%%get_ref_by_spec(RefSpec, ?INDEX_ENTRY(_, _, _, _, Refs)) ->
%%    querying_utils:first_occurrence(
%%        fun({FkSpec, _}) ->
%%            FkSpec == RefSpec
%%        end,
%%        Refs).

format_refs(?INDEX_ENTRY(_, _, _, _, Refs)) ->
    lists:map(fun(?INDEX_REF(FkName, FkSpec, FkValue, _FkVersion)) ->
        {FkName, {FkSpec, FkValue}}
    end, Refs).

delete_entry(TableName, IndexEntry, TxId) ->
    PIdxName = indexing:generate_pindex_key(TableName),
    [PIdxKey] = querying_utils:build_keys(PIdxName, ?PINDEX_DT, ?AQL_METADATA_BUCKET),

    PKey = entry_key(IndexEntry),

    AssignOp = crdt_utils:to_insert_op(?CRDT_VARCHAR, d),
    StateUpdate = {state, AssignOp},
    Update = querying_utils:create_crdt_update(PIdxKey, ?INDEX_OPERATION, {PKey, StateUpdate}),
    ok = querying_utils:write_keys(Update, TxId),
    false.
