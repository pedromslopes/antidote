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
%%% @doc This module allows to encode/decode binary record information
%%%      that was transformed by the Antidote Protocol Buffers client.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(protobufs_utils).

%% API
-export([encode_key/1,
    decode_key/1,
    encode_update/2,
    encode_update/1,
    encode_crdt/1,
    decode_object/2,
    decode_object/1,
    decode_update/2,
    decode_update/1]).

encode_key({Key, Type, Bucket}) ->
    BinKey = querying_utils:to_binary(Key),
    BinBucket = querying_utils:to_binary(Bucket),
    {BinKey, Type, BinBucket}.

decode_key({EncKey, Type, EncBucket}) ->
    Key = list_to_atom(binary_to_list(EncKey)),
    Bucket = list_to_atom(binary_to_list(EncBucket)),
    {Key, Type, Bucket}.

encode_update(CRDT, Op) ->
    encode_update({encode_crdt(CRDT), Op}).

encode_update({counter_b, {increment, {Value, Actor}}}) ->
    {increment, {Value, term_to_binary(Actor)}};
encode_update({counter_b, {decrement, {Value, Actor}}}) ->
    {decrement, {Value, term_to_binary(Actor)}};
encode_update({counter_b, {transfer, {Value, To, Actor}}}) ->
    {transfer, {Value, term_to_binary(To), term_to_binary(Actor)}};
encode_update({counter, CounterUpd}) ->
    CounterUpd;
encode_update({reg, {assign, Value}}) ->
    {assign, term_to_binary(Value)};
encode_update({mvreg, {assign, Value}}) ->
    {assign, term_to_binary(Value)};
encode_update({mvreg, {reset, {}} = MVRegUpd}) ->
    MVRegUpd;
encode_update({flag, FlagUpd}) ->
    FlagUpd;
encode_update({set, {reset, {}} = SetUpd}) ->
    SetUpd;
encode_update({set, {SetOp, Elems}}) when is_list(Elems) ->
    {SetOp, [term_to_binary(Elem) || Elem <- Elems]};
encode_update({set, {SetOp, Elem}}) ->
    {SetOp, term_to_binary(Elem)};
encode_update({map, {update, Updates}}) when is_list(Updates) ->
    {update, [encode_map_entry_update(Update) || Update <- Updates]};
encode_update({map, {update, Update}}) ->
    {update, encode_map_entry_update(Update)};
encode_update({map, {remove, Removes}}) when is_list(Removes) ->
    {remove, [encode_map_entry_remove(Remove) || Remove <- Removes]};
encode_update({map, {remove, Remove}}) ->
    {remove, encode_map_entry_remove(Remove)};
encode_update({map, {batch, {Updates, Removes}}}) ->
    EncUpdates = [encode_map_entry_update(Update) || Update <- Updates],
    EncRemoves = [encode_map_entry_remove(Remove) || Remove <- Removes],
    {batch, {EncUpdates, EncRemoves}};
encode_update({map, {reset, {}} = MapUpd}) ->
    MapUpd;
encode_update({index, {update, Ops}}) when is_list(Ops) ->
    EncOps =
        lists:map(fun(Op) ->
            {update, EncOp} = encode_update({index, {update, Op}}),
            EncOp
        end, Ops),
    {update, EncOps};
encode_update({index, {update, {Key, Ops}}}) when is_list(Ops) ->
    EncKey = term_to_binary(Key),
    EncOps =
        lists:map(fun
                      ({FName, FType, {assign, {K, T, B}}}) ->
                          {FName, FType, {assign, encode_key({K, T, B})}};
                      ({FName, FType, Op}) ->
                          {FName, FType, encode_update({encode_crdt(FType), Op})}
                  end, Ops),
    {update, {EncKey, EncOps}};
encode_update({index, {update, {Key, {assign, {K, T, B}}}}}) ->
    EncKey = term_to_binary(Key),
    EncOp = {assign, encode_key({K, T, B})},
    {update, {EncKey, EncOp}};
encode_update({index, {update, {Key, Op}}}) ->
    encode_update({index, {update, {Key, [Op]}}});
encode_update({index, {remove, Keys}}) when is_list(Keys) ->
    {remove, [term_to_binary(Key) || Key <- Keys]};
encode_update({index, {remove, Key}}) ->
    {remove, term_to_binary(Key)}.

encode_crdt(antidote_crdt_counter_b) -> counter_b;
encode_crdt(antidote_crdt_counter_pn) -> counter;
encode_crdt(antidote_crdt_counter_fat) -> counter;
encode_crdt(antidote_crdt_register_lww) -> reg;
encode_crdt(antidote_crdt_register_mv) -> mvreg;
encode_crdt(antidote_crdt_flag_ew) -> flag;
encode_crdt(antidote_crdt_flag_dw) -> flag;
encode_crdt(antidote_crdt_set_go) -> set;
encode_crdt(antidote_crdt_set_aw) -> set;
encode_crdt(antidote_crdt_set_rw) -> set;
encode_crdt(antidote_crdt_map_rr) -> map;
encode_crdt(antidote_crdt_map_go) -> map;
encode_crdt(antidote_crdt_index) -> index;
encode_crdt(antidote_crdt_index_p) -> index.

decode_object(CRDT, Object) ->
    decode_object({encode_crdt(CRDT), Object}).

decode_object({counter_b, {Incs, Decs}}) ->
    DecIncs =
        orddict:fold(
            fun
                ({_K, K}, V, Acc) ->
                    RawK = querying_utils:to_term(K),
                    orddict:store({RawK, RawK}, V, Acc);
                (K, V, Acc) ->
                    orddict:store(querying_utils:to_term(K), V, Acc)
            end, orddict:new(), Incs),
    DecDecs = orddict:fold(
        fun(K, V, Acc) ->
            orddict:store(querying_utils:to_term(K), V, Acc)
        end, orddict:new(), Decs),
    {DecIncs, DecDecs};
decode_object({counter, Counter}) ->
    Counter;
decode_object({reg, Register}) ->
    querying_utils:to_term(Register);
decode_object({mvreg, MVRegister}) ->
    [querying_utils:to_term(Val) || Val <- MVRegister];
decode_object({flag, Flag}) ->
    Flag;
decode_object({set, Set}) ->
    [querying_utils:to_term(Elem) || Elem <- Set];
decode_object({map, Map}) ->
    [decode_map_entry(Entry) || Entry <- Map];
decode_object({index, Index}) ->
    lists:map(
        fun
            ({Key, BObjs}) when is_list(BObjs) ->
                {querying_utils:to_term(Key), [decode_key(Elem) || Elem <- BObjs]};
            ({Key, BObj}) ->
                {querying_utils:to_term(Key), decode_key(BObj)}
    end, Index).

decode_update(CRDT, Op) ->
    decode_update({encode_crdt(CRDT), Op}).

decode_update({counter_b, {increment, {Value, Actor}}}) ->
    {increment, {Value, querying_utils:to_term(Actor)}};
decode_update({counter_b, {decrement, {Value, Actor}}}) ->
    {decrement, {Value, querying_utils:to_term(Actor)}};
decode_update({counter_b, {transfer, {Value, To, Actor}}}) ->
    {transfer, {Value, querying_utils:to_term(To), querying_utils:to_term(Actor)}};
decode_update({counter, CounterUpd}) ->
    CounterUpd;
decode_update({reg, {assign, Value}}) ->
    {assign, querying_utils:to_term(Value)};
decode_update({mvreg, {assign, Value}}) ->
    {assign, querying_utils:to_term(Value)};
decode_update({mvreg, {reset, {}} = MVRegUpd}) ->
    MVRegUpd;
decode_update({flag, FlagUpd}) ->
    FlagUpd;
decode_update({set, {reset, {}} = SetUpd}) ->
    SetUpd;
decode_update({set, {SetOp, Elems}}) when is_list(Elems) ->
    {SetOp, [querying_utils:to_term(Elem) || Elem <- Elems]};
decode_update({set, {SetOp, Elem}}) ->
    {SetOp, querying_utils:to_term(Elem)};
decode_update({map, {update, Updates}}) when is_list(Updates) ->
    {update, [decode_map_entry_update(Update) || Update <- Updates]};
decode_update({map, {update, Update}}) ->
    {update, decode_map_entry_update(Update)};
decode_update({map, {remove, Removes}}) when is_list(Removes) ->
    {remove, [decode_map_entry_remove(Remove) || Remove <- Removes]};
decode_update({map, {remove, Remove}}) ->
    {remove, decode_map_entry_remove(Remove)};
decode_update({map, {batch, {Updates, Removes}}}) ->
    DecUpdates = [decode_map_entry_update(Update) || Update <- Updates],
    DecRemoves = [decode_map_entry_remove(Remove) || Remove <- Removes],
    {batch, {DecUpdates, DecRemoves}};
decode_update({map, {reset, {}} = MapUpd}) ->
    MapUpd;
decode_update({index, {update, Ops}}) when is_list(Ops) ->
    DecOps =
        lists:map(fun(Op) ->
            {update, DecOp} = decode_update({index, {update, Op}}),
            DecOp
                  end, Ops),
    {update, DecOps};
decode_update({index, {update, {Key, Ops}}}) when is_list(Ops) ->
    DecKey = binary_to_term(Key),
    DecOps =
        lists:map(fun({FieldName, FieldType, Op}) ->
            {FieldName, FieldType, decode_update({encode_crdt(FieldType), Op})}
                  end, Ops),
    {update, {DecKey, DecOps}};
decode_update({index, {update, {Key, Op}}}) ->
    DecKey = querying_utils:to_term(Key),
    DecOp = decode_update({reg, Op}),
    {update, {DecKey, DecOp}};
decode_update({index, {remove, Keys}}) when is_list(Keys) ->
    {remove, [querying_utils:to_term(Key) || Key <- Keys]};
decode_update({index, {remove, Key}}) ->
    {remove, querying_utils:to_term(Key)}.

%% ====================================================================
%% Internal functions
%% ====================================================================

encode_map_entry_update({{Key, Type}, Op}) ->
    EncKey = term_to_binary(Key),
    EncUpd = encode_update({encode_crdt(Type), Op}),
    {{EncKey, Type}, EncUpd}.

encode_map_entry_remove({Key, Type}) ->
    {term_to_binary(Key), Type}.

decode_map_entry_update({{Key, Type}, Op}) ->
    DecKey = querying_utils:to_term(Key),
    DecUpd = decode_update({encode_crdt(Type), Op}),
    {{DecKey, Type}, DecUpd}.

decode_map_entry_remove({Key, Type}) ->
    {querying_utils:to_term(Key), Type}.

decode_map_entry({{Key, Type}, Value}) ->
    DecKey = querying_utils:to_term(Key),
    DecObj = decode_object({encode_crdt(Type), Value}),
    {{DecKey, Type}, DecObj}.
