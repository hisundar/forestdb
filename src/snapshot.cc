/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "common.h"
#include "avltree.h"
#include "snapshot.h"
#include "fdb_internal.h"

#include "memleak.h"

#ifdef __DEBUG
#ifndef __DEBUG_SNAP
    #undef DBG
    #undef DBGCMD
    #undef DBGSW
    #define DBG(...)
    #define DBGCMD(...)
    #define DBGSW(n, ...)
#endif
#endif

// lexicographically compares two variable-length binary streams
static int _snp_keycmp(void *key1, size_t keylen1, void *key2, size_t keylen2)
{
    if (keylen1 == keylen2) {
        return memcmp(key1, key2, keylen1);
    }else {
        size_t len = MIN(keylen1, keylen2);
        int cmp = memcmp(key1, key2, len);
        if (cmp != 0) return cmp;
        else {
            return (int)((int)keylen1 - (int)keylen2);
        }
    }
}

static int _snp_seqnum_cmp(struct avl_node *a, struct avl_node *b, void *aux)
{
    struct snap_wal_entry *aa, *bb;
    aa = _get_entry(a, struct snap_wal_entry, avl_seq);
    bb = _get_entry(b, struct snap_wal_entry, avl_seq);
    return (aa->seqnum - bb->seqnum);
}

static int _snp_wal_cmp(struct avl_node *a, struct avl_node *b, void *aux)
{
    struct _fdb_key_cmp_info *info = (struct _fdb_key_cmp_info*)aux;
    struct snap_wal_entry *aa, *bb;
    aa = _get_entry(a, struct snap_wal_entry, avl);
    bb = _get_entry(b, struct snap_wal_entry, avl);

    if (info->kvs_config.custom_cmp) {
        // custom compare function for variable-length key
        if (info->kvs) {
            // multi KV instance mode
            // KV ID should be compared separately
            size_t size_chunk = info->kvs->root->config.chunksize;
            fdb_kvs_id_t a_id, b_id;
            buf2kvid(size_chunk, aa->key, &a_id);
            buf2kvid(size_chunk, bb->key, &b_id);

            if (a_id < b_id) {
                return -1;
            } else if (a_id > b_id) {
                return 1;
            } else {
                return info->kvs_config.custom_cmp(
                            (uint8_t*)aa->key + size_chunk,
                            aa->keylen - size_chunk,
                            (uint8_t*)bb->key + size_chunk,
                            bb->keylen - size_chunk);
            }
        } else {
            return info->kvs_config.custom_cmp(aa->key, aa->keylen,
                                               bb->key, bb->keylen);
        }
    } else {
        return _snp_keycmp(aa->key, aa->keylen, bb->key, bb->keylen);
    }
}


fdb_status snap_init(struct snap_handle *shandle, fdb_kvs_handle *handle)
{
    shandle->key_tree = (struct avl_tree *) malloc(sizeof(struct avl_tree));
    if (!shandle->key_tree) { // LCOV_EXCL_START
        return FDB_RESULT_ALLOC_FAIL;
    } // LCOV_EXCL_STOP
    shandle->cmp_info.kvs_config = handle->kvs_config;
    shandle->cmp_info.kvs = handle->kvs;

    avl_init(shandle->key_tree, (void *)&shandle->cmp_info);
    shandle->seq_tree = (struct avl_tree *) malloc(sizeof(struct avl_tree));
    if (!shandle->seq_tree) { // LCOV_EXCL_START
        return FDB_RESULT_ALLOC_FAIL;
    } // LCOV_EXCL_STOP
    avl_init(shandle->seq_tree, NULL);
    spin_init(&shandle->lock);
    shandle->ref_cnt = 1;
    shandle->type = FDB_SNAP_NORMAL;
    return FDB_RESULT_SUCCESS;
}

INLINE void _snap_cache_item(struct snap_wal_entry *item, fdb_doc *doc)

{
    item->metalen = doc->metalen;
    item->bodylen = doc->bodylen;
    if (!item->metalen && item->bodylen <= sizeof(item->_body)) {
        memcpy(&item->_body, doc->body, doc->bodylen);
        item->flag |= WAL_ITEM_OFF_IS_BODY;
    } else {
        item->flag &= ~WAL_ITEM_OFF_IS_BODY;
        item->doc_body = (struct _fdb_snap_doc *)malloc(
                sizeof(struct _fdb_snap_doc));
        if (item->metalen) {
            item->doc_body->meta = malloc(item->metalen);
            memcpy(item->doc_body->meta, doc->meta, item->metalen);
        } else {
            item->doc_body->meta = NULL;
        }
        if (item->bodylen) {
            item->doc_body->body = malloc(item->bodylen);
            memcpy(item->doc_body->body, doc->body, item->bodylen);
        } else {
            item->doc_body->body = NULL;
        }
    }
    item->flag |= WAL_ITEM_HAS_FULL_DOC;
}

INLINE void _snap_update_cache_item(struct snap_wal_entry *item, fdb_doc *doc)

{
    if (!(item->flag & WAL_ITEM_HAS_FULL_DOC) ||
         item->flag & WAL_ITEM_OFF_IS_BODY) { // nothing to free
        _snap_cache_item(item, doc);
        return;
    }
    item->metalen = doc->metalen;
    item->bodylen = doc->bodylen;
    if (!item->metalen && item->bodylen <= sizeof(item->_body)) {
        free(item->doc_body->meta);
        free(item->doc_body->body);
        free(item->doc_body);
        memcpy(&item->_body, doc->body, doc->bodylen);
        item->flag |= WAL_ITEM_OFF_IS_BODY;
    } else {
        item->doc_body = (struct _fdb_snap_doc *)malloc(
                sizeof(struct _fdb_snap_doc));
        if (item->metalen) {
            item->doc_body->meta = realloc(item->doc_body->meta, item->metalen);
            memcpy(item->doc_body->meta, doc->meta, item->metalen);
        }
        if (item->bodylen) {
            item->doc_body->body = realloc(item->doc_body->body, item->bodylen);
            memcpy(item->doc_body->body, doc->body, item->bodylen);
        }
    }
    item->flag |= WAL_ITEM_HAS_FULL_DOC;
}

fdb_status snap_insert(struct snap_handle *shandle, fdb_doc *doc,
                       uint64_t offset)
{
    struct snap_wal_entry query;
    struct snap_wal_entry *item;
    struct avl_node *node;
    memset(&query, 0, sizeof(snap_wal_entry));
    query.key = doc->key;
    query.keylen = doc->keylen;
    node = avl_search(shandle->key_tree, &query.avl, _snp_wal_cmp);

    if (!node) {
        item = (struct snap_wal_entry *) malloc(sizeof(struct snap_wal_entry));
        item->keylen = doc->keylen;
        item->key = doc->key;
        item->seqnum = doc->seqnum;
        item->flag = 0;
        if (doc->deleted) {
            if (!offset) { // logically deleted item can never be at offset 0
                item->action = WAL_ACT_REMOVE; // must be a purged item
            } else {
                item->action = WAL_ACT_LOGICAL_REMOVE;
            }
        } else {
            item->action = WAL_ACT_INSERT;
        }
        if (item->action != WAL_ACT_REMOVE &&
            offset == BLK_NOT_FOUND) { // Fully cached
            _snap_cache_item(item, doc);
        } else {
            item->offset = offset;
        }
        avl_insert(shandle->key_tree, &item->avl, _snp_wal_cmp);
        avl_insert(shandle->seq_tree, &item->avl_seq, _snp_seqnum_cmp);

        // Note: same logic in wal_commit
        shandle->stat.wal_ndocs++;
        if (doc->deleted) {
            shandle->stat.wal_ndeletes++;
        }
    } else {
        // replace existing node with new values so there are no duplicates
        item = _get_entry(node, struct snap_wal_entry, avl);
        free(item->key);
        item->keylen = doc->keylen;
        item->key = doc->key;
        if (item->seqnum != doc->seqnum) { // Re-index duplicate into seqtree
            item->seqnum = doc->seqnum;
            avl_remove(shandle->seq_tree, &item->avl_seq);
            avl_insert(shandle->seq_tree, &item->avl_seq, _snp_seqnum_cmp);
        }

        // Note: same logic in wal_commit
        if (item->action == WAL_ACT_INSERT &&
            doc->deleted) {
            shandle->stat.wal_ndeletes++;
        } else if (item->action == WAL_ACT_LOGICAL_REMOVE &&
                   !doc->deleted) {
            shandle->stat.wal_ndeletes--;
        }

        item->action = doc->deleted ? WAL_ACT_LOGICAL_REMOVE : WAL_ACT_INSERT;
        if (offset == BLK_NOT_FOUND) { // fully cached in-memory doc
            _snap_update_cache_item(item, doc);
        } else { // cached item being replaced with a persisted item
            if (item->flag & WAL_ITEM_HAS_FULL_DOC) {
                snap_free_cache_item(item);
            }
            item->offset = offset;
        }
    }

    return FDB_RESULT_SUCCESS;
}

INLINE void _snap_copy_item(struct snap_wal_entry *item, fdb_doc *doc,
                            bool copy_key) {
    if (copy_key) {
        doc->keylen = item->keylen;
        if (!doc->key) {
            doc->key = malloc(doc->keylen);
        }
        memcpy(doc->key, item->key, doc->keylen);
    }
    doc->metalen = item->metalen;
    if (item->metalen) {
        if (!doc->meta) {
            doc->meta = malloc(item->metalen);
        }
        memcpy(doc->meta, item->doc_body->meta, item->metalen);
    }
    doc->bodylen = item->bodylen;
    if (item->bodylen) {
        if (!doc->body) {
            doc->body = malloc(item->bodylen);
        }
        if (item->flag & WAL_ITEM_OFF_IS_BODY) {
            memcpy(doc->body, &item->_body, item->bodylen);
        } else {
            memcpy(doc->body, &item->doc_body->body, item->bodylen);
        }
    }
    doc->seqnum = item->seqnum;
    doc->size_ondisk = item->keylen + item->metalen + item->bodylen
                     + DOCIO_OVERHEAD_BYTES;
    doc->offset = BLK_NOT_FOUND;
}

fdb_status snap_find(struct snap_handle *shandle, fdb_doc *doc,
                     uint64_t *offset)
{
    struct snap_wal_entry query;
    struct avl_node *node;
    memset(&query, 0, sizeof(snap_wal_entry));
    if (doc->seqnum == SEQNUM_NOT_USED || (doc->key && doc->keylen > 0)) {
        if (!shandle->key_tree) {
            return FDB_RESULT_KEY_NOT_FOUND;
        }
        // search by key
        query.key = doc->key;
        query.keylen = doc->keylen;
        node = avl_search(shandle->key_tree, &query.avl, _snp_wal_cmp);
        if (!node) {
            return FDB_RESULT_KEY_NOT_FOUND;
        } else {
            struct snap_wal_entry *item;
            item = _get_entry(node, struct snap_wal_entry, avl);
            if (item->action == WAL_ACT_INSERT) {
                doc->deleted = false;
            } else {
                doc->deleted = true;
                if (item->action == WAL_ACT_REMOVE) {
                    *offset = BLK_NOT_FOUND;
                }
            }
            if (item->flag & WAL_ITEM_HAS_FULL_DOC) {
                _snap_copy_item(item, doc, false);
                *offset = BLK_NOT_FOUND; // indicates that the doc was in-mem
            } else {
                *offset = item->offset;
            }
            return FDB_RESULT_SUCCESS;
        }
    } else {
        if (!shandle->seq_tree) {
            return FDB_RESULT_KEY_NOT_FOUND;
        }
        // search by sequence number
        query.seqnum = doc->seqnum;
        node = avl_search(shandle->seq_tree, &query.avl_seq, _snp_seqnum_cmp);
        if (!node) {
            return FDB_RESULT_KEY_NOT_FOUND;
        } else {
            struct snap_wal_entry *item;
            item = _get_entry(node, struct snap_wal_entry, avl_seq);
            if (item->action == WAL_ACT_INSERT) {
                doc->deleted = false;
            } else {
                doc->deleted = true;
                if (item->action == WAL_ACT_REMOVE) {
                    *offset = BLK_NOT_FOUND;
                }
            }
            if (item->flag & WAL_ITEM_HAS_FULL_DOC) {
                _snap_copy_item(item, doc, true);
                *offset = BLK_NOT_FOUND; // indicates that the doc was in-mem
            } else {
                *offset = item->offset;
            }
            return FDB_RESULT_SUCCESS;
        }
    }
    return FDB_RESULT_KEY_NOT_FOUND;
}

fdb_status snap_clone(struct snap_handle *shandle_in, fdb_seqnum_t in_seqnum,
                      struct snap_handle **shandle, fdb_seqnum_t snap_seqnum)
{
    if (snap_seqnum == FDB_SNAPSHOT_INMEM ||
        in_seqnum == snap_seqnum) {
        spin_lock(&shandle_in->lock);
        shandle_in->ref_cnt++;
        spin_unlock(&shandle_in->lock);
        *shandle = shandle_in;
        return FDB_RESULT_SUCCESS;
    }

    return FDB_RESULT_INVALID_ARGS;
}

fdb_status snap_close(struct snap_handle *shandle)
{
    struct avl_node *a;
    struct snap_wal_entry *snap_item;

    spin_lock(&shandle->lock);
    assert(shandle->ref_cnt);

    if (--shandle->ref_cnt == 0) {
        if (shandle->key_tree) {
            a = avl_first(shandle->key_tree);
            while (a) {
                snap_item = _get_entry(a, struct snap_wal_entry, avl);
                a = avl_next(a);
                avl_remove(shandle->key_tree, &snap_item->avl);
                free(snap_item->key);
                if (snap_item->flag & WAL_ITEM_HAS_FULL_DOC) {
                    snap_free_cache_item(snap_item);
                }
                free(snap_item);
            }
            free(shandle->key_tree);
            free(shandle->seq_tree);
        }
        spin_unlock(&shandle->lock);
        free(shandle);
    } else {
        spin_unlock(&shandle->lock);
    }

    return FDB_RESULT_SUCCESS;
}

fdb_status snap_get_stat(struct snap_handle *shandle, struct kvs_stat *stat)
{
    *stat = shandle->stat;
    return FDB_RESULT_SUCCESS;
}

