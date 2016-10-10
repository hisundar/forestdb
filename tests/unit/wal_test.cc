/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include <stdio.h>
#include <stdlib.h>

#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "wal.h"
#include "filemgr.h"
#include "libforestdb/forestdb.h"
#include "test.h"
#include "fdb_engine.h"

void wal_basic_test()
{
    TEST_INIT();

    Wal *wal;
    FileMgr *file;
    struct _fdb_key_cmp_info cmp_info;
    KvsInfo def_kvs;

    FdbEngine::init(nullptr);
    int ndocs = 90000;
    FileMgrConfig config(4096, // block size
                         5, // number of buffercache blocks
                         0, // flag
                         8, // chunk size
                         FILEMGR_CREATE, // create if does not exist
                         FDB_SEQTREE_USE, // create and use sequence trees
                         0, // prefetch thread duration 0 = disabled
                         8, // num wal shards
                         0, // num block cache shards
                         FDB_ENCRYPTION_NONE, // encryption type
                         0x00, // encryption key size in bytes
                         0, // block reusing threshold
                         0); // num keeping headers
    cmp_info.kvs_config = fdb_get_default_kvs_config();
    cmp_info.kvs = &def_kvs;

    int i, r;
    char buf[32];
    memset(buf, 0, sizeof(buf));
    fdb_status s;

    r = system(SHELL_DEL" wal_testfile* > errorlog.txt");
    (void)r;

    std::string fname("./wal_testfile");

    filemgr_open_result result = FileMgr::open(fname, get_filemgr_ops(),
                                               &config, nullptr);
    file = result.file;
    wal = file->getWal();
    fdb_doc wal_doc;
    fdb_txn *txn = file->getGlobalTxn();

    for (i=0; i < ndocs; ++i) {
        sprintf(buf, "%08dkey%05d", 0, i);
        wal_doc.keylen = 16; // 8 bytes KVID + 8 bytes of key string
        wal_doc.bodylen = sizeof(i);
        wal_doc.key = &buf[0];
        wal_doc.seqnum = i;
        wal_doc.deleted = false;
        wal_doc.metalen = 0;
        wal_doc.meta = nullptr;
        wal_doc.size_ondisk = wal_doc.bodylen;
        wal_doc.flags = 0;
        union Wal::indexedValue value;
        value.offset = uint64_t(i);

        s = wal->insert_Wal(txn, &cmp_info,
                            &wal_doc,
                            value,
                            Wal::INS_BY_WRITER);
        TEST_CHK(s == FDB_RESULT_SUCCESS);

    }

    for (i=0; i < ndocs; ++i) {
        sprintf(buf, "%08dkey%05d", 0, i);
        wal_doc.keylen = 16; // 8 bytes KVID + 8 bytes of key string
        wal_doc.bodylen = sizeof(i);
        wal_doc.key = &buf[0];
        wal_doc.seqnum = SEQNUM_NOT_USED;
        wal_doc.deleted = false;
        wal_doc.flags = 0;
        union Wal::indexedValue value_out;

        s = wal->find_Wal(txn, &cmp_info, nullptr, &wal_doc,
                                     &value_out);
        TEST_CHK(s == FDB_RESULT_SUCCESS);
        TEST_CHK(value_out.offset == uint64_t(i));
    }

    for (int i = FDB_LATENCY_WAL_INS; i < FDB_LATENCY_NUM_STATS; ++i) {
        fdb_latency_stat stat;
        memset(&stat, 0, sizeof(fdb_latency_stat));
        LatencyStats::get(file, fdb_latency_stat_type(i), &stat);
        fprintf(stderr, "%s:\t%u\t%u\t%u\t%" _F64 "\n",
                fdb_latency_stat_name(i),
                stat.lat_min, stat.lat_avg, stat.lat_max, stat.lat_count);
    }

    s = FileMgr::close(file,
                       true, // cleanup cache on close
                       fname.c_str(),
                       nullptr);

    TEST_CHK(s == FDB_RESULT_SUCCESS);

    FdbEngine::destroyInstance();
    TEST_RESULT("wal basic test");
}

void wal_ref_ptr_test()
{
    TEST_INIT();

    Wal *wal;
    FileMgr *file;
    struct _fdb_key_cmp_info cmp_info;
    KvsInfo def_kvs;

    FdbEngine::init(nullptr);
    int ndocs = 90000;
    FileMgrConfig config(4096, // block size
                         5, // number of buffercache blocks
                         0, // flag
                         8, // chunk size
                         FILEMGR_CREATE, // create if does not exist
                         FDB_SEQTREE_USE, // create and use sequence trees
                         0, // prefetch thread duration 0 = disabled
                         8, // num wal shards
                         0, // num block cache shards
                         FDB_ENCRYPTION_NONE, // encryption type
                         0x00, // encryption key size in bytes
                         0, // block reusing threshold
                         0); // num keeping headers
    cmp_info.kvs_config = fdb_get_default_kvs_config();
    cmp_info.kvs = &def_kvs;
    fdb_status s;
    int i, r;

    r = system(SHELL_DEL" wal_testfile* > errorlog.txt");
    (void)r;

    std::string fname("./wal_testfile");

    filemgr_open_result result = FileMgr::open(fname, get_filemgr_ops(),
                                               &config, NULL);
    file = result.file;
    wal = file->getWal();
    fdb_doc wal_doc;
    fdb_txn *txn = file->getGlobalTxn();

    // Allocate a big buffer and load just the keys into it..
    struct cldoc {
        char kv_id[8];
        char key[8];
    };
    struct cldoc *commit_log = new cldoc[ndocs];
    // Load all the keys into this big buffer...
    for (i=0; i < ndocs; ++i) { // load the buf with null terminated keys..
        sprintf((char *)&commit_log[i], "%08dke%05d", 0, i);
    }

    for (i=0; i < ndocs; ++i) {
        wal_doc.keylen = 16; // 8 bytes KVID + 8 bytes of key string
        wal_doc.bodylen = 0;
        wal_doc.key = &commit_log[i]; // simply point to the buffer position
        wal_doc.seqnum = i;
        wal_doc.deleted = false;
        wal_doc.metalen = 0;
        wal_doc.meta = nullptr;
        wal_doc.size_ondisk = wal_doc.bodylen;
        wal_doc.flags = FDB_DOC_MEMORY_SHARED; // Tell WAL to share key memory
        union Wal::indexedValue value;
        value.doc_ptr = &commit_log[i]; // no separate value, just point to key

        s = wal->insert_Wal(txn, &cmp_info, &wal_doc, value,
                            Wal::INS_BY_WRITER);
        TEST_CHK(s == FDB_RESULT_SUCCESS);

    }

    for (i=0; i < ndocs; ++i) {
        wal_doc.keylen = 16; // 8 bytes KVID + 8 bytes of key string
        wal_doc.key = &commit_log[i];
        wal_doc.seqnum = SEQNUM_NOT_USED;
        union Wal::indexedValue value_out;

        s = wal->find_Wal(txn, &cmp_info, nullptr, &wal_doc, &value_out);
        TEST_CHK(s == FDB_RESULT_SUCCESS);
        TEST_CHK(wal_doc.flags & FDB_DOC_MEMORY_SHARED);
        TEST_CHK(value_out.doc_ptr == &commit_log[i]);
    }

    for (int i = FDB_LATENCY_WAL_INS; i < FDB_LATENCY_NUM_STATS; ++i) {
        fdb_latency_stat stat;
        memset(&stat, 0, sizeof(fdb_latency_stat));
        LatencyStats::get(file, fdb_latency_stat_type(i), &stat);
        fprintf(stderr, "%s:\t%u\t%u\t%u\t%" _F64 "\n",
                fdb_latency_stat_name(i),
                stat.lat_min, stat.lat_avg, stat.lat_max, stat.lat_count);
    }

    s = FileMgr::close(file,
                       true, // cleanup cache on close
                       fname.c_str(),
                       nullptr);

    TEST_CHK(s == FDB_RESULT_SUCCESS);
    delete [] commit_log;

    FdbEngine::destroyInstance();
    TEST_RESULT("wal reference pointer test");
}

#include<chrono>
#include<algorithm>
#include<vector>
#include<set>
#include "checksum.h"
#define NBUCKET 65536

INLINE int _wal_keycmp(void *key1, size_t keylen1, void *key2, size_t keylen2)
{
    if (keylen1 == keylen2) {
        return memcmp(key1, key2, keylen1);
    } else {
        size_t len = MIN(keylen1, keylen2);
        int cmp = memcmp(key1, key2, len);
        if (cmp != 0) return cmp;
        else {
            return (int)((int)keylen1 - (int)keylen2);
        }
    }
}

INLINE int _wal_kvs_cmp(struct avl_node *a, struct avl_node *b, void *aux)
{
    struct wal_item_header *aa, *bb;
    aa = _get_entry(a, struct wal_item_header, avl_key);
    bb = _get_entry(b, struct wal_item_header, avl_key);

    return _wal_keycmp(aa->key, aa->keylen, bb->key, bb->keylen);
}

INLINE int _wal_hash_cmp(struct hash_elem *a, struct hash_elem *b)
{
    struct wal_item_header *aa, *bb;
    aa = _get_entry(a, struct wal_item_header, hash_key);
    bb = _get_entry(b, struct wal_item_header, hash_key);

    return _wal_keycmp(aa->key, aa->keylen, bb->key, bb->keylen);
}


struct hasher {
    size_t operator() (const struct wal_item_header *a) const {
        return get_checksum((const uint8_t *)a->key, a->keylen, CRC_DEFAULT);
    }
} hash_crc;

static uint32_t _wal_hasher(struct hash *hash, struct hash_elem *e)
{
    struct wal_item_header *a = _get_entry(e, struct wal_item_header, hash_key);
    return ((unsigned)get_checksum((const uint8_t *)a->key, a->keylen, CRC_DEFAULT) &  (NBUCKET-1)); 
}

struct lex_comparator {
    bool operator()(struct wal_item_header *a,
                           struct wal_item_header *b) {
        return (_wal_keycmp(a->key, a->keylen, b->key, b->keylen) < 0);
    }
} compare_less;

void free_func(struct hash_elem *ha) {
    struct wal_item_header *h = _get_entry(ha, struct wal_item_header, hash_key);
    delete h;
}

uint64_t load_test(int n, int opt) {


    // Following indexes are tested..
    std::vector<wal_item_header *> indexV;
    std::set<wal_item_header *, lex_comparator> indexM;
    std::unordered_set<wal_item_header *, hasher> indexU;
    struct avl_tree indexA;
    struct hash indexH;
    if (opt == 0) {
        indexV.reserve(n);
    } else if (opt == 2){
        avl_init(&indexA, NULL);
    } else if (opt == 3) {
        hash_init(&indexH, NBUCKET, _wal_hasher, _wal_hash_cmp);
    } else if (opt == 4) {
        indexU.reserve(n);
    }

    struct wal_item_header **headers = new struct wal_item_header*[n];
    char **keyz = new char *[n*16];

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < n; ++i) {
        headers[i] = new struct wal_item_header();
        keyz[i] = new char[16];
        sprintf(keyz[i], "%08dk%06d", i, i); // , i); //rand()%1000000);
        headers[i]->key = keyz[i];
        headers[i]->keylen = 16;
        if (opt == 0) {
            indexV.push_back(headers[i]);
        } else if (opt == 1) {
            indexM.insert(headers[i]);
        } else if (opt == 2) {
            avl_insert(&indexA, &headers[i]->avl_key, _wal_kvs_cmp);
        } else if (opt == 3) {
            hash_insert(&indexH, &headers[i]->hash_key);
        } else if (opt == 4) {
            indexU.insert(headers[i]);
        }
    }
    if (opt == 0) {
        std::sort(indexV.begin(), indexV.end(), compare_less);
    }

    auto access_start = std::chrono::high_resolution_clock::now();
    if (opt == 0) {
        for (auto it = indexV.begin(); it != indexV.end(); ++it) {
            struct wal_item_header *hdr = (*it);
            delete ((char *)hdr->key);
            delete hdr;
            //printf("set key = %s\n", (*it)->key);
        }
    } else if (opt == 1) {
        for (auto it = indexM.begin(); it != indexM.end(); ++it) {
            struct wal_item_header *hdr = (*it);
            delete ((char *)hdr->key);
            delete hdr;
            //printf("set key = %s\n", (*it)->key);
        }
    } else if (opt == 2) {
        for (auto it = avl_first(&indexA); it; it = avl_next(it)) {
            struct wal_item_header *hdr = _get_entry(it, wal_item_header, avl_key);
            delete ((char *)hdr->key);
            //printf("set key = %s\n", (*it)->key);
        }
        for (int i = 0; i < n; ++i) {
            delete headers[i];
        }
    } else if (opt == 3) {
        hash_free_active(&indexH, free_func);
    } else if (opt == 4) {
        for (auto it = indexU.begin(); it != indexU.end(); ++it) {
            struct wal_item_header *hdr = (*it);
            delete ((char *)hdr->key);
            delete hdr;
            //printf("set key = %s\n", (*it)->key);
        }
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto diff1 = std::chrono::duration_cast<std::chrono::microseconds>(end_time
                  - access_start).count();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end_time
                  - start_time).count();
    uint64_t adiff = uint64_t(diff1);
    uint64_t tdiff = uint64_t(diff);
    if (opt == 0) {
        fprintf(stdout, "Indexing %d items with vector and sorting took %"
                _F64 "us total\t%" _F64 "us to index\t%" _F64 "us to access\n", n, tdiff, tdiff - adiff, adiff);
    } else if (opt == 1) {
        fprintf(stdout, "Indexing %d items into a sorted std::set took  %"
                _F64 "us total\t%" _F64 "us to index\t%" _F64 "us to access\n", n, tdiff, tdiff - adiff, adiff);
    } else if (opt == 2) {
        fprintf(stdout, "Indexing %d items into a sorted avl tree took  %"
                _F64 "us total\t%" _F64 "us to index\t%" _F64 "us to access\n", n, tdiff, tdiff - adiff, adiff);
    } else if (opt == 3) {
        fprintf(stdout, "Indexing %d items into unsorted avl hash took  %"
                _F64 "us total\t%" _F64 "us to index\t%" _F64 "us to access\n", n, tdiff, tdiff - adiff, adiff);
    } else if (opt == 4) {
        fprintf(stdout, "Indexing %d items into unsorted set took       %"
                _F64 "us total\t%" _F64 "us to index\t%" _F64 "us to access\n", n, tdiff, tdiff - adiff, adiff);
    }

    delete [] keyz;
    delete [] headers;
    return tdiff;
}

void load_tester(int num_items) {
    std::map<uint64_t, int> times;
    for (int i = 0; i < 5; ++i)
        times.insert(std::make_pair(load_test(num_items, i), i));
    uint64_t prev = 0;
    for (auto it = times.begin(); it != times.end(); ++it) {
        switch(it->second) {
            case 0: printf("Vector"); break;
            case 1: printf("Set"); break;
            case 2: printf("AVL Tree"); break;
            case 3: printf("FDB Hash"); break;
            case 4: printf("STL set"); break;
        }
        if (prev) {
            printf(" %llu us %llu percent slower\n", it->first, (it->first - prev) * 100 / it->first);
        } else {
            printf(" %llu winner!\n", it->first);
            prev = it->first;
        }
    }
}

int main() {
    load_tester(4096);
    load_tester(40960);
    load_tester(819200);
    return 0;
    wal_basic_test();
    wal_ref_ptr_test();

    return 0;
}
