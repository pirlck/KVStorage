//
// Created by cl-k on 6/13/15.
//

#ifndef HASHDB_HASHDB_H
#define HASHDB_HASHDB_H

#include <stdint.h>
#include "bloom.h"

#define HASHDB_KEY_MAX_SZ	256
#define HASHDB_VALUE_MAX_SZ	128
#define HASHDB_DEFAULT_TNUM	10000000
#define HASHDB_DEFAULT_BNUM	10000000
#define HASHDB_DEFAULT_CNUM	1000000

typedef struct hash_entry {
    uint8_t cached;		/* cached or not */
    char *key;			/* key of <key, value> */
    void *value;		/* value of <key, value> */
    uint32_t ksize;		/* size of the key */
    uint32_t vsize;		/* size of the value */
    uint32_t tsize;		/* total size of the entry */
    uint32_t hash;		/* second hash value */
    uint64_t off;		/* offset of the entry */
    uint64_t left;		/* offset of the left child */
    uint64_t right;		/* offset of the right child */
} HASH_ENTRY;
#define HASH_ENTRY_SZ sizeof(HASH_ENTRY)

typedef struct hash_bucket {
    uint64_t off;		/* offset of the first entry in the bucket */
} HASH_BUCKET;
#define HASH_BUCKET_SZ sizeof(HASH_BUCKET)

typedef struct hashdb_header{
    uint32_t magic;		/* magic number */
    uint32_t cnum;		/* number of cache items */
    uint32_t bnum;		/* number of hash buckets */
    uint64_t tnum;		/* number of total items */
    uint64_t boff;		/* offset of bloom filter */
    uint64_t hoff;		/* offset of hash buckets */
    uint64_t voff;		/* offset of hash values */
} HASHDB_HDR;
#define HASHDB_HDR_SZ sizeof(HASHDB_HDR)
#define HASHDB_MAGIC 20091209

typedef uint32_t (*hashfunc_t)(const char *);
typedef struct hashdb
{
    char *dbname;		/* hashdb filename */
    int fd;			/* hashdb fd */
    HASHDB_HDR header;	/* hashdb header */
    Bloom *bloom;		/* bloom filter */
    HASH_BUCKET *bucket;	/* hash buckets */
    HASH_ENTRY *cache;	/* hash item cache */
    hashfunc_t hash_func1;	/* hash function for hash bucket */
    hashfunc_t hash_func2;	/* hash function for btree in the hash bucket */
} HASHDB;
#define HASHDB_SZ  sizeof(HASHDB)

HASHDB *hashdb_new(uint64_t tnum, uint32_t bnum, uint32_t cnum, \
	hashfunc_t hash_func1, hashfunc_t hash_func2);
int hashdb_open(HASHDB *db, const char *path);
int hashdb_close(HASHDB *db, int flash);
int hashdb_set(HASHDB *db, char *key, void *value, int vsize);
int hashdb_get(HASHDB *db, char *key, void *value, int *vsize);
int hashdb_unlink(HASHDB *db);


#endif //HASHDB_HASHDB_H
