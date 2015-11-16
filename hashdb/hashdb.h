#ifndef __HASHDB__
#define __HASHDB__

#include <stdint.h>
#include "bloom.h"


#define HASHDB_KEY_MAX_SZ           256
#define HASHDB_VALUE_MAX_SZ         128
#define HASHDB_TNUM                 10000000        /**/
#define HASHDB_BNUM                 10000000        /*Bucket Nums*/
#define HASHDB_CNUM                 1000000         /**/


typedef struct hash_entry{
    uint8_t status ;                            /*if the entry be cached*/
    char*   key;
    char*   value;

    uint64_t off;                               /*first element's offset */
    uint64_t left;
    uint64_t right;

}ENTRY;

#define HASHDB_ENTRY_SIZE   sizeof(ENTRY)


//why not index by A[i]?
typedef struct hashdb_bucket{
    uint64_t entry_off;                         /*first entry's offset*/
}BUCKET;

#define HASH_BUCKET_SIZE    sizeof(BUCKET)

typedef struct hashdb_header{
    uint32_t cache_num;
    uint32_t bucket_num;
    uint32_t total_num;
    uint32_t bloom_bloom_filter;
    uint32_t bucket_off;
    uint32_t entry_off;
}HASHDB_HDR;

#define HASHDB_HDR_SIZE sizeof(HASHDB_HDR)


typedef uint32_t (hash_func_t*)(const char*);


typedef struct hashdb{
    uint32_t fd;                                /*uncached items be storaged in the disk*/
    char* hashdb_name;
    HASHDB_HDR header;
    BLOOM* bloom;                               /*blomm filter to check a item if exists*/
    BUCKET* buckets;                            /*store the offset of cacahed items*/
    ENTRY* entries_cache;                       /*hash entries(items) be cached*/
    hash_func_t hash_func;                      /*hash func for the db*/
}HASHDB;

#define HASHDB_SIZE   sizeof(HASHDB)

HASHDB* hashdb_new(uint64_t tnum,uint64_t bnum,uint64_t cnum,\
hash_func_t hash_func);


/**/
HASHDB* hashdb_open(HASHDB* hashdb ,char* db_path);

/**/
int hashdb_set(HASHDB* hashdb,char* key, char* value,int value_size);

/**/
int hashdb_get(HASHDB* hashdb,char* key,char* value,int* value_size);

/**/
int hashdb_close(HASHDB* hashdb,int flash);

/**/
int hashdb_unlink(HASHDB* hashdb);



#endif // __HASHDB__



