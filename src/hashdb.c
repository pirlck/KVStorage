//
//
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>
#include "hashdb.h"

int hashdb_swapout(HashDB *db, uint32_t hash1, uint32_t hash2, HASH_ENTRY *he);
int hashdb_swapin(HashDB *db, char *key, uint32_t hash1, uint32_t hash2, HASH_ENTRY *he);

HashDB* hashdb_new(uint64_t tnum, uint32_t bnum, uint32_t cnum, \
	hashfunc_t hash_func1, hashfunc_t hash_func2)
{
    HashDB *db = NULL;

    if (!(db = (HashDB* )malloc(HASHDB_SZ))) {
        return NULL;
    }

    db->header.tnum = tnum;
    db->header.bnum = bnum;
    db->header.cnum = cnum;
    db->hash_func1 = hash_func1;
    db->hash_func2 = hash_func2;

    return db;
}

int hashdb_readahead(HashDB *db)
{
    uint32_t i, max, pos;
    char key[HASHDB_KEY_MAX_SZ] = {0};
    char value[HASHDB_VALUE_MAX_SZ] = {0};
    HASH_ENTRY he;
    ssize_t rsize, hesize;

    if (!db || !db->fd || !db->bloom || !db->bucket || !db->cache) {
        return -1;
    }

    hesize = HASH_ENTRY_SZ;
    max = (db->header.cnum < db->header.bnum) ? db->header.cnum : db->header.bnum;
    for(i = 0; i < max; i++) {
        if (-1 == lseek(db->fd, db->bucket[i].off, SEEK_SET)) {
            return -1;
        }

        rsize = hesize;
        if (rsize != read(db->fd, &he, rsize)) {
            return -1;
        }

        rsize = HASHDB_KEY_MAX_SZ;
        if (rsize != read(db->fd, key, rsize)) {
            return -1;
        }

        rsize = HASHDB_VALUE_MAX_SZ;
        if (rsize != read(db->fd, value, rsize)) {
            return -1;
        }

        pos = db->hash_func1(key) % db->header.cnum;
        memcpy(&db->cache[pos], &he, hesize);
        db->cache[pos].key = strdup(key);
        if (NULL == (db->cache[pos].value = malloc(he.vsize))) {
            return -1;
        }
        memcpy(db->cache[pos].value, value, he.vsize);
        db->cache[pos].cached = 1;
    }

    return 0;
}

int hashdb_open(HashDB *db, const char *path)
{
    int f_ok;
    uint64_t i;
    int rwsize;

    if (!db || !path) {
        return -1;
    }

    f_ok = access(path, F_OK);
    db->dbname = strdup(path);			//duplicat a pointer of string
    db->fd = open(path, O_RDWR|O_CREAT, 0666);
    if (-1 == db->fd) {
        free(db->dbname);
        return -1;
    }

    if (0 == f_ok) {/* existed hashdb */
        rwsize = HASHDB_HDR_SZ;
        if (rwsize != read(db->fd, &(db->header), rwsize)) {
            goto OUT;
        }
        if (db->header.magic != HASHDB_MAGIC) {
            printf("the file is not hashdb\n");
            goto OUT;
        }
    } else {/* new hashdb */
        db->header.magic = HASHDB_MAGIC;
        db->header.boff = HASHDB_HDR_SZ;
        db->header.hoff = db->header.boff + (db->header.tnum + CHAR_BIT -1)/CHAR_BIT;
        db->header.voff = db->header.hoff + (db->header.bnum * HASH_BUCKET_SZ);
    }

    /* initial cache  */
    db->bloom = bloom_create(db->header.tnum);
    if (!db->bloom) {
        goto OUT;
    }

    db->bucket = (HASH_BUCKET *)malloc(db->header.bnum * HASH_BUCKET_SZ);
    if (!db->bucket) {
        goto OUT;
    }
    for (i = 0; i < db->header.bnum; i++) {
        db->bucket[i].off = 0;
    }

    db->cache = (HASH_ENTRY *)malloc(db->header.cnum * HASH_ENTRY_SZ);
    if (!db->cache) {
        goto OUT;
    }
    for (i = 0; i < db->header.cnum; i++) {
        db->cache[i].cached = 0;
        db->cache[i].off = 0;
        db->cache[i].left = 0;
        db->cache[i].right = 0;
    }

    if (0 == f_ok) {
        /* if hashdb exists, then read data to fill up cache */
        if (-1 == lseek(db->fd, db->header.boff, SEEK_SET)) {
            goto OUT;
        }
        rwsize = db->header.hoff - db->header.boff;
        if (rwsize != read(db->fd, db->bloom->a, rwsize)) {
            goto OUT;
        }

        rwsize = db->header.voff - db->header.hoff;
        if (rwsize != read(db->fd, db->bucket, rwsize)) {
            goto OUT;
        }

        if (-1 == hashdb_readahead(db)) {
            goto OUT;
        }

    } else {
        /* prealloc space in the file */
        if (-1 == lseek(db->fd, 0, SEEK_SET)) {
            exit(-1);
        }
        rwsize = db->header.boff;
        if (rwsize != write(db->fd, &(db->header), rwsize)) {
            exit(-1);
        }
        rwsize = db->header.hoff - db->header.boff;
        if (rwsize != write(db->fd, db->bloom->a, rwsize)) {
            exit(-1);
        }
        rwsize = db->header.voff - db->header.hoff;
        if (rwsize != write(db->fd, db->bucket, rwsize)) {
            exit(-1);
        }
    }
    return 0;
/*
    if (db->dbname) {
        close(db->fd);
        unlink(db->dbname);
        free(db->dbname);
    }

    if (db->bloom) {
        bloom_destroy(db->bloom);
    }

    if (db->bucket) {
        free(db->bucket);
    }

    if (db->cache) {
        free(db->cache);
    }

    return -1;
*/
}

int hashdb_close(HashDB *db, int flash)
{
    uint64_t i;
    uint32_t hash1, hash2;
    ssize_t wsize;
    int ret = 0;

    if (!db || !db->bloom || !db->bucket || !db->cache) {
        return -1;
    }

    if (!flash) {
        exit(-1);
    }

    /* flush cached data to file */
    for (i = 0; i < db->header.cnum; i++) {
        if (db->cache[i].cached) {
            hash1 = db->hash_func1(db->cache[i].key);
            hash2 = db->cache[i].hash;
            if (-1 == hashdb_swapout(db, hash1, hash2, &db->cache[i])) {
                ret = -1;
                exit(-1);
            }
        }
    }

    if (-1 == lseek(db->fd, 0, SEEK_SET)) {
        ret = -1;
        eixt(-1);
    }
    wsize = HASHDB_HDR_SZ;
    if (wsize != write(db->fd, &db->header, wsize)) {
        ret = -1;
        exit(-1);
    }

    wsize = db->header.hoff - db->header.boff;
    if (wsize != write(db->fd, db->bloom->a, wsize)) {
        ret = -1;
        exit(-1);
    }

    wsize = db->header.voff - db->header.hoff;
    if (wsize != write(db->fd, db->bucket, wsize)) {
        ret = -1;
        eixt(-1);
    }

    /* destroy cache */
    close(db->fd);
    if (db->bloom) {
        bloom_destroy(db->bloom);
    }
    if (db->bucket) {
        free(db->bucket);
    }
    if (db->cache) {
        for (i = 0; i < db->header.cnum; i++) {
            if (db->cache[i].key) {
                free(db->cache[i].key);
            }
            if (db->cache[i].value) {
                free(db->cache[i].value);
            }
        }
        free(db->cache);
    }

    return ret;
}

int hashdb_swapout(HashDB *db, uint32_t hash1, uint32_t hash2, HASH_ENTRY *he)
{
    char key[HASHDB_KEY_MAX_SZ] = {0};
    char value[HASHDB_VALUE_MAX_SZ] = {0};
    uint64_t root;
    uint32_t pos;
    int cmp, hebuf_sz, lr = 0;
    void *hebuf = NULL;
    char *hkey = NULL;
    void *hvalue = NULL;
    HASH_ENTRY *hentry = NULL;
    HASH_ENTRY parent;
    ssize_t rwsize;


    if (!db) {
        return -1;
    }

    if (!he || !he->cached) {
        return 0;
    }

    /* find offset and parent of the hash entry */
    if (he->off == 0) {							//
        hebuf_sz = HASH_ENTRY_SZ + HASHDB_KEY_MAX_SZ + HASHDB_VALUE_MAX_SZ;
        if (NULL == (hebuf = (void *)malloc(hebuf_sz))) {
            return -1;
        }
        pos = hash1 % db->header.bnum;		
        root = db->bucket[pos].off;			
        parent.off = 0;						
        /* search entry with given key and hash in btree */
        while(root) {										//off_t lseek(int fd, off_t offset, int whence);
            if (-1 == lseek(db->fd, root, SEEK_SET)) {		//The offset is set to offset bytes
                free(hebuf);
                return -1;
            }
            memset(hebuf, 0, hebuf_sz);
            rwsize = read(db->fd, hebuf, hebuf_sz);
            if (rwsize != hebuf_sz) {
                free(hebuf);
                return -1;
            }

            hentry = (HASH_ENTRY *)hebuf;
            hkey = (char *)(hebuf + HASH_ENTRY_SZ);
            hvalue = (void *)(hebuf + HASH_ENTRY_SZ + HASHDB_KEY_MAX_SZ);
            memcpy(&parent, hebuf, HASH_ENTRY_SZ);
            if (hentry->hash > hash2) {
                root = hentry->left;			
                lr = 0;
            } else if (hentry->hash < hash2) {
                root = hentry->right;
                lr = 1;
            } else {
                cmp = strcmp(hkey, he->key);
                if (cmp > 0) {
                    root = hentry->left;
                    lr = 0;
                } else if (cmp < 0) {
                    root = hentry->right;
                    lr = 1;
                } else {
                  ;
                }
            }
        }

        if (hebuf) {
            free(hebuf);
        }
        /* append mode */
        if (-1 == (he->off = lseek(db->fd, 0, SEEK_END))) {
            return -1;
        }

        if (!db->bucket[pos].off) {
            db->bucket[pos].off = he->off;
        }

        /* make relationship with parent*/
        if (parent.off) {
            (lr == 0)? (parent.left = he->off): (parent.right = he->off);
            if (-1 == lseek(db->fd, parent.off, SEEK_SET)) {
                return -1;
            }
            rwsize = HASH_ENTRY_SZ;
            if (write(db->fd, &parent, rwsize) != rwsize) {
                return -1;
            }
        }
    }

    /* flush cached hash entry to file */
    if (-1 == lseek(db->fd, he->off, SEEK_SET)) {
        return -1;
    }

    rwsize = HASH_ENTRY_SZ;
    if (rwsize != write(db->fd, he, rwsize)) {
        return -1;
    }
    sprintf(key, "%s", he->key);
    if (HASHDB_KEY_MAX_SZ != write(db->fd, key, HASHDB_KEY_MAX_SZ)) {
        return -1;
    }
    memcpy(value, he->value, he->vsize);
    if (HASHDB_VALUE_MAX_SZ != write(db->fd, value, HASHDB_VALUE_MAX_SZ)) {
        return -1;
    }

    //
    if (he->key) {
        free(he->key);
        he->key = NULL;
    }
    if (he->value) {
        free(he->value);
        he->value = NULL;
    }
    he->off = 0;
    he->left = 0;
    he->right = 0;
    he->cached = 0;			

    return 0;
}


int hashdb_swapin(HashDB *db, char *key, uint32_t hash1, uint32_t hash2, HASH_ENTRY *he)
{
    uint32_t pos;
    uint64_t root;
    int cmp, hebuf_sz;
    void *hebuf = NULL;
    char *hkey = NULL;
    void *hvalue = NULL;
    HASH_ENTRY *hentry = NULL;
    int  rsize;

    if (!db || !key || he->cached) {
        return -1;
    }

    hebuf_sz = HASH_ENTRY_SZ + HASHDB_KEY_MAX_SZ + HASHDB_VALUE_MAX_SZ;
    if (NULL == (hebuf = (void *)malloc(hebuf_sz))) {
        return -1;
    }

    pos = hash1 % db->header.bnum;
    root = db->bucket[pos].off;
    /* search entry with given key and hash in btree */
    while (root) {
        if (-1 == lseek(db->fd, root, SEEK_SET)) {
            free(hebuf);
            return -1;
        }
        memset(hebuf, 0, hebuf_sz);
        rsize = read(db->fd, hebuf, hebuf_sz);
        if (rsize != hebuf_sz) {
            free(hebuf);
            return -1;
        }

        hentry = (HASH_ENTRY *)hebuf;
        hkey = (char *)(hebuf + HASH_ENTRY_SZ);
        hvalue = (void *)(hebuf + HASH_ENTRY_SZ + HASHDB_KEY_MAX_SZ);
        if (hentry->hash > hash2) {
            root = hentry->left;
        } else if (hentry->hash < hash2) {
            root = hentry->right;
        } else {
            cmp = strcmp(hkey, key);
            if (cmp == 0) { /* find the entry */
                memcpy(he, hebuf, HASH_ENTRY_SZ);
                he->key = strdup(hkey);
                if (NULL == (he->value = malloc(he->vsize))) {
                    return -1;
                }
                memcpy(he->value, hvalue, he->vsize);
                he->cached = 1;
                free(hebuf);
                return 0;
            } else if (cmp > 0) {
                root = hentry->left;
            } else {
                root = hentry->right;
            }
        }
    }

    if (hebuf) {
        free(hebuf);
    }
    return -1;
}

int hashdb_set(HashDB *db, char *key, void *value, int vsize)
{
    int pos;
    uint32_t hash1, hash2;
    uint32_t he_hash1, he_hash2;

    if (!db || !key || !value) {
        return -1;
    }

    hash1 = db->hash_func1(key);
    hash2 = db->hash_func2(key);
    /* cache swap in/out with set-associative */
    pos = hash1 % db->header.cnum;
    if ((db->cache[pos].cached) && ((hash2 != db->cache[pos].hash) || (strcmp(key, db->cache[pos].key) != 0))) {
        he_hash1 = db->hash_func1(db->cache[pos].key);
        he_hash2 = db->cache[pos].hash;
        if (-1 == hashdb_swapout(db, he_hash1, he_hash2, &db->cache[pos])) {
            return -1;
        }
    }

    if (!db->cache[pos].cached && (bloom_check(db->bloom, 2, hash1, hash2))) {
        if ( -1 == hashdb_swapin(db, key, hash1, hash2, &db->cache[pos])) {
            return -1;
        }
    }

    if ((strlen(key) > HASHDB_KEY_MAX_SZ) || (vsize > HASHDB_VALUE_MAX_SZ)) {
        return -1;
    }

    /* fill up cache hash entry */
    if (db->cache[pos].key) {
        free(db->cache[pos].key);
    }
    if (db->cache[pos].value) {
        free(db->cache[pos].value);
    }

    //strdup
    db->cache[pos].key = strdup(key);
    db->cache[pos].ksize = strlen(key);
    if (NULL == (db->cache[pos].value = malloc(vsize))) {
        return -1;
    }
    memcpy(db->cache[pos].value, value, vsize);
    db->cache[pos].vsize = vsize;

    db->cache[pos].tsize = HASH_ENTRY_SZ + HASHDB_KEY_MAX_SZ + HASHDB_VALUE_MAX_SZ;
    db->cache[pos].hash = hash2;
    if (!db->cache[pos].cached) {
        /* it's a new entry */
        db->cache[pos].off = 0;
        db->cache[pos].left = 0;
        db->cache[pos].right = 0;
        bloom_setbit(db->bloom, 2, hash1, hash2);
        db->cache[pos].cached = 1;
    }

    return 0;
}

//该函数搞定
int hashdb_get(HashDB *db, char *key, void *value, int *vsize)
{
    int pos, ret;
    uint32_t hash1, hash2;
    uint32_t he_hash1, he_hash2;

    if (!db || !key) {
        return -1;
    }

    hash1 = db->hash_func1(key);
    hash2 = db->hash_func2(key);
    /* check if the value is set */
    if (!bloom_check(db->bloom, 2, hash1, hash2)) {
        return -1;
    }

    //计算出缓存位置
    pos = hash1 % db->header.cnum;
    if ((db->cache[pos].cached) && ((hash2 != db->cache[pos].hash) || (strcmp(key, db->cache[pos].key) != 0))) {
        he_hash1 = db->hash_func1(db->cache[pos].key);
        he_hash2 = db->cache[pos].hash;
        if (-1 == hashdb_swapout(db, he_hash1, he_hash2, &db->cache[pos])) {
            return -1;
        }
    }

    //没有缓存的话，则从磁盘中换入
    if (!db->cache[pos].cached) {
        if (0 != (ret = hashdb_swapin(db, key, hash1, hash2, &db->cache[pos]))) {
            return ret;
        }
    }

    memcpy(value, db->cache[pos].value, db->cache[pos].vsize);
    *vsize = db->cache[pos].vsize;

    return 0;
}

int hashdb_unlink(HashDB *db)
{
    if (!db) {
        return -1;
    }

    if (db->dbname) {
        unlink(db->dbname);
        free(db->dbname);
    }

    return 0;
}

