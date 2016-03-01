//
// a simple test for hashdb 
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hashdb.h"

uint32_t sax_hash(const char *key)
{
	uint32_t h = 0;
	while(*key) h ^= (h<<5)+(h>>2) + (unsigned char)*key++;
	return h;
}

uint32_t sdbm_hash(const char *key)
{
	uint32_t h = 0;
	while(*key) h = (unsigned char)*key++ + (h<<6) + (h<<16) - h;
	return h;
}

float time_fly(struct timeval tstart, struct timeval tend)
{
	float tf;

	tf = (tend.tv_sec - tstart.tv_sec) * 1000000 + (tend.tv_usec - tstart.tv_usec);
	tf /= 1000000;

	return tf;
}

//gettimeofday(&ststart, NULL);
//gettimeofday(&stend, NULL);
void SET_TEST(HashDB* db, char* key, char* value)
{
    
    int i;
    int max = 10000;
    for (i = 0; i < max; i++) {
        sprintf(key, "%d", i);
        sprintf(value, "%d", i);
        if (-1 == hashdb_set(db, key, value, strlen(value))) {
            printf("hashdb_set failed\n");
            exit(-1);
        }
        if (true) {
            printf("set %s value = %s\n", key, value);
        }
    }
}

int main(int argc,char* argv[])
{
    char key[HASHDB_KEY_MAX_SZ] = {0};
    char value[HASHDB_VALUE_MAX_SZ] = {0};
    char* dbname = "toddler";
    uint32_t max = 0;
    HashDB *db  = NULL;
    struct timeval ststart = {0}, stend = {0};
    struct timeval gtstart= {0}, gtend = {0};
	
    db = hashdb_new(HASHDB_DEFAULT_TNUM, HASHDB_DEFAULT_BNUM, HASHDB_DEFAULT_CNUM, sax_hash, sdbm_hash);
    if (!db) {
        printf("hashdb_new failed!\n");
        exit(-1);
    }

    if (-1 == hashdb_open(db, dbname)) {
        printf("hashdb_open failed!\n");
        exit(-1);
    }

	//add the Time consumption
    SET_TEST(db,key,value);

    return 0;
}
