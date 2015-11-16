/*
*
*
*
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/times.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>
#include "hashdb.h"


/*malloc the hash db,and then assign the paraments*/
HASHDB* hashdb_new(uint64_t tnum,uin64_t bnum,uint64_t cnum,hash_func_t func)
{
    HASHDB* db = malloc(HASHDB_SIZE);

    /*check if the db create error*/
    if(!db)
    {
        return NULL;
    }

    /*assign the factors of the db*/
    db->header.total_num = tnum;

    db->hash_func = func;

    return db;
}


/*
*
*
*
*
*/



























