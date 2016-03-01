//
// Created by cl-k on 6/1/15.
//

#ifndef HASHDB_BLOOM_H
#define HASHDB_BLOOM_H

#include <stdlib.h>
#include <stdint.h>

typedef struct {
    size_t asize;
    unsigned char *a;
} Bloom;

Bloom *bloom_create(size_t size);

int bloom_destroy(Bloom *bloom);

int bloom_setbit(Bloom *bloom, int n, ...);

int bloom_check(Bloom *bloom, int n, ...);



#endif //HASHDB_BLOOM_H
