//
// Created by cl-k on 6/1/15.
//
//Thanks to Ã«∞ËœÃ”„&&skeeto/gmail-bloom-filter
#include <limits.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include "bloom.h"


#define SETBIT(a, n) (a[n/CHAR_BIT] |= (1<<(n%CHAR_BIT)))
#define GETBIT(a, n) (a[n/CHAR_BIT] & (1<<(n%CHAR_BIT)))

Bloom *bloom_create(size_t size)
{
    Bloom *bloom;

    if(!(bloom = (Bloom *)malloc(sizeof(Bloom)))) {
        return NULL;
    }

    if(!(bloom->a = (unsigned char*)calloc((size + CHAR_BIT-1)/CHAR_BIT, sizeof(char)))) {
        free(bloom);
        return NULL;
    }
    bloom->asize = size;

    return bloom;
}

int bloom_destroy(Bloom *bloom)
{
    free(bloom->a);
    free(bloom);

    return 0;
}

int bloom_setbit(Bloom *bloom, int n, ...)
{
    va_list l;
    uint32_t pos;
    int i;

    va_start(l, n);
    for (i = 0; i < n; i++) {
        pos = va_arg(l, uint32_t);
        SETBIT(bloom->a, pos % bloom->asize);
    }
    va_end(l);

    return 0;
}

int bloom_check(Bloom *bloom, int n, ...)
{
    va_list l;
    uint32_t pos;
    int i;

    va_start(l, n);
    for (i = 0; i < n; i++) {
        pos = va_arg(l, uint32_t);
        if(!(GETBIT(bloom->a, pos % bloom->asize))) {
            return 0;
        }
    }
    va_end(l);

    return 1;
}
