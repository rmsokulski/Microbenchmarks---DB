#ifndef _ORDERS_H_
#define _ORDERS_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
//#include "header.h"


#define buf_size 1024
typedef struct {
   int *orderkeys;
   int *custkeys;
    float *prices;
    int *priorities;
    char **statuses;
    char **dates;
    char **priorities_char;
    char **clerks;
    char **comments;
}__attribute__((aligned(64))) Table;

typedef struct {
   int *orderkeys;
   int *custkeys;
    float *prices;
   int size;
}__attribute__((aligned(64))) Orders_out;


#endif
