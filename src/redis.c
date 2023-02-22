//
// Created by kieren jiang on 2023/2/20.
//

#include "redis.h"
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
int main(int argc, char **argv) {
    printf("hello world!\r\n");

    int *c = malloc(sizeof(int) * 10);

    for (int i=0;i<10;i++) {
        c[i] = i;
        printf("num->c[%d]=%d\n", i, c[i]);
    }

    return -1;
}