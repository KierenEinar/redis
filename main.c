#include <stdio.h>
#include <stdlib.h>
#include "zskiplist.h"

int main() {
    printf("Hello, World!\n");
    printf("%d\n", rand());
    printf("%d\n", rand());
    printf("%d\n", rand());
    printf("%d\n", rand());
    printf("%d\n", RAND_MAX);

    zskiplistNode n;

    printf("%d byte\n", sizeof(n));
    printf("%d byte\n", sizeof(&n));
    printf("%d byte\n", sizeof(struct zskiplistNode));
    printf("%d byte\n", sizeof(int64_t));
    return 0;
}
