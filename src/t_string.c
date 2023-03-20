//
// Created by kieren jiang on 2023/3/17.
//
#include "server.h"

#define SET_NO_FLAGS 1 << 0
#define SET_NX 1 << 1
#define SET_XX 1 << 2
#define SET_EX 1 << 3
#define SET_PX 1 << 4

#define UNIT_SECONDS 0
#define UNIT_MICROSECONDS 1

int setCommand(client *c) {

    robj *expire = NULL;
    int flags = SET_NO_FLAGS;
    int unit = -1;
    for (int j=3;j<c->argc;j++) {

       char *a = c->argv[j]->ptr;
       robj *next = (j == c->argc - 1) ? NULL : c->argv[j+1];

       if ((a[0] == 'n' || a[0] == 'N') &&
                (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && !(flags & SET_XX) ) {
           flags |= SET_NX;
       } else if ((a[0] == 'x' || a[0] == 'X') &&
                (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && !(flags & SET_NX) ) {
           flags |= SET_XX;
       } else if ((a[0] == 'e' || a[0] == 'E') &&
                  (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && !(flags & SET_PX) ) {
           flags |= SET_EX;
           expire = next;
           unit = UNIT_SECONDS;
           j++;
       } else if ((a[0] == 'p' || a[0] == 'P') &&
                  (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && !(flags & SET_EX) ) {
           flags |= SET_PX;
           expire = next;
           unit = UNIT_MICROSECONDS;
           j++;
       }
    }

    c->argv[2] = tryObjectEncoding(c->argv[2]);

    setGenericCommand(c, c->argv[1], c->argv[2], expire, unit, flags);

}

