//
// Created by kieren jiang on 2023/5/7.
//

#include "utils.h"
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
int string2ll(const char *s, size_t slen, long long *value) {

    const char *p = s;
    size_t pidx = 0;
    unsigned char negative = 0;
    unsigned long long v = 0;
    if (pidx == slen) {
        return 0;
    }

//    if (p[0] == '0' && slen == 1) {
//        *value = 0;
//        return 1;
//    }

    if (p[0] == '-') {
        p++;
        pidx++;
        negative = 1;
        if (pidx == slen) {
            return 0;
        }
    }

    // check first digest, must be [1, 9]
    if (p[0] > '0' && p[0] <= '9') {
        v = p[0] - '0';
        p++;
        pidx++;
    } else if (p[0] == '0' && pidx == slen - 1) { // -0 or 0
        *value = 0;
        return 1;
    } else {
        return 0;
    }

    while (pidx < slen && p[0] >= '0' && p[0] <= '9') {

        if (v > ULLONG_MAX / 10) { // overflow
            return 0;
        }

        v *= 10;

        if (v > (ULLONG_MAX - ( p[0] - '0' )) ) { // overflow
            return 0;
        }

        v += (p[0] - '0');

        p++;
        pidx++;
    }

    if (pidx != slen) {
        return 0;
    }

    if (!negative) {
        if (v > LLONG_MAX) {
            return 0;
        }
        *value = v;
    } else {
        if (v > (unsigned long long)(LLONG_MAX) + 1L) {
            return 0;
        }
        *value = -v;
    }

    return 1;

}

int string2l(const char *s, size_t slen, long *value) {

    long long v;
    if (!string2ll(s, slen, &v)) {
        return 0;
    }
    if (v > 0) {
        if (v > LONG_MAX) {
            return 0;
        }
        *value = v;
    } else {
        if (v < LONG_MIN) {
            return 0;
        }
        *value = v;
    }

    return 1;
}

int ll2string(char *s, size_t slen, long long value) {

    unsigned char negative = value > 0 ? 0 : 1;
    unsigned long long v = negative ? -value : value;

    char p[LLMAXSIZE];
    size_t vlen = 0;

    char *ss = s;

    while (v > 0) {
        p[vlen] = '0' + v % 10;
        v /= 10;
        vlen++;
        if (vlen > slen) {
            return 0;
        }
    }

    if (negative) {
        p[vlen] = '-';
        vlen++;
        if (vlen > slen) {
            return 0;
        }
    }

    // check if s contains '\0'
    if (vlen+1 > slen) {
        return 0;
    }

    // reverse string

    for (int i=vlen-1; i>=0; i--) {
        *ss = p[i];
        ss++;
    }
    *ss = '\0';
    return vlen;
}

#ifdef RUN_UT
int main(int argc, char **argv) {

    char *s[] = {
        "-0",
        "0",
        "-1",
        "-9223372036854775808",
        "-9223372036854775809",
        "9223372036854775807",
        "9223372036854775808",
        "187777",
        "1987",
        "1",
        "abc",
        "1ab",
        "0a",
        "-0a",
        "-11h"
    };

    printf("sizeof(char*)=%lu\n", sizeof(char *));
    printf("sizeof(int*)=%lu\n", sizeof(int *));
    printf("sizeof(s)=%lu\n", sizeof(s) / sizeof(char *));

    for (int i=0; i< sizeof(s)/sizeof(char *); i++) {
        char *ss = s[i];
        long long v = 0;
        int ok = string2ll(ss, strlen(ss), &v);
        printf("string2ll, %s to %lld, ok=%d\n", ss, v, ok);
    }

    printf("finished\n");

    long long v[] = {

        LONG_LONG_MAX,
        LONG_LONG_MIN,
            0,
            -1,
            1,
            19887,
            981
    };

    char p[LLMAXSIZE];

    for (int i=0; i< sizeof(v)/ sizeof(long long); i++) {
        int vlen = ll2string(p, LLMAXSIZE, v[i]);
        printf("ll2string, %lld to %s, vlen=%d\n",v[i], p, vlen);
    }

    return 0;
}

#endif

