//
// Created by kieren jiang on 2023/5/7.
//

#include "utils.h"
#include <limits.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

typedef struct _cstring {
    char *s;
    size_t used;
    size_t free;
}_cstring;

_cstring *cstringnew(size_t cap) {
    _cstring *cstr;
    cstr = zmalloc(sizeof(*cstr));
    cstr->free = cap;
    cstr->s = zmalloc(sizeof(char) * (cap + 1));
    cstr->used = 0;
    return cstr;
}

void cstrfree(_cstring *cstr) {
    zfree(cstr->s);
    zfree(cstr);
}

_cstring *cstringcatstr(_cstring *cstr, const char *s, size_t len) {

    if (cstr->free < len) {
        size_t incr_cap = cstr->free + cstr->used;
        if (incr_cap < len) {
            incr_cap = len * 2;
        }
        cstr->s = zrealloc(cstr->s, cstr->free + cstr->used + incr_cap + 1);
        cstr->free += incr_cap;
    }

    memcpy(cstr->s + cstr->used, s, len);
    cstr->used+=len;
    cstr->free-=len;
    cstr->s[cstr->used] = '\0';
    return cstr;
}

char* cstringcopystr(_cstring *cstr) {
    char *s = zmalloc(cstr->used+1);
    memcpy(s, cstr->s, cstr->used+1);
    return s;
}

int is_hex_digit(char c) {
    return  (c >= '0' && c <= '9') ||
            (c >= 'A' && c <= 'Z') ||
            (c >= 'a' && c <= 'z') ? 1 : 0;
}

int hex_digit_to_int(char c) {
    switch (c) {
        case '0':
            return 0;
        case '1':
            return 1;
        case '2':
            return 2;
        case '3':
            return 3;
        case '4':
            return 4;
        case '5':
            return 5;
        case '6':
            return 6;
        case '7':
            return 7;
        case '8':
            return 8;
        case '9':
            return 9;
        case 'a':
        case 'A':
            return 10;
        case 'b':
        case 'B':
            return 11;
        case 'c':
        case 'C':
            return 12;
        case 'd':
        case 'D':
            return 13;
        case 'e':
        case 'E':
            return 14;
        case 'f':
        case 'F':
            return 15;
        default:
            return 0;
    }
}

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

size_t ll2string(char *s, size_t slen, long long value) {

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

// split a line into multi args
char** stringsplitargs(const char *line, int *argc) {

    const char *p = line;
    char **vector = NULL;
    _cstring *current = NULL;

    *argc = 0;
    while (1) {

        int done = 0;
        int inq = 0; // is in quote context
        int insq = 0; // is in single quote context

        if (*p && isspace(*p)) p++;

        if (*p) {

            if (current == NULL) {
                current = cstringnew(32);
            }

            while (!done) {
                if (inq) {

                    if (*p == '\\' && *(p+1) == 'x' && is_hex_digit(*(p+2)) && is_hex_digit(*(p+3))) {
                        unsigned char byte;
                        byte = hex_digit_to_int(*(p+2)) * 16
                               + hex_digit_to_int(*(p+3));

                        current = cstringcatstr(current, (char *)&byte, 1);
                        p+=3;
                    } else if (*p == '\\' && *(p+1)) {
                        unsigned char byte;
                        p++;
                        switch (*p) {
                            case 'r': byte = '\r'; break;
                            case 'n': byte = '\n'; break;
                            case 't': byte = '\t'; break;
                            case 'a': byte = '\a'; break;
                            case 'b': byte = '\b'; break;
                            default:
                                byte = *p;
                                break;
                        }
                        current = cstringcatstr(current, (char *)&byte, 1);
                    } else if (*p == '"') {
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done = 1;
                    } else if (!*p) {
                        goto err;
                    } else {
                        current = cstringcatstr(current, p, 1);
                    }

                } else if (insq) {
                    if (*p == '\\' && *(p+1) == '\'') {
                        current = cstringcatstr(current, "'", 1);
                        p++;
                    } else if (*p == '\'') {
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done = 1;
                    } else if (!*p) {
                        goto err;
                    } else {
                        current = cstringcatstr(current, p, 1);
                    }

                } else {

                    switch (*p) {
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                        case '\0':
                            done = 1;
                            break;
                        case '"':
                            inq = 1;
                            break;
                        case '\'':
                            insq = 1;
                            break;
                        default:
                            current = cstringcatstr(current, p, 1);
                            break;
                    }
                }

                if (*p) p++;

            }

            if (current->used > 0) {
                vector = realloc(vector, sizeof(char*) * ((*argc) + 1));
                vector[*argc] = cstringcopystr(current);
                (*argc)++;
            }

            cstrfree(current);
            current = NULL;

        } else {
            if (vector == NULL) return zmalloc(sizeof(void*));
            return vector;
        }
    }

err:
    if (current != NULL) {
        cstrfree(current);
        current = NULL;
    }

    for (int i=0; i<*argc; i++) {
        zfree(vector[i]);
    }
    zfree(vector);
    return NULL;
}

unsigned long u_rev(unsigned long value) {

    int t = sizeof(value) * 8;
    unsigned long m = ~0UL;
    while (t>>=1>0) {
        // e.g. 16bits
        //  11111111 11111111
        //  00000000 11111111   xor <<8
        //  00001111 00001111   xor <<4
        //  00110011 00110011   xor <<2
        //  01010101 01010101   xor <<1
        m^=(m<<t);
        value = ((value >> t) & m) | ((value << t) & ~m);
    }
    return value;
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

