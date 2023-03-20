//
// Created by kieren jiang on 2023/3/20.
//

#include <limits.h>
#include <stdlib.h>
int string2ll(char *s, int len, long long *v) {

    unsigned long long value = 0, maxValue;
    int negative = 0;
    int j = 0;


    // fast fail
    if (len == 0) {
        return 0;
    }

    // check negative and ptr move forward
    if (s[0] == '-') {
        negative = 1;
        j++;
    }

    // c is the first and the last element, fast success
    if (s[j] == '0' && j == (len-1)) {
        *v = 0;
        return 1;
    }

    // first element should between [1,9], fast failed
    if (s[j] < '1' || s[j] > '9') {
        return 0;
    }

    maxValue = negative ? -(~LONG_LONG_MAX) : LONG_LONG_MAX;

    for (;j<len;j++) {

        if (s[j] < '0' || s[j] > '9') { // char out of range
            return 0;
        }

        if (value > maxValue/10) { // out of range
            return 0;
        }

        value = value * 10 + (s[j] - '0');
        if (value > maxValue) {
            return 0;
        }

    }


    if (negative) {
        if (v != NULL) *v = -(value);
    } else {
        if (v != NULL) *v = value;
    }

    return 1;
}

int string2l(char *s, int len, long *v) {
    long long value;

    if (!string2ll(s, len, &value)) {
        return 0;
    }

    if (value >= 0 && value <= LONG_MAX) {
        if (v) *v = value;
        return 1;
    }

    if (value < 0 && value <= LONG_MIN) {
        if (v) *v = value;
        return 1;
    }

    return 0;
}


