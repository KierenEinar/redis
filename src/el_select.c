//
// Created by kieren jiang on 2023/5/16.
//

#include <sys/select.h>
#include <memory.h>

typedef struct elApiState {

    fd_set rfds;
    fd_set wfds;

    fd_set _rfds;
    fd_set _wfds;
}elApiState;

static int elApiCreate(eventLoop *el) {

    elApiState *state = malloc(sizeof(*state));
    if (!state) return EL_ERR;
    FD_ZERO(&state->rfds);
    FD_ZERO(&state->wfds);
    el->apiData = state;
    return 0;
}

static int elApiAddEvent(eventLoop *el, int fd, int mask) {

    elApiState *state = el->apiData;
    if (mask & EL_READABLE) FD_SET(fd, &state->rfds);
    if (mask & EL_WRITABLE) FD_SET(fd, &state->wfds);
    return 0;
}

static int elApiDeleteEvent(eventLoop *el, int fd, int mask) {

    elApiState *state = el->apiData;
    if (mask & EL_READABLE) FD_CLR(fd, &state->rfds);
    if (mask & EL_WRITABLE) FD_CLR(fd, &state->wfds);
    return 0;
}

static void elApiFree(eventLoop *el) {
    elApiState *state = el->apiData;
    free(state);
}

static int elApiPoll(eventLoop *el, struct timeval *tvp) {

    elApiState *state = el->apiData;
    memcpy(&state->_rfds, &state->rfds, sizeof(fd_set));
    memcpy(&state->_wfds, &state->wfds, sizeof(fd_set));

    int retnums = select(el->maxfd+1, &state->_rfds, &state->_wfds, NULL, tvp);

    if (retnums) {
        int ix = 0;
        for (int j=0; j<el->maxfd+1; j++) {
            fileEvent *fe = &el->events[j];
            if (fe->mask == EL_NONE) continue;
            int mask = EL_NONE;
            if (fe->mask & EL_READABLE && FD_ISSET(j, &state->_rfds)) mask |= EL_READABLE;
            if (fe->mask & EL_WRITABLE && FD_ISSET(j, &state->_wfds)) mask |= EL_WRITABLE;
            if (mask != EL_NONE) {
                el->fired[ix].mask = mask;
                el->fired[ix].fd = j;
                ix++;
            }
        }
    }

    return retnums;

}