//
// Created by kieren jiang on 2023/5/15.
//

#include "el.h"
#include <poll.h>
#include "el_select.c"

void elGetTime(long *when_sec, long *when_ms) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    *when_sec = tv.tv_sec;
    *when_ms = tv.tv_usec / 1000;
}

void addMilliSecondsToNow(long long milliseconds, long *sec, long *ms) {

    long cur_sec, cur_ms, when_sec, when_ms;
    elGetTime(&cur_sec, &cur_ms);

    when_sec = cur_sec + milliseconds /1000;
    when_ms = cur_ms + milliseconds % 1000;
    if (when_ms > 1000) {
        when_sec+=1;
        when_ms-=1000;
    }

    *sec = when_sec;
    *ms = when_ms;
}


timerEvent *elSearchNearestTimer(eventLoop *el) {

    timerEvent *te = el->timerHead;
    timerEvent *shortest = NULL;

    while (te) {
        if (shortest == NULL || (te->sec < shortest->sec ||
            (te->sec == shortest->sec && te->ms < shortest->ms) && te->id > EL_TIMER_DELETED)) {
            shortest = te;
        }

        te = te->next;
    }

    return shortest;

}

eventLoop* elCreateEventLoop(int setsize) {

    eventLoop *el = malloc(sizeof(*el));
    if (el == NULL) return NULL;

    el->events = malloc(sizeof(fileEvent) * setsize);
    el->fired = malloc(sizeof(firedEvent) * setsize);

    if (el->events == NULL || el->fired == NULL) goto err;

    el->stop = 0;
    el->maxfd = -1;
    el->setsize = setsize;
    el->lastTime = time(NULL);
    el->nextTimerEventId = 1;
    el->timerHead = NULL;

    if (elApiCreate(el) == -1)
        goto err;

    for (int i=0; i<setsize; i++) {
        el->events[i].mask = EL_NONE;
    }

    el->beforeSleepProc = NULL;
    el->afterSleepProc = NULL;

    return el;

err:
    if (el->events) free(el->events);
    if (el->fired) free(el->fired);
    free(el);
    return NULL;
}

void elDeleteEventLoop(eventLoop* el) {
    elApiFree(el);
    free(el->events);
    free(el->fired);
    free(el);
}

int elCreateFileEvent(eventLoop* el, int fd, int mask, fileProc proc, void *clientData) {

    if (fd >= el->setsize) {
        errno = ERANGE;
        return EL_ERR;
    }

    if (elApiAddEvent(el, fd, mask) == -1) {
        return EL_ERR;
    }

    fileEvent *event = &el->events[fd];
    event->mask |= mask;
    if (mask & EL_READABLE) event->rfileProc = proc;
    if (mask & EL_WRITABLE) event->wfileProc = proc;
    event->clientData = clientData;

    if (fd > el->maxfd) {
        el->maxfd = fd;
    }

    return EL_OK;

}

int elDeleteFileEvent(eventLoop* el, int fd, int mask) {

    if (fd >= el->setsize) {
        errno = ERANGE;
        return EL_ERR;
    }

    fileEvent *event = &el->events[fd];

    if (mask & EL_WRITABLE) mask |= EL_BARRIER;

    event->mask &= ~mask;
    if (elApiDeleteEvent(el, fd, mask) == -1) {
        return EL_ERR;
    }

    if (el->maxfd == fd && event->mask == EL_NONE) {

        int j;
        for (j = el->maxfd - 1; j>=0; j--) {
            if (el->events[j].mask != EL_NONE) {
                el->maxfd = j;
                break;
            }
        }
    }


    return EL_OK;

}

int elGetFileEvent(eventLoop* el, int fd, int mask) {

    if (el->setsize <= fd) return -1;
    if (el->events[fd].mask & mask) return 1;
    return 0;
}

long long elCreateTimerEvent(eventLoop* el, long long ms, timerProc proc, timerFinalizeProc finalizeProc, void *clientData) {

   timerEvent *timer = malloc(sizeof(*timer));
   timer->clientData = clientData;
   timer->timerProc = proc;
   long when_sec, when_ms;
   addMilliSecondsToNow(ms, &when_sec, &when_ms);
   timer->id = el->nextTimerEventId++;
   timer->next = el->timerHead;
   if (el->timerHead != NULL) {
       el->timerHead->prev = timer;
   }
   timer->prev = NULL;
   timer->sec = when_sec;
   timer->ms = when_ms;
   timer->timerFinalizeProc = finalizeProc;
   el->timerHead = timer;
   return timer->id;
}


int elDeleteTimerEvent(eventLoop* el, long long id) {

    timerEvent *timer = el->timerHead;

    while (timer) {
        if (timer->id == id) {
            timer->id = EL_TIMER_DELETED;
            return EL_OK;
        }
        timer = timer->next;
    }

    return EL_ERR;
}

static int elProcessTimerEvents(eventLoop *el) {

    long nowSec, nowMs;

    int processed = 0;

    timerEvent *te = el->timerHead;
    time_t nowSecs = time(NULL);

    // check if the lasttime been set the feature clock, we choose to fire all
    if (nowSecs < el->lastTime)
        while (te) {
            te->sec = 0;
            te->ms = 0;
            te = te->next;
        }

    el->lastTime = time(NULL);

    long long maxEventId = el->nextTimerEventId;

    while (te) {

        timerEvent *next = te->next;

        if (te->id == EL_TIMER_DELETED) { // unlink if has been deleted

            if (te->prev) {
                te->prev->next = te->next;
            } else {
                el->timerHead = te->next;
            }

            if (te->next) {
                te->next->prev = te->prev;
            }

            te->timerFinalizeProc(el, te->clientData);
            free(te);
            te = next;
            continue;
        }

        if (te->id > maxEventId) {
            te = next;
            continue;
        }

        elGetTime(&nowSec, &nowMs);

        if (te->sec > nowSec || (te->ms == nowMs && te->ms > nowMs)) {
            te = next;
            continue;
        }

        long long retval = te->timerProc(el, te->id, te->clientData);
        if (retval) {
            addMilliSecondsToNow(retval, &te->sec, &te->ms);
        } else {
            te->id = EL_TIMER_DELETED;
        }

        te = next;
        processed++;
    }


    return processed;

}


int elProcessEvents(eventLoop *el) {

    timerEvent *nearest = elSearchNearestTimer(el);
    struct timeval tv;
    struct timeval *tvp = NULL;

    if (nearest) {
        long now_sec, now_ms;
        elGetTime(&now_sec, &now_ms);

        long long ms = nearest->sec * 1000 + nearest->ms - (now_sec * 1000 + now_ms);

        if (ms > 0) {
            tv.tv_sec = ms / 1000;
            tv.tv_usec = (ms % 1000) * 1000;
        } else {
            tv.tv_sec = 0;
            tv.tv_usec = 0;
        }
        tvp = &tv;
    }

    long nums = elApiPoll(el, tvp);

    if (el->afterSleepProc)
        el->afterSleepProc(el);

    int j;
    firedEvent *fired;
    fileEvent *event;
    int processed = 0;
    for (j=0; j<nums; j++) {

        fired = &el->fired[j];
        event = &el->events[fired->fd];
        int proc = 0;
        int invert = event->mask & EL_BARRIER;
        if (!invert && fired->mask & event->mask & EL_READABLE) {
            event->rfileProc(el, fired->fd, EL_READABLE, event->clientData);
            proc++;
        }


        if (fired->mask & event->mask & EL_WRITABLE) {
            if (!proc || event->rfileProc != event->wfileProc) {
                event->wfileProc(el, fired->fd, EL_READABLE, event->clientData);
                proc++;
            }
        }

        if (invert && fired->mask & event->mask & EL_READABLE ) {
            if (!proc || event->rfileProc != event->wfileProc) {
                event->rfileProc(el, fired->fd, EL_READABLE, event->clientData);
                proc++;
            }
        }

        processed++;
    }

    processed+= elProcessTimerEvents(el);

    return processed;
}

void elStop(eventLoop *el, int stop) {
    el->stop = stop;
}

void elMain(eventLoop *el) {
    while (!el->stop) {
        elProcessEvents(el);
    }

    elDeleteEventLoop(el);
}

void elSetBeforeSleepProc(eventLoop *el, beforeSleepProc proc) {
    el->beforeSleepProc = proc;
}

void elSetAfterSleepProc(eventLoop *el, beforeSleepProc proc) {
    el->afterSleepProc = proc;
}

int elWait(int fd, int mask, long long milliseconds) {

    struct pollfd pollfd;
    int retmask = 0, retval;
    memset(&pollfd, 0, sizeof(pollfd));
    pollfd.fd = fd;
    if (mask & EL_READABLE) pollfd.events |= POLLIN;
    if (mask & EL_WRITABLE) pollfd.events |= POLLOUT;
    if ((retval = poll(&pollfd, 1, milliseconds)) > 0) {
        if (pollfd.revents & POLLIN)  retmask |= EL_READABLE;
        if (pollfd.revents & POLLOUT) retmask |= EL_WRITABLE;
        if (pollfd.revents & POLLHUP) retmask |= EL_WRITABLE;
        return retmask;
    }
    return retval;
}


