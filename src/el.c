//
// Created by kieren jiang on 2023/3/30.
//

#include "el.h"


eventLoop* elCreateEventLoop(int setsize) {
    eventLoop *el;
    if (el = zmalloc(sizeof(*el))==NULL) goto err;


    el->events = zcalloc(sizeof(fileEvent), setsize);
    el->fires = zcalloc(sizeof(fireEvent), setsize);
    timeEvent *timeHead = zmalloc(sizeof(*timeHead));
    el->setsize = setsize;
    el->timerHead = timeHead;
    el->lasttime = time(NULL);
    el->maxfd = -1;
    el->stop = 0;
    elApiCreate(el);

    for (int j=0; j<setsize; j++) {
        el->events[j].mask = EL_NONE;
    }
    return el;

err:
    if (el) {
        zfree(el->events);
        zfree(el->fires);
        zfree(el);
    }
    return NULL;
}

int elAddFileEvent(eventLoop *el, int fd, int mask, fileProc proc, void *clientData) {

    if (fd >= el->setsize) {
        errno = ERANGE;
        return EL_ERR;
    }
    int res = elApiAddEvent(el, fd, mask);
    if (res == EL_ERR)
        return EL_ERR;

    fileEvent *fe = &el->events[fd];

    if (mask & EL_READABLE) fe->rFileProc = proc;
    if (mask & EL_WRITABLE) fe->wFileProc = proc;
    fe->mask |= mask;
    fe->clientData = clientData;
    if (fd > el->maxfd) el->maxfd = fd;
    return EL_OK;
}

void elDeleteFileEvent(eventLoop *el, int fd, int mask) {

    if (fd >= el->setsize)
        return;

    if (el->events[fd].mask == EL_NONE)
        return;

    if (!elApiDelEvent(el, fd, mask));
        return;

    fileEvent *fe = &el->events[fd];

    fe->mask = fe->mask & ~mask;

    if (fe->mask == EL_NONE) {
        for (int j=el->setsize-1; j>=0; j--) {
            if (el->events[j].mask != EL_NONE ) {
                el->maxfd = el->events[j].fd;
                break;
            }
        }
    }

}

void elGetNow(long *sec, long *milliseconds) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    *sec = tv.tv_sec;
    *milliseconds = tv.tv_usec / 1000;
}

void elAddMillisecondsToNow(long long milliseconds, long *when_sec, long *when_ms) {

    long now_sec, now_ms, sec, ms;
    elGetNow(now_sec, now_ms);

    sec = now_sec + milliseconds / 1000;
    ms = now_ms + milliseconds % 1000;
    if (ms >= 1000) {
        sec+=1;
        ms-=1000;
    }

    *when_sec = sec;
    *when_ms = ms;

}

timeEvent *elSearchNearestTimeEvent(eventLoop *el) {
    timeEvent *te = el->timerHead;
    timeEvent *nearest = NULL;

    while (te) {

        if (!nearest || nearest->when_sec > te->when_sec ||
            (nearest->when_sec == te->when_sec && nearest->when_ms > te->when_ms)) {
            nearest = te;
        }
        te = te->next;
    }

    return nearest;

}

int elAddTimeEvent(eventLoop *el, long long milliseconds, timeProc proc, finalizeTimeProc finalize, void *clientData) {

    timeEvent *te = zmalloc(sizeof(*te));
    te->id = ++el->nextEventId;
    te->clientData = clientData;
    te->timeProc = proc;
    elAddMillisecondsToNow(milliseconds, &te->when_sec, &te->when_ms);

    te->next = el->timerHead;
    te->finalize = finalize;
    el->timerHead->prev = te;
    el->timerHead = te;

    return te->id;

}

int elDeleteTimeEvent(eventLoop *el, int id) {

    if (id >= el->nextEventId)
        return EL_ERR;

    timeEvent *te = el->timerHead;

    while (te) {
        if (te->id == id) {
            te->id = EVENT_DELETION_ID;
            return EL_OK;
        }
        te = te->next;
    }
    return EL_ERR;
}

int elProcessEvents(eventLoop *el, int flags) {
    int processed;

    if (el->maxfd != -1 || (flags & TIME_EVENTS && !(flags & DONT_WAIT))) {

        timeEvent *shortest;

        struct timeval tv, *tvp;

        if (flags & TIME_EVENTS  && !(flags & DONT_WAIT)) {
            shortest = elSearchNearestTimeEvent(el);
        }

        if (shortest) {
            long sec, ms;
            elGetNow(&sec, &ms);

            long te_ms = (shortest->when_sec * 1000 +  shortest->when_ms);
            te_ms -= (sec * 1000 + ms);

            if (te_ms > 0) {
                tv.tv_sec = te_ms / 1000;
                tv.tv_usec = (te_ms % 1000) * 1000;
            } else {
                tv.tv_sec = 0;
                tv.tv_usec = 0;
            }

            tvp = &tv;

        } else {

            if (flags & DONT_WAIT) {
                tv.tv_sec = 0;
                tv.tv_usec = 0;
            }

            tvp = &tv;

        }

        long event_nums = elApiPoll(el, tv);

        for (long j=0; j<event_nums; j++) {
            int fired = 0;
            fireEvent f = el->fires[j];
            if (f.mask & EL_READABLE) {
                fileEvent e = el->events[f.fd];
                if (e.rFileProc != NULL) {
                    e.rFileProc(el, f.fd, f.mask, e.clientData);
                    fired++;
                }
            }

            if (!fired && f.mask & EL_WRITABLE) {
                fileEvent e = el->events[f.fd];
                if (e.wFileProc != NULL) {
                    e.wFileProc(el, f.fd, f.mask, e.clientData);
                    fired++;
                }
            }

            if (fired) processed++;

        }
    }

    if (flags & TIME_EVENTS) {
        processed += elProcessTimeEvents(el, flags);
    }

    return processed;
}

int elProcessTimeEvents(eventLoop *el, int flags) {

    long processed = 0;

    time_t now = time(NULL);

    long maxid = el->nextEventId;

    if (el->lasttime > now) {
        timeEvent *te = el->timerHead;
        while (te) {
            te->when_sec = 0;
            te->when_ms = 0;
            te = te->next;
        }
    }

    el->lasttime = now;

    // handle deletion
    timeEvent *te = el->timerHead;
    while (te) {

        if (te->id >= maxid) {
            te = te->next;
            continue;
        }

        timeEvent *next = te->next;
        if (te->id == EVENT_DELETION_ID) {
            if (te == el->timerHead) {
                el->timerHead = te->next;
            }
            if (te->prev) te->prev->next = next;
            if (next) next->prev = te->prev;
            if (te->finalize)
                te->finalize(el, te->id, te->clientData);
            zfree(te);
            te= next;
            continue;
        }


        long sec, ms;
        elGetNow(&sec, &ms);

        if (te->when_sec < sec || (te->when_sec == sec && (te->when_ms < ms))) {
            long retval = te->timeProc(el, te->id, te->clientData);
            if (retval != EL_NO_MORE) {
                elAddMillisecondsToNow(retval, &te->when_sec, &te->when_ms);
            } else {
                te->id = EVENT_DELETION_ID;
            }
            processed++;
        }

        te = next;
    }

    return processed;

}

void elMain(eventLoop *el, int flag) {

    el->stop = 0;
    while (!el->stop) {
        elProcessEvents(el, flag);
    }
}

void elWait(eventLoop *el, long long milliseconds) {




}