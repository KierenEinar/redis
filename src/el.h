//
// Created by kieren jiang on 2023/3/30.
//

#ifndef REDIS_EL_H
#define REDIS_EL_H

// writable or readable flag
#define EL_NONE     0
#define EL_READABLE 1
#define EL_WRITABLE 2

// file or time events
#define FILE_EVENTS 1
#define TIME_EVENTS 2
#define ALL_EVENTS (FILE_EVENTS | TIME_EVENTS)
#define DONT_WAIT 4

#define EVENT_DELETION_ID -1
#define EL_NO_MORE -1


// status
#define EL_OK 0
#define EL_ERR -1

#include <time.h>
#include "zmalloc.h"
#include <errno.h>
#include <sys/time.h>
struct eventLoop;

typedef void fileProc(struct eventLoop *eventLoop, int fd, int mask, void *clientData);
typedef long timeProc(struct eventLoop *eventLoop, int id, void *clientData);
typedef void finalizeTimeProc(struct eventLoop *eventLoop, int id, void *clientData);

typedef struct fileEvent {
    int fd;
    int mask;
    fileProc *wFileProc;
    fileProc *rFileProc;
    void *clientData;
} fileEvent;

typedef struct timeEvent {
    long long id;
    unsigned long when_sec;
    unsigned long long when_ms;
    timeProc *timeProc;
    finalizeTimeProc *finalize;
    void *clientData;
    struct timeEvent *prev;
    struct timeEvent *next;
} timeEvent;

typedef struct fireEvent {
    int fd;
    int mask;
}fireEvent;

typedef struct eventLoop {
    int maxfd;
    int setsize;
    fileEvent *events;
    fireEvent *fires;

    int nextEventId;
    timeEvent *timerHead;
    time_t lasttime;
    int stop;

    void *apiData;

} eventLoop;

eventLoop* elCreateEventLoop(int setsize);
int elAddFileEvent(eventLoop *el, int fd, int mask, fileProc proc, void *clientData);
void elDeleteFileEvent(eventLoop *el, int fd, int mask);

int elAddTimeEvent(eventLoop *el, long long milliseconds, timeProc proc, finalizeTimeProc finalize,void *clientData);
int elDeleteTimeEvent(eventLoop *el, int id);

void elMain(eventLoop *el, int flag);
void elWait(eventLoop *el, long long milliseconds);
int elProcessEvents(eventLoop *el, int flags);
int elProcessTimeEvents(eventLoop *el, int flags);



#endif //REDIS_EL_H
