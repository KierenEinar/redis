//
// Created by kieren jiang on 2023/5/15.
//

#ifndef CMAKE_DEMO_EL_H
#define CMAKE_DEMO_EL_H

#include <time.h>
#include <sys/time.h>
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>

// define file event mask
#define EL_NONE 0
#define EL_READABLE 1
#define EL_WRITABLE 2
#define EL_BARRIER 4

// define time event
#define EL_TIMER_DELETED -1

// define ok and error
#define EL_OK 0
#define EL_ERR -1

struct eventLoop;

typedef void(fileProc) (struct eventLoop *el, int fd, int mask, void *clientData);
typedef long long (timerProc) (struct eventLoop *el, int id, void *clientData);
typedef void(timerFinalizeProc) (struct eventLoop *el, void *clientData);
typedef void(beforeSleepProc) (struct eventLoop *el);

typedef struct timerEvent {
    long long id;
    long sec;
    long ms;
    timerProc *timerProc;
    timerFinalizeProc *timerFinalizeProc;
    struct timerEvent *prev;
    struct timerEvent *next;
    void *clientData;
}timerEvent;

typedef struct fileEvent {
    int mask;
    fileProc *rfileProc;
    fileProc *wfileProc;
    void *clientData;
}fileEvent;

typedef struct firedEvent {
    int fd;
    int mask;
}firedEvent;

typedef struct eventLoop {
    unsigned long nextTimerEventId;
    timerEvent *timerHead;
    beforeSleepProc *beforeSleepProc;
    beforeSleepProc *afterSleepProc;
    time_t lastTime;
    int setsize;
    int maxfd;
    int stop;
    fileEvent  *events;
    firedEvent *fired;
    void *apiData;
}eventLoop;

eventLoop* elCreateEventLoop(int setsize);
void elDeleteEventLoop(eventLoop* el);
int elCreateFileEvent(eventLoop* el, int fd, int mask, fileProc proc, void *clientData);
int elDeleteFileEvent(eventLoop* el, int fd, int mask);
int elGetFileEvent(eventLoop* el, int fd, int mask);
long long elCreateTimerEvent(eventLoop* el, long long ms, timerProc proc, timerFinalizeProc finalizeProc, void *clientData);
int elDeleteTimerEvent(eventLoop* el, long long id);
int elProcessEvents(eventLoop *el);

void elMain(eventLoop *el);
int  elWait(int fd, int mask, long long milliseconds);

void elStop(eventLoop *el, int stop);
void elSetBeforeSleepProc(eventLoop *el, beforeSleepProc proc);
void elSetAfterSleepProc(eventLoop *el, beforeSleepProc proc);
#endif //CMAKE_DEMO_EL_H
