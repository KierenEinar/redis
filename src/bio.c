//
// Created by kieren jiang on 2023/5/9.
//

#include "bio.h"
#include "server.h"

static pthread_mutex_t thread_mutex[BIO_NUMS_OPS];
static pthread_cond_t thread_newjob_cond[BIO_NUMS_OPS];
static pthread_t threads[BIO_NUMS_OPS];
static list *jobs[BIO_NUMS_OPS];
static unsigned long bio_pending[BIO_NUMS_OPS];

void bioInit(void) {

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    size_t stack_size;
    for (int i=0; i<BIO_NUMS_OPS; i++) {
        pthread_mutex_init(&thread_mutex[i], NULL);
        pthread_cond_init(&thread_newjob_cond[i], NULL);
        jobs[i] = listCreate();
    }

    pthread_attr_getstacksize(&attr, &stack_size);
    if (!stack_size) stack_size = 1;
    while (stack_size < REDIS_THREAD_STACK_SIZE) {
        stack_size = stack_size << 1;
    }
    if (pthread_attr_setstacksize(&attr, stack_size) != 0) {
        // todo server log set stack failed
    }

    for (int i=0; i<BIO_NUMS_OPS; i++) {
        void *argv = (long)i;
        pthread_create(&threads[i], &attr, bioProcessBackgroundJobs, argv);
    }


}

void bioProcessBackgroundJobs(void *argv) {

    listNode *ln;
    bio_job *job;


    long type = (long)argv;

    if (type >= BIO_NUMS_OPS) {
        // todo serverlog error
        return;
    }
    // todo server log thread self
    pthread_setcanceltype(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcancelstate(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    pthread_mutex_lock(&thread_mutex[type]);

    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    // block the alarm signal, make sure that only the main thread would the watchdog signal
    if (pthread_sigmask(SIG_BLOCK, &set, NULL) != 0) {
        // todo server error log
    }



    while (1) {

        if (listLength(&jobs[type]) == 0) {
            pthread_cond_wait(&thread_newjob_cond[type], &thread_mutex[type]);
            continue;
        }

        ln = listFirst(&jobs[type]);
        job = ln->value;
        pthread_mutex_unlock(&thread_mutex[type]);

        if (type == BIO_CLOSE_FILE) {
            int fd = (int)job->arg1;
            close(fd);
        } else if (type == BIO_AOF_FSYNC) {
            int fd = (int)job->arg1;
            fsync(fd);
        } else {

        }

        pthread_mutex_lock(&thread_mutex[type]);
        listDelNode(&jobs[type], ln);
        bio_pending[type]--;
    }


}