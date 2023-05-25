//
// Created by kieren jiang on 2023/5/9.
//

#include "bio.h"
#include "server.h"

static pthread_mutex_t thread_mutex[BIO_NUMS_OPS];
static pthread_cond_t thread_newjob_cond[BIO_NUMS_OPS];
static pthread_cond_t thread_waitstep_cond[BIO_NUMS_OPS];
static pthread_t threads[BIO_NUMS_OPS];
static list *jobs[BIO_NUMS_OPS];
static unsigned long bio_pending[BIO_NUMS_OPS];

unsigned long bioPendingJobsOfType(int type) {
    unsigned long val;
    pthread_mutex_lock(&thread_mutex[type]);
    val = bio_pending[type];
    pthread_mutex_unlock(&thread_mutex[type]);
    return val;
}

void bioInit(void) {

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    size_t stack_size;
    for (int i=0; i<BIO_NUMS_OPS; i++) {
        pthread_mutex_init(&thread_mutex[i], NULL);
        pthread_cond_init(&thread_newjob_cond[i], NULL);
        pthread_cond_init(&thread_waitstep_cond[i], NULL);
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
    // block the alarm signal, make sure that only the main thread would receive the watchdog signal
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

        zfree(job);
        pthread_mutex_lock(&thread_mutex[type]);
        listDelNode(&jobs[type], ln);
        bio_pending[type]--;
        pthread_cond_broadcast(&thread_waitstep_cond[type]);
    }


}

void bioCreateBackgroundJob(int type, void *arg1, void *arg2, void *arg3) {

    bio_job *job = zmalloc(sizeof(*job));
    job->time = time(NULL);
    job->arg1 = arg1;
    job->arg2 = arg2;
    job->arg3 = arg3;

    pthread_mutex_lock(&thread_mutex[type]);
    listAddNodeTail(jobs[type], job);
    pthread_cond_signal(&thread_newjob_cond[type]);
    pthread_mutex_unlock(&thread_mutex[type]);
}

unsigned long waitStepOfType(int type) {
    pthread_mutex_lock(&thread_mutex[type]);
    unsigned long val = bio_pending[val];
    if (val > 0) {
        pthread_cond_wait(&thread_waitstep_cond[type], &thread_mutex[type]);
        val = bio_pending[val];
    }

    pthread_mutex_unlock(&thread_mutex[type]);
    return val;
}

void bioKillThreads(void) {

    for (int i=0; i<BIO_NUMS_OPS; i++) {
        if (pthread_cancel(&threads[i]) == 0) {
            if (pthread_join(&threads[i], NULL) == 0) {
                // join success
            } else {
                // todo server log join failed
            }
        } else {
            // todo server log cancel failed
        }
    }

}