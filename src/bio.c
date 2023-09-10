//
// Created by kieren jiang on 2023/5/9.
//

#include "bio.h"
#include "server.h"


static pthread_t threads[BIO_NUMS_OPS];
static pthread_mutex_t mutex[BIO_NUMS_OPS];
static pthread_cond_t new_job_cond[BIO_NUMS_OPS];
static pthread_cond_t job_finish_cond[BIO_NUMS_OPS];
static list *jobs[BIO_NUMS_OPS];
static unsigned long pending_jobs[BIO_NUMS_OPS];

void bioInit(void) {

    int j;
    pthread_attr_t attr;
    size_t thread_stack_size;
    pthread_t thread;

    for (j=0; j<BIO_NUMS_OPS; j++) {
        pthread_mutex_init(&mutex[j], NULL);
        pthread_cond_init(&new_job_cond[j], NULL);
        pthread_cond_init(&job_finish_cond[j], NULL);
        jobs[j] = listCreate();
        pending_jobs[j] = 0;
    }

    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr, &thread_stack_size);
    while (thread_stack_size < REDIS_THREAD_STACK_SIZE) {
        thread_stack_size <<= 1;
    }
    pthread_attr_setstacksize(&attr, thread_stack_size);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    for (j=0; j<BIO_NUMS_OPS; j++) {
        if (pthread_create(&thread, &attr, bioProcessBackgroundJobs, (void*)(long)j) != 0) {
            exit(-1);
        }
        threads[j] = thread;
    }

    pthread_attr_destroy(&attr);
}

void *bioProcessBackgroundJobs(void *argv) {
    long job_type;
    sigset_t sigset;

    job_type = (long)argv;
    if (job_type >= BIO_NUMS_OPS) {
        exit(-1);
    }


    pthread_mutex_lock(&mutex[job_type]);

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    sigemptyset(&sigset);
    sigaddset(&sigset, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &sigset, NULL);

    while (1) {
        listNode *node;
        bio_job *job;

        if (pending_jobs[job_type] == 0) {
            pthread_cond_wait(&new_job_cond[job_type], &mutex[job_type]);
            continue;
        }
        debug("bioProcessBackgroundJobs...\r\n");
        node = listFirst(jobs[job_type]);
        pthread_mutex_unlock(&mutex[job_type]);
        job = node->value;
        if (job_type == BIO_AOF_FSYNC) {
            debug("bioProcessBackgroundJobs start flushAppendOnlyFile ...\r\n");
            fsync(server.aof_fd);
            debug("bioProcessBackgroundJobs finished flushAppendOnlyFile ...\r\n");
        }
        pthread_mutex_lock(&mutex[job_type]);
        pthread_cond_broadcast(&job_finish_cond[job_type]);
        listDelNode(jobs[job_type], node);
        pending_jobs[job_type]--;
        va_end(job->va_args);
        zfree(job);
    }

}

void bioCreateBackgroundJob(int type, ...) {
    bio_job *job;
    va_list list;

    if (type >= BIO_NUMS_OPS) {
        exit(-1);
    }

    va_start(list, type);
    job = zmalloc(sizeof(*job));
    job->time = time(NULL);
    va_copy(job->va_args, list);
    va_end(list);

    pthread_mutex_lock(&mutex[type]);
    pending_jobs[type]++;
    listAddNodeTail(jobs[type], job);
    pthread_cond_broadcast(&new_job_cond[type]);
    pthread_mutex_unlock(&mutex[type]);

}

int bioWaitJobFinish(int type) {

    unsigned long pending_job;

    if (type >= BIO_NUMS_OPS) {
        exit(-1);
    }

    pthread_mutex_lock(&mutex[type]);
    pending_job = pending_jobs[type];
    if (pending_job == 0) {
        pthread_mutex_unlock(&mutex[type]);
        return 0;
    }

    pthread_cond_wait(&job_finish_cond[type], &mutex[type]);
    pthread_mutex_unlock(&mutex[type]);
    return 1;
}

unsigned long bioPendingJobsOfType(int type) {

    unsigned long pending;

    if (type >= BIO_NUMS_OPS) {
        exit(-1);
    }
    pthread_mutex_lock(&mutex[type]);
    pending = pending_jobs[type];
    pthread_mutex_unlock(&mutex[type]);

    return pending;
}

void bioKillThreads(void) {

    int j;
    for (j=0; j<BIO_NUMS_OPS; j++) {
        if (pthread_cancel(threads[j]) == 0) {
            pthread_join(threads[j], NULL);
        }
    }
}