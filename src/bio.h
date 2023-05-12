//
// Created by kieren jiang on 2023/5/9.
//

#ifndef REDIS_BIO_H
#define REDIS_BIO_H

#include <stdarg.h>
#include <sys/time.h>
void bioInit(void);
unsigned long long bioPendingJobsOfType(int type);
void bioProcessBackgroundJobs(void *argv);
void bioKillThreads(void);

#define BIO_CLOSE_FILE 0
#define BIO_AOF_FSYNC 1
#define BIO_LAZY_FREE 2
#define BIO_NUMS_OPS 3

typedef struct bio_job {
    time_t time;
    void *arg1;
}bio_job;


#endif //REDIS_BIO_H
