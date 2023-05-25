//
// Created by kieren jiang on 2023/5/25.
//

#include "server.h"

size_t syncRead(int fd, char *ptr, size_t size, long long timeout) {

    mstime_t start = mstime();
    long long waiting = timeout;
    size_t nread, totread = 0;

    if (size == 0)
        return 0;

    while (1) {

        nread = read(fd, ptr, size);
        if (nread == 0) return -1;
        if (nread == -1) {
            if (errno != EAGAIN) return -1;
        } else {
            size-=nread;
            ptr+=nread;
            totread+=nread;
        }

        if (size == 0)
            return totread;

        waiting = waiting > REDIS_WAIT_RESOLUTION ? waiting : REDIS_WAIT_RESOLUTION;

        elWait(fd, EL_READABLE, waiting);

        long long elapsed = mstime() - start;

        if (elapsed >= timeout) {
            errno = ETIMEDOUT;
            return -1;
        }

        waiting = timeout - elapsed;

    }

}
