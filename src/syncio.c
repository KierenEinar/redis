//
// Created by kieren jiang on 2023/5/25.
//

#include "server.h"

size_t syncWrite(int fd, char *ptr, size_t size, long long timeout) {

    size_t totwrite;
    size_t nwrite;
    long long elapsed_timeout;
    mstime_t start;


    totwrite = -1;
    elapsed_timeout = timeout;
    start = mstime();

    while (size) {

        elapsed_timeout = elapsed_timeout > REDIS_WAIT_RESOLUTION ? elapsed_timeout : REDIS_WAIT_RESOLUTION;
        if (elWait(fd, EL_WRITABLE, elapsed_timeout) & EL_WRITABLE) {
            if ((nwrite = write(fd, ptr, size)) == -1) {
                debug("syncWrite write failed, fd=%d\n", fd);
                if (errno != EAGAIN) {
                    return -1;
                }
            } else {
                ptr+=nwrite;
                totwrite+=nwrite;
                size-=nwrite;
            }
        }

        mstime_t now = mstime();
        elapsed_timeout = timeout - (now - start);
        if (elapsed_timeout <0) {
            errno = ETIMEDOUT;
            return -1;
        }

    }

    return totwrite;

}

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


size_t syncReadLine(int fd, char *ptr, size_t size, long long timeout) {

    size_t nread = 0;

    while (size) {
        char c;
        if (syncRead(fd, &c, 1, timeout) == -1) return -1;
        if (c == '\n') {
            *ptr = '\0';
            if (nread && *(ptr-1) == '\r') {
                *(ptr-1) = '\0';
                nread--;
            }
            return nread;
        } else {
            nread+=1;
            *ptr++ = c;
            *ptr = '\0';
        }
        size--;
    }

    return nread;

}
