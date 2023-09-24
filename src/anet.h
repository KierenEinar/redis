//
// Created by kieren jiang on 2023/5/29.
//

#ifndef REDIS_ANET_H
#define REDIS_ANET_H

#include "server.h"

#define ANET_ERR -1
#define ANET_OK 0


int anetNonBlock(int fd);
int anetBlock(int fd);
int anetTcpServer(char *err, int port, int backlog);
int anetTcp6Server(char *err, int port, int backlog);
int anetTcpAccept(char *err, int fd, char *ip, size_t iplen, int *port);
void closeListeningSockets();
#endif //REDIS_ANET_H
