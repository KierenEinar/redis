//
// Created by kieren jiang on 2023/5/10.
//

#ifndef REDIS_CONFIG_H
#define REDIS_CONFIG_H

#ifdef __linux__
#define fsync fdatasync
#else
#define fsync fsync
#endif

#endif //REDIS_CONFIG_H
