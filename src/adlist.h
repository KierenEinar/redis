//
// Created by kieren jiang on 2023/5/12.
//

#ifndef REDIS_ADLIST_H
#define REDIS_ADLIST_H

typedef struct listNode {
    struct listNode *prev;
    struct listNode *next;
    void *value;
}listNode;

typedef struct list {
     listNode *head;
     listNode *tail;
     unsigned long len;
     void *(*dup)(void *key);
     void (*free)(void *ptr);
}list;

// functions implemented as macros
#define listSetFreeMethod(l, m) ((l)->free = (m))
#define listSetDupMethod(l, d) ((l)->dup = (d))
#define listFirst(l) ((l)->head)
#define listLast(l) ((l)->tail)
#define listLength(l) ((l)->len)

// ------------------- API --------------------
list* listCreate();
void listDelNode(list* l, listNode* ln);
void listAddNodeTail(list* l, void *value);
void listAddNodeHead(list* l, void *value);
void listEmpty(list *l);

#endif //REDIS_ADLIST_H
