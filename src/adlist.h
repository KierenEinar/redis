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
     void *(*dup)(void *ptr);
     void (*free)(void *ptr);
     int (*match)(void *ptr, void *key);
}list;

// ------------------- API --------------------
list* listCreate();
unsigned long listLength(list* l);
listNode* listFirst(list* l);
void listDelNode(list* l, listNode* ln);
#endif //REDIS_ADLIST_H
