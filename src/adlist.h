//
// Created by kieren jiang on 2023/5/12.
//

#ifndef REDIS_ADLIST_H
#define REDIS_ADLIST_H

typedef void (free)(void *ptr);
typedef int (match)(void *ptr, void *key);
typedef void *(dup)(void *ptr);
typedef struct listNode {
    struct listNode *prev;
    struct listNode *next;
    void *value;
}listNode;

typedef struct list {
     listNode *head;
     listNode *tail;
     unsigned long len;
     dup *dup;
     free *free;
     match *match;
}list;

// ------------------- API --------------------
list* listCreate();
unsigned long listLength(list* l);
listNode* listFirst(list* l);
listNode* listLast(list* l);
void listDelNode(list* l, listNode* ln);
void listAddNodeTail(list* l, void *value);
void listSetFreeMethod(list* l, free *free);
void listSetDupMethod(list* l, dup *dup);
void listSetMatchMethod(list* l, match *match);


#endif //REDIS_ADLIST_H
