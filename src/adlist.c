//
// Created by kieren jiang on 2023/5/12.
//

#include "adlist.h"
#include "zmalloc.h"

#include <stdlib.h>
list* listCreate() {

    list *l = zmalloc(sizeof(*l));
    l->len = 0;
    l->head = NULL;
    l->tail = NULL;
    l->free = NULL;
    l->dup = NULL;
    return l;
}

void listDelNode(list* l, listNode* ln) {

}

void listAddNodeTail(list* l, void *value) {
    listNode *node = zmalloc(sizeof(node));
    node->value = value;
    if (l->len == 0) {
        l->head = l->tail = node;
        node->prev = node->next = NULL;
    } else {
        l->tail->next = node;
        node->next = NULL;
        node->prev = l->tail;
        l->tail = node;
    }
    l->len++;
}

void listAddNodeHead(list* l, void *value) {
    listNode *node = zmalloc(sizeof(node));

    node->value = value;
    if (l->len == 0) {
        l->head = l->tail = node;
        node->prev = node->next = NULL;
    } else {
        node->next = l->head;
        l->head->prev = node;
        node->prev = NULL;
        l->head = node;
    }
    l->len++;
}

void listEmpty(list *l) {
    listNode *head = listFirst(l);
    listNode *tail = listLast(l);
    while (head != tail) {
        listNode *next = head->next;
        if (l->free)
            l->free(head->value);
        else
            zfree(head->value);
        zfree(head);
        head = next;
        l->len--;
    }
}