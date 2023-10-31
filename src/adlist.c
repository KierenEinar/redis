//
// Created by kieren jiang on 2023/5/12.
//

#include "adlist.h"
#include "server.h"

#include <stdlib.h>
list* listCreate() {
    list *l;
    l = zmalloc(sizeof(*l));
    l->len = 0l;
    l->head = NULL;
    l->tail = NULL;
    l->free = NULL;
    l->dup = NULL;
    l->match = NULL;
    return l;
}

void listDelNode(list* l, listNode* ln) {

    if (ln->next)
        ln->next->prev = ln->prev;
    else
        l->tail = ln->prev;

    if (ln->prev)
        ln->prev->next = ln->next;
    else
        l->head = ln->next;

    l->len--;

    if (l->free) l->free(ln);

}

void listAddNodeTail(list* l, void *value) {
    listNode *node;

    node = zmalloc(sizeof(*node));
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
    listNode *node;
    node = zmalloc(sizeof(*node));

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

listNode *listSearchKey(list *l, void *value) {

    listNode *ln = l->head;
    while (ln) {

        if (l->match) {
            if (l->match(ln->value, value) == 0)
                return ln;
        } else {
            if (ln->value == value)
                return ln;
        }
        ln = ln->next;
    }
    return NULL;
}

void listEmpty(list *l) {
    listNode *current = listFirst(l);
    listNode *next;
    while (l->len--) {
        next = current->next;
        if (l->free) l->free(current->value);
        zfree(current);
        current = next;
    }

    l->head = l->tail = NULL;
}

void listRelease(list *l) {
    listEmpty(l);
    zfree(l);
}


void listRewind(list *l, listIter *li) {
    li->next = l->head;
    li->direction = LIST_ITER_DIR_FORWARD;
}

listNode *listNext(listIter *li) {

    listNode *current = li->next;
    if (current) {
        if (li->direction == LIST_ITER_DIR_FORWARD)
            li->next = current->next;
        else
            li->next = current->prev;
    }
    return current;
}

list *listDup(list *l) {

    list *c;
    listIter li;
    listNode *ln;

    c = listCreate();
    listRewind(l, &li);
    while ((ln = listNext(&li)) != NULL) {
        listNode *n;
        n = zmalloc(sizeof(*n));
        if (l->dup) {
            n->value = ln->value;
        } else {
            n->value = l->dup(ln->value);
        }
        listAddNodeTail(c, n);
    }
    return c;
}