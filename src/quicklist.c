//
// Created by kieren jiang on 2023/7/25.
//

#include "quicklist.h"

const int fill_size_offset[] = {4096, 8192, 16384, 32768, 65536};

#define FILL_SIZE_LEN ((sizeof(fill_size_offset)) / (sizeof(fill_size_offset[0])))

quicklist *quicklistCreate(void) {

    quicklist *list = zmalloc(sizeof(*list));
    list->count = list->len = 0;
    list->head = list->tail = NULL;
    return list;
}

#define FILL_MAX 1 << 15
quicklist *quicklistNew (int fill) {

    if (fill > FILL_MAX) {
        fill = FILL_MAX;
    } else if (fill < -5) {
        fill = -5;
    }

    quicklist *ql = quicklistCreate();
    ql->fill = fill;
    return ql;
}


quicklistNode *quicklistNodeCreate() {
    quicklistNode *node = zmalloc(sizeof(*node));
    node->prev = node->next = NULL;
    node->zl = NULL;
    node->size = 0;
    node->count = 0;
    node->encoding = QUICK_LIST_ENCODING_RAW;
    return node;
}

static void _quicklistUpdateNodeSz(quicklistNode *node, unsigned int size) {
    node->size+=size;
}

static void quicklistNodeInsert(quicklist *quicklist, quicklistNode *old, quicklistNode *new_node, int after) {
    if (after) {

        new_node->prev = old;
        if (old) {
            if (old->next) {
                old->next->prev = new_node;
                new_node->next = old->next;
            }
            old->next = new_node;
            if (old == quicklist->tail) {
                quicklist->tail = new_node;
            }
        }

    } else {

        new_node->next = old;

        if (old) {
            if (old->prev) {
                old->prev->next = new_node;
                new_node->prev = old->prev;
            }
            old->prev = new_node;
            if (old == quicklist->head) {
                quicklist->head = new_node;
            }
        }
    }

    if (old == NULL) {
        quicklist->head = quicklist->tail = new_node;
    }

    quicklist->len++;
}

static int _quicklistNodeSizeMeetRequirement(quicklistNode *node, unsigned int size, int fill) {

    if (fill > -1) {
        return 0;
    }

    unsigned int sz = node->size + size;

    if (-fill-1 < FILL_SIZE_LEN) {
        int offset = fill_size_offset[-fill-1];
        if (sz <= offset) return 1;
    }

    return 0;
}


#define SAFE_MAX_SIZE 8192
static int _safeSizeLimit(unsigned int size) {
    return size <= SAFE_MAX_SIZE;
}

static int _quicklistNodeCountMeetRequirement(quicklistNode *node, int fill) {
    return node->count < fill;
}


static int _quicklistAllowInsert(quicklist *quicklist, quicklistNode *node, unsigned int size) {

    if (_quicklistNodeSizeMeetRequirement(node, size, quicklist->fill)) {
        return 1;
    } else if (!_safeSizeLimit(size)) {
        return 0;
    } else if (_quicklistNodeCountMeetRequirement(node, quicklist->fill)) {
        return 1;
    } else {
        return 0;
    }

}

static void _quicklistInsert(quicklist *quicklist, quicklistEntry *entry, void *value, unsigned int size, int after) {

    quicklistNode *prev, *prev_prev, *next, *next_next, *node;
    int full, at_tail, at_head, next_full, prev_full;

    prev = prev_prev = next = next_next = NULL;
    full = at_tail = at_head = next_full = prev_full = 0;
    node = entry->node;

    if (!node) {
        node = quicklistNodeCreate();
        node->count++;
        node->zl = ziplistPush(ziplistNew(), value, size, ZIPLIST_INSERT_TAIL);
        _quicklistUpdateNodeSz(node, size);
        quicklistNodeInsert(quicklist, NULL, node, 1);
        return;
    }

    if (!_quicklistAllowInsert(quicklist, node, size)) {
        full = 1;
    }

    if (after && entry->idx == node->count) {
        at_tail = 1;
        if (node->next) {
            if (!_quicklistAllowInsert(quicklist, node->next, size))
                next_full = 1;
        }
    }

    if (!after && entry->idx == 0) {
        at_head = 1;
        if (node->prev) {
            if (!_quicklistAllowInsert(quicklist, node->prev, size))
                prev_full = 1;
        }
    }


    if (!full && after) {

        if (!node->zl) {
            node->zl = ziplistPush(ziplistNew(), entry->zlentry, size, ZIPLIST_INSERT_TAIL);
        } else {
            node->zl = ziplistPush(node->zl, entry->zlentry, size, ZIPLIST_INSERT_TAIL);
        }
        _quicklistUpdateNodeSz(node, size);
        node->count++;

    } else if (!full && !after) {

    }


    quicklist->len++;


}
