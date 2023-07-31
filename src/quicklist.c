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

static void _quicklistUpdateNodeSz(quicklistNode *node) {
    node->size = ziplistBloblen(node->zl);
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

static int _quicklistNodeSizeMeetRequirement(unsigned int sz, int fill) {

    if (fill > -1) {
        return 0;
    }

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


static int _quicklistNodeAllowInsert(quicklist *quicklist, quicklistNode *node, unsigned int size) {

    int ziplist_overhead = 0;

    if (size < 254) {
        ziplist_overhead = 1;
    } else {
        ziplist_overhead = 5;
    }

    if (size < 64) {
        ziplist_overhead += 1;
    } else if (size <= 16384) {
        ziplist_overhead += 2;
    } else {
        ziplist_overhead += 4;
    }

    unsigned int new_sz = node->size + size + ziplist_overhead;

    if (_quicklistNodeSizeMeetRequirement(new_sz, quicklist->fill)) {
        return 1;
    } else if (!_safeSizeLimit(new_sz)) {
        return 0;
    } else if (_quicklistNodeCountMeetRequirement(node, quicklist->fill)) {
        return 1;
    } else {
        return 0;
    }

}

static int _quicklistNodeAllowMerge(quicklist *quicklist, quicklistNode *a, quicklistNode *b) {

    if (!a || !b) {
        return 0;
    }

    unsigned int new_sz = a->size + b->size;

    if (_quicklistNodeSizeMeetRequirement(new_sz, quicklist->fill)) {
        return 1;
    } else if (!_safeSizeLimit(new_sz)) {
        return 0;
    } else if (_quicklistNodeCountMeetRequirement(node, quicklist->fill)) {
        return 1;
    } else {
        return 0;
    }

}

quicklistNode *quicklistNodeDup(quicklistNode *node) {
    quicklistNode *newnode = zmalloc(sizeof(*newnode));
    newnode->prev = node->prev;
    newnode->next = node->next;
    newnode->count = node->count;
    newnode->size = node->size;
    newnode->encoding = node->encoding;
    newnode->zl = ziplistdup(node->zl);
    return newnode;
}


static quicklistNode *_quicklistSplitNode(quicklist *quicklist, quicklistEntry *entry, int after) {

    quicklistNode *newnode = quicklistNodeDup(entry->node);

    int orig_start_truncate, new_start_truncate, orig_extent_truncate, new_extent_truncate;

    orig_start_truncate = after ? (entry->idx + 1) : 0;
    orig_extent_truncate = after ? -1 : entry->idx + 1;

    new_start_truncate = after ? 0 : entry->idx;
    new_extent_truncate = after ? (entry->idx + 1): -1;

    entry->node->zl = ziplistDeleteRange(entry->node->zl, orig_start_truncate, orig_extent_truncate);
    entry->node->count = ziplistlen(entry->node->zl);
    _quicklistUpdateNodeSz(entry->node);

    newnode->zl = ziplistDeleteRange(newnode->zl, new_start_truncate, new_extent_truncate);
    newnode->count = ziplistlen(newnode->zl);;
    _quicklistUpdateNodeSz(newnode);
    return newnode;

}

void quicklistDeleteNode(quicklist *ql, quicklistNode *node) {

    if (ql->head == node) {
        ql->head = node->next;
    }

    if (ql->tail == node) {
        ql->tail = node->prev;
    }

    if (node->prev) {
        node->prev->next = node->next;
    }

    if (node->next) {
        node->next->prev = node->prev;
    }

    zfree(node->zl);
    zfree(node);
    ql->len--;
}

static quicklistNode* _quicklistNodeMergeZiplist(quicklist *quicklist, quicklistNode *a, quicklistNode *b) {


    if (ziplistMerge(&a->zl, &b->zl)) {

        quicklistNode *keep, *nokeep;

        if (a->zl != NULL) {
            keep = a;
            nokeep = b;
        } else {
            keep = b;
            nokeep = a;
        }

        keep->count+=nokeep->count;
        keep->size = ziplistBloblen(keep->zl);

        quicklistDeleteNode(quicklist, nokeep);

        return keep;

    }

    return NULL;


}

// [prev_prev, prev]
// [next, next_next]
// [prev, center]
// [center, next]
static void _quicklistMergeNode(quicklist *quicklist, quicklistNode *center) {

    quicklistNode *prev_prev, *prev, *next, *next_next, *target;

    if (center->prev) {
        prev = center->prev;
        if (prev) {
            if (prev->prev) prev_prev = prev->prev;
        }
    }

    if (center->next) {
        next = center->next;
        if (next) {
            if (next->next) next_next = next->next;
        }
    }


    if (_quicklistNodeAllowMerge(quicklist, prev, prev_prev)) {
        _quicklistNodeMergeZiplist(quicklist, prev, prev_prev);
        prev = prev_prev = NULL;
    }

    if (_quicklistNodeAllowMerge(quicklist, next, next_next)) {
        _quicklistNodeMergeZiplist(quicklist, next, next_next);
        next = next_next = NULL;
    }

    if (_quicklistNodeAllowMerge(quicklist, center, center->prev)) {
        target = _quicklistNodeMergeZiplist(quicklist, center, center->prev);
    }

    if (target && target->next) {
        if (_quicklistNodeAllowMerge(quicklist, target, target->next)) {
            _quicklistNodeMergeZiplist(quicklist, target, target->next);
        }
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
        _quicklistUpdateNodeSz(node);
        quicklistNodeInsert(quicklist, NULL, node, 1);
        return;
    }

    if (!_quicklistNodeAllowInsert(quicklist, node, size)) {
        full = 1;
    }

    if (after && entry->idx == node->count) {
        at_tail = 1;
        if (node->next) {
            if (!_quicklistNodeAllowInsert(quicklist, node->next, size))
                next_full = 1;
        }
    }

    if (!after && entry->idx == 0) {
        at_head = 1;
        if (node->prev) {
            if (!_quicklistNodeAllowInsert(quicklist, node->prev, size))
                prev_full = 1;
        }
    }

    if (!full && after) {

        if (!node->zl) {
            node->zl = ziplistPush(ziplistNew(), entry->zlentry, size, ZIPLIST_INSERT_TAIL);
        } else {
            node->zl = ziplistPush(node->zl, entry->zlentry, size, ZIPLIST_INSERT_TAIL);
        }
        _quicklistUpdateNodeSz(node);
        node->count++;

    } else if (!full && !after) {

        if (!node->zl) {
            node->zl = ziplistPush(ziplistNew(), entry->zlentry, size, ZIPLIST_INSERT_HEAD);
        } else {
            node->zl = ziplistPush(node->zl, entry->zlentry, size, ZIPLIST_INSERT_TAIL);
        }
        _quicklistUpdateNodeSz(node);
        node->count++;
    } else if (full && at_tail && !next_full && after) {

        if (!node->next) {
            quicklistNode *nextnode = quicklistNodeCreate();
            quicklistNodeInsert(quicklist, node, nextnode, QUICK_LIST_INSERT_AFTER);
        }

        if (node->next->zl == NULL) {
            node->next->zl = ziplistPush(ziplistNew(), value, size, ZIPLIST_INSERT_HEAD);
        } else {
            node->next->zl = ziplistPush(node->next->zl, value, size, ZIPLIST_INSERT_HEAD);
        }

        _quicklistUpdateNodeSz(node->next);
        node->next->count++;
    } else if (full && at_head && !prev_full && !after) {

        if (!node->prev) {
            quicklistNode *prevnode = quicklistNodeCreate();
            quicklistNodeInsert(quicklist, node, prevnode, QUICK_LIST_INSERT_BEFORE);
        }

        if (node->prev->zl == NULL) {
            node->prev->zl = ziplistPush(ziplistNew(), value, size, ZIPLIST_INSERT_TAIL);
        } else {
            node->prev->zl = ziplistPush(node->prev->zl, value, size, ZIPLIST_INSERT_TAIL);
        }
        _quicklistUpdateNodeSz(node->prev);
        node->prev->count++;
    } else if (full && at_tail && next_full && after) {

        quicklistNode *nextnode = quicklistNodeCreate();
        nextnode->zl = ziplistPush(ziplistNew(), value, size, ZIPLIST_INSERT_HEAD);
        quicklistNodeInsert(quicklist, node, nextnode, QUICK_LIST_INSERT_AFTER);
        _quicklistUpdateNodeSz(nextnode);
        nextnode->count++;

    } else if (full && at_head && prev_full && !after) {

        quicklistNode *prevnode = quicklistNodeCreate();
        prevnode->zl = ziplistPush(ziplistNew(), value, size, ZIPLIST_INSERT_HEAD);
        quicklistNodeInsert(quicklist, node, prevnode, QUICK_LIST_INSERT_BEFORE);
        _quicklistUpdateNodeSz(prevnode);
        prevnode->count++;

    } else if (full) {

        quicklistNode *newnode = _quicklistSplitNode(quicklist, entry, after);
        ziplistPush(newnode->zl, value, size, after ? ZIPLIST_INSERT_HEAD : ZIPLIST_INSERT_TAIL);
        quicklistNodeInsert(quicklist, node, newnode, after);
        _quicklistMergeNode(quicklist, node);
    }

    quicklist->count++;


}
