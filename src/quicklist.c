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

static int _quicklistNodeCountMeetRequirement(int count, int fill) {
    return count < fill;
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
    } else if (_quicklistNodeCountMeetRequirement(node->count, quicklist->fill)) {
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
    } else if (_quicklistNodeCountMeetRequirement(a->count+b->count, quicklist->fill)) {
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

    quicklistNode *new_node = quicklistNodeDup(entry->node);

    int orig_start_truncate, new_start_truncate, orig_extent_truncate, new_extent_truncate;

    orig_start_truncate = after ? (entry->idx + 1) : 0;
    orig_extent_truncate = after ? -1 : entry->idx + 1;

    new_start_truncate = after ? 0 : entry->idx;
    new_extent_truncate = after ? (entry->idx + 1): -1;

    entry->node->zl = ziplistDeleteRange(entry->node->zl, orig_start_truncate, orig_extent_truncate);
    entry->node->count = ziplistlen(entry->node->zl);
    _quicklistUpdateNodeSz(entry->node);

    new_node->zl = ziplistDeleteRange(new_node->zl, new_start_truncate, new_extent_truncate);
    new_node->count = ziplistlen(new_node->zl);;
    _quicklistUpdateNodeSz(new_node);
    return new_node;

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


        keep->count = ziplistlen(keep->zl);
        _quicklistUpdateNodeSz(keep);

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
    prev_prev = prev = next = next_next = target = NULL;
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
        center = NULL;
    } else {
        target = center;
    }

    if (_quicklistNodeAllowMerge(quicklist, target, target->next)) {
        _quicklistNodeMergeZiplist(quicklist, target, target->next);
    }

}

void _quicklistInsert(quicklist *quicklist, quicklistEntry *entry, void *value, unsigned int size, int after) {

    int full, at_tail, at_head, next_full, prev_full;
    quicklistNode *node;
    full = at_tail = at_head = next_full = prev_full = 0;
    node = entry->node;

    if (!node) {
        node = quicklistNodeCreate();
        node->count++;
        node->zl = ziplistPush(ziplistNew(), value, size, ZIPLIST_INSERT_TAIL);
        _quicklistUpdateNodeSz(node);
        quicklistNodeInsert(quicklist, NULL, node, 1);
        quicklist->count++;
        return;
    }

    if (!_quicklistNodeAllowInsert(quicklist, node, size)) {
        full = 1;
    }

    if (after && entry->idx == (node->count - 1)) {
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

        unsigned char *next = ziplistNext(node->zl, entry->zlentry);

        if (!next) {
            node->zl = ziplistPush(node->zl, entry->zlentry, size, ZIPLIST_INSERT_TAIL);
        } else {
            node->zl = ziplistInsert(node->zl, entry->zlentry, value, size);
        }
        _quicklistUpdateNodeSz(node);
        node->count++;

    } else if (!full && !after) {

        node->zl = ziplistInsert(node->zl, entry->zlentry, value, size);
        _quicklistUpdateNodeSz(node);
        node->count++;
    } else if (full && at_tail && node->next && !next_full && after) {

        node->next->zl = ziplistPush(node->next->zl, value, size, ZIPLIST_INSERT_HEAD);
        _quicklistUpdateNodeSz(node->next);
        node->next->count++;
    } else if (full && at_head && node->prev && !prev_full && !after) {

        node->prev->zl = ziplistPush(node->prev->zl, value, size, ZIPLIST_INSERT_TAIL);
        _quicklistUpdateNodeSz(node->prev);
        node->prev->count++;
    } else if (full && at_tail && next_full && after) {

        quicklistNode *next = quicklistNodeCreate();
        next->zl = ziplistPush(ziplistNew(), value, size, ZIPLIST_INSERT_HEAD);
        quicklistNodeInsert(quicklist, node, next, QUICK_LIST_INSERT_AFTER);
        _quicklistUpdateNodeSz(next);
        next->count++;

    } else if (full && at_head && prev_full && !after) {

        quicklistNode *prev = quicklistNodeCreate();
        prev->zl = ziplistPush(ziplistNew(), value, size, ZIPLIST_INSERT_HEAD);
        quicklistNodeInsert(quicklist, node, prev, QUICK_LIST_INSERT_BEFORE);
        _quicklistUpdateNodeSz(prev);
        prev->count++;

    } else if (full) {

        quicklistNode *new_node = _quicklistSplitNode(quicklist, entry, after);
        ziplistPush(new_node->zl, value, size, after ? ZIPLIST_INSERT_HEAD : ZIPLIST_INSERT_TAIL);
        quicklistNodeInsert(quicklist, node, new_node, after);
        _quicklistMergeNode(quicklist, node);
    }

    quicklist->count++;


}


void quicklistInsertBefore(quicklist *ql, quicklistEntry *entry, void *data, unsigned int size) {
    _quicklistInsert(ql, entry, data, size, 1);
}

void quicklistInsertAfter(quicklist *ql, quicklistEntry *entry, void *data, unsigned int size) {
    _quicklistInsert(ql, entry, data, size, 0);
}