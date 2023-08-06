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

static int _quicklistDelZlEntry(quicklist *ql, quicklistNode *node, unsigned char *p) {

    node->zl = __ziplistDelete(node->zl, p, 1);

    if (ziplistlen(node->zl) == 0) {
        quicklistDeleteNode(ql, node);
        return 1;
    }

    node->count--;
    _quicklistUpdateNodeSz(node);
    ql->len--;
    return 0;
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
        node->zl = ziplistPush(ziplistNew(), (unsigned char*)value, size, ZIPLIST_INSERT_TAIL);
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
            node->zl = ziplistInsert(node->zl, entry->zlentry, (unsigned char*)value, size);
        }
        _quicklistUpdateNodeSz(node);
        node->count++;

    } else if (!full && !after) {

        node->zl = ziplistInsert(node->zl, entry->zlentry, (unsigned char*)value, size);
        _quicklistUpdateNodeSz(node);
        node->count++;
    } else if (full && at_tail && node->next && !next_full && after) {

        node->next->zl = ziplistPush(node->next->zl, (unsigned char*)value, size, ZIPLIST_INSERT_HEAD);
        _quicklistUpdateNodeSz(node->next);
        node->next->count++;
    } else if (full && at_head && node->prev && !prev_full && !after) {

        node->prev->zl = ziplistPush(node->prev->zl, (unsigned char*)value, size, ZIPLIST_INSERT_TAIL);
        _quicklistUpdateNodeSz(node->prev);
        node->prev->count++;
    } else if (full && at_tail && next_full && after) {

        quicklistNode *next = quicklistNodeCreate();
        next->zl = ziplistPush(ziplistNew(), (unsigned char*)value, size, ZIPLIST_INSERT_HEAD);
        quicklistNodeInsert(quicklist, node, next, QUICK_LIST_INSERT_AFTER);
        _quicklistUpdateNodeSz(next);
        next->count++;

    } else if (full && at_head && prev_full && !after) {

        quicklistNode *prev = quicklistNodeCreate();
        prev->zl = ziplistPush(ziplistNew(), (unsigned char*)value, size, ZIPLIST_INSERT_HEAD);
        quicklistNodeInsert(quicklist, node, prev, QUICK_LIST_INSERT_BEFORE);
        _quicklistUpdateNodeSz(prev);
        prev->count++;

    } else if (full) {

        quicklistNode *new_node = _quicklistSplitNode(quicklist, entry, after);
        ziplistPush(new_node->zl, (unsigned char*)value, size, after ? ZIPLIST_INSERT_HEAD : ZIPLIST_INSERT_TAIL);
        quicklistNodeInsert(quicklist, node, new_node, after);
        _quicklistMergeNode(quicklist, node);
    }

    quicklist->count++;


}

int quicklistPushHead(quicklist *ql, void *data, unsigned int size) {
    return quicklistPush(ql, data, size, QUICK_LIST_INSERT_BEFORE);
}

int quicklistPushTail(quicklist *ql, void *data, unsigned int size) {
    return quicklistPush(ql, data, size, QUICK_LIST_INSERT_AFTER);
}

int quicklistPush(quicklist *ql, void *data, unsigned int size, int where) {

    int full, new_node;
    quicklistNode *push_at, *push_node;

    push_at = (where == QUICK_LIST_INSERT_AFTER) ? ql->tail : ql->head;

    if (_quicklistNodeAllowInsert(ql, push_at, size)) {
        push_node = push_at;
    } else {
        new_node = 1;
        push_node = quicklistNodeCreate();
        quicklistNodeInsert(ql, push_at, push_node, where == QUICK_LIST_INSERT_AFTER ? 1 : 0);
        push_node->zl = ziplistNew();
        ql->count++;
    }

    push_node->zl = ziplistPush(push_node->zl, (unsigned char*)data, size, where == QUICK_LIST_INSERT_AFTER ? ZIPLIST_INSERT_TAIL : ZIPLIST_INSERT_HEAD);
    push_node->count++;
    _quicklistUpdateNodeSz(push_node);

    ql->len++;

    return push_at != push_node;

}

void quicklistInsertBefore(quicklist *ql, quicklistEntry *entry, void *data, unsigned int size) {
    _quicklistInsert(ql, entry, data, size, 1);
}

void quicklistInsertAfter(quicklist *ql, quicklistEntry *entry, void *data, unsigned int size) {
    _quicklistInsert(ql, entry, data, size, 0);
}

void initEntry(quicklistEntry *entry) {

    entry->node = NULL;
    entry->idx = 0;
    entry->zlentry = NULL;
    entry->ql = NULL;
    entry->size = 0;
    entry->str = NULL;
    entry->llvalue = -123456789;
}

int quicklistIndex(quicklist *ql, long long idx, quicklistEntry *entry) {

    int forward;
    long long entry_idx = 0;
    long long accum = 0;
    unsigned char *p;

    forward = idx >= 0 ? 1 : 0;

    if (idx < 0)
        idx = -idx + 1;

    initEntry(entry);

    quicklistNode *node = idx > 0 ? ql->head : ql->tail;

    while (node) {

        if (accum + node->count > idx) {
            break;
        } else {
            accum += node->count;
        }
        node = forward ? node->next : node->prev;
    }

    if (!node)
        return 0;

    entry_idx = idx - accum;

    if (!forward) entry_idx = -entry_idx - 1;

    p = ziplistIndex(node->zl, (int)(entry_idx));

    if (ziplistGet(p, &entry->str, &entry->size, &entry->llvalue)) {
        return 1;
    }

    return 0;

}

int quicklistDelRange(quicklist *ql, const long start, const long count) {

    unsigned long extent = count;

    if (start >= 0 && count > ql->count - start) {
        extent = ql->count - start;
    } else if (start < 0 && -start > count) {
        extent = -start;
    }

    quicklistEntry entry;

    if (!quicklistIndex(ql, start, &entry)) {
        return 0;
    }

    quicklistNode *node = entry.node;

    while (extent) {

        quicklistNode *next = node->next;
        unsigned long del = 0;
        int del_entire_node = 0;
        if (entry.idx == 0 && extent >= node->count) {
            del = node->count;
            del_entire_node = 1;
        } else if (entry.idx > 0 && extent >= node->count) {
            del = node->count - extent;
        } else if (entry.idx < 0) {
            del = -entry.idx;
            if (del > extent) {
                del = extent;
            }
        } else {
            del = extent;
        }

        if (del_entire_node) {
            quicklistDeleteNode(ql, node);
        } else {

            node->zl = ziplistDeleteRange(node->zl, entry.idx, del);
            node->count-=del;
            if (node->count == 0) {
                quicklistDeleteNode(ql, node);
            }
            ql->count-=del;
        }

        extent-=del;
        node = next;
        entry.idx = 0;
    }

    return 1;

}

int quicklistPopCustom(quicklist *ql, int where, void **data, unsigned int *size, long long *value, void *(*saver)(void *data, unsigned int size)) {

    quicklistNode *node;
    long long svalue;
    int idx;
    unsigned char *p;
    unsigned char *sstr = NULL;
    unsigned int ssize;

    if (data) {
        *data = NULL;
    }
    if (size) {
        *size = 0;
    }
    if (value) {
        *value = -123456789;
    }

    if (where == QUICK_LIST_TAIL && ql->tail) {
        node = ql->tail;
        idx = 0;
    } else if (where == QUICK_LIST_HEAD && ql->tail) {
        node = ql->head;
        idx = -1;
    } else {
        return 0;
    }

    p = ziplistIndex(node->zl, idx);

    if (ziplistGet(p, &sstr, &ssize, &svalue)) {

        if (sstr) {
            *data = saver((void *)sstr, ssize);
            *size = ssize;
        } else {
            *value = svalue;
        }

        _quicklistDelZlEntry(ql, node, p);

        return 1;
    }

    return 0;

}

void *quicklistSaver(void *data, unsigned int size) {

    unsigned char *pop_data;
    pop_data = zmalloc(size);
    memcpy(pop_data, data, size);
    return pop_data;
}

// quicklistCreateIterator create a new iter.
quicklistIter *quicklistCreateIterator(quicklist *ql, int direction) {

    quicklistIter *iter;
    iter = zmalloc(sizeof(*iter));

    iter->ql = ql;
    if (direction == AL_LIST_FORWARD) {
        iter->current = ql->head;
        iter->offset = 0;
    } else {
        iter->current = ql->tail;
        iter->offset = -1;
    }
    iter->direction = direction;
    iter->zi = NULL;
    return iter;
}

// iter the next entry.
int quicklistNext(quicklistIter *iter, quicklistEntry *entry) {

    initEntry(entry);

    if (entry->ql == NULL) {
        return 0;
    }

    if (iter->current == NULL) {
        return 0;
    }

    int update_offset = 0;

    if (!iter->zi) {
        iter->zi = ziplistIndex(iter->current->zl, iter->offset);

    } else {
        if (iter->direction == AL_LIST_FORWARD) {
            iter->zi = ziplistNext(iter->current->zl, iter->zi);
        } else {
            iter->zi = ziplistPrev(iter->current->zl, iter->zi);
        }
        ziplistGet(iter->zi, &entry->str, &entry->size, &entry->llvalue);
    }

    if (iter->zi) update_offset = 1;
    iter->offset += update_offset;

    entry->idx = iter->offset;
    entry->node = iter->current;
    entry->zlentry = iter->zi;

    if (iter->zi) {
        return 1;
    }

    if (iter->direction == AL_LIST_FORWARD) {
        iter->current = iter->current->next;
        iter->offset = 0;
    } else {
        iter->current = iter->current->prev;
        iter->offset = -1;
    }

    return quicklistNext(iter, entry);

}

// delete the entry while iterating.
void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry) {

    int deleted_node = _quicklistDelZlEntry(entry->ql, entry->node, entry->zlentry);

    if (deleted_node) {
        if (iter->direction == AL_LIST_FORWARD) {
            iter->current = iter->current->next;
            iter->offset = 0;
        } else {
            iter->current = iter->current->prev;
            iter->offset = -1;
        }
    }
    iter->zi = NULL;
}

void quicklistRelease(quicklist *ql) {
    unsigned int len;
    len = ql->len;
    quicklistDelRange(ql, 0, len);
    zfree(ql);
}


// release the iter.
void quicklistReleaseIter(quicklistIter *iter) {
    zfree(iter);
}


void quicklistTest() {

    // test insert
    quicklist *ql;
    char *data = "hello world";
    char *pop_data;
    unsigned int pop_data_size;
    long long pop_llvalue;
    ql = quicklistNew(-2);
    quicklistPushTail(ql, data, strlen(data));


    quicklistPopCustom(ql, QUICK_LIST_HEAD, (void *)&pop_data,
                       &pop_data_size, &pop_llvalue, &quicklistSaver);

    fprintf(stdout, "pop data = %s", pop_data);
}