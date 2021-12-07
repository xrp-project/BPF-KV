#ifndef DB_TYPES_H
#define DB_TYPES_H


// Data-level information
typedef unsigned long meta__t;
typedef unsigned long key__t;
typedef unsigned char val__t[64];
typedef unsigned long ptr__t;

#define META_SIZE sizeof(meta__t)
#define KEY_SIZE sizeof(key__t)
#define VAL_SIZE sizeof(val__t)
#define PTR_SIZE sizeof(ptr__t)
#define BLK_SIZE 512
#define BLK_SIZE_LOG 9
#define SCRATCH_SIZE 4096

#define ROOT_NODE_OFFSET 0

// Node offset "encoding"
#define FILE_MASK ((ptr__t)1 << 63)

// Node-level information
#define INTERNAL 0
#define LEAF 1

#define NODE_CAPACITY ((BLK_SIZE - 2 * META_SIZE) / (KEY_SIZE + PTR_SIZE))
#define LOG_CAPACITY  ((BLK_SIZE) / (VAL_SIZE))
#define FANOUT NODE_CAPACITY

static __inline ptr__t value_base(ptr__t ptr) {
    return ptr & ~(BLK_SIZE - 1);
}

static __inline ptr__t value_offset(ptr__t ptr) {
    return ptr & (BLK_SIZE - 1);
}

static __inline ptr__t encode(ptr__t ptr) {
    return ptr | FILE_MASK;
}

static __inline ptr__t decode(ptr__t ptr) {
    return ptr & (~FILE_MASK);
}

static inline ptr__t is_file_offset(ptr__t ptr) {
    return ptr & FILE_MASK;
}

typedef struct _Node {
    meta__t next;
    meta__t type;
    key__t key[NODE_CAPACITY];
    ptr__t ptr[NODE_CAPACITY];
} Node;

_Static_assert(sizeof(Node) == BLK_SIZE, "Nodes must be block sized");

typedef struct _Log {
    val__t val[LOG_CAPACITY];
} Log;

/* State Flags for BPF Functions */
#define REACHED_LEAF 1

/* struct used to communicate with BPF function via scratch buffer */
struct Query {
    /* everything is a long to make debugging in gdb easier */
    key__t key;
    long found;
    long state_flags;

    val__t value;
    /* Used to store file offset to the value once we've located it via a leaf node */
    ptr__t value_ptr;

    /* Current node (possibly a leaf being used to process results) and its parent */
    Node current_node;
    ptr__t current_parent;
};


#define SG_KEYS 32

struct MaybeValue {
    char found;
    val__t value;
};

struct ScatterGatherQuery {
    ptr__t root_pointer;
    ptr__t value_ptr;
    unsigned int state_flags;
    int current_index;
    int n_keys;
    key__t keys[SG_KEYS];
    struct MaybeValue values[SG_KEYS];
};

static inline struct ScatterGatherQuery new_sg_query(void) {
    struct ScatterGatherQuery sgq = { 0 };
    return sgq;
}

#define RNG_KEYS 32
#define RNG_BEGIN_EXCLUSIVE 1u
#define RNG_END_INCLUSIVE 1u << 1


/* State flags for internal use */
#define RNG_RESUME 1
#define RNG_TRAVERSE 2
#define RNG_READ_VALUE 3
#define RNG_READ_NODE 4


/* Agg operations */
#define AGG_NONE 0
#define AGG_SUM 1

struct KeyValue {
    key__t key;
    val__t value;
};

/**
 * By default range queries are inclusive of [range_begin] and exclusive of [range_end].
 *
 * This can be controlled by setting the [BEGIN_EXCLUSIVE] and [END_INCLUSIVE] flags.
 */
struct RangeQuery {
    key__t range_begin;
    key__t range_end;
    unsigned int flags;
    unsigned int agg_op;

    /* Number of populated values */
    int len;
    struct KeyValue kv[RNG_KEYS];
    long agg_value;

    /* Internal data: Pointer to leaf node used by the BPF to resume the query */
    unsigned int _state;
    ptr__t _resume_from_leaf;
    unsigned int _node_key_ix;
    Node _current_node;
};

static inline int empty_range(struct RangeQuery const *query) {
    if (query->range_begin > query->range_end) {
        return 1;
    }

    unsigned int begin_exclusive = query->flags & RNG_BEGIN_EXCLUSIVE;
    unsigned int end_inclusive = query->flags & RNG_END_INCLUSIVE;

    int equal = query->range_begin == query->range_end;
    if (equal && (begin_exclusive || !end_inclusive)) {
        return 1;
    }

    int diff_one = query->range_begin + 1 == query->range_end;
    if (diff_one && (begin_exclusive && !end_inclusive)) {
        return 1;
    }
    return 0;
}

/**
 * Prepare the RangeQuery for resubmission / resumption in the kernel.
 * @param query
 * @return 0 if query is ready to resume, 1 if complete / empty range
 */
static inline int prep_range_resume(struct RangeQuery *query) {
    query->len = 0;
    query->_state = RNG_RESUME;
    /* Check if there are no more keys to retrieve */
    return empty_range(query);
}

/* Mark a range query complete; `empty_range` will return true after marking the query with this function */
static __inline void mark_range_query_complete(struct RangeQuery *query) {
    query->range_begin = query->range_end;
    query->flags |= RNG_BEGIN_EXCLUSIVE;
    query->flags &= ~RNG_END_INCLUSIVE;
}

/**
 * Set a new range for the query and "clear" existing data by setting len to 0.
 * @param query
 * @param begin
 * @param end
 * @param flags
 */
static inline void set_range(struct RangeQuery *query, key__t begin, key__t end, long flags) {
    query->range_begin = begin;
    query->range_end = end;
    query->flags = flags;
    query->len = 0;
    query->_state = RNG_TRAVERSE;
    query->_resume_from_leaf = ROOT_NODE_OFFSET;
}

_Static_assert (sizeof(struct Query) <= SCRATCH_SIZE, "struct Query too large for scratch page");
_Static_assert (sizeof(struct ScatterGatherQuery) <= SCRATCH_SIZE, "struct ScatterGatherQuery too large for scratch page");
_Static_assert (sizeof(struct RangeQuery) <= SCRATCH_SIZE, "struct RangeQuery too large for scratch page");

static inline struct Query new_query(long key) {
    struct Query query = {
        .key = key,
        .found = 0,
        .state_flags = 0,
        .value = { 0 },
        .value_ptr = 0,
        .current_node = { 0 },
        .current_parent = 0
    };
    return query;
}

#endif /* DB_TYPES_H */

