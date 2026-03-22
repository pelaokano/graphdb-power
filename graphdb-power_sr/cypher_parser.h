#ifndef CYPHER_PARSER_H
#define CYPHER_PARSER_H

#include "cypher_lexer.h"

#define MAX_ITEMS     32
#define MAX_IDENT_LEN 128
#define MAX_EXPR_LEN  512

/* ------------------------------------------------------------------ */
/*  Expression types                                                    */
/* ------------------------------------------------------------------ */

typedef enum {
    EXPR_IDENT,       /* n  */
    EXPR_PROP,        /* n.name */
    EXPR_STRING,
    EXPR_INT,
    EXPR_FLOAT,
    EXPR_BOOL,
    EXPR_NULL,
    EXPR_PARAM,       /* $param */
    EXPR_BINOP,       /* left OP right */
    EXPR_UNOP,        /* NOT expr */
    EXPR_FUNC,        /* count(n) */
    EXPR_LIST,        /* [1, 2, 3] */
    EXPR_MAP,         /* {key: val} */
    EXPR_CASE         /* CASE WHEN ... END */
} ExprType;

typedef struct Expr Expr;

typedef struct {
    char  key[MAX_IDENT_LEN];
    Expr *value;
} MapEntry;

struct Expr {
    ExprType type;

    /* EXPR_IDENT, EXPR_STRING, EXPR_PARAM */
    char sval[MAX_EXPR_LEN];

    /* EXPR_PROP */
    char var[MAX_IDENT_LEN];
    char prop[MAX_IDENT_LEN];

    /* EXPR_INT */
    long long ival;

    /* EXPR_FLOAT */
    double fval;

    /* EXPR_BOOL */
    int bval;

    /* EXPR_BINOP */
    char  op[16];
    Expr *left;
    Expr *right;

    /* EXPR_UNOP */
    Expr *operand;

    /* EXPR_FUNC */
    char  func_name[MAX_IDENT_LEN];
    Expr *args[MAX_ITEMS];
    int   arg_count;
    int   distinct;

    /* EXPR_LIST */
    Expr *list_items[MAX_ITEMS];
    int   list_count;

    /* EXPR_MAP */
    MapEntry map_entries[MAX_ITEMS];
    int      map_count;

    /* alias: AS alias */
    char alias[MAX_IDENT_LEN];
};

/* ------------------------------------------------------------------ */
/*  Node / Relationship patterns                                        */
/* ------------------------------------------------------------------ */

typedef struct {
    char  var[MAX_IDENT_LEN];
    char  labels[MAX_ITEMS][MAX_IDENT_LEN];
    int   label_count;
    Expr *props;        /* EXPR_MAP or NULL */
} NodePattern;

typedef struct {
    char  var[MAX_IDENT_LEN];
    char  types[MAX_ITEMS][MAX_IDENT_LEN];
    int   type_count;
    Expr *props;
    int   dir;          /* 0=undirected 1=right 2=left */
    int   min_hops;     /* variable length: -1 = unset */
    int   max_hops;
} RelPattern;

typedef struct {
    NodePattern nodes[MAX_ITEMS];
    RelPattern  rels[MAX_ITEMS];
    int         node_count;
    int         rel_count;
} Pattern;

/* ------------------------------------------------------------------ */
/*  Clause AST nodes                                                    */
/* ------------------------------------------------------------------ */

typedef struct {
    Pattern  pattern;
    int      optional;
} MatchClause;

typedef struct {
    Expr *condition;   /* WHERE predicate */
} WhereClause;

typedef struct {
    Expr  *items[MAX_ITEMS];
    int    count;
    int    distinct;
} ReturnClause;

typedef struct {
    Pattern pattern;
} CreateClause;

typedef struct {
    Pattern pattern;
    Expr   *on_create[MAX_ITEMS];  /* SET expressions */
    int     on_create_count;
    Expr   *on_match[MAX_ITEMS];
    int     on_match_count;
} MergeClause;

typedef struct {
    char  var[MAX_IDENT_LEN];
    Expr *value;
} SetItem;

typedef struct {
    SetItem items[MAX_ITEMS];
    int     count;
} SetClause;

typedef struct {
    char var[MAX_IDENT_LEN];
    char prop[MAX_IDENT_LEN];   /* empty = remove label */
    char label[MAX_IDENT_LEN];  /* empty = remove property */
} RemoveItem;

typedef struct {
    RemoveItem items[MAX_ITEMS];
    int        count;
} RemoveClause;

typedef struct {
    char vars[MAX_ITEMS][MAX_IDENT_LEN];
    int  count;
    int  detach;
} DeleteClause;

typedef struct {
    Expr *items[MAX_ITEMS];
    int   count;
} WithClause;

typedef struct {
    Expr *list_expr;
    char  var[MAX_IDENT_LEN];
} UnwindClause;

typedef struct {
    Expr *keys[MAX_ITEMS];
    int   ascending[MAX_ITEMS];
    int   count;
} OrderByClause;

typedef struct {
    long long value;
} SkipClause;

typedef struct {
    long long value;
} LimitClause;

/* ------------------------------------------------------------------ */
/*  Clause discriminator                                                */
/* ------------------------------------------------------------------ */

typedef enum {
    CLAUSE_MATCH,
    CLAUSE_WHERE,
    CLAUSE_RETURN,
    CLAUSE_CREATE,
    CLAUSE_MERGE,
    CLAUSE_SET,
    CLAUSE_REMOVE,
    CLAUSE_DELETE,
    CLAUSE_WITH,
    CLAUSE_UNWIND,
    CLAUSE_ORDER_BY,
    CLAUSE_SKIP,
    CLAUSE_LIMIT,
    CLAUSE_UNION
} ClauseType;

typedef struct {
    ClauseType type;
    union {
        MatchClause   match;
        WhereClause   where;
        ReturnClause  ret;
        CreateClause  create;
        MergeClause   merge;
        SetClause     set;
        RemoveClause  remove;
        DeleteClause  del;
        WithClause    with;
        UnwindClause  unwind;
        OrderByClause order_by;
        SkipClause    skip;
        LimitClause   limit;
    } data;
    int all_flag;   /* UNION ALL */
} Clause;

/* ------------------------------------------------------------------ */
/*  Query root                                                          */
/* ------------------------------------------------------------------ */

#define MAX_CLAUSES 64

typedef struct {
    Clause clauses[MAX_CLAUSES];
    int    count;
    char   error[512];
} Query;

/* ------------------------------------------------------------------ */
/*  Parser API                                                          */
/* ------------------------------------------------------------------ */

Query *query_parse(const char *cypher);
void   query_free(Query *q);

#endif /* CYPHER_PARSER_H */
