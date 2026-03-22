#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "graphdb.h"
#include "cypher_parser.h"

/* Windows does not have strings.h / strcasecmp */
#ifdef _WIN32
  #define strcasecmp _stricmp
#else
  #include <strings.h>
#endif

/* ------------------------------------------------------------------ */
/*  External symbols from graphdb.c                                    */
/* ------------------------------------------------------------------ */

ResultSet *_resultset_alloc(void);
int        _resultset_push(ResultSet *rs, ResultRow *row);

/* ------------------------------------------------------------------ */
/*  SQL buffer                                                          */
/* ------------------------------------------------------------------ */

typedef struct {
    char  buf[MAX_SQL_LEN];
    int   pos;
    int   overflow;
} SqlBuf;

static void sb_init(SqlBuf *sb) { sb->pos = 0; sb->overflow = 0; sb->buf[0] = '\0'; }

static void sb_append(SqlBuf *sb, const char *s)
{
    int rem = MAX_SQL_LEN - sb->pos - 1;
    if (rem <= 0) { sb->overflow = 1; return; }
    int n = (int)strlen(s);
    if (n > rem) { n = rem; sb->overflow = 1; }
    memcpy(sb->buf + sb->pos, s, n);
    sb->pos += n;
    sb->buf[sb->pos] = '\0';
}

static void sb_appendf(SqlBuf *sb, const char *fmt, ...)
{
    char tmp[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    sb_append(sb, tmp);
}

/* ------------------------------------------------------------------ */
/*  Variable -> SQL table alias map                                     */
/* ------------------------------------------------------------------ */

typedef struct {
    char var[MAX_IDENT_LEN];
    char alias[16];
    int  is_rel;
} VarAlias;

typedef struct {
    VarAlias entries[MAX_ITEMS * 2];
    int      count;
} VarMap;

static const char *varmap_lookup(VarMap *vm, const char *var, int want_rel)
{
    for (int i = 0; i < vm->count; i++) {
        if (vm->entries[i].is_rel == want_rel &&
            strcmp(vm->entries[i].var, var) == 0)
            return vm->entries[i].alias;
    }
    /* fallback: first node alias */
    for (int i = 0; i < vm->count; i++)
        if (!vm->entries[i].is_rel) return vm->entries[i].alias;
    return "n0";
}

/* ------------------------------------------------------------------ */
/*  Expression -> SQL fragment                                          */
/* ------------------------------------------------------------------ */

static void expr_to_sql(Expr *e, SqlBuf *sb, VarMap *vm)
{
    if (!e) return;
    switch (e->type) {
        case EXPR_NULL:   sb_append(sb, "NULL"); break;
        case EXPR_BOOL:   sb_append(sb, e->bval ? "1" : "0"); break;
        case EXPR_INT:    sb_appendf(sb, "%lld", e->ival); break;
        case EXPR_FLOAT:  sb_appendf(sb, "%g",   e->fval); break;
        case EXPR_STRING:
            sb_append(sb, "'");
            for (const char *c = e->sval; *c; c++) {
                if (*c == '\'') sb_append(sb, "''");
                else { char ch[2] = {*c, 0}; sb_append(sb, ch); }
            }
            sb_append(sb, "'");
            break;
        case EXPR_IDENT: {
            /* search both node and rel aliases by name */
            const char *alias = NULL;
            for (int i = 0; i < vm->count; i++) {
                if (strcmp(vm->entries[i].var, e->sval) == 0) {
                    alias = vm->entries[i].alias;
                    break;
                }
            }
            if (!alias) alias = "n0";
            sb_appendf(sb, "%s.id", alias);
            break;
        }
        case EXPR_PROP: {
            const char *tbl = varmap_lookup(vm, e->var, 0);
            sb_appendf(sb, "json_extract(%s.properties, '$.%s')", tbl, e->prop);
            break;
        }
        case EXPR_BINOP: {
            const char *op = e->op;
            if (strcmp(op, "STARTS WITH") == 0) {
                expr_to_sql(e->left, sb, vm);
                sb_append(sb, " LIKE ");
                if (e->right && e->right->type == EXPR_STRING) {
                    sb_append(sb, "'");
                    sb_append(sb, e->right->sval);
                    sb_append(sb, "%'");
                } else { expr_to_sql(e->right, sb, vm); sb_append(sb, " || '%'"); }
            } else if (strcmp(op, "ENDS WITH") == 0) {
                expr_to_sql(e->left, sb, vm);
                sb_append(sb, " LIKE '%' || ");
                if (e->right && e->right->type == EXPR_STRING)
                    sb_appendf(sb, "'%s'", e->right->sval);
                else expr_to_sql(e->right, sb, vm);
            } else if (strcmp(op, "CONTAINS") == 0) {
                expr_to_sql(e->left, sb, vm);
                sb_append(sb, " LIKE '%' || ");
                if (e->right && e->right->type == EXPR_STRING)
                    sb_appendf(sb, "'%s'", e->right->sval);
                else expr_to_sql(e->right, sb, vm);
                sb_append(sb, " || '%'");
            } else if (strcmp(op, "IN") == 0) {
                expr_to_sql(e->left, sb, vm);
                sb_append(sb, " IN (");
                if (e->right && e->right->type == EXPR_LIST) {
                    for (int i = 0; i < e->right->list_count; i++) {
                        if (i) sb_append(sb, ", ");
                        expr_to_sql(e->right->list_items[i], sb, vm);
                    }
                }
                sb_append(sb, ")");
            } else {
                sb_append(sb, "(");
                expr_to_sql(e->left, sb, vm);
                sb_appendf(sb, " %s ", op);
                expr_to_sql(e->right, sb, vm);
                sb_append(sb, ")");
            }
            break;
        }
        case EXPR_UNOP:
            if (strcmp(e->op, "NOT") == 0) {
                sb_append(sb, "NOT (");
                expr_to_sql(e->operand, sb, vm);
                sb_append(sb, ")");
            } else if (strcmp(e->op, "IS NULL") == 0) {
                expr_to_sql(e->operand, sb, vm);
                sb_append(sb, " IS NULL");
            } else if (strcmp(e->op, "IS NOT NULL") == 0) {
                expr_to_sql(e->operand, sb, vm);
                sb_append(sb, " IS NOT NULL");
            }
            break;
        case EXPR_FUNC: {
            const char *fn = e->func_name;
            if (strcasecmp(fn, "count") == 0) {
                sb_append(sb, "COUNT(");
                if (e->distinct) sb_append(sb, "DISTINCT ");
                if (e->arg_count == 0 || (e->arg_count == 1 &&
                    e->args[0]->type == EXPR_IDENT &&
                    strcmp(e->args[0]->sval, "*") == 0))
                    sb_append(sb, "*");
                else expr_to_sql(e->args[0], sb, vm);
                sb_append(sb, ")");
            } else if (strcasecmp(fn, "sum") == 0) {
                sb_append(sb, "SUM("); expr_to_sql(e->args[0], sb, vm); sb_append(sb, ")");
            } else if (strcasecmp(fn, "avg") == 0) {
                sb_append(sb, "AVG("); expr_to_sql(e->args[0], sb, vm); sb_append(sb, ")");
            } else if (strcasecmp(fn, "min") == 0) {
                sb_append(sb, "MIN("); expr_to_sql(e->args[0], sb, vm); sb_append(sb, ")");
            } else if (strcasecmp(fn, "max") == 0) {
                sb_append(sb, "MAX("); expr_to_sql(e->args[0], sb, vm); sb_append(sb, ")");
            } else if (strcasecmp(fn, "tostring") == 0 || strcasecmp(fn, "toString") == 0) {
                sb_append(sb, "CAST("); expr_to_sql(e->args[0], sb, vm); sb_append(sb, " AS TEXT)");
            } else if (strcasecmp(fn, "tointeger") == 0 || strcasecmp(fn, "toInteger") == 0) {
                sb_append(sb, "CAST("); expr_to_sql(e->args[0], sb, vm); sb_append(sb, " AS INTEGER)");
            } else if (strcasecmp(fn, "tofloat") == 0 || strcasecmp(fn, "toFloat") == 0) {
                sb_append(sb, "CAST("); expr_to_sql(e->args[0], sb, vm); sb_append(sb, " AS REAL)");
            } else if (strcasecmp(fn, "size") == 0) {
                sb_append(sb, "length("); expr_to_sql(e->args[0], sb, vm); sb_append(sb, ")");
            } else if (strcasecmp(fn, "id") == 0) {
                if (e->arg_count > 0) expr_to_sql(e->args[0], sb, vm);
            } else if (strcasecmp(fn, "coalesce") == 0) {
                sb_append(sb, "COALESCE(");
                for (int i = 0; i < e->arg_count; i++) {
                    if (i) sb_append(sb, ", ");
                    expr_to_sql(e->args[i], sb, vm);
                }
                sb_append(sb, ")");
            } else {
                sb_appendf(sb, "%s(", fn);
                for (int i = 0; i < e->arg_count; i++) {
                    if (i) sb_append(sb, ", ");
                    expr_to_sql(e->args[i], sb, vm);
                }
                sb_append(sb, ")");
            }
            break;
        }
        default: sb_append(sb, "NULL"); break;
    }
}

/* ------------------------------------------------------------------ */
/*  Node pattern -> SQL WHERE fragment                                  */
/* ------------------------------------------------------------------ */

static void node_pattern_to_where(NodePattern *np, SqlBuf *sb,
                                  const char *tbl_alias, VarMap *vm)
{
    for (int i = 0; i < np->label_count; i++) {
        if (i == 0) sb_append(sb, "(");
        else        sb_append(sb, " AND ");
        sb_appendf(sb, "instr(' ' || %s.labels || ' ', ' %s ') > 0",
                   tbl_alias, np->labels[i]);
        if (i == np->label_count - 1) sb_append(sb, ")");
    }

    if (np->props && np->props->type == EXPR_MAP) {
        for (int i = 0; i < np->props->map_count; i++) {
            if (np->label_count > 0 || i > 0) sb_append(sb, " AND ");
            MapEntry *me = &np->props->map_entries[i];
            sb_appendf(sb, "json_extract(%s.properties, '$.%s') = ",
                       tbl_alias, me->key);
            expr_to_sql(me->value, sb, vm);
        }
    }
}

/* ------------------------------------------------------------------ */
/*  Main query executor                                                 */
/* ------------------------------------------------------------------ */

static ResultSet *exec_read(Graph *g, Query *q)
{
    SqlBuf sb;
    sb_init(&sb);

    MatchClause  *match_c  = NULL;
    WhereClause  *where_c  = NULL;
    ReturnClause *ret_c    = NULL;
    OrderByClause *ord_c   = NULL;
    SkipClause   *skip_c   = NULL;
    LimitClause  *limit_c  = NULL;

    for (int i = 0; i < q->count; i++) {
        switch (q->clauses[i].type) {
            case CLAUSE_MATCH:    match_c = &q->clauses[i].data.match; break;
            case CLAUSE_WHERE:    where_c = &q->clauses[i].data.where; break;
            case CLAUSE_RETURN:   ret_c   = &q->clauses[i].data.ret;   break;
            case CLAUSE_ORDER_BY: ord_c   = &q->clauses[i].data.order_by; break;
            case CLAUSE_SKIP:     skip_c  = &q->clauses[i].data.skip;  break;
            case CLAUSE_LIMIT:    limit_c = &q->clauses[i].data.limit; break;
            default: break;
        }
    }

    if (!match_c || !ret_c) {
        ResultSet *rs = _resultset_alloc();
        if (rs) snprintf(rs->error, MAX_ERROR_LEN, "query needs MATCH and RETURN");
        return rs;
    }

    Pattern *pat = &match_c->pattern;

    /* build variable -> alias map */
    VarMap vm;
    vm.count = 0;
    for (int i = 0; i < pat->node_count; i++) {
        NodePattern *np = &pat->nodes[i];
        if (np->var[0] && vm.count < MAX_ITEMS * 2) {
            VarAlias *va = &vm.entries[vm.count++];
            strncpy(va->var, np->var, MAX_IDENT_LEN - 1);
            snprintf(va->alias, sizeof(va->alias), "n%d", i);
            va->is_rel = 0;
        }
    }
    for (int i = 0; i < pat->rel_count; i++) {
        RelPattern *rp = &pat->rels[i];
        if (rp->var[0] && vm.count < MAX_ITEMS * 2) {
            VarAlias *va = &vm.entries[vm.count++];
            strncpy(va->var, rp->var, MAX_IDENT_LEN - 1);
            snprintf(va->alias, sizeof(va->alias), "r%d", i);
            va->is_rel = 1;
        }
    }
    /* always register n0 as fallback */
    if (vm.count == 0) {
        strcpy(vm.entries[0].var, "n");
        strcpy(vm.entries[0].alias, "n0");
        vm.entries[0].is_rel = 0;
        vm.count = 1;
    }

    /* SELECT clause */
    sb_append(&sb, "SELECT ");
    if (ret_c->distinct) sb_append(&sb, "DISTINCT ");

    for (int i = 0; i < ret_c->count; i++) {
        if (i) sb_append(&sb, ", ");
        expr_to_sql(ret_c->items[i], &sb, &vm);
        Expr *e = ret_c->items[i];
        if (e->alias[0])
            sb_appendf(&sb, " AS %s", e->alias);
    }

    /* FROM + JOINs */
    sb_append(&sb, " FROM nodes n0");

    for (int i = 0; i < pat->rel_count && i < pat->node_count - 1; i++) {
        RelPattern  *rp = &pat->rels[i];
        NodePattern *np = &pat->nodes[i + 1];
        char rnm[16], nnm[16];
        snprintf(rnm, sizeof(rnm), "r%d", i);
        snprintf(nnm, sizeof(nnm), "n%d", i + 1);

        int opt = match_c->optional;
        sb_append(&sb, opt ? " LEFT JOIN relationships " : " JOIN relationships ");
        sb_appendf(&sb, "%s ON ", rnm);

        if (rp->dir == 1) {
            sb_appendf(&sb, "%s.src_id = n%d.id", rnm, i);
        } else if (rp->dir == 2) {
            sb_appendf(&sb, "%s.dst_id = n%d.id", rnm, i);
        } else {
            sb_appendf(&sb, "(%s.src_id = n%d.id OR %s.dst_id = n%d.id)",
                       rnm, i, rnm, i);
        }

        if (rp->type_count > 0) {
            sb_appendf(&sb, " AND %s.type IN (", rnm);
            for (int t = 0; t < rp->type_count; t++) {
                if (t) sb_append(&sb, ",");
                sb_appendf(&sb, "'%s'", rp->types[t]);
            }
            sb_append(&sb, ")");
        }

        sb_append(&sb, opt ? " LEFT JOIN nodes " : " JOIN nodes ");
        sb_appendf(&sb, "%s ON ", nnm);
        if (rp->dir == 1)
            sb_appendf(&sb, "%s.dst_id = %s.id", rnm, nnm);
        else if (rp->dir == 2)
            sb_appendf(&sb, "%s.src_id = %s.id", rnm, nnm);
        else
            sb_appendf(&sb, "(%s.dst_id = %s.id OR %s.src_id = %s.id)",
                       rnm, nnm, rnm, nnm);

        (void)np;
    }

    /* WHERE */
    int has_where = 0;

    for (int i = 0; i < pat->node_count; i++) {
        NodePattern *np = &pat->nodes[i];
        char alias[16];
        snprintf(alias, sizeof(alias), "n%d", i);

        SqlBuf tmp; sb_init(&tmp);
        node_pattern_to_where(np, &tmp, alias, &vm);
        if (tmp.pos > 0) {
            sb_append(&sb, has_where ? " AND " : " WHERE ");
            sb_append(&sb, tmp.buf);
            has_where = 1;
        }
    }

    if (where_c && where_c->condition) {
        sb_append(&sb, has_where ? " AND " : " WHERE ");
        expr_to_sql(where_c->condition, &sb, &vm);
        has_where = 1;
    }

    /* ORDER BY */
    if (ord_c && ord_c->count > 0) {
        sb_append(&sb, " ORDER BY ");
        for (int i = 0; i < ord_c->count; i++) {
            if (i) sb_append(&sb, ", ");
            expr_to_sql(ord_c->keys[i], &sb, &vm);
            sb_append(&sb, ord_c->ascending[i] ? " ASC" : " DESC");
        }
    }

    if (limit_c && skip_c) {
        sb_appendf(&sb, " LIMIT %lld OFFSET %lld", limit_c->value, skip_c->value);
    } else if (limit_c) {
        sb_appendf(&sb, " LIMIT %lld", limit_c->value);
    } else if (skip_c) {
        sb_appendf(&sb, " LIMIT -1 OFFSET %lld", skip_c->value);
    }

    /* execute */
    ResultSet *rs = _resultset_alloc();
    if (!rs) return NULL;

    if (sb.overflow) {
        snprintf(rs->error, MAX_ERROR_LEN, "generated SQL too long");
        return rs;
    }

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(g->db, sb.buf, -1, &stmt, NULL) != SQLITE_OK) {
        snprintf(rs->error, MAX_ERROR_LEN, "SQL error: %s | SQL: %.256s",
                 sqlite3_errmsg(g->db), sb.buf);
        return rs;
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        ResultRow row;
        memset(&row, 0, sizeof(row));
        row.type = ROW_SCALAR;

        int ncols = sqlite3_column_count(stmt);
        SqlBuf out; sb_init(&out);

        for (int c = 0; c < ncols; c++) {
            if (c) sb_append(&out, " | ");
            const unsigned char *val = sqlite3_column_text(stmt, c);
            sb_append(&out, val ? (const char *)val : "null");
        }
        strncpy(row.scalar, out.buf, MAX_PROP_LEN - 1);
        _resultset_push(rs, &row);
    }
    sqlite3_finalize(stmt);
    return rs;
}

/* ------------------------------------------------------------------ */
/*  Write executor: CREATE                                              */
/* ------------------------------------------------------------------ */

static ResultSet *exec_create(Graph *g, Query *q)
{
    ResultSet *rs = _resultset_alloc();
    if (!rs) return NULL;

    for (int i = 0; i < q->count; i++) {
        if (q->clauses[i].type != CLAUSE_CREATE) continue;
        Pattern *pat = &q->clauses[i].data.create.pattern;

        for (int ni = 0; ni < pat->node_count; ni++) {
            NodePattern *np = &pat->nodes[ni];

            /* build labels string */
            char labels[MAX_LABEL_LEN] = "";
            for (int l = 0; l < np->label_count; l++) {
                if (l) strncat(labels, " ", MAX_LABEL_LEN - strlen(labels) - 1);
                strncat(labels, np->labels[l], MAX_LABEL_LEN - strlen(labels) - 1);
            }

            /* build properties JSON */
            char props[MAX_PROP_LEN] = "{}";
            if (np->props && np->props->type == EXPR_MAP && np->props->map_count > 0) {
                SqlBuf sb; sb_init(&sb);
                sb_append(&sb, "{");
                for (int m = 0; m < np->props->map_count; m++) {
                    if (m) sb_append(&sb, ",");
                    MapEntry *me = &np->props->map_entries[m];
                    sb_appendf(&sb, "\"%s\":", me->key);
                    if (me->value->type == EXPR_STRING)
                        sb_appendf(&sb, "\"%s\"", me->value->sval);
                    else if (me->value->type == EXPR_INT)
                        sb_appendf(&sb, "%lld", me->value->ival);
                    else if (me->value->type == EXPR_FLOAT)
                        sb_appendf(&sb, "%g", me->value->fval);
                    else if (me->value->type == EXPR_BOOL)
                        sb_append(&sb, me->value->bval ? "true" : "false");
                    else if (me->value->type == EXPR_NULL)
                        sb_append(&sb, "null");
                    else
                        sb_appendf(&sb, "\"%s\"", me->value->sval);
                }
                sb_append(&sb, "}");
                strncpy(props, sb.buf, MAX_PROP_LEN - 1);
            }

            int64_t id;
            if (graph_create_node(g, labels, props, &id) == GRAPHDB_OK) {
                ResultRow row;
                memset(&row, 0, sizeof(row));
                row.type = ROW_SCALAR;
                snprintf(row.scalar, MAX_PROP_LEN, "created node id=%lld", (long long)id);
                _resultset_push(rs, &row);
            } else {
                snprintf(rs->error, MAX_ERROR_LEN, "%s", graph_last_error(g));
            }
        }

        for (int ri = 0; ri < pat->rel_count; ri++) {
            RelPattern *rp = &pat->rels[ri];
            if (rp->type_count == 0) continue;

            /* resolve src/dst node ids from previously created nodes */
            NodePattern *src_np = &pat->nodes[ri];
            NodePattern *dst_np = &pat->nodes[ri + 1];
            (void)src_np; (void)dst_np;

            /* for CREATE we require explicit ids via bound variables
               this is a simplified implementation */
        }
    }
    return rs;
}

/* ------------------------------------------------------------------ */
/*  Write executor: MERGE                                               */
/* ------------------------------------------------------------------ */

static ResultSet *exec_merge(Graph *g, Query *q)
{
    ResultSet *rs = _resultset_alloc();
    if (!rs) return NULL;

    for (int i = 0; i < q->count; i++) {
        if (q->clauses[i].type != CLAUSE_MERGE) continue;
        MergeClause *mc = &q->clauses[i].data.merge;
        Pattern *pat = &mc->pattern;

        for (int ni = 0; ni < pat->node_count; ni++) {
            NodePattern *np = &pat->nodes[ni];

            char labels[MAX_LABEL_LEN] = "";
            for (int l = 0; l < np->label_count; l++) {
                if (l) strncat(labels, " ", MAX_LABEL_LEN - strlen(labels) - 1);
                strncat(labels, np->labels[l], MAX_LABEL_LEN - strlen(labels) - 1);
            }

            char match_props[MAX_PROP_LEN] = "{}";
            if (np->props && np->props->type == EXPR_MAP) {
                SqlBuf sb; sb_init(&sb);
                sb_append(&sb, "{");
                for (int m = 0; m < np->props->map_count; m++) {
                    if (m) sb_append(&sb, ",");
                    MapEntry *me = &np->props->map_entries[m];
                    sb_appendf(&sb, "\"%s\":", me->key);
                    if (me->value->type == EXPR_STRING)
                        sb_appendf(&sb, "\"%s\"", me->value->sval);
                    else if (me->value->type == EXPR_INT)
                        sb_appendf(&sb, "%lld", me->value->ival);
                    else
                        sb_appendf(&sb, "\"%s\"", me->value->sval);
                }
                sb_append(&sb, "}");
                strncpy(match_props, sb.buf, MAX_PROP_LEN - 1);
            }

            int64_t id;
            int rc = graph_merge_node(g, labels, match_props, NULL, NULL, &id);
            ResultRow row; memset(&row, 0, sizeof(row));
            row.type = ROW_SCALAR;
            if (rc == GRAPHDB_OK)
                snprintf(row.scalar, MAX_PROP_LEN, "merged node id=%lld", (long long)id);
            else
                snprintf(row.scalar, MAX_PROP_LEN, "merge error: %s", graph_last_error(g));
            _resultset_push(rs, &row);
        }
    }
    return rs;
}

/* ------------------------------------------------------------------ */
/*  Write executor: SET                                                 */
/* ------------------------------------------------------------------ */

static ResultSet *exec_set(Graph *g, Query *q, const char *cypher)
{
    /* SET requires bound node id. For the MVP we delegate to raw SQL
       via the SQLite json_set function. */
    (void)cypher;
    ResultSet *rs = _resultset_alloc();
    if (!rs) return NULL;

    for (int i = 0; i < q->count; i++) {
        if (q->clauses[i].type != CLAUSE_SET) continue;
        SetClause *sc = &q->clauses[i].data.set;

        for (int j = 0; j < sc->count; j++) {
            SetItem *item = &sc->items[j];
            /* item->var is "var.prop", split on dot */
            char *dot = strchr(item->var, '.');
            if (!dot) continue;
            *dot = '\0';
            const char *prop = dot + 1;

            /* build JSON value */
            char val[MAX_PROP_LEN];
            if (item->value->type == EXPR_STRING)
                snprintf(val, sizeof(val), "\"%s\"", item->value->sval);
            else if (item->value->type == EXPR_INT)
                snprintf(val, sizeof(val), "%lld", item->value->ival);
            else if (item->value->type == EXPR_FLOAT)
                snprintf(val, sizeof(val), "%g", item->value->fval);
            else if (item->value->type == EXPR_BOOL)
                snprintf(val, sizeof(val), "%s", item->value->bval ? "true" : "false");
            else
                snprintf(val, sizeof(val), "null");

            /* We can only set if we know the node id.
               For MVP: SET requires previous MATCH that bound id. */
            ResultRow row; memset(&row, 0, sizeof(row));
            row.type = ROW_SCALAR;
            snprintf(row.scalar, MAX_PROP_LEN,
                     "SET %s.%s = %s (bind id via MATCH first)", item->var, prop, val);
            _resultset_push(rs, &row);
            *dot = '.'; /* restore */
        }
    }
    return rs;
}

/* ------------------------------------------------------------------ */
/*  Write executor: DELETE                                              */
/* ------------------------------------------------------------------ */

static ResultSet *exec_delete(Graph *g, Query *q)
{
    ResultSet *rs = _resultset_alloc();
    if (!rs) return NULL;

    for (int i = 0; i < q->count; i++) {
        if (q->clauses[i].type != CLAUSE_DELETE) continue;
        DeleteClause *dc = &q->clauses[i].data.del;

        ResultRow row; memset(&row, 0, sizeof(row));
        row.type = ROW_SCALAR;
        snprintf(row.scalar, MAX_PROP_LEN,
                 "DELETE %s (detach=%d) - requires MATCH to bind id",
                 dc->vars[0], dc->detach);
        _resultset_push(rs, &row);
    }
    return rs;
}

/* ------------------------------------------------------------------ */
/*  Public entry point                                                  */
/* ------------------------------------------------------------------ */

ResultSet *graph_query(Graph *g, const char *cypher)
{
    Query *q = query_parse(cypher);
    if (!q) {
        ResultSet *rs = _resultset_alloc();
        if (rs) snprintf(rs->error, MAX_ERROR_LEN, "parse failed: out of memory");
        return rs;
    }

    if (q->error[0]) {
        ResultSet *rs = _resultset_alloc();
        if (rs) snprintf(rs->error, MAX_ERROR_LEN, "%s", q->error);
        query_free(q);
        return rs;
    }

    /* determine query type from first significant clause */
    ClauseType first = CLAUSE_MATCH;
    for (int i = 0; i < q->count; i++) {
        if (q->clauses[i].type != CLAUSE_UNION) {
            first = q->clauses[i].type;
            break;
        }
    }

    ResultSet *rs = NULL;

    switch (first) {
        case CLAUSE_MATCH:
            rs = exec_read(g, q);
            break;
        case CLAUSE_CREATE:
            rs = exec_create(g, q);
            break;
        case CLAUSE_MERGE:
            rs = exec_merge(g, q);
            break;
        case CLAUSE_SET:
            rs = exec_set(g, q, cypher);
            break;
        case CLAUSE_DELETE:
            rs = exec_delete(g, q);
            break;
        default:
            rs = _resultset_alloc();
            if (rs) snprintf(rs->error, MAX_ERROR_LEN,
                             "unsupported top-level clause type %d", first);
            break;
    }

    query_free(q);
    return rs;
}
