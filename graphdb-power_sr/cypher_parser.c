#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "cypher_parser.h"

/* ------------------------------------------------------------------ */
/*  Parser state                                                        */
/* ------------------------------------------------------------------ */

typedef struct {
    Lexer  *lexer;
    Token   current;
    Token   lookahead;
    int     has_error;
    char    error[512];
    Query  *query;
} Parser;

/* ------------------------------------------------------------------ */
/*  Internal helpers                                                    */
/* ------------------------------------------------------------------ */

static void parser_error(Parser *p, const char *msg)
{
    if (!p->has_error) {
        snprintf(p->error, sizeof(p->error),
                 "line %d: %s (got '%s')", p->current.line, msg,
                 p->current.value);
        p->has_error = 1;
    }
}

static Token advance(Parser *p)
{
    p->current   = p->lookahead;
    p->lookahead = lexer_next(p->lexer);
    return p->current;
}

static int check(Parser *p, TokenType t)
{
    return p->current.type == t;
}

static int check_la(Parser *p, TokenType t)
{
    return p->lookahead.type == t;
}

static int match(Parser *p, TokenType t)
{
    if (check(p, t)) { advance(p); return 1; }
    return 0;
}

static int expect(Parser *p, TokenType t)
{
    if (check(p, t)) { advance(p); return 1; }
    char msg[128];
    snprintf(msg, sizeof(msg), "expected '%s'", token_type_name(t));
    parser_error(p, msg);
    return 0;
}

static int at_clause_boundary(Parser *p)
{
    switch (p->current.type) {
        case TOK_MATCH:   case TOK_OPTIONAL: case TOK_WHERE:
        case TOK_RETURN:  case TOK_CREATE:   case TOK_MERGE:
        case TOK_DELETE:  case TOK_DETACH:   case TOK_SET:
        case TOK_REMOVE:  case TOK_WITH:     case TOK_UNWIND:
        case TOK_ORDER:   case TOK_SKIP:     case TOK_LIMIT:
        case TOK_UNION:   case TOK_EOF:      case TOK_SEMICOLON:
            return 1;
        default: return 0;
    }
}

/* ------------------------------------------------------------------ */
/*  Expression allocator                                                */
/* ------------------------------------------------------------------ */

static Expr *expr_alloc(ExprType t)
{
    Expr *e = calloc(1, sizeof(Expr));
    if (e) { e->type = t; }
    return e;
}

static void expr_free(Expr *e)
{
    if (!e) return;
    if (e->left)    expr_free(e->left);
    if (e->right)   expr_free(e->right);
    if (e->operand) expr_free(e->operand);
    for (int i = 0; i < e->arg_count;  i++) expr_free(e->args[i]);
    for (int i = 0; i < e->list_count; i++) expr_free(e->list_items[i]);
    for (int i = 0; i < e->map_count;  i++) expr_free(e->map_entries[i].value);
    free(e);
}

/* ------------------------------------------------------------------ */
/*  Forward declarations                                                */
/* ------------------------------------------------------------------ */

static Expr   *parse_expr(Parser *p);
static Expr   *parse_or_expr(Parser *p);
static Expr   *parse_and_expr(Parser *p);
static Expr   *parse_not_expr(Parser *p);
static Expr   *parse_comparison(Parser *p);
static Expr   *parse_add_expr(Parser *p);
static Expr   *parse_primary(Parser *p);
static Expr   *parse_map_literal(Parser *p);
static Expr   *parse_list_literal(Parser *p);
static void    parse_pattern(Parser *p, Pattern *pat);
static void    parse_node_pattern(Parser *p, NodePattern *np);
static void    parse_rel_pattern(Parser *p, RelPattern *rp, int dir_left);

/* ------------------------------------------------------------------ */
/*  Expression parsing                                                  */
/* ------------------------------------------------------------------ */

static Expr *parse_expr(Parser *p)       { return parse_or_expr(p); }

static Expr *parse_or_expr(Parser *p)
{
    Expr *left = parse_and_expr(p);
    while (!p->has_error && check(p, TOK_OR)) {
        advance(p);
        Expr *right = parse_and_expr(p);
        Expr *e = expr_alloc(EXPR_BINOP);
        strcpy(e->op, "OR");
        e->left = left; e->right = right;
        left = e;
    }
    return left;
}

static Expr *parse_and_expr(Parser *p)
{
    Expr *left = parse_not_expr(p);
    while (!p->has_error && check(p, TOK_AND)) {
        advance(p);
        Expr *right = parse_not_expr(p);
        Expr *e = expr_alloc(EXPR_BINOP);
        strcpy(e->op, "AND");
        e->left = left; e->right = right;
        left = e;
    }
    return left;
}

static Expr *parse_not_expr(Parser *p)
{
    if (check(p, TOK_NOT)) {
        advance(p);
        Expr *e = expr_alloc(EXPR_UNOP);
        strcpy(e->op, "NOT");
        e->operand = parse_comparison(p);
        return e;
    }
    return parse_comparison(p);
}

static Expr *parse_comparison(Parser *p)
{
    Expr *left = parse_add_expr(p);
    if (p->has_error) return left;

    char op[16] = "";

    if      (check(p, TOK_EQ))  { strcpy(op, "=");  advance(p); }
    else if (check(p, TOK_NEQ)) { strcpy(op, "<>"); advance(p); }
    else if (check(p, TOK_LT))  { strcpy(op, "<");  advance(p); }
    else if (check(p, TOK_GT))  { strcpy(op, ">");  advance(p); }
    else if (check(p, TOK_LTE)) { strcpy(op, "<="); advance(p); }
    else if (check(p, TOK_GTE)) { strcpy(op, ">="); advance(p); }
    else if (check(p, TOK_IN))  { strcpy(op, "IN"); advance(p); }
    else if (check(p, TOK_IS)) {
        advance(p);
        int neg = 0;
        if (check(p, TOK_NOT)) { advance(p); neg = 1; }
        if (!check(p, TOK_NULL)) { parser_error(p, "expected NULL after IS"); return left; }
        advance(p);
        Expr *e = expr_alloc(EXPR_UNOP);
        strcpy(e->op, neg ? "IS NOT NULL" : "IS NULL");
        e->operand = left;
        return e;
    }
    else if (check(p, TOK_STARTS)) {
        advance(p);                                           /* consume STARTS */
        if (check(p, TOK_WITH) || check(p, TOK_IDENT)) advance(p); /* consume WITH */
        strcpy(op, "STARTS WITH");
    }
    else if (check(p, TOK_ENDS)) {
        advance(p);                                           /* consume ENDS */
        if (check(p, TOK_WITH) || check(p, TOK_IDENT)) advance(p); /* consume WITH */
        strcpy(op, "ENDS WITH");
    }
    else if (check(p, TOK_CONTAINS)) {
        advance(p);                                           /* consume CONTAINS */
        strcpy(op, "CONTAINS");
    }

    if (op[0]) {
        /* tokens already consumed above; parse right-hand side directly */
        Expr *right = parse_add_expr(p);
        Expr *e = expr_alloc(EXPR_BINOP);
        strncpy(e->op, op, sizeof(e->op) - 1);
        e->left = left; e->right = right;
        return e;
    }
    return left;
}

static Expr *parse_add_expr(Parser *p)
{
    Expr *left = parse_primary(p);
    while (!p->has_error && (check(p, TOK_PLUS) || check(p, TOK_DASH))) {
        char op[4];
        strcpy(op, check(p, TOK_PLUS) ? "+" : "-");
        advance(p);
        Expr *right = parse_primary(p);
        Expr *e = expr_alloc(EXPR_BINOP);
        strcpy(e->op, op);
        e->left = left; e->right = right;
        left = e;
    }
    return left;
}

static Expr *parse_primary(Parser *p)
{
    /* null / bool */
    if (check(p, TOK_NULL))  { Expr *e = expr_alloc(EXPR_NULL);  advance(p); return e; }
    if (check(p, TOK_TRUE))  { Expr *e = expr_alloc(EXPR_BOOL);  e->bval = 1; advance(p); return e; }
    if (check(p, TOK_FALSE)) { Expr *e = expr_alloc(EXPR_BOOL);  e->bval = 0; advance(p); return e; }

    /* string */
    if (check(p, TOK_STRING)) {
        Expr *e = expr_alloc(EXPR_STRING);
        strncpy(e->sval, p->current.value, MAX_EXPR_LEN - 1);
        advance(p); return e;
    }

    /* int */
    if (check(p, TOK_INT)) {
        Expr *e = expr_alloc(EXPR_INT);
        e->ival = atoll(p->current.value);
        advance(p); return e;
    }

    /* float */
    if (check(p, TOK_FLOAT)) {
        Expr *e = expr_alloc(EXPR_FLOAT);
        e->fval = atof(p->current.value);
        advance(p); return e;
    }

    /* param */
    if (check(p, TOK_PARAM)) {
        Expr *e = expr_alloc(EXPR_PARAM);
        strncpy(e->sval, p->current.value, MAX_EXPR_LEN - 1);
        advance(p); return e;
    }

    /* list literal */
    if (check(p, TOK_LBRACKET)) return parse_list_literal(p);

    /* map literal */
    if (check(p, TOK_LBRACE)) return parse_map_literal(p);

    /* grouped expression */
    if (check(p, TOK_LPAREN)) {
        advance(p);
        Expr *e = parse_expr(p);
        expect(p, TOK_RPAREN);
        return e;
    }

    /* EXISTS { MATCH ... } */
    if (check(p, TOK_EXISTS)) {
        advance(p);
        Expr *e = expr_alloc(EXPR_FUNC);
        strcpy(e->func_name, "EXISTS");
        /* skip subquery block */
        if (check(p, TOK_LBRACE)) {
            int depth = 1; advance(p);
            while (depth > 0 && !check(p, TOK_EOF)) {
                if (check(p, TOK_LBRACE)) depth++;
                if (check(p, TOK_RBRACE)) depth--;
                advance(p);
            }
        }
        return e;
    }

    /* ident, prop access, function call */
    if (check(p, TOK_IDENT) || p->current.type >= TOK_MATCH) {
        char name[MAX_IDENT_LEN];
        strncpy(name, p->current.value, MAX_IDENT_LEN - 1);
        advance(p);

        /* function call */
        if (check(p, TOK_LPAREN)) {
            advance(p);
            Expr *e = expr_alloc(EXPR_FUNC);
            strncpy(e->func_name, name, MAX_IDENT_LEN - 1);
            if (check(p, TOK_DISTINCT)) { e->distinct = 1; advance(p); }
            while (!check(p, TOK_RPAREN) && !check(p, TOK_EOF) && !p->has_error) {
                if (e->arg_count < MAX_ITEMS)
                    e->args[e->arg_count++] = parse_expr(p);
                if (!match(p, TOK_COMMA)) break;
            }
            expect(p, TOK_RPAREN);
            /* alias */
            if (check(p, TOK_AS)) { advance(p); strncpy(e->alias, p->current.value, MAX_IDENT_LEN-1); advance(p); }
            return e;
        }

        /* property access: name.prop */
        if (check(p, TOK_DOT)) {
            advance(p);
            Expr *e = expr_alloc(EXPR_PROP);
            strncpy(e->var,  name, MAX_IDENT_LEN - 1);
            strncpy(e->prop, p->current.value, MAX_IDENT_LEN - 1);
            advance(p);
            /* alias */
            if (check(p, TOK_AS)) { advance(p); strncpy(e->alias, p->current.value, MAX_IDENT_LEN-1); advance(p); }
            return e;
        }

        Expr *e = expr_alloc(EXPR_IDENT);
        strncpy(e->sval, name, MAX_EXPR_LEN - 1);
        if (check(p, TOK_AS)) { advance(p); strncpy(e->alias, p->current.value, MAX_IDENT_LEN-1); advance(p); }
        return e;
    }

    Expr *e = expr_alloc(EXPR_NULL);
    parser_error(p, "unexpected token in expression");
    return e;
}

static Expr *parse_map_literal(Parser *p)
{
    expect(p, TOK_LBRACE);
    Expr *e = expr_alloc(EXPR_MAP);
    while (!check(p, TOK_RBRACE) && !check(p, TOK_EOF) && !p->has_error) {
        if (e->map_count >= MAX_ITEMS) { parser_error(p, "too many map entries"); break; }
        strncpy(e->map_entries[e->map_count].key, p->current.value, MAX_IDENT_LEN - 1);
        advance(p);
        expect(p, TOK_COLON);
        e->map_entries[e->map_count].value = parse_expr(p);
        e->map_count++;
        if (!match(p, TOK_COMMA)) break;
    }
    expect(p, TOK_RBRACE);
    return e;
}

static Expr *parse_list_literal(Parser *p)
{
    expect(p, TOK_LBRACKET);
    Expr *e = expr_alloc(EXPR_LIST);
    while (!check(p, TOK_RBRACKET) && !check(p, TOK_EOF) && !p->has_error) {
        if (e->list_count < MAX_ITEMS)
            e->list_items[e->list_count++] = parse_expr(p);
        if (!match(p, TOK_COMMA)) break;
    }
    expect(p, TOK_RBRACKET);
    return e;
}

/* ------------------------------------------------------------------ */
/*  Pattern parsing                                                     */
/* ------------------------------------------------------------------ */

static void parse_node_pattern(Parser *p, NodePattern *np)
{
    expect(p, TOK_LPAREN);

    /* optional variable */
    if (check(p, TOK_IDENT)) {
        strncpy(np->var, p->current.value, MAX_IDENT_LEN - 1);
        advance(p);
    }

    /* labels :Label1:Label2 */
    while (check(p, TOK_COLON) && !p->has_error) {
        advance(p);
        if (np->label_count < MAX_ITEMS) {
            strncpy(np->labels[np->label_count++],
                    p->current.value, MAX_IDENT_LEN - 1);
            advance(p);
        }
    }

    /* properties */
    if (check(p, TOK_LBRACE)) {
        np->props = parse_map_literal(p);
    }

    expect(p, TOK_RPAREN);
}

static void parse_rel_pattern(Parser *p, RelPattern *rp, int dir_left)
{
    /* -[...] or -[...]-> consumed, bracket already consumed by caller */
    if (check(p, TOK_RBRACKET)) { rp->dir = dir_left ? 2 : 0; return; }

    /* optional variable */
    if (check(p, TOK_IDENT)) {
        strncpy(rp->var, p->current.value, MAX_IDENT_LEN - 1);
        advance(p);
    }

    /* types :TYPE1|TYPE2 */
    while (check(p, TOK_COLON) && !p->has_error) {
        advance(p);
        if (rp->type_count < MAX_ITEMS) {
            strncpy(rp->types[rp->type_count++],
                    p->current.value, MAX_IDENT_LEN - 1);
            advance(p);
        }
        if (check(p, TOK_PIPE)) advance(p);
    }

    /* variable length *min..max */
    if (check(p, TOK_STAR)) {
        advance(p);
        rp->min_hops = 1;
        rp->max_hops = -1;
        if (check(p, TOK_INT)) {
            rp->min_hops = atoi(p->current.value);
            advance(p);
        }
        if (check(p, TOK_DOT) && check_la(p, TOK_DOT)) {
            advance(p); advance(p);
            if (check(p, TOK_INT)) {
                rp->max_hops = atoi(p->current.value);
                advance(p);
            }
        }
    }

    /* properties */
    if (check(p, TOK_LBRACE)) rp->props = parse_map_literal(p);
}

static void parse_pattern(Parser *p, Pattern *pat)
{
    parse_node_pattern(p, &pat->nodes[pat->node_count++]);

    while (!p->has_error) {
        int dir_left = 0;

        if (!check(p, TOK_DASH) && !check(p, TOK_ARROW_LEFT)) break;

        if (check(p, TOK_ARROW_LEFT)) { dir_left = 1; advance(p); }
        else advance(p); /* consume '-' */

        if (check(p, TOK_LBRACKET)) {
            advance(p);
            RelPattern *rp = &pat->rels[pat->rel_count++];
            parse_rel_pattern(p, rp, dir_left);
            expect(p, TOK_RBRACKET);
        } else {
            /* implicit rel with no variable */
            pat->rel_count++;
        }

        /* consume trailing -> or - */
        if (check(p, TOK_ARROW_RIGHT)) { pat->rels[pat->rel_count-1].dir = 1; advance(p); }
        else if (check(p, TOK_DASH))   { advance(p); }

        if (pat->node_count < MAX_ITEMS)
            parse_node_pattern(p, &pat->nodes[pat->node_count++]);
    }
}

/* ------------------------------------------------------------------ */
/*  Clause parsers                                                      */
/* ------------------------------------------------------------------ */

static void parse_match(Parser *p, Clause *c, int optional)
{
    c->type = CLAUSE_MATCH;
    c->data.match.optional = optional;
    parse_pattern(p, &c->data.match.pattern);
}

static void parse_where(Parser *p, Clause *c)
{
    c->type = CLAUSE_WHERE;
    c->data.where.condition = parse_expr(p);
}

static void parse_return(Parser *p, Clause *c)
{
    c->type = CLAUSE_RETURN;
    if (check(p, TOK_DISTINCT)) { c->data.ret.distinct = 1; advance(p); }

    while (!at_clause_boundary(p) && !p->has_error) {
        if (c->data.ret.count >= MAX_ITEMS) break;
        c->data.ret.items[c->data.ret.count++] = parse_expr(p);
        if (!match(p, TOK_COMMA)) break;
    }
}

static void parse_create(Parser *p, Clause *c)
{
    c->type = CLAUSE_CREATE;
    parse_pattern(p, &c->data.create.pattern);
}

static void parse_merge(Parser *p, Clause *c)
{
    c->type = CLAUSE_MERGE;
    parse_pattern(p, &c->data.merge.pattern);

    while (check(p, TOK_ON) && !p->has_error) {
        advance(p);
        int is_create = 0;
        if (check(p, TOK_CREATE)) { is_create = 1; advance(p); }
        else if (check(p, TOK_MATCH)) { advance(p); }

        if (!match(p, TOK_SET)) { parser_error(p, "expected SET after ON CREATE/MATCH"); break; }

        Expr **arr   = is_create ? c->data.merge.on_create : c->data.merge.on_match;
        int  *cnt    = is_create ? &c->data.merge.on_create_count : &c->data.merge.on_match_count;

        while (!at_clause_boundary(p) && !p->has_error) {
            if (*cnt >= MAX_ITEMS) break;
            arr[(*cnt)++] = parse_expr(p);
            if (!match(p, TOK_COMMA)) break;
        }
    }
}

static void parse_set(Parser *p, Clause *c)
{
    c->type = CLAUSE_SET;
    while (!at_clause_boundary(p) && !p->has_error) {
        if (c->data.set.count >= MAX_ITEMS) break;
        SetItem *item = &c->data.set.items[c->data.set.count++];
        strncpy(item->var, p->current.value, MAX_IDENT_LEN - 1);
        advance(p);
        expect(p, TOK_DOT);
        strncpy(item->var + strlen(item->var), ".", 1);
        strncpy(item->var + strlen(item->var), p->current.value, MAX_IDENT_LEN - strlen(item->var) - 1);
        advance(p);
        expect(p, TOK_EQ);
        item->value = parse_expr(p);
        if (!match(p, TOK_COMMA)) break;
    }
}

static void parse_remove(Parser *p, Clause *c)
{
    c->type = CLAUSE_REMOVE;
    while (!at_clause_boundary(p) && !p->has_error) {
        if (c->data.remove.count >= MAX_ITEMS) break;
        RemoveItem *item = &c->data.remove.items[c->data.remove.count++];
        strncpy(item->var, p->current.value, MAX_IDENT_LEN - 1);
        advance(p);
        if (check(p, TOK_DOT)) {
            advance(p);
            strncpy(item->prop, p->current.value, MAX_IDENT_LEN - 1);
            advance(p);
        } else if (check(p, TOK_COLON)) {
            advance(p);
            strncpy(item->label, p->current.value, MAX_IDENT_LEN - 1);
            advance(p);
        }
        if (!match(p, TOK_COMMA)) break;
    }
}

static void parse_delete(Parser *p, Clause *c, int detach)
{
    c->type = CLAUSE_DELETE;
    c->data.del.detach = detach;
    while (!at_clause_boundary(p) && !p->has_error) {
        if (c->data.del.count >= MAX_ITEMS) break;
        strncpy(c->data.del.vars[c->data.del.count++],
                p->current.value, MAX_IDENT_LEN - 1);
        advance(p);
        if (!match(p, TOK_COMMA)) break;
    }
}

static void parse_with(Parser *p, Clause *c)
{
    c->type = CLAUSE_WITH;
    if (check(p, TOK_DISTINCT)) advance(p);
    while (!at_clause_boundary(p) && !p->has_error) {
        if (c->data.with.count >= MAX_ITEMS) break;
        c->data.with.items[c->data.with.count++] = parse_expr(p);
        if (!match(p, TOK_COMMA)) break;
    }
}

static void parse_unwind(Parser *p, Clause *c)
{
    c->type = CLAUSE_UNWIND;
    c->data.unwind.list_expr = parse_expr(p);
    if (check(p, TOK_AS)) {
        advance(p);
        strncpy(c->data.unwind.var, p->current.value, MAX_IDENT_LEN - 1);
        advance(p);
    }
}

static void parse_order_by(Parser *p, Clause *c)
{
    c->type = CLAUSE_ORDER_BY;
    while (!at_clause_boundary(p) && !p->has_error) {
        if (c->data.order_by.count >= MAX_ITEMS) break;
        int i = c->data.order_by.count++;
        c->data.order_by.keys[i]      = parse_expr(p);
        c->data.order_by.ascending[i] = 1;
        if      (check(p, TOK_ASC))  { advance(p); }
        else if (check(p, TOK_DESC)) { c->data.order_by.ascending[i] = 0; advance(p); }
        if (!match(p, TOK_COMMA)) break;
    }
}

/* ------------------------------------------------------------------ */
/*  Query root                                                          */
/* ------------------------------------------------------------------ */

Query *query_parse(const char *cypher)
{
    Query *q = calloc(1, sizeof(Query));
    if (!q) return NULL;

    Lexer *lexer = lexer_init(cypher);
    if (!lexer) { free(q); return NULL; }

    Parser p;
    memset(&p, 0, sizeof(p));
    p.lexer   = lexer;
    p.query   = q;
    p.current = lexer_next(lexer);
    p.lookahead = lexer_next(lexer);

    while (!check(&p, TOK_EOF) && !check(&p, TOK_SEMICOLON) && !p.has_error) {
        if (q->count >= MAX_CLAUSES) { parser_error(&p, "too many clauses"); break; }
        Clause *c = &q->clauses[q->count++];

        switch (p.current.type) {
            case TOK_MATCH:
                advance(&p);
                parse_match(&p, c, 0);
                break;
            case TOK_OPTIONAL:
                advance(&p);
                if (!match(&p, TOK_MATCH)) parser_error(&p, "expected MATCH after OPTIONAL");
                parse_match(&p, c, 1);
                break;
            case TOK_WHERE:
                advance(&p);
                parse_where(&p, c);
                break;
            case TOK_RETURN:
                advance(&p);
                parse_return(&p, c);
                break;
            case TOK_CREATE:
                advance(&p);
                parse_create(&p, c);
                break;
            case TOK_MERGE:
                advance(&p);
                parse_merge(&p, c);
                break;
            case TOK_SET:
                advance(&p);
                parse_set(&p, c);
                break;
            case TOK_REMOVE:
                advance(&p);
                parse_remove(&p, c);
                break;
            case TOK_DETACH:
                advance(&p);
                if (!match(&p, TOK_DELETE)) parser_error(&p, "expected DELETE after DETACH");
                parse_delete(&p, c, 1);
                break;
            case TOK_DELETE:
                advance(&p);
                parse_delete(&p, c, 0);
                break;
            case TOK_WITH:
                advance(&p);
                parse_with(&p, c);
                break;
            case TOK_UNWIND:
                advance(&p);
                parse_unwind(&p, c);
                break;
            case TOK_ORDER:
                advance(&p);
                if (!match(&p, TOK_BY)) parser_error(&p, "expected BY after ORDER");
                parse_order_by(&p, c);
                break;
            case TOK_SKIP:
                advance(&p);
                c->type = CLAUSE_SKIP;
                c->data.skip.value = atoll(p.current.value);
                advance(&p);
                break;
            case TOK_LIMIT:
                advance(&p);
                c->type = CLAUSE_LIMIT;
                c->data.limit.value = atoll(p.current.value);
                advance(&p);
                break;
            case TOK_UNION:
                advance(&p);
                c->type     = CLAUSE_UNION;
                c->all_flag = match(&p, TOK_ALL);
                break;
            default:
                parser_error(&p, "unexpected token");
                advance(&p);
                break;
        }
    }

    if (p.has_error)
        strncpy(q->error, p.error, sizeof(q->error) - 1);

    lexer_free(lexer);
    return q;
}

void query_free(Query *q)
{
    if (!q) return;
    /* free expressions in each clause */
    for (int i = 0; i < q->count; i++) {
        Clause *c = &q->clauses[i];
        switch (c->type) {
            case CLAUSE_WHERE:
                expr_free(c->data.where.condition); break;
            case CLAUSE_RETURN:
                for (int j = 0; j < c->data.ret.count; j++)
                    expr_free(c->data.ret.items[j]);
                break;
            case CLAUSE_SET:
                for (int j = 0; j < c->data.set.count; j++)
                    expr_free(c->data.set.items[j].value);
                break;
            case CLAUSE_MERGE:
                for (int j = 0; j < c->data.merge.on_create_count; j++)
                    expr_free(c->data.merge.on_create[j]);
                for (int j = 0; j < c->data.merge.on_match_count; j++)
                    expr_free(c->data.merge.on_match[j]);
                break;
            case CLAUSE_WITH:
                for (int j = 0; j < c->data.with.count; j++)
                    expr_free(c->data.with.items[j]);
                break;
            case CLAUSE_UNWIND:
                expr_free(c->data.unwind.list_expr); break;
            case CLAUSE_ORDER_BY:
                for (int j = 0; j < c->data.order_by.count; j++)
                    expr_free(c->data.order_by.keys[j]);
                break;
            default: break;
        }
    }
    free(q);
}
