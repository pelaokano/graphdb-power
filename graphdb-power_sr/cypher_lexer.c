#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "cypher_lexer.h"

/* ------------------------------------------------------------------ */
/*  Keyword table                                                       */
/* ------------------------------------------------------------------ */

typedef struct { const char *word; TokenType type; } Keyword;

static const Keyword KEYWORDS[] = {
    { "MATCH",        TOK_MATCH        },
    { "OPTIONAL",     TOK_OPTIONAL     },
    { "WHERE",        TOK_WHERE        },
    { "RETURN",       TOK_RETURN       },
    { "CREATE",       TOK_CREATE       },
    { "MERGE",        TOK_MERGE        },
    { "DELETE",       TOK_DELETE       },
    { "DETACH",       TOK_DETACH       },
    { "SET",          TOK_SET          },
    { "REMOVE",       TOK_REMOVE       },
    { "WITH",         TOK_WITH         },
    { "UNWIND",       TOK_UNWIND       },
    { "ORDER",        TOK_ORDER        },
    { "BY",           TOK_BY           },
    { "SKIP",         TOK_SKIP         },
    { "LIMIT",        TOK_LIMIT        },
    { "UNION",        TOK_UNION        },
    { "ALL",          TOK_ALL          },
    { "DISTINCT",     TOK_DISTINCT     },
    { "AS",           TOK_AS           },
    { "ON",           TOK_ON           },
    { "AND",          TOK_AND          },
    { "OR",           TOK_OR           },
    { "NOT",          TOK_NOT          },
    { "IN",           TOK_IN           },
    { "IS",           TOK_IS           },
    { "NULL",         TOK_NULL         },
    { "TRUE",         TOK_TRUE         },
    { "FALSE",        TOK_FALSE        },
    { "STARTS",       TOK_STARTS       },
    { "ENDS",         TOK_ENDS         },
    { "CONTAINS",     TOK_CONTAINS     },
    { "EXISTS",       TOK_EXISTS       },
    { "ASC",          TOK_ASC          },
    { "DESC",         TOK_DESC         },
    { "CASE",         TOK_CASE         },
    { "WHEN",         TOK_WHEN         },
    { "THEN",         TOK_THEN         },
    { "ELSE",         TOK_ELSE         },
    { "END",          TOK_END_KW       },
    { "COUNT",        TOK_COUNT        },
    { "COLLECT",      TOK_COLLECT      },
    { "SUM",          TOK_SUM          },
    { "AVG",          TOK_AVG          },
    { "MIN",          TOK_MIN          },
    { "MAX",          TOK_MAX          },
    { "shortestPath", TOK_SHORTEST_PATH},
    { "WITH",         TOK_WITH         },
    { NULL,           TOK_EOF          }
};

/* ------------------------------------------------------------------ */
/*  Lexer init / free                                                   */
/* ------------------------------------------------------------------ */

Lexer *lexer_init(const char *input)
{
    Lexer *l = calloc(1, sizeof(Lexer));
    if (!l) return NULL;
    l->input  = input;
    l->len    = (int)strlen(input);
    l->pos    = 0;
    l->line   = 1;
    return l;
}

void lexer_free(Lexer *l)
{
    free(l);
}

/* ------------------------------------------------------------------ */
/*  Internal helpers                                                    */
/* ------------------------------------------------------------------ */

static char peek(Lexer *l)
{
    return (l->pos < l->len) ? l->input[l->pos] : '\0';
}

static char advance(Lexer *l)
{
    char c = l->input[l->pos++];
    if (c == '\n') l->line++;
    return c;
}

static void skip_whitespace(Lexer *l)
{
    while (l->pos < l->len && isspace((unsigned char)peek(l)))
        advance(l);
}

static void skip_line_comment(Lexer *l)
{
    while (l->pos < l->len && peek(l) != '\n')
        advance(l);
}

static Token make_tok(TokenType type, const char *val, int line)
{
    Token t;
    t.type = type;
    t.line = line;
    strncpy(t.value, val ? val : "", TOKEN_VALUE_LEN - 1);
    t.value[TOKEN_VALUE_LEN - 1] = '\0';
    return t;
}

static TokenType check_keyword(const char *word)
{
    char upper[TOKEN_VALUE_LEN];
    int i;
    for (i = 0; word[i] && i < TOKEN_VALUE_LEN - 1; i++)
        upper[i] = (char)toupper((unsigned char)word[i]);
    upper[i] = '\0';

    for (int k = 0; KEYWORDS[k].word != NULL; k++) {
        if (strcmp(upper, KEYWORDS[k].word) == 0)
            return KEYWORDS[k].type;
        /* case sensitive: shortestPath */
        if (strcmp(word, KEYWORDS[k].word) == 0)
            return KEYWORDS[k].type;
    }
    return TOK_IDENT;
}

/* ------------------------------------------------------------------ */
/*  Next token                                                          */
/* ------------------------------------------------------------------ */

Token lexer_next(Lexer *l)
{
    skip_whitespace(l);

    if (l->pos >= l->len)
        return make_tok(TOK_EOF, "", l->line);

    int line = l->line;
    char c   = peek(l);

    /* line comment */
    if (c == '/' && l->pos + 1 < l->len && l->input[l->pos + 1] == '/') {
        skip_line_comment(l);
        return lexer_next(l);
    }

    /* single char tokens */
    switch (c) {
        case '(': advance(l); return make_tok(TOK_LPAREN,    "(", line);
        case ')': advance(l); return make_tok(TOK_RPAREN,    ")", line);
        case '[': advance(l); return make_tok(TOK_LBRACKET,  "[", line);
        case ']': advance(l); return make_tok(TOK_RBRACKET,  "]", line);
        case '{': advance(l); return make_tok(TOK_LBRACE,    "{", line);
        case '}': advance(l); return make_tok(TOK_RBRACE,    "}", line);
        case ',': advance(l); return make_tok(TOK_COMMA,     ",", line);
        case ';': advance(l); return make_tok(TOK_SEMICOLON, ";", line);
        case ':': advance(l); return make_tok(TOK_COLON,     ":", line);
        case '.': advance(l); return make_tok(TOK_DOT,       ".", line);
        case '+': advance(l); return make_tok(TOK_PLUS,      "+", line);
        case '/': advance(l); return make_tok(TOK_SLASH,     "/", line);
        case '%': advance(l); return make_tok(TOK_PERCENT,   "%", line);
        case '|': advance(l); return make_tok(TOK_PIPE,      "|", line);
        case '^': advance(l); return make_tok(TOK_CARET,     "^", line);
        case '=': advance(l); return make_tok(TOK_EQ,        "=", line);
        case '*':
            advance(l);
            return make_tok(TOK_STAR, "*", line);
    }

    /* two char operators */
    if (c == '-') {
        advance(l);
        if (peek(l) == '>') { advance(l); return make_tok(TOK_ARROW_RIGHT, "->", line); }
        if (peek(l) == '[') return make_tok(TOK_DASH, "-", line);
        return make_tok(TOK_DASH, "-", line);
    }
    if (c == '<') {
        advance(l);
        if (peek(l) == '-') { advance(l); return make_tok(TOK_ARROW_LEFT, "<-", line); }
        if (peek(l) == '>') { advance(l); return make_tok(TOK_NEQ,        "<>", line); }
        if (peek(l) == '=') { advance(l); return make_tok(TOK_LTE,        "<=", line); }
        return make_tok(TOK_LT, "<", line);
    }
    if (c == '>') {
        advance(l);
        if (peek(l) == '=') { advance(l); return make_tok(TOK_GTE, ">=", line); }
        return make_tok(TOK_GT, ">", line);
    }
    if (c == '!') {
        advance(l);
        if (peek(l) == '=') { advance(l); return make_tok(TOK_NEQ, "!=", line); }
        return make_tok(TOK_ERROR, "!", line);
    }

    /* string literal */
    if (c == '"' || c == '\'') {
        char quote = c;
        advance(l);
        char buf[TOKEN_VALUE_LEN];
        int  bi = 0;
        while (l->pos < l->len && peek(l) != quote && bi < TOKEN_VALUE_LEN - 1) {
            char ch = advance(l);
            if (ch == '\\' && l->pos < l->len) {
                char esc = advance(l);
                switch (esc) {
                    case 'n':  buf[bi++] = '\n'; break;
                    case 't':  buf[bi++] = '\t'; break;
                    case '\\': buf[bi++] = '\\'; break;
                    default:   buf[bi++] = esc;  break;
                }
            } else {
                buf[bi++] = ch;
            }
        }
        if (peek(l) == quote) advance(l);
        buf[bi] = '\0';
        return make_tok(TOK_STRING, buf, line);
    }

    /* integer / float */
    if (isdigit((unsigned char)c)) {
        char buf[TOKEN_VALUE_LEN];
        int  bi = 0;
        int  is_float = 0;
        while (l->pos < l->len &&
               (isdigit((unsigned char)peek(l)) || peek(l) == '.') &&
               bi < TOKEN_VALUE_LEN - 1) {
            if (peek(l) == '.') is_float = 1;
            buf[bi++] = advance(l);
        }
        buf[bi] = '\0';
        return make_tok(is_float ? TOK_FLOAT : TOK_INT, buf, line);
    }

    /* backtick-escaped identifier */
    if (c == '`') {
        advance(l);
        char buf[TOKEN_VALUE_LEN];
        int bi = 0;
        while (l->pos < l->len && peek(l) != '`' && bi < TOKEN_VALUE_LEN - 1)
            buf[bi++] = advance(l);
        if (peek(l) == '`') advance(l);
        buf[bi] = '\0';
        return make_tok(TOK_IDENT, buf, line);
    }

    /* identifier or keyword */
    if (isalpha((unsigned char)c) || c == '_') {
        char buf[TOKEN_VALUE_LEN];
        int bi = 0;
        while (l->pos < l->len &&
               (isalnum((unsigned char)peek(l)) || peek(l) == '_') &&
               bi < TOKEN_VALUE_LEN - 1)
            buf[bi++] = advance(l);
        buf[bi] = '\0';
        TokenType kw = check_keyword(buf);
        return make_tok(kw, buf, line);
    }

    /* parameter $name */
    if (c == '$') {
        advance(l);
        char buf[TOKEN_VALUE_LEN];
        int bi = 0;
        buf[bi++] = '$';
        while (l->pos < l->len &&
               (isalnum((unsigned char)peek(l)) || peek(l) == '_') &&
               bi < TOKEN_VALUE_LEN - 1)
            buf[bi++] = advance(l);
        buf[bi] = '\0';
        return make_tok(TOK_PARAM, buf, line);
    }

    /* unknown */
    char unknown[2] = { advance(l), '\0' };
    return make_tok(TOK_ERROR, unknown, line);
}

Token lexer_peek(Lexer *l)
{
    int   saved_pos  = l->pos;
    int   saved_line = l->line;
    Token t          = lexer_next(l);
    l->pos  = saved_pos;
    l->line = saved_line;
    return t;
}

const char *token_type_name(TokenType t)
{
    switch (t) {
        case TOK_IDENT:        return "IDENT";
        case TOK_STRING:       return "STRING";
        case TOK_INT:          return "INT";
        case TOK_FLOAT:        return "FLOAT";
        case TOK_PARAM:        return "PARAM";
        case TOK_LPAREN:       return "(";
        case TOK_RPAREN:       return ")";
        case TOK_LBRACKET:     return "[";
        case TOK_RBRACKET:     return "]";
        case TOK_LBRACE:       return "{";
        case TOK_RBRACE:       return "}";
        case TOK_COMMA:        return ",";
        case TOK_SEMICOLON:    return ";";
        case TOK_COLON:        return ":";
        case TOK_DOT:          return ".";
        case TOK_DASH:         return "-";
        case TOK_ARROW_RIGHT:  return "->";
        case TOK_ARROW_LEFT:   return "<-";
        case TOK_STAR:         return "*";
        case TOK_EQ:           return "=";
        case TOK_NEQ:          return "<>";
        case TOK_LT:           return "<";
        case TOK_GT:           return ">";
        case TOK_LTE:          return "<=";
        case TOK_GTE:          return ">=";
        case TOK_PLUS:         return "+";
        case TOK_DASH_MINUS:   return "-";
        case TOK_MATCH:        return "MATCH";
        case TOK_WHERE:        return "WHERE";
        case TOK_RETURN:       return "RETURN";
        case TOK_CREATE:       return "CREATE";
        case TOK_DELETE:       return "DELETE";
        case TOK_SET:          return "SET";
        case TOK_MERGE:        return "MERGE";
        case TOK_AND:          return "AND";
        case TOK_OR:           return "OR";
        case TOK_NOT:          return "NOT";
        case TOK_EOF:          return "EOF";
        default:               return "?";
    }
}
