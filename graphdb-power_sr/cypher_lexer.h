#ifndef CYPHER_LEXER_H
#define CYPHER_LEXER_H

#define TOKEN_VALUE_LEN 256

typedef enum {
    /* literals */
    TOK_IDENT = 0,
    TOK_STRING,
    TOK_INT,
    TOK_FLOAT,
    TOK_PARAM,

    /* punctuation */
    TOK_LPAREN,
    TOK_RPAREN,
    TOK_LBRACKET,
    TOK_RBRACKET,
    TOK_LBRACE,
    TOK_RBRACE,
    TOK_COMMA,
    TOK_SEMICOLON,
    TOK_COLON,
    TOK_DOT,
    TOK_DASH,
    TOK_DASH_MINUS,
    TOK_ARROW_RIGHT,
    TOK_ARROW_LEFT,
    TOK_STAR,
    TOK_PIPE,
    TOK_CARET,
    TOK_SLASH,
    TOK_PERCENT,
    TOK_PLUS,

    /* comparison */
    TOK_EQ,
    TOK_NEQ,
    TOK_LT,
    TOK_GT,
    TOK_LTE,
    TOK_GTE,

    /* keywords */
    TOK_MATCH,
    TOK_OPTIONAL,
    TOK_WHERE,
    TOK_RETURN,
    TOK_CREATE,
    TOK_MERGE,
    TOK_DELETE,
    TOK_DETACH,
    TOK_SET,
    TOK_REMOVE,
    TOK_WITH,
    TOK_UNWIND,
    TOK_ORDER,
    TOK_BY,
    TOK_SKIP,
    TOK_LIMIT,
    TOK_UNION,
    TOK_ALL,
    TOK_DISTINCT,
    TOK_AS,
    TOK_ON,
    TOK_AND,
    TOK_OR,
    TOK_NOT,
    TOK_IN,
    TOK_IS,
    TOK_NULL,
    TOK_TRUE,
    TOK_FALSE,
    TOK_STARTS,
    TOK_ENDS,
    TOK_CONTAINS,
    TOK_EXISTS,
    TOK_ASC,
    TOK_DESC,
    TOK_CASE,
    TOK_WHEN,
    TOK_THEN,
    TOK_ELSE,
    TOK_END_KW,
    TOK_COUNT,
    TOK_COLLECT,
    TOK_SUM,
    TOK_AVG,
    TOK_MIN,
    TOK_MAX,
    TOK_SHORTEST_PATH,

    TOK_EOF,
    TOK_ERROR
} TokenType;

typedef struct {
    TokenType type;
    char      value[TOKEN_VALUE_LEN];
    int       line;
} Token;

typedef struct {
    const char *input;
    int         pos;
    int         len;
    int         line;
} Lexer;

Lexer      *lexer_init(const char *input);
void        lexer_free(Lexer *l);
Token       lexer_next(Lexer *l);
Token       lexer_peek(Lexer *l);
const char *token_type_name(TokenType t);

#endif /* CYPHER_LEXER_H */
