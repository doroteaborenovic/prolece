grammar Expr;

program
    :  expr EOF
    ;

expr : selectExpr
    (joinExpr)*
    ;

selectExpr
    : SELECT ( OP | selectList) 
        fromExpr (whereExpr)? SEMI
    ;
selectList
    : selectItem (COMMA selectItem)*
    ;

selectItem
    : table_name DOT column
    | table_name DOT OP
    ;

whereExpr
    :WHERE condition
    ;

condition
    : NOT condition                # notCond
    | condition AND condition      # andCond
    | condition OR condition       # orCond
    | Lzagrada condition Dzagrada  # parenCond
    | tacnost                      # equalityCond
    ;
tacnost
    : term uporedi term            # uporedjivanje
    | term IS NULL                 # isNullProvera
    ;
term
    : INT                          # intVal
    | STRING                       # stringVal
    | table_name DOT column        # fullColumnVal
    | column                       # simpleColumnVal
    ;

table_name : ID ;
column     : ID ;

uporedi
    : EQ | NEQ | LT | GT | LTE | GTE
    ;


fromExpr: FROM table_name ;

joinExpr: JOIN table_name ON stat ;

stat: table_name DOT column EQ table_name DOT column ;

EQ     : '=';
NEQ    : '!=';
LT     : '<';
GT     : '>';
LTE    : '<=';
GTE    : '>=';
DOT    : '.';
COMMA  : ',';
SEMI   : ';';
OP     : '*';

SELECT : 'SELECT';
FROM   : 'FROM';
JOIN   : 'JOIN';
ON     : 'ON';
WHERE  : 'WHERE';
NOT    : 'NOT';
AND    : 'AND';
OR     : 'OR';
IS     : 'IS';
NULL   : 'NULL';
Lzagrada: '(';
Dzagrada: ')';

INT :[0-9]+;
ID     : [a-zA-Z][a-zA-Z0-9_]*;
STRING : '"' ( ~["] | '\\' . )* '"' ;

WS
    : [ \t\r\n]+ -> skip
    ;
