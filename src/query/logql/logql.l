%%
\{ "LBRACE"
\} "RBRACE"
=~ "RE"
!~ "NRE"
!= "NEQ"
= "EQ"
, "COMMA"
[a-zA-Z_][a-zA-Z0-9_]* "IDENTIFIER"
("([^"\\]|\\.)*"|`[^`]*`) "STRING"
[\t\r\n ]+ ;
