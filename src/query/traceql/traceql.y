%start Root
%avoid_insert "IDENTIFIER" "STRING" "NUMBER" "DURATION"
%left "OR2"
%left "AND2"
%left "DESC" "ANC"
%%
Root -> Result<SpansetExpr, ()>:
    SpansetExpr { $1 }
  ;

SpansetExpr -> Result<SpansetExpr, ()>:
    Spanset { Ok(SpansetExpr::Filter($1?)) }
  | SpansetExpr "AND2" SpansetExpr { sop(SpansetOp::And,        $1, $3) }
  | SpansetExpr "OR2"  SpansetExpr { sop(SpansetOp::Or,         $1, $3) }
  | SpansetExpr "DESC" SpansetExpr { sop(SpansetOp::Descendant, $1, $3) }
  | SpansetExpr "ANC"  SpansetExpr { sop(SpansetOp::Ancestor,   $1, $3) }
  | "LPAREN" SpansetExpr "RPAREN" { $2 }
  ;

Spanset -> Result<Option<FieldExpr>, ()>:
    "LBRACE" "RBRACE"           { Ok(None) }
  | "LBRACE" FieldExpr "RBRACE" { Ok(Some($2?)) }
  ;

FieldExpr -> Result<FieldExpr, ()>:
    FieldExpr "AND2" FieldExpr { Ok(FieldExpr::And(Box::new($1?), Box::new($3?))) }
  | FieldExpr "OR2"  FieldExpr { Ok(FieldExpr::Or(Box::new($1?), Box::new($3?))) }
  | "LPAREN" FieldExpr "RPAREN" { $2 }
  | Field FOp Field { Ok(FieldExpr::Cmp { lhs: Box::new($1?), op: $2, rhs: Box::new($3?) }) }
  | Field { Ok(FieldExpr::Field(Box::new($1?))) }
  ;

FOp -> FieldOp:
    "EQ"  { FieldOp::Eq }
  | "NEQ" { FieldOp::Neq }
  | "RE"  { FieldOp::Re }
  | "NRE" { FieldOp::Nre }
  | "GT"  { FieldOp::Gt }
  | "GTE" { FieldOp::Gte }
  | "LT"  { FieldOp::Lt }
  | "LTE" { FieldOp::Lte }
  ;

Field -> Result<Field, ()>:
    IdentPath {
        // A bare single identifier is an intrinsic (name, duration, status, …);
        // a bare *dotted* path (e.g. Grafana's `service.name`) is an unscoped
        // attribute — Tempo accepts it without a leading dot or scope.
        let p = $1?;
        if p.contains('.') {
            Ok(Field::Attr { scope: AttrScope::Unscoped, path: p })
        } else {
            Ok(Field::Intrinsic(p))
        }
    }
  | "TRUE"  { Ok(Field::Bool(true)) }
  | "FALSE" { Ok(Field::Bool(false)) }
  | "STRING" { Ok(Field::Str(unquote($lexer.span_str($1.map_err(|_| ())?.span())))) }
  | "NUMBER" {
        $lexer.span_str($1.map_err(|_| ())?.span())
            .parse::<f64>().map(Field::Num).map_err(|_| ())
    }
  | "DURATION" { Ok(Field::Duration($lexer.span_str($1.map_err(|_| ())?.span()).to_string())) }
  | "DOT" IdentPath { Ok(Field::Attr { scope: AttrScope::Unscoped, path: $2? }) }
  | Scope "DOT" IdentPath { Ok(Field::Attr { scope: $1, path: $3? }) }
  ;

Scope -> AttrScope:
    "SPAN"            { AttrScope::Span }
  | "RESOURCE"        { AttrScope::Resource }
  | "EVENT"           { AttrScope::Event }
  | "INSTRUMENTATION" { AttrScope::Instrumentation }
  | "LINK"            { AttrScope::Link }
  | "PARENT"          { AttrScope::Parent }
  ;

IdentPath -> Result<String, ()>:
    "IDENTIFIER" { Ok($lexer.span_str($1.map_err(|_| ())?.span()).to_string()) }
  | IdentPath "DOT" "IDENTIFIER" {
        let mut s = $1?;
        s.push('.');
        s.push_str($lexer.span_str($3.map_err(|_| ())?.span()));
        Ok(s)
    }
  ;
%%
use crate::query::traceql::ast::{AttrScope, Field, FieldExpr, FieldOp, SpansetExpr, SpansetOp};

/// Build a spanset-combining expression.
fn sop(
    op: SpansetOp,
    lhs: Result<SpansetExpr, ()>,
    rhs: Result<SpansetExpr, ()>,
) -> Result<SpansetExpr, ()> {
    Ok(SpansetExpr::Op { lhs: Box::new(lhs?), op, rhs: Box::new(rhs?) })
}

/// Strip surrounding double quotes and unescape Go-style escapes.
fn unquote(s: &str) -> String {
    let inner = s
        .strip_prefix('"')
        .and_then(|x| x.strip_suffix('"'))
        .unwrap_or(s);
    crate::query::loki::unescape_dquoted(inner)
}
