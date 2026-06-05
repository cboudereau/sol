%start Root
%avoid_insert "IDENTIFIER" "STRING" "NUMLIT"
%%
Root -> Result<LogPipeline, ()>:
    Pipeline { $1 }
  ;

Pipeline -> Result<LogPipeline, ()>:
    Sel { Ok(LogPipeline { selector: $1?, stages: vec![] }) }
  | Pipeline PStage { let mut p = $1?; p.stages.push($2?); Ok(p) }
  ;

Sel -> Result<Selector, ()>:
    "LBRACE" "RBRACE" { Ok(Selector::default()) }
  | "LBRACE" Matchers "RBRACE" { Ok(Selector { matchers: $2? }) }
  ;

Matchers -> Result<Vec<LabelMatcher>, ()>:
    Matcher { Ok(vec![$1?]) }
  | Matchers "COMMA" Matcher { let mut v = $1?; v.push($3?); Ok(v) }
  ;

Matcher -> Result<LabelMatcher, ()>:
    "IDENTIFIER" MOp "STRING" {
        let name = $lexer.span_str($1.map_err(|_| ())?.span()).to_string();
        let value = unquote($lexer.span_str($3.map_err(|_| ())?.span()));
        Ok(LabelMatcher { name, op: $2, value })
    }
  ;

MOp -> MatchOp:
    "EQ"  { MatchOp::Eq }
  | "NEQ" { MatchOp::Neq }
  | "RE"  { MatchOp::Re }
  | "NRE" { MatchOp::Nre }
  ;

PStage -> Result<Stage, ()>:
    "PIPE_EQ"  "STRING" { Ok(Stage::Line { op: LineOp::Contains,    value: unquote($lexer.span_str($2.map_err(|_| ())?.span())) }) }
  | "NEQ"      "STRING" { Ok(Stage::Line { op: LineOp::NotContains, value: unquote($lexer.span_str($2.map_err(|_| ())?.span())) }) }
  | "PIPE_RE"  "STRING" { Ok(Stage::Line { op: LineOp::Re,          value: unquote($lexer.span_str($2.map_err(|_| ())?.span())) }) }
  | "NRE"      "STRING" { Ok(Stage::Line { op: LineOp::Nre,         value: unquote($lexer.span_str($2.map_err(|_| ())?.span())) }) }
  | "PIPE_PAT" "STRING" { Ok(Stage::Line { op: LineOp::Pattern,     value: unquote($lexer.span_str($2.map_err(|_| ())?.span())) }) }
  | "BANG_PAT" "STRING" { Ok(Stage::Line { op: LineOp::NotPattern,  value: unquote($lexer.span_str($2.map_err(|_| ())?.span())) }) }
  | "PIPE" "JSON"        { Ok(Stage::Json) }
  | "PIPE" "LOGFMT"      { Ok(Stage::Logfmt) }
  | "PIPE" "UNPACK"      { Ok(Stage::Unpack) }
  | "PIPE" "DECOLORIZE"  { Ok(Stage::Decolorize) }
  | "PIPE" "REGEXP" "STRING"      { Ok(Stage::Regexp(unquote($lexer.span_str($3.map_err(|_| ())?.span())))) }
  | "PIPE" "PATTERN" "STRING"     { Ok(Stage::Pattern(unquote($lexer.span_str($3.map_err(|_| ())?.span())))) }
  | "PIPE" "LINE_FORMAT" "STRING" { Ok(Stage::LineFormat(unquote($lexer.span_str($3.map_err(|_| ())?.span())))) }
  | "PIPE" "DROP" Idents          { Ok(Stage::Drop($3?)) }
  | "PIPE" "KEEP" Idents          { Ok(Stage::Keep($3?)) }
  | "PIPE" "LABEL_FORMAT" Fmts    { Ok(Stage::LabelFormat($3?)) }
  | "PIPE" LFilter                { Ok(Stage::LabelFilter($2?)) }
  ;

LFilter -> Result<LabelFilter, ()>:
    "IDENTIFIER" Cmp Val {
        let name = $lexer.span_str($1.map_err(|_| ())?.span()).to_string();
        Ok(LabelFilter { name, op: $2, value: $3? })
    }
  ;

Cmp -> CmpOp:
    "EQ"   { CmpOp::Eq }
  | "EQEQ" { CmpOp::EqEq }
  | "NEQ"  { CmpOp::Neq }
  | "RE"   { CmpOp::Re }
  | "NRE"  { CmpOp::Nre }
  | "GT"   { CmpOp::Gt }
  | "GTE"  { CmpOp::Gte }
  | "LT"   { CmpOp::Lt }
  | "LTE"  { CmpOp::Lte }
  ;

Val -> Result<String, ()>:
    "STRING" { Ok(unquote($lexer.span_str($1.map_err(|_| ())?.span()))) }
  | "NUMLIT" { Ok($lexer.span_str($1.map_err(|_| ())?.span()).to_string()) }
  ;

Idents -> Result<Vec<String>, ()>:
    "IDENTIFIER" { Ok(vec![$lexer.span_str($1.map_err(|_| ())?.span()).to_string()]) }
  | Idents "COMMA" "IDENTIFIER" {
        let mut v = $1?;
        v.push($lexer.span_str($3.map_err(|_| ())?.span()).to_string());
        Ok(v)
    }
  ;

Fmts -> Result<Vec<(String, String)>, ()>:
    Fmt { Ok(vec![$1?]) }
  | Fmts "COMMA" Fmt { let mut v = $1?; v.push($3?); Ok(v) }
  ;

Fmt -> Result<(String, String), ()>:
    "IDENTIFIER" "EQ" "STRING" {
        let dst = $lexer.span_str($1.map_err(|_| ())?.span()).to_string();
        Ok((dst, unquote($lexer.span_str($3.map_err(|_| ())?.span()))))
    }
  | "IDENTIFIER" "EQ" "IDENTIFIER" {
        let dst = $lexer.span_str($1.map_err(|_| ())?.span()).to_string();
        let src = $lexer.span_str($3.map_err(|_| ())?.span()).to_string();
        Ok((dst, src))
    }
  ;
%%
use crate::query::logql::ast::{
    CmpOp, LabelFilter, LabelMatcher, LineOp, LogPipeline, MatchOp, Selector, Stage,
};

/// Strip surrounding quotes/backticks; double-quoted strings use Go-style
/// escapes (`\\` → `\`), backticks are raw.
fn unquote(s: &str) -> String {
    if let Some(inner) = s.strip_prefix('`').and_then(|x| x.strip_suffix('`')) {
        return inner.to_string();
    }
    let inner = s
        .strip_prefix('"')
        .and_then(|x| x.strip_suffix('"'))
        .unwrap_or(s);
    crate::query::loki::unescape_dquoted(inner)
}
