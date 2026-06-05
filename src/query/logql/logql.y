%start Root
%avoid_insert "IDENTIFIER" "STRING" "NUMLIT"
%left "OR"
%left "AND" "UNLESS"
%nonassoc "EQEQ" "NEQ" "GT" "GTE" "LT" "LTE"
%left "PLUS" "MINUS"
%left "STAR" "SLASH" "PCT"
%right "CARET"
%%
Root -> Result<LogQlExpr, ()>:
    Pipeline   { Ok(LogQlExpr::Log($1?)) }
  | SampleExpr { Ok(LogQlExpr::Sample($1?)) }
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
  | "PIPE" "UNWRAP" "IDENTIFIER" { Ok(Stage::Unwrap($lexer.span_str($3.map_err(|_| ())?.span()).to_string())) }
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

SampleExpr -> Result<SampleExpr, ()>:
    SampleExpr "OR"     SampleExpr { bin(BinOp::Or,     $1, $3) }
  | SampleExpr "AND"    SampleExpr { bin(BinOp::And,    $1, $3) }
  | SampleExpr "UNLESS" SampleExpr { bin(BinOp::Unless, $1, $3) }
  | SampleExpr "EQEQ"   SampleExpr { bin(BinOp::Eq,     $1, $3) }
  | SampleExpr "NEQ"    SampleExpr { bin(BinOp::Neq,    $1, $3) }
  | SampleExpr "GT"     SampleExpr { bin(BinOp::Gt,     $1, $3) }
  | SampleExpr "GTE"    SampleExpr { bin(BinOp::Gte,    $1, $3) }
  | SampleExpr "LT"     SampleExpr { bin(BinOp::Lt,     $1, $3) }
  | SampleExpr "LTE"    SampleExpr { bin(BinOp::Lte,    $1, $3) }
  | SampleExpr "PLUS"   SampleExpr { bin(BinOp::Add,    $1, $3) }
  | SampleExpr "MINUS"  SampleExpr { bin(BinOp::Sub,    $1, $3) }
  | SampleExpr "STAR"   SampleExpr { bin(BinOp::Mul,    $1, $3) }
  | SampleExpr "SLASH"  SampleExpr { bin(BinOp::Div,    $1, $3) }
  | SampleExpr "PCT"    SampleExpr { bin(BinOp::Mod,    $1, $3) }
  | SampleExpr "CARET"  SampleExpr { bin(BinOp::Pow,    $1, $3) }
  | "LPAREN" SampleExpr "RPAREN" { $2 }
  | "NUMLIT" {
        $lexer.span_str($1.map_err(|_| ())?.span())
            .parse::<f64>().map(SampleExpr::Number).map_err(|_| ())
    }
  | Aggregation { $1 }
  ;

Aggregation -> Result<SampleExpr, ()>:
    "IDENTIFIER" "LPAREN" LogRange "RPAREN" {
        let op = $lexer.span_str($1.map_err(|_| ())?.span()).to_string();
        Ok(SampleExpr::RangeAgg { op, param: None, range: Box::new($3?), grouping: None })
    }
  | "IDENTIFIER" "LPAREN" "NUMLIT" "COMMA" LogRange "RPAREN" {
        let op = $lexer.span_str($1.map_err(|_| ())?.span()).to_string();
        let param = $lexer.span_str($3.map_err(|_| ())?.span()).to_string();
        Ok(SampleExpr::RangeAgg { op, param: Some(param), range: Box::new($5?), grouping: None })
    }
  | "IDENTIFIER" "LPAREN" SampleExpr "RPAREN" {
        let op = $lexer.span_str($1.map_err(|_| ())?.span()).to_string();
        Ok(SampleExpr::VectorAgg { op, param: None, inner: Box::new($3?), grouping: None })
    }
  | "IDENTIFIER" "LPAREN" "NUMLIT" "COMMA" SampleExpr "RPAREN" {
        let op = $lexer.span_str($1.map_err(|_| ())?.span()).to_string();
        let param = $lexer.span_str($3.map_err(|_| ())?.span()).to_string();
        Ok(SampleExpr::VectorAgg { op, param: Some(param), inner: Box::new($5?), grouping: None })
    }
  | "IDENTIFIER" Group "LPAREN" LogRange "RPAREN" {
        let op = $lexer.span_str($1.map_err(|_| ())?.span()).to_string();
        Ok(SampleExpr::RangeAgg { op, param: None, range: Box::new($4?), grouping: Some($2?) })
    }
  | "IDENTIFIER" Group "LPAREN" SampleExpr "RPAREN" {
        let op = $lexer.span_str($1.map_err(|_| ())?.span()).to_string();
        Ok(SampleExpr::VectorAgg { op, param: None, inner: Box::new($4?), grouping: Some($2?) })
    }
  ;

Group -> Result<Grouping, ()>:
    "BY" "LPAREN" Idents "RPAREN"      { Ok(Grouping { without: false, labels: $3? }) }
  | "WITHOUT" "LPAREN" Idents "RPAREN" { Ok(Grouping { without: true,  labels: $3? }) }
  | "BY" "LPAREN" "RPAREN"            { Ok(Grouping { without: false, labels: vec![] }) }
  | "WITHOUT" "LPAREN" "RPAREN"       { Ok(Grouping { without: true,  labels: vec![] }) }
  ;

LogRange -> Result<LogRange, ()>:
    Pipeline "LBRACKET" "NUMLIT" "RBRACKET" OffsetOpt {
        Ok(LogRange {
            pipeline: $1?,
            interval: $lexer.span_str($3.map_err(|_| ())?.span()).to_string(),
            offset: $5?,
        })
    }
  ;

OffsetOpt -> Result<Option<String>, ()>:
    { Ok(None) }
  | "OFFSET" "NUMLIT" { Ok(Some($lexer.span_str($2.map_err(|_| ())?.span()).to_string())) }
  ;
%%
use crate::query::logql::ast::{
    BinOp, CmpOp, Grouping, LabelFilter, LabelMatcher, LineOp, LogPipeline, LogQlExpr, LogRange,
    MatchOp, SampleExpr, Selector, Stage,
};

/// Build a binary metric expression.
fn bin(op: BinOp, lhs: Result<SampleExpr, ()>, rhs: Result<SampleExpr, ()>) -> Result<SampleExpr, ()> {
    Ok(SampleExpr::Binary { op, lhs: Box::new(lhs?), rhs: Box::new(rhs?) })
}

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
