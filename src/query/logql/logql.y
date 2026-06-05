%start Query
%avoid_insert "IDENTIFIER" "STRING"
%%
Query -> Result<Selector, ()>:
    "LBRACE" "RBRACE" { Ok(Selector::default()) }
  | "LBRACE" Matchers "RBRACE" { Ok(Selector { matchers: $2? }) }
  ;

Matchers -> Result<Vec<LabelMatcher>, ()>:
    Matcher { Ok(vec![$1?]) }
  | Matchers "COMMA" Matcher { let mut v = $1?; v.push($3?); Ok(v) }
  ;

Matcher -> Result<LabelMatcher, ()>:
    "IDENTIFIER" Op "STRING" {
        let name = $lexer.span_str($1.map_err(|_| ())?.span()).to_string();
        let raw = $lexer.span_str($3.map_err(|_| ())?.span());
        Ok(LabelMatcher { name, op: $2, value: unquote(raw) })
    }
  ;

Op -> MatchOp:
    "EQ"  { MatchOp::Eq }
  | "NEQ" { MatchOp::Neq }
  | "RE"  { MatchOp::Re }
  | "NRE" { MatchOp::Nre }
  ;
%%
use crate::query::logql::ast::{LabelMatcher, MatchOp, Selector};

/// Strip surrounding quotes/backticks; double-quoted strings use Go-style
/// escapes (`\\` → `\`), backticks are raw — shared with the line lexer via
/// [`crate::query::loki::unescape_dquoted`].
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
