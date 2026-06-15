# sol-demo — Design Doc

## Context

The SOL demo replaces OTel Collector Contrib with Vector for the full traces pipeline (gateway → load balancer → tail sampling → Tempo). Steps 1–3 are complete, but the tail sampling config has a known limitation:

The OTel config kept ERROR traces while excluding 4xx errors using an `and` sub-policy. Vector's `tail_sampling` uses first-match-wins with no composite policies, so all ERROR traces are currently kept (including 4xx).

This design adds AND policy composition and string attribute extensions to close the gap.

## Functional Requirements

### <a id="fr1"></a>FR1 — AND composite policy for tail sampling

Add an `and` policy type to the `tail_sampling` transform that evaluates multiple sub-policies and returns `Sample` only when ALL sub-policies return `Sample`. If any sub-policy returns `Pending`, the composite returns `Pending`. If any returns `Drop`, the composite returns `Drop`.

The config format must support recursive nesting (an `and` can contain another `and`).

### <a id="fr2"></a>FR2 — StringAttribute invert_match and regex support

Extend the `string_attribute` policy with:
- `invert_match: bool` — inverts the match result (Sample ↔ Pending)
- `enabled_regex_matching: bool` — treats `values` as regex patterns instead of exact strings

This is required to replicate the OTel config that excludes 4xx errors: `key: error.type, values: [4..], enabled_regex_matching: true, invert_match: true`.

### <a id="fr3"></a>FR3 — Update demo config to use new policies

Update `sol-collector.yaml` to use the `and` policy with `string_attribute` invert/regex for the error policy, matching the original OTel config.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — No backward compatibility constraint

This is a demo/pre-release codebase. Configs can be freely adjusted. No need to preserve backward compatibility for existing tail_sampling configs or string_attribute defaults.

## Non-goals

- OR composite policy — already covered: the top-level policy list evaluates as OR (first-match-wins). With AND available, any AND/OR combination is expressible.
- Servicegraph transform — deferred to a separate workspace. The `span_metrics` transform provides per-service RED metrics as a partial substitute.

## Rabbit holes

- **Recursive AND validation**: deeply nested AND configs could cause stack overflow during build. Cap: don't add depth validation — real configs are shallow (2-3 levels max).

## Design

### Tail sampling AND policy

Add `And(AndConfig)` variant to `PolicyConfig` enum in `src/transforms/tail_sampling/policies.rs`. The `AndConfig` contains `sub_policies: Vec<PolicyConfig>` which are recursively built into `Vec<Box<dyn SamplingPolicy>>`. The evaluation is: all must return `Sample` for the composite to return `Sample`.

### StringAttribute extensions

Add `invert_match: bool` and `enabled_regex_matching: bool` to `StringAttributeConfig`. When `enabled_regex_matching` is true, compile values as `regex::Regex` at build time. When `invert_match` is true, swap Sample↔Pending in the result.

## Target config example

The demo `sol-collector.yaml` should look like this once implemented, matching the original OTel config:

```yaml
policies:
  # Keep traces with latency >= 100ms
  - type: latency
    name: latency-policy
    threshold_ms: 100

  # Keep ERROR traces, excluding 4xx HTTP errors
  - type: and
    name: error-policy
    sub_policies:
      - type: status_code
        name: status-code-error-policy
        status_codes: ["ERROR"]
      - type: string_attribute
        name: http-status-code-error-policy
        key: error.type
        values: ["4.."]
        enabled_regex_matching: true
        invert_match: true

  # 10% probabilistic sampling of remaining traces
  - type: probabilistic
    name: probabilistic-policy
    sampling_percentage: 10.0
```

For comparison, the OTel Collector Contrib equivalent:

```yaml
policies:
  - name: latency-policy
    type: latency
    latency: {threshold_ms: 100}
  - name: error-policy
    type: and
    and:
      and_sub_policy:
        - name: status_code-error-policy
          type: status_code
          status_code: {status_codes: [ERROR]}
        - name: http-status-code-error-policy
          type: string_attribute
          string_attribute:
            key: error.type
            values: [4..]
            enabled_regex_matching: true
            invert_match: true
```

Vector uses flat fields (serde `tag = "type"` puts fields at the same level) while OTel nests under the type name. The semantics are identical.

## Cross-cutting Concerns

- **Testing**: unit tests for each policy type, demo config updated
