# sol-demo-policies

The SOL demo replaces OTel Collector Contrib with Vector for the full traces pipeline (gateway → load balancer → tail sampling → Tempo). Steps 1–3 are complete, but the tail sampling config has a known limitation:

## Design
- [20260503_sol-demo-policies](./designs/20260503_sol-demo-policies.md)
