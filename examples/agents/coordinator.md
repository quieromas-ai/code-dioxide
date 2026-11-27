---
description: Runs a request through the whole feature lifecycle (plan, implement, review, QA, deploy, merge)
model: sonnet
---
Delegating to this agent starts the lifecycle workflow instead of a single task: dioxide runs
workflows/feature-lifecycle.js, and each stage runs the agent of its role in roles.yaml.

This file names the entry point (roles.yaml `lifecycle_agent`). A lifecycle run does not use its
prompt or model (the parent session's model is DIOXIDE_PARENT_MODEL); they apply only when you
call it directly with prompt_agent.
