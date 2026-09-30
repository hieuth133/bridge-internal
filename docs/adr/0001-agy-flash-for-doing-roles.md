# Tester and Developer run on Gemini Flash through `agy`, not as Claude subagents

The thinking Roles (BA, Tech Lead, Reviewer) run as Claude Opus subagents. The doing Roles (Tester, Developer) do most of the typing, so they run on a cheaper, faster model: Gemini Flash (high effort). The Orchestrator calls them with the Antigravity command-line tool, `agy -p ... --agent <role>`.

We chose this over Claude subagents (simpler: no extra tool, no second login) because of cost. What it costs us: `agy` must be installed and logged in, and it runs with `--dangerously-skip-permissions`. To limit that risk, `agy` never runs on `main`, only on a feature branch, and the Orchestrator re-runs every test itself.
