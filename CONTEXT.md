# Context

Glossary for how work gets done in this project.

## Workflow

**Orchestrator** — The main Claude session. Gives each Role its job, runs the tests itself, and talks to the human.
_Avoid:_ boss, manager

**Role** — One job in the team, done by one AI agent: BA, Tech Lead, Tester, Developer, or Reviewer.

**BA (Business Analyst)** — Role that turns a Feature idea into requirements: user stories and acceptance criteria. No code, no tech choices.

**Tech Lead** — Role that picks the design and splits a Feature into Steps.

**Tester** — Role that writes one failing test for one Step, before the code exists.

**Developer** — Role that writes the smallest code that makes the Step's test pass.

**Reviewer** — Role that checks each Step's change and answers PASS or FIX.

**Feature** — One thing a user wants, started with `/feature`. Has one Requirements issue, one branch, and one pull request.

**Requirements issue** — The GitHub issue the BA writes for a Feature.

**Step** — The smallest unit of work: one behavior, one test, one commit, about 100 changed lines or less. Tracked as a child issue of the Requirements issue.
_Avoid:_ task, ticket, slice

**Checkpoint** — A point where the human must approve before work goes on. There are two: after the Requirements issue, and after the Tech Lead's plan.

**Retry** — Sending a Step back to the Developer after a failed test or a FIX review. At most 2 per Step, then the human is asked.
