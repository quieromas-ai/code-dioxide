// meta must be a pure literal: the Workflow tool rejects any expression in it (even "a" + "b").
export const meta = {
  name: "feature-lifecycle",
  description:
    "Plan, set up a worktree, implement, review, QA, deploy and merge one feature, pausing for a human at gates and when a stage asks.",
  phases: [
    { title: "Plan" },
    { title: "Setup" },
    { title: "Implement" },
    { title: "Review" },
    { title: "QA" },
    { title: "Deploy" },
    { title: "Merge" },
  ],
};

// Determinism rule: every agent() call is cached under a key chained over call order, prompt and
// options. A resumed run replays finished stages only if every earlier call is byte-identical, so
// nothing here may depend on a clock, randomness or the environment, and a human answer may only
// change calls made after the point where it is consumed. Answers are referenced by path; their
// text is read by the stage, never inlined.

// ---------------------------------------------------------------- schemas

const STRINGS = { type: "array", items: { type: "string" } };
// A full commit id (SHA-1 or SHA-256), as `git rev-parse HEAD` prints it.
const FULL_SHA = { type: "string", pattern: "^[0-9a-f]{40,64}$" };
const QUESTION = {
  type: "object",
  properties: { question: { type: "string" } },
  required: ["question"],
};

const PLAN_SCHEMA = {
  type: "object",
  properties: {
    steps: STRINGS,
    acceptance: STRINGS,
    needs_deploy: { type: "boolean" },
    needs_human: QUESTION,
  },
  required: ["steps", "acceptance", "needs_deploy"],
};

const SETUP_SCHEMA = {
  type: "object",
  properties: {
    head_sha: { type: "string" },
    reused: { type: "boolean" },
    exit_code: { type: "integer" },
    error: { type: "string" },
  },
  required: ["head_sha", "exit_code"],
};

const IMPLEMENT_SCHEMA = {
  type: "object",
  properties: {
    commits: STRINGS,
    tests_green: { type: "boolean" },
    head_sha: FULL_SHA,
    needs_human: QUESTION,
  },
  required: ["commits", "tests_green", "head_sha"],
};

const REVIEW_SCHEMA = {
  type: "object",
  properties: {
    verdict: { enum: ["approve", "changes"] },
    findings: STRINGS,
    needs_human: QUESTION,
  },
  required: ["verdict", "findings"],
};

const QA_SCHEMA = {
  type: "object",
  properties: {
    gate: { enum: ["green", "red"] },
    failures: STRINGS,
    evidence: { type: "string" },
    head_sha: FULL_SHA,
    needs_human: QUESTION,
  },
  required: ["gate", "failures", "evidence", "head_sha"],
};

const DEPLOY_SCHEMA = {
  type: "object",
  properties: { deployed: { type: "boolean" }, health_checks: STRINGS, needs_human: QUESTION },
  required: ["deployed", "health_checks"],
};

const MERGE_SCHEMA = {
  type: "object",
  properties: {
    merge_sha: { type: "string" },
    pr_url: { type: "string" },
    already_merged: { type: "boolean" },
    needs_human: { type: "boolean" },
    reason: { type: "string" },
    exit_code: { type: "integer" },
    error: { type: "string" },
  },
  required: ["already_merged", "exit_code"],
};

const INTERPRET_SCHEMA = {
  type: "object",
  properties: {
    action: { enum: ["continue", "amend", "abort"] },
    extra_rounds: { type: "integer", minimum: 0 },
    guidance: { type: "string" },
  },
  required: ["action", "extra_rounds", "guidance"],
};

// ---------------------------------------------------------------- run state

const A = args;
const ROLES = A.roles;
const INPUTS = `${A.run_dir}/inputs`;
const BRIEF = `${INPUTS}/brief.md`;
const PINNED_PLAN = `${INPUTS}/plan.json`;
const STAGE_SCRIPTS = `${A.run_dir}/script/stage`;
const SUBJECT_FILE = `${A.run_dir}/out/merge-subject.txt`; // out/ is the run's only writable dir
const ANSWERS = A.answers || [];

let epoch = 0; // k of the last consumed answer; labels after it carry "@a{k}"
let nextAnswer = 0; // index into ANSWERS; answers are consumed strictly in order
const guidance = []; // human guidance from interpret stages, in the order it was given
const outcome = { branch: A.branch, gate: null, review: null };

class Halt {
  constructor(value) {
    this.value = value;
  }
}

function halt(status, stage, extra = {}) {
  throw new Halt({ status, stage, ...extra, result: { ...outcome } });
}

function label(base) {
  return epoch === 0 ? base : `${base}@a${epoch}`;
}

function sh(value) {
  return "'" + String(value).replace(/'/g, "'\\''") + "'";
}

function answerPath(k) {
  return `${INPUTS}/answers/${k}.md`;
}

// The next unconsumed answer if it belongs to this ask point, else null.
function takeAnswer(askLabel, mode) {
  const answer = ANSWERS[nextAnswer];
  if (!answer || answer.label !== askLabel || answer.mode !== mode) return null;
  nextAnswer += 1;
  epoch = answer.k;
  return answer;
}

// An unconsumed answer left over at an ask or at the end means the answers do not fit this
// run's history.
function requireAnswersConsumed(stage, where) {
  const pending = ANSWERS[nextAnswer];
  if (pending) {
    halt("stage_failed", stage, {
      error: `answer ${pending.k} (${pending.label}, ${pending.mode}) was not consumed by ${where}`,
    });
  }
}

function ask(stage, kind, question) {
  requireAnswersConsumed(stage, "this ask");
  halt("needs_human", stage, { kind, question });
}

// JSON with recursively sorted keys: a cached result whose keys come back in another order must
// not change a later prompt.
function canonical(value) {
  const sorted = (v) => {
    if (Array.isArray(v)) return v.map(sorted);
    if (v === null || typeof v !== "object") return v;
    return Object.fromEntries(
      Object.keys(v)
        .sort()
        .map((key) => [key, sorted(v[key])]),
    );
  };
  return JSON.stringify(sorted(value), null, 2);
}

function questionOf(result) {
  const asked = result.needs_human;
  if (!asked) return null;
  if (typeof asked === "object" && asked.question) return asked.question;
  return "This stage needs a human decision.";
}

// ---------------------------------------------------------------- prompts

function preamble() {
  return [
    `You are one stage of an automated feature workflow (task ${A.task_id}). Stage contract:`,
    "- No human is available while you work. Do not ask questions in prose; if a decision " +
      "only a human can make blocks you and your output schema has a needs_human field, set " +
      "it with one clear question and stop.",
    `- Work only in the task worktree ${A.worktree}. You have no working-directory option: ` +
      `use absolute paths and \`git -C ${A.worktree}\`. Change nothing outside it unless this ` +
      "stage says so.",
    "- This stage may be a re-run after an interruption: first check what is already done " +
      "and never repeat a completed side effect.",
    "- Do not delegate to other agents or orchestration tools.",
    `- Task inputs are read-only files under ${INPUTS}/; read them from disk when referenced.`,
    "- Finish by returning only the structured output this stage asks for.",
  ].join("\n");
}

// Only for agent stages that do judgement work; script stages (setup, merge) never get it.
function guidanceSection() {
  if (guidance.length === 0) return "";
  return "Human guidance so far (oldest first):\n" + guidance.map((g) => `- ${g}`).join("\n");
}

function planSection(plan) {
  if (plan) return "Approved plan:\n" + canonical(plan);
  return `Approved plan: read ${PINNED_PLAN}.`;
}

function answersSection(ks) {
  if (ks.length === 0) return "";
  const files = ks.map((k) => `- ${answerPath(k)}`).join("\n");
  return (
    "You asked a human a question in an earlier run of this stage. Read the answer(s) and act " +
    "on them:\n" +
    files
  );
}

function prompt(...sections) {
  return [preamble(), ...sections].filter((s) => s).join("\n\n");
}

function planPrompt(previous) {
  return prompt(
    `Stage: plan. Read the task brief at ${BRIEF} and inspect the repository at ${A.repo} ` +
      `(base branch ${A.base_branch}). Write an implementation plan: ordered steps, ` +
      "acceptance criteria, and whether the change needs a deploy before merge. The plan " +
      "covers the implementation work only: creating the worktree and merging are done by the " +
      "workflow itself after review and QA, and are not plan steps.",
    A.plan_pinned ? `A previously approved plan is in ${PINNED_PLAN}; revise it.` : "",
    previous ? "Your previous plan was:\n" + canonical(previous) : "",
    guidanceSection(),
  );
}

// Script stages are mechanical: the preamble, the exact command, and how to relay its result.
// No plan content and no human guidance reach them.
// Script stages also run without a role (SCRIPT_AGENT): the runtime's default agent, on
// A.script_model when the user set one (a cheap model is enough), else on the parent session's
// model. A user-defined persona (a planner, or any other role) may forbid writes or commands,
// and dioxide cannot rely on what a role definition says.
const SCRIPT_AGENT = null;
const SCRIPT_MODEL = A.script_model || null;
const SCRIPT_RESULT =
  "Run it once. Return the JSON object it prints on stdout verbatim, adding exit_code (its " +
  "exit status as an integer). If it prints no JSON, return exit_code, its stderr as error, " +
  "and empty values for the other required fields. Do not retry or work around a failure. " +
  "This stage makes no decision of its own: running this command was decided by the workflow " +
  "and the approvals before it.";

function command(script, ...params) {
  return ["bash", `${STAGE_SCRIPTS}/${script}`, ...params].map((p, i) => (i ? sh(p) : p)).join(" ");
}

function setupPrompt() {
  return prompt(
    "Stage: setup. Run exactly this command and nothing else:\n" +
      command("setup.sh", A.repo, A.base_branch, A.branch, A.worktree) +
      "\n" +
      SCRIPT_RESULT,
  );
}

function implementPrompt(plan) {
  return prompt(
    "Stage: implement. Ground truth first: run " +
      `\`git -C ${sh(A.worktree)} log ${sh(`${A.base_branch}..${A.branch}`)}\` and inspect ` +
      "the worktree to see what is already done; continue from there.",
    `Implement the task in ${BRIEF} following the plan, committing on branch ${A.branch}. ` +
      "Report the commits on the branch, whether the tests pass and the head sha (the full " +
      `sha printed by \`git -C ${sh(A.worktree)} rev-parse HEAD\`).`,
    planSection(plan),
    guidanceSection(),
  );
}

function fixPrompt(kind, plan, items) {
  return prompt(
    `Stage: ${kind} fixes. Address every item below in the worktree, committing on branch ` +
      `${A.branch}. The task is in ${BRIEF}. Report the new commits, whether the tests pass and ` +
      `the head sha (the full sha printed by \`git -C ${sh(A.worktree)} rev-parse HEAD\`).`,
    "Items:\n" + items.map((item) => `- ${item}`).join("\n"),
    planSection(plan),
    guidanceSection(),
  );
}

function reviewPrompt(plan) {
  return prompt(
    `Stage: review. Review the change \`git -C ${sh(A.worktree)} diff ` +
      `${sh(`${A.base_branch}...${A.branch}`)}\` against the task in ${BRIEF} and the plan. ` +
      "Do not modify files or commits. Return approve, or changes with concrete findings.",
    planSection(plan),
    guidanceSection(),
  );
}

function qaPrompt(plan, stageLabel) {
  return prompt(
    `Stage: QA. Verify branch ${A.branch} in the worktree against the acceptance criteria of ` +
      `the task in ${BRIEF} and the plan. Do not modify files or commits, with one exception: ` +
      `create the directory ${A.run_dir}/out/evidence/${stageLabel}/ and save every evidence ` +
      "file (screenshots, logs) there; it is the only place you may write. Return green or " +
      "red with the failures, your evidence with the paths of those files, and the head sha " +
      `you verified (the full sha printed by \`git -C ${sh(A.worktree)} rev-parse HEAD\`).`,
    planSection(plan),
    guidanceSection(),
  );
}

function deployPrompt(plan) {
  return prompt(
    `Stage: deploy. Deploy branch ${A.branch} from the worktree and run health checks. First ` +
      "check whether this exact head is already deployed and healthy; if so, do not redeploy.",
    plan ? "" : `If the plan in ${PINNED_PLAN} says no deploy is needed, deploy nothing.`,
    planSection(plan),
    guidanceSection(),
  );
}

function mergePrompt(approvedHead) {
  const cmd = command(
    "merge.sh",
    A.repo,
    A.base_branch,
    A.branch,
    A.worktree,
    approvedHead,
    A.merge_strategy,
    A.task_id,
    SUBJECT_FILE,
  );
  return prompt(
    `Stage: merge. First write a one-line commit subject for this change to ${SUBJECT_FILE}, ` +
      `following the repository's own commit conventions (see its docs and \`git -C ` +
      `${sh(A.repo)} log\`). Then run exactly this command:\n` +
      cmd +
      "\n" +
      SCRIPT_RESULT,
  );
}

function interpretPrompt(askLabel, kind, question, k) {
  return prompt(
    `Stage: interpret a human answer. The workflow paused at ${askLabel} (${kind}) and asked:\n` +
      question,
    `Read the answer in ${answerPath(k)} and classify it: continue (go on as planned), amend ` +
      "(re-plan with the answer's changes) or abort (stop the task). extra_rounds is the number " +
      "of further review/QA rounds the answer grants (0 if none). guidance holds only what the " +
      "human added beyond the decision itself, in a few sentences; when the answer adds nothing " +
      '(for example a bare "approved"), guidance is an empty string. Never restate or ' +
      "summarise the plan, the question or earlier guidance in it. For amend, guidance carries " +
      "the changes the human asked for.",
  );
}

// ---------------------------------------------------------------- stages

// One agent stage. A stage that asked a human before is re-run with the answer path(s) appended
// to its unchanged prompt, under the new epoch's label; that is the only intended cache miss.
// Script stages (setup, merge) pass `asks: false`: their result is checked by the caller.
// A null agentType leaves it out of the options: the runtime's default agent runs the stage.
// A null model leaves it out too (the agent's or the session's model).
async function stage(
  base,
  agentType,
  phaseTitle,
  schema,
  buildPrompt,
  { asks = true, model = null } = {},
) {
  const answered = asks ? takeReruns([base])[0] : [];
  const stageLabel = label(base);
  const text = [buildPrompt(), answersSection(answered)].filter((s) => s).join("\n\n");
  const options = { label: stageLabel, phase: phaseTitle, schema };
  if (agentType) options.agentType = agentType;
  if (model) options.model = model;
  const result = await agent(text, options);
  if (result === null) halt("stage_failed", stageLabel);
  const question = asks ? questionOf(result) : null;
  if (question !== null) ask(stageLabel, "question", question);
  return result;
}

// Consumes the rerun answers addressed to any of `bases` (in answer order; each consumption
// moves the epoch, so the labels are recomputed every time). Returns the answer ks per base.
function takeReruns(bases) {
  const answered = bases.map(() => []);
  for (let found = true; found; ) {
    found = false;
    for (let i = 0; i < bases.length && !found; i++) {
      const answer = takeAnswer(label(bases[i]), "rerun");
      if (answer) {
        answered[i].push(answer.k);
        found = true;
      }
    }
  }
  return answered;
}

// A script stage's result must look like script output: an integer exit_code, 0 to go on.
// This is not forgery-proof (an agent can still type a plausible result); it turns an agent's
// own refusal or improvisation into a visible stage failure instead of a gate ask that human
// guidance could never clear.
function scriptExit(stageLabel, result) {
  if (!Number.isInteger(result.exit_code)) {
    halt("stage_failed", stageLabel, {
      error: "the stage did not return the script's result (no integer exit_code)",
    });
  }
  if (result.exit_code !== 0) {
    halt("stage_failed", stageLabel, {
      error: result.error || `stage script exited ${result.exit_code}`,
    });
  }
}

function scriptFailed(stageLabel, result, ok) {
  if (result.error || !ok) {
    halt("stage_failed", stageLabel, { error: result.error || "stage script returned no result" });
  }
}

// A gate ask: the asking stage stays cached; its answer is interpreted by a new stage.
async function gate(base, phaseTitle, kind, question) {
  const askLabel = label(base);
  const answer = takeAnswer(askLabel, "after");
  if (!answer) ask(askLabel, kind, question);
  const interpretLabel = `interpret-${askLabel}-${answer.k}`;
  const decision = await agent(interpretPrompt(askLabel, kind, question, answer.k), {
    label: interpretLabel,
    phase: phaseTitle,
    agentType: ROLES.planner,
    schema: INTERPRET_SCHEMA,
  });
  if (decision === null) halt("stage_failed", interpretLabel);
  if (typeof decision.guidance === "string" && decision.guidance.trim()) {
    guidance.push(decision.guidance);
  }
  if (decision.action === "abort") halt("cancelled", interpretLabel);
  return decision;
}

async function planStage() {
  let previous = null;
  for (;;) {
    const plan = await stage("plan", ROLES.planner, "Plan", PLAN_SCHEMA, () => planPrompt(previous));
    if (!A.plan_gate) return plan;
    const question =
      "Approve this plan (continue), change it (amend, say how) or stop the task (abort)?\n" +
      canonical({ steps: plan.steps, acceptance: plan.acceptance });
    const decision = await gate("plan", "Plan", "plan_approval", question);
    if (decision.action === "continue") return plan;
    previous = plan;
  }
}

// All reviewers of round n in parallel. A reviewer's answered question re-runs the whole round in
// the new epoch. Only calls before the first changed call replay from cache: reviewers listed
// before the asking one replay; the asking one and those listed after it run live again (their
// prompts are unchanged, so this costs a re-run but changes nothing else).
async function reviewRound(n, plan) {
  const bases = ROLES.reviewers.map((slug) => `review-r${n}-${slug}`);
  const answered = takeReruns(bases);
  const labels = bases.map(label);
  const verdicts = await parallel(
    ROLES.reviewers.map((slug, i) => () => {
      const text = [reviewPrompt(plan), answersSection(answered[i])].filter((s) => s);
      return agent(text.join("\n\n"), {
        label: labels[i],
        phase: "Review",
        agentType: slug,
        schema: REVIEW_SCHEMA,
      });
    }),
  );
  const failed = verdicts.findIndex((v) => v === null);
  if (failed >= 0) halt("stage_failed", labels[failed]);
  const asking = verdicts.findIndex((v) => questionOf(v) !== null);
  if (asking >= 0) ask(labels[asking], "question", questionOf(verdicts[asking]));
  const findings = verdicts.flatMap((v, i) => v.findings.map((f) => `[${ROLES.reviewers[i]}] ${f}`));
  return { approved: verdicts.every((v) => v.verdict === "approve"), findings };
}

function roundLimit(configured) {
  return Math.max(1, configured);
}

// Rounds of check -> fix until the check passes. After `limit` failed checks a human decides:
// continue (a new epoch with extra_rounds or the configured limit) or amend (re-plan). The last
// failed check asks at once, without a fix after it, so the question lists the open items.
// The round budget and the label number are separate. The budget restarts per epoch: when an
// answered question re-runs a stage inside the loop, that round is round 1 of the new epoch's
// budget. The label number `n` never goes back within one pass, so labels stay unique; it
// restarts at r1 only when the loop is entered anew (after a gate, under a new epoch).
async function rounds({ name, phaseTitle, configured, check, fixKind, fixBase }) {
  let limit = roundLimit(configured);
  for (;;) {
    let last = null;
    let n = 0;
    for (let used = 0; used < limit; ) {
      n += 1;
      used += 1;
      let roundEpoch = epoch;
      last = await check(n);
      if (epoch !== roundEpoch) used = 1;
      if (last.passed) return last;
      if (used >= limit) break;
      roundEpoch = epoch;
      await stage(fixBase(n), ROLES.implementer, phaseTitle, IMPLEMENT_SCHEMA, () =>
        fixKind(last.items),
      );
      if (epoch !== roundEpoch) used = 1;
    }
    const question =
      `${name} did not pass after ${limit} round(s). Open items:\n` +
      last.items.map((item) => `- ${item}`).join("\n") +
      "\nContinue (optionally granting extra rounds), amend the plan, or abort?";
    const decision = await gate(`${name}-r${n}`, phaseTitle, "rounds_exhausted", question);
    if (decision.action === "amend") return null;
    limit = decision.extra_rounds > 0 ? decision.extra_rounds : roundLimit(configured);
  }
}

function reviewLoop(plan) {
  return rounds({
    name: "review",
    phaseTitle: "Review",
    configured: A.limits.review_rounds,
    check: async (n) => {
      const round = await reviewRound(n, plan);
      return { passed: round.approved, items: round.findings };
    },
    fixKind: (items) => fixPrompt("review", plan, items),
    fixBase: (n) => `fix-r${n}`,
  });
}

function qaLoop(plan) {
  return rounds({
    name: "qa",
    phaseTitle: "QA",
    configured: A.limits.qa_rounds,
    check: async (n) => {
      // The prompt names the epoch-free label, so a re-run's prompt stays unchanged.
      const base = `qa-r${n}`;
      const qa = await stage(base, ROLES.qa, "QA", QA_SCHEMA, () => qaPrompt(plan, base));
      return { passed: qa.gate === "green", items: qa.failures, head: qa.head_sha };
    },
    fixKind: (items) => fixPrompt("QA", plan, items),
    fixBase: (n) => `qa-fix-r${n}`,
  });
}

async function lifecycle() {
  let replan = !A.plan_pinned;
  let plan = null;
  for (;;) {
    outcome.review = null;
    outcome.gate = null;
    if (replan) plan = await planStage();
    replan = true; // any later amend re-plans, even from a pinned plan
    const setup = await stage("setup", SCRIPT_AGENT, "Setup", SETUP_SCHEMA, setupPrompt, {
      asks: false,
      model: SCRIPT_MODEL,
    });
    scriptExit(label("setup"), setup);
    scriptFailed(label("setup"), setup, Boolean(setup.head_sha));
    await stage("implement", ROLES.implementer, "Implement", IMPLEMENT_SCHEMA, () =>
      implementPrompt(plan),
    );
    if ((await reviewLoop(plan)) === null) continue;
    outcome.review = "approve";
    const qa = await qaLoop(plan);
    if (qa === null) continue;
    outcome.gate = "green";
    // With a pinned plan (plan === null) the script cannot read plan.json, so the deploy agent
    // decides from that file whether a deploy is needed, and deployed:false is accepted there.
    const deploy = ROLES.deployer && (plan === null || plan.needs_deploy);
    if (deploy) {
      const deployed = await stage("deploy", ROLES.deployer, "Deploy", DEPLOY_SCHEMA, () =>
        deployPrompt(plan),
      );
      if (!deployed.deployed && plan !== null) {
        halt("stage_failed", label("deploy"), { error: "deploy reported deployed=false" });
      }
    }
    const done = await mergeLoop(plan, qa.head);
    if (done !== null) return done;
  }
}

// Merge the QA-approved head. merge.sh's needs_human is a gate (merge_blocked): continue
// re-qualifies the branch with a fresh QA loop in the new epoch and merges its head; amend
// re-plans (null); abort cancels.
async function mergeLoop(plan, approvedHead) {
  let head = approvedHead;
  for (;;) {
    const mergeLabel = label("merge");
    const buildPrompt = () => mergePrompt(head);
    const merged = await stage("merge", SCRIPT_AGENT, "Merge", MERGE_SCHEMA, buildPrompt, {
      asks: false,
      model: SCRIPT_MODEL,
    });
    scriptExit(mergeLabel, merged);
    if (merged.needs_human) {
      // merge.sh prints needs_human only together with a non-empty reason.
      if (typeof merged.reason !== "string" || !merged.reason.trim()) {
        halt("stage_failed", mergeLabel, {
          error: "the stage did not return the script's result (needs_human without a reason)",
        });
      }
      const question =
        `Merging ${A.branch} into ${A.base_branch} is blocked: ` +
        merged.reason +
        "\nContinue (re-run QA on the branch, then merge again), amend the plan, or abort?";
      const decision = await gate("merge", "Merge", "merge_blocked", question);
      if (decision.action === "amend") return null;
      // Deliberately no deploy before the second merge: the new QA pass qualifies the new head.
      const qa = await qaLoop(plan);
      if (qa === null) return null;
      head = qa.head;
      continue;
    }
    scriptFailed(mergeLabel, merged, Boolean(merged.merge_sha || merged.pr_url));
    if (merged.merge_sha) outcome.merge_sha = merged.merge_sha;
    if (merged.pr_url) outcome.pr_url = merged.pr_url;
    requireAnswersConsumed(mergeLabel, "the completed run");
    return { status: "completed", stage: mergeLabel, result: { ...outcome } };
  }
}

try {
  return await lifecycle();
} catch (err) {
  if (err instanceof Halt) return err.value;
  throw err;
}
