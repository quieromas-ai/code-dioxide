// Node harness for unit-testing Workflow scripts without running Claude.
//
//   node harness.mjs <script> <args.json> <responses.json>
//
// Injects agent/parallel/pipeline/phase/log/args/budget, makes Date.now,
// argless `new Date()` and Math.random throw (as in the real runtime), serves
// agent() results from a per-label queue (missing or exhausted -> null) and
// prints {calls: [...], return: ...} as JSON on stdout. The script's own
// console output is redirected to stderr so it cannot corrupt that JSON. A
// script failure prints "workflow error: <message>" plus the stack on stderr
// and exits 1.
import { readFileSync } from "node:fs";
import { format } from "node:util";

const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const FORBIDDEN = " is unavailable in workflow scripts (it would break resume)";

function forbid(name) {
  return () => {
    throw new Error(name + FORBIDDEN);
  };
}

function installForbiddenGlobals() {
  Math.random = forbid("Math.random()");
  globalThis.Date = new Proxy(Date, {
    construct(target, params, newTarget) {
      if (params.length === 0) throw new Error("new Date() without arguments" + FORBIDDEN);
      return Reflect.construct(target, params, newTarget);
    },
    get(target, key, receiver) {
      if (key === "now") return forbid("Date.now()");
      return Reflect.get(target, key, receiver);
    },
  });
}

// Real scripts start with a pure-literal `export const meta`, which is not
// valid inside a function body; the rest uses top-level await/return.
function scriptBody(source) {
  return source.replace(/^(\s*(?:\/\/[^\n]*\n\s*)*)export\s+const\s+meta\b/, "$1const meta");
}

function redirectConsoleToStderr() {
  for (const method of ["log", "info", "debug", "warn", "error"]) {
    console[method] = (...parts) => process.stderr.write(format(...parts) + "\n");
  }
}

function toQueues(responses) {
  if (responses === null || typeof responses !== "object" || Array.isArray(responses)) {
    throw new Error("responses must be an object mapping label -> array of results");
  }
  return new Map(
    Object.entries(responses).map(([label, queue]) => {
      if (!Array.isArray(queue)) {
        throw new Error(`responses for label "${label}" must be an array, got ${typeof queue}`);
      }
      return [label, [...queue]];
    }),
  );
}

function buildRuntime(responses, calls) {
  const queues = toQueues(responses);
  let currentPhase = null;

  async function agent(prompt, opts = {}) {
    const label = opts.label ?? "";
    calls.push({
      label,
      agentType: opts.agentType ?? null,
      model: opts.model ?? null,
      optionKeys: Object.keys(opts).sort(),
      phase: opts.phase ?? currentPhase,
      prompt,
      schema: opts.schema ?? null,
    });
    await Promise.resolve();
    const queue = queues.get(label);
    return queue && queue.length > 0 ? queue.shift() : null;
  }

  // A thunk/stage that throws yields null for its slot, never a rejection.
  const parallel = (thunks) =>
    Promise.all(thunks.map((thunk) => (async () => thunk())().catch(() => null)));

  const pipeline = (items, ...stages) =>
    Promise.all(
      items.map(async (item, index) => {
        try {
          let result = item;
          for (const stage of stages) result = await stage(result, item, index);
          return result;
        } catch {
          return null;
        }
      }),
    );

  const phase = (title) => {
    currentPhase = title;
  };
  const log = () => {};
  const budget = { total: null, spent: () => 0, remaining: () => Infinity };
  return { agent, parallel, pipeline, phase, log, budget };
}

async function main() {
  const [scriptPath, argsPath, responsesPath] = process.argv.slice(2);
  const source = readFileSync(scriptPath, "utf8");
  const parsedArgs = JSON.parse(readFileSync(argsPath, "utf8"));
  const responses = JSON.parse(readFileSync(responsesPath, "utf8"));
  const calls = [];
  const rt = buildRuntime(responses, calls);
  const body = new AsyncFunction(
    "agent", "parallel", "pipeline", "phase", "log", "args", "budget",
    scriptBody(source),
  );
  installForbiddenGlobals();
  redirectConsoleToStderr();
  // JSON null stands for "args not provided" (undefined in the real runtime).
  const args = parsedArgs === null ? undefined : parsedArgs;
  const result = await body(rt.agent, rt.parallel, rt.pipeline, rt.phase, rt.log, args, rt.budget);
  process.stdout.write(JSON.stringify({ calls, return: result === undefined ? null : result }));
}

main().catch((err) => {
  const message = err && err.message ? err.message : String(err);
  const stack = err && err.stack ? "\n" + err.stack : "";
  process.stderr.write(`workflow error: ${message}${stack}\n`);
  process.exitCode = 1;
});
