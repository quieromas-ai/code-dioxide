"""Tests for workflows/stage/setup.sh and merge.sh against real temp git repos."""

import json
import os
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

STAGE_DIR = Path(__file__).resolve().parent.parent / "workflows" / "stage"
SETUP = STAGE_DIR / "setup.sh"
MERGE = STAGE_DIR / "merge.sh"
NOTES_REF = "refs/notes/dioxide"


def _env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("GIT_", "DIOXIDE_TEST_"))}
    env["GIT_CONFIG_GLOBAL"] = os.devnull
    env["GIT_CONFIG_SYSTEM"] = os.devnull
    env["GIT_TERMINAL_PROMPT"] = "0"
    env.update(extra or {})
    return env


def git(cwd: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
        check=True,
        env=_env(),
    )
    return done.stdout.strip()


def commit_file(cwd: Path, name: str, content: str, message: str | None = None) -> str:
    (cwd / name).write_text(content)
    git(cwd, "add", name)
    git(cwd, "commit", "-q", "-m", message or f"add {name}")
    return git(cwd, "rev-parse", "HEAD")


def run_script(
    script: Path, *args: str, extra_env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(script), *args],
        capture_output=True,
        text=True,
        env=_env(extra_env),
        timeout=60,
    )


def run_json(script: Path, *args: str, extra_env: dict[str, str] | None = None) -> dict[str, Any]:
    done = run_script(script, *args, extra_env=extra_env)
    assert done.returncode == 0, done.stderr
    result: dict[str, Any] = json.loads(done.stdout)
    return result


@dataclass
class Task:
    task_id: str
    branch: str
    worktree: Path
    head: str
    subject_file: Path


@dataclass
class Repo:
    path: Path
    root: Path  # parent dir for worktrees and subject files
    base: str = "main"

    def make_task(self, task_id: str, filename: str, content: str = "work\n") -> Task:
        branch = f"feat/{task_id}"
        worktree = self.root / "wt" / task_id
        setup = run_json(SETUP, str(self.path), self.base, branch, str(worktree))
        head = commit_file(worktree, filename, content, f"wip {task_id}")
        assert setup["reused"] is False
        subject_file = self.root / f"{task_id}.subject"
        subject_file.write_text(f"feat: add {filename}\n\nBody line for {task_id}.\n")
        return Task(task_id, branch, worktree, head, subject_file)

    def merge_args(
        self, task: Task, strategy: str = "squash", head: str | None = None
    ) -> list[str]:
        return [
            str(self.path),
            self.base,
            task.branch,
            str(task.worktree),
            head or task.head,
            strategy,
            task.task_id,
            str(task.subject_file),
        ]

    def merge(
        self,
        task: Task,
        strategy: str = "squash",
        head: str | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        return run_json(MERGE, *self.merge_args(task, strategy, head), extra_env=extra_env)

    def base_sha(self) -> str:
        return git(self.path, "rev-parse", f"refs/heads/{self.base}")

    def note(self, commit: str) -> str:
        return git(self.path, "notes", f"--ref={NOTES_REF}", "show", commit)

    def is_ancestor(self, commit: str, of: str | None = None) -> bool:
        done = subprocess.run(
            ["git", "-C", str(self.path), "merge-base", "--is-ancestor", commit, of or self.base],
            env=_env(),
        )
        return done.returncode == 0

    def noted_commits_on_base(self, task_id: str) -> list[str]:
        listing = git(self.path, "notes", f"--ref={NOTES_REF}", "list")
        found = []
        for line in listing.splitlines():
            blob, commit = line.split()
            if git(self.path, "cat-file", "blob", blob) == task_id and self.is_ancestor(commit):
                found.append(commit)
        return found


@pytest.fixture
def repo(tmp_path: Path) -> Repo:
    path = tmp_path / "repo"
    path.mkdir()
    git(path, "init", "-q", "-b", "main")
    git(path, "config", "user.name", "Test User")
    git(path, "config", "user.email", "test@example.com")
    commit_file(path, "a.txt", "one\n", "initial")
    return Repo(path=path, root=tmp_path)


@pytest.fixture(params=["base_checked_out", "base_not_checked_out"])
def repo_mode(request: pytest.FixtureRequest, repo: Repo) -> Repo:
    """The repo with base checked out (ff-only path) or not (update-ref compare-and-set path)."""
    if request.param == "base_not_checked_out":
        git(repo.path, "checkout", "-q", "-b", "side")
    return repo


def test_setup_idempotent(repo: Repo) -> None:
    worktree = repo.root / "wt" / "t1"
    args = (str(repo.path), "main", "feat/t1", str(worktree))
    first = run_json(SETUP, *args)
    assert first["reused"] is False
    assert first["head_sha"] == repo.base_sha()
    assert git(worktree, "symbolic-ref", "HEAD") == "refs/heads/feat/t1"

    head = commit_file(worktree, "b.txt", "b\n")
    second = run_json(SETUP, *args)
    assert second == {"head_sha": head, "reused": True}
    assert git(worktree, "rev-parse", "HEAD") == head  # nothing reset


def test_setup_keeps_existing_branch_commits(repo: Repo) -> None:
    task = repo.make_task("t1", "b.txt")
    git(repo.path, "worktree", "remove", "--force", str(task.worktree))
    again = run_json(SETUP, str(repo.path), "main", task.branch, str(task.worktree))
    assert again == {"head_sha": task.head, "reused": False}


def test_setup_prunes_registered_worktree_with_missing_dir(repo: Repo) -> None:
    task = repo.make_task("t1", "b.txt")
    shutil.rmtree(task.worktree)  # directory gone, admin entry still registered
    again = run_json(SETUP, str(repo.path), "main", task.branch, str(task.worktree))
    assert again == {"head_sha": task.head, "reused": False}
    assert git(task.worktree, "symbolic-ref", "HEAD") == f"refs/heads/{task.branch}"
    assert repo.is_ancestor(task.branch, task.branch)  # branch untouched


def test_setup_killed_during_add_rerun_is_complete(repo: Repo) -> None:
    for i in range(50):
        (repo.path / f"f{i}.txt").write_text(f"{i}\n")
    git(repo.path, "add", ".")
    git(repo.path, "commit", "-q", "-m", "many files")
    worktree = repo.root / "wt" / "t1"
    args = (str(repo.path), "main", "feat/t1", str(worktree))
    run_json(SETUP, *args)
    # Recreate what git 2.55 leaves when `git worktree add` (and its `reset --hard` child) is
    # killed mid-checkout: the admin entry stays locked "initializing", the index was never
    # written (an empty index.lock remains) and only part of the files are checked out.
    admin = Path(git(worktree, "rev-parse", "--absolute-git-dir"))
    (admin / "locked").write_text("initializing\n")
    (admin / "index").unlink()
    (admin / "index.lock").write_text("")
    for i in range(25, 50):
        (worktree / f"f{i}.txt").unlink()
    (worktree / "a.txt").unlink()

    again = run_json(SETUP, *args)
    assert again == {"head_sha": repo.base_sha(), "reused": False}
    assert git(worktree, "symbolic-ref", "HEAD") == "refs/heads/feat/t1"
    assert git(worktree, "status", "--porcelain") == ""
    assert sorted(p.name for p in worktree.iterdir() if p.name != ".git") == sorted(
        ["a.txt", *(f"f{i}.txt" for i in range(50))]
    )
    assert "locked" not in git(repo.path, "worktree", "list", "--porcelain")
    assert run_json(SETUP, *args) == {"head_sha": repo.base_sha(), "reused": True}


def test_setup_user_locked_worktree_is_reused_untouched(repo: Repo) -> None:
    task = repo.make_task("t1", "b.txt")
    git(repo.path, "worktree", "lock", "--reason", "mine", str(task.worktree))
    (task.worktree / "b.txt").write_text("uncommitted work\n")
    again = run_json(SETUP, str(repo.path), "main", task.branch, str(task.worktree))
    assert again == {"head_sha": task.head, "reused": True}
    assert (task.worktree / "b.txt").read_text() == "uncommitted work\n"


def test_setup_rejects_relative_paths(repo: Repo) -> None:
    done = run_script(SETUP, "repo", "main", "feat/x", str(repo.root / "wt" / "x"))
    assert done.returncode == 2
    assert done.stdout == ""
    done = run_script(SETUP, str(repo.path), "main", "feat/x", "relative/wt")
    assert done.returncode == 2
    assert done.stdout == ""


def test_merge_squash_basic(repo_mode: Repo) -> None:
    repo = repo_mode
    start = repo.base_sha()
    task = repo.make_task("t1", "b.txt")
    result = repo.merge(task)

    sha = result["merge_sha"]
    assert result["already_merged"] is False
    assert "needs_human" not in result
    assert repo.base_sha() == sha
    assert git(repo.path, "rev-parse", f"{sha}^") == start
    assert git(repo.path, "rev-list", "--count", f"{start}..{sha}") == "1"
    assert git(repo.path, "rev-parse", f"{sha}^{{tree}}") == git(
        task.worktree, "rev-parse", "HEAD^{tree}"
    )
    # message is the subject file verbatim (exact bytes): no trailers, nothing added
    raw = subprocess.run(
        ["git", "-C", str(repo.path), "cat-file", "commit", sha],
        capture_output=True,
        text=True,
        check=True,
        env=_env(),
    ).stdout
    assert raw.split("\n\n", 1)[1] == task.subject_file.read_text()
    assert git(task.worktree, "rev-parse", f"refs/heads/{task.branch}") == task.head
    assert git(task.worktree, "status", "--porcelain") == ""
    assert git(repo.path, "log", "-1", "--format=%an <%ae>", sha) == "Test User <test@example.com>"
    assert repo.note(sha) == "t1"
    assert git(repo.path, "show", f"{sha}:b.txt") == "work"
    if repo.base == git(repo.path, "symbolic-ref", "--short", "HEAD"):
        assert git(repo.path, "status", "--porcelain") == ""
        assert (repo.path / "b.txt").read_text() == "work\n"


def test_merge_already_merged_returns_existing(repo: Repo) -> None:
    task = repo.make_task("t1", "b.txt")
    first = repo.merge(task)
    tip = repo.base_sha()

    again = repo.merge(task)
    assert again == {"merge_sha": first["merge_sha"], "already_merged": True}
    assert repo.base_sha() == tip

    # step 1 comes before the tip check: a stale approved head still resolves to the merge
    stale = repo.merge(task, head="0" * 40)
    assert stale["already_merged"] is True
    assert stale["merge_sha"] == first["merge_sha"]


def test_merge_tip_mismatch_needs_human(repo: Repo) -> None:
    task = repo.make_task("t1", "b.txt")
    commit_file(task.worktree, "c.txt", "sneaky\n")  # post-approval change
    start = repo.base_sha()

    result = repo.merge(task)
    assert result["needs_human"] is True
    assert result["already_merged"] is False
    assert "approved head" in result["reason"]
    assert repo.base_sha() == start
    assert not repo.noted_commits_on_base("t1")


def test_merge_conflict_needs_human(repo: Repo) -> None:
    task = repo.make_task("t1", "a.txt", "from task\n")
    base_tip = commit_file(repo.path, "a.txt", "from base\n", "base edit")

    result = repo.merge(task)
    assert result["needs_human"] is True
    assert "a.txt" in result["reason"]
    assert repo.base_sha() == base_tip
    assert git(task.worktree, "status", "--porcelain") == ""  # merge was aborted
    assert git(task.worktree, "rev-parse", "HEAD") == task.head


def test_merge_base_checked_out_dirty_unrelated_ok(repo: Repo) -> None:
    commit_file(repo.path, "unrelated.txt", "clean\n")
    task = repo.make_task("t1", "b.txt")
    (repo.path / "unrelated.txt").write_text("my local edit\n")
    (repo.path / "scratch.txt").write_text("untracked\n")

    result = repo.merge(task)
    assert result["already_merged"] is False
    assert repo.base_sha() == result["merge_sha"]
    assert (repo.path / "unrelated.txt").read_text() == "my local edit\n"
    assert (repo.path / "scratch.txt").read_text() == "untracked\n"
    assert (repo.path / "b.txt").read_text() == "work\n"  # ff-only checked the merge out
    status = git(repo.path, "status", "--porcelain").splitlines()
    assert sorted(line.strip() for line in status) == ["?? scratch.txt", "M unrelated.txt"]


def test_merge_base_moved_retries(repo_mode: Repo) -> None:
    repo = repo_mode
    task = repo.make_task("t1", "b.txt")
    if git(repo.path, "symbolic-ref", "--short", "HEAD") == "main":
        mover = f"git -C {shlex.quote(str(repo.path))} commit -q --allow-empty -m moved"
    else:
        mover = (
            f"git -C {shlex.quote(str(repo.path))} commit-tree -p main -m moved "
            f"$(git -C {shlex.quote(str(repo.path))} rev-parse 'main^{{tree}}') "
            f"| xargs git -C {shlex.quote(str(repo.path))} update-ref refs/heads/main"
        )
    result = repo.merge(task, extra_env={"DIOXIDE_TEST_BEFORE_ADVANCE_CMD": mover})

    sha = result["merge_sha"]
    assert "needs_human" not in result
    assert repo.base_sha() == sha
    assert git(repo.path, "log", "-1", "--format=%s", f"{sha}^") == "moved"  # built on moved base
    assert repo.noted_commits_on_base("t1") == [sha]  # one commit reachable from base
    assert git(repo.path, "show", f"{sha}:b.txt") == "work"


def test_merge_concurrent_two_tasks(repo_mode: Repo) -> None:
    repo = repo_mode
    tasks = [repo.make_task("t1", "one.txt", "1\n"), repo.make_task("t2", "two.txt", "2\n")]
    procs = [
        subprocess.Popen(
            ["bash", str(MERGE), *repo.merge_args(task)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=_env(),
        )
        for task in tasks
    ]
    results = []
    for proc in procs:
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, err
        results.append(json.loads(out))

    for task, result in zip(tasks, results, strict=True):
        assert "needs_human" not in result, result
        sha = result["merge_sha"]
        assert repo.is_ancestor(sha), f"{task.task_id} commit lost"
        assert repo.note(sha) == task.task_id
        assert repo.noted_commits_on_base(task.task_id) == [sha]
    assert results[0]["merge_sha"] != results[1]["merge_sha"]
    assert git(repo.path, "show", "main:one.txt") == "1"
    assert git(repo.path, "show", "main:two.txt") == "2"
    if repo.base == git(repo.path, "symbolic-ref", "--short", "HEAD"):
        assert git(repo.path, "status", "--porcelain") == ""


def test_merge_concurrent_same_task(repo_mode: Repo) -> None:
    repo = repo_mode
    task = repo.make_task("t1", "b.txt")
    start = repo.base_sha()
    second = " ".join(
        shlex.quote(a)
        for a in ["env", "-u", "DIOXIDE_TEST_BEFORE_ADVANCE_CMD", "bash", str(MERGE)]
        + repo.merge_args(task)
    )
    result = repo.merge(task, extra_env={"DIOXIDE_TEST_BEFORE_ADVANCE_CMD": f"{second} >&2"})

    assert "needs_human" not in result, result
    assert result["already_merged"] is True
    sha = result["merge_sha"]
    assert repo.base_sha() == sha
    assert git(repo.path, "rev-list", "--count", f"{start}..{sha}") == "1"
    assert repo.noted_commits_on_base("t1") == [sha]
    assert git(repo.path, "show", f"{sha}:b.txt") == "work"


def test_merge_abbreviated_approved_head_merges(repo: Repo) -> None:
    task = repo.make_task("t1", "b.txt")
    result = repo.merge(task, head=task.head[:7])
    assert "needs_human" not in result, result
    assert result["already_merged"] is False
    assert repo.base_sha() == result["merge_sha"]
    assert repo.noted_commits_on_base("t1") == [result["merge_sha"]]


@pytest.mark.parametrize("head", ["deadbeef", "HEAD", "feat/t1", "main", "", "abc"])
def test_merge_unresolvable_approved_head_needs_human(repo: Repo, head: str) -> None:
    task = repo.make_task("t1", "b.txt")
    start = repo.base_sha()
    args = repo.merge_args(task)
    args[4] = head
    result = run_json(MERGE, *args)
    assert result["needs_human"] is True
    assert result["already_merged"] is False
    assert "does not resolve" in result["reason"]
    assert repo.base_sha() == start
    assert not repo.noted_commits_on_base("t1")


def test_merge_squash_pushes_nothing(repo: Repo) -> None:
    remote = repo.root / "remote.git"
    git(repo.root, "init", "-q", "--bare", str(remote))
    git(repo.path, "remote", "add", "origin", str(remote))
    task = repo.make_task("t1", "b.txt")
    git(repo.path, "push", "-q", "origin", "main", task.branch)
    before = git(repo.path, "ls-remote", "origin")
    notes_before = git(remote, "for-each-ref")

    result = repo.merge(task)
    assert result["already_merged"] is False
    assert repo.base_sha() == result["merge_sha"]
    assert git(repo.path, "ls-remote", "origin") == before
    assert git(remote, "for-each-ref") == notes_before
    assert result["merge_sha"] not in before


@pytest.mark.parametrize("kill_at", ["after_note", "after_advance"])
def test_merge_kill_after_update_rerun_idempotent(repo: Repo, kill_at: str) -> None:
    task = repo.make_task("t1", "b.txt")
    start = commit_file(repo.path, "c.txt", "base moved on\n")  # makes step 3 a real merge

    killed = run_script(MERGE, *repo.merge_args(task), extra_env={"DIOXIDE_TEST_KILL_AT": kill_at})
    assert killed.returncode == -9
    assert killed.stdout == ""
    if kill_at == "after_note":
        assert repo.base_sha() == start  # base not advanced; the noted commit is unreachable
        assert repo.noted_commits_on_base("t1") == []
        assert git(task.worktree, "rev-parse", "HEAD") == task.head  # branch never moves
        assert git(task.worktree, "status", "--porcelain") == ""
    else:
        assert repo.base_sha() != start

    result = repo.merge(task)
    assert "needs_human" not in result, result
    assert result["already_merged"] is (kill_at == "after_advance")
    sha = result["merge_sha"]
    assert repo.base_sha() == sha
    assert git(repo.path, "rev-list", "--count", f"{start}..{sha}") == "1"
    assert repo.noted_commits_on_base("t1") == [sha]
    assert git(repo.path, "show", f"{sha}:b.txt") == "work"
    assert git(repo.path, "show", f"{sha}:c.txt") == "base moved on"

    third = repo.merge(task)
    assert third == {"merge_sha": sha, "already_merged": True}
    assert repo.base_sha() == sha


def test_merge_evil_merge_after_approval_needs_human(repo: Repo) -> None:
    task = repo.make_task("t1", "b.txt")
    start = commit_file(repo.path, "c.txt", "base moved on\n")
    # after approval: merge base without committing, smuggle in a file, commit
    git(task.worktree, "merge", "--no-ff", "--no-commit", "main")
    (task.worktree / "evil.txt").write_text("unreviewed\n")
    git(task.worktree, "add", "evil.txt")
    git(task.worktree, "commit", "-q", "-m", "merge main")

    result = repo.merge(task)
    assert result["needs_human"] is True
    assert "approved head" in result["reason"]
    assert repo.base_sha() == start
    assert not (repo.path / "evil.txt").exists()
    assert not repo.noted_commits_on_base("t1")


def test_merge_staged_change_needs_human(repo: Repo) -> None:
    task = repo.make_task("t1", "b.txt")
    start = commit_file(repo.path, "c.txt", "base moved on\n")
    (task.worktree / "staged.txt").write_text("never reviewed\n")
    git(task.worktree, "add", "staged.txt")

    result = repo.merge(task)
    assert result["needs_human"] is True
    assert "staged" in result["reason"]
    assert repo.base_sha() == start
    assert git(task.worktree, "diff", "--cached", "--name-only") == "staged.txt"  # left alone


def test_merge_unstaged_and_untracked_never_enter_tree(repo: Repo) -> None:
    task = repo.make_task("t1", "b.txt")
    commit_file(repo.path, "c.txt", "base moved on\n")
    (task.worktree / "b.txt").write_text("edited, not committed\n")
    (task.worktree / "scratch.txt").write_text("untracked\n")

    result = repo.merge(task)
    sha = result["merge_sha"]
    assert git(repo.path, "show", f"{sha}:b.txt") == "work"
    assert git(repo.path, "ls-tree", "--name-only", sha).split() == ["a.txt", "b.txt", "c.txt"]
    assert (task.worktree / "b.txt").read_text() == "edited, not committed\n"
    assert (task.worktree / "scratch.txt").exists()


def test_merge_kill_during_step3_rerun_ok(repo: Repo) -> None:
    task = repo.make_task("t1", "b.txt")
    start = commit_file(repo.path, "c.txt", "base moved on\n")

    killed = run_script(
        MERGE, *repo.merge_args(task), extra_env={"DIOXIDE_TEST_KILL_AT": "during_step3"}
    )
    assert killed.returncode == -9
    assert killed.stdout == ""
    # killed with the uncommitted merge still in progress in the task worktree
    git(task.worktree, "rev-parse", "-q", "--verify", "MERGE_HEAD")
    assert repo.base_sha() == start

    result = repo.merge(task)
    assert "needs_human" not in result, result
    assert result["already_merged"] is False
    sha = result["merge_sha"]
    assert repo.base_sha() == sha
    assert git(repo.path, "rev-list", "--count", f"{start}..{sha}") == "1"
    assert repo.noted_commits_on_base("t1") == [sha]
    assert git(repo.path, "show", f"{sha}:b.txt") == "work"
    assert git(repo.path, "show", f"{sha}:c.txt") == "base moved on"
    assert git(task.worktree, "rev-parse", f"refs/heads/{task.branch}") == task.head
    assert git(task.worktree, "status", "--porcelain") == ""


def test_merge_unknown_strategy_json_escaped(repo: Repo) -> None:
    task = repo.make_task("t1", "b.txt")
    weird = 'bad "quoted" \\ back\nnew\tline'
    result = repo.merge(task, strategy=weird)
    assert result["needs_human"] is True
    assert result["reason"] == f"unknown merge strategy: {weird}"


def test_merge_pr_without_remote_needs_human(repo: Repo) -> None:
    task = repo.make_task("t1", "b.txt")
    start = repo.base_sha()
    result = repo.merge(task, strategy="pr")
    assert result["needs_human"] is True
    assert "remote" in result["reason"]
    assert repo.base_sha() == start


def test_merge_usage_errors(repo: Repo) -> None:
    assert run_script(MERGE, "only", "three", "args").returncode == 2
    task = repo.make_task("t1", "b.txt")
    task.subject_file.write_text("")
    done = run_script(MERGE, *repo.merge_args(task))
    assert done.returncode == 2
    assert done.stdout == ""
    assert run_script(SETUP, "x").returncode == 2
    task.subject_file.write_text("subject\n")
    relative = run_script(MERGE, "repo", *repo.merge_args(task)[1:])
    assert relative.returncode == 2
    assert relative.stdout == ""
    assert "absolute" in relative.stderr
    args = repo.merge_args(task)
    args[3] = "relative/wt"
    relative = run_script(MERGE, *args)
    assert relative.returncode == 2
    assert "absolute" in relative.stderr
    assert run_script(MERGE, *repo.merge_args(task)).returncode == 0


@pytest.mark.parametrize("script", [SETUP, MERGE])
def test_scripts_are_executable(script: Path) -> None:
    assert os.access(script, os.X_OK)
