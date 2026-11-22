#!/usr/bin/env bash
# merge.sh <repo> <base> <branch> <worktree> <approved_head> <strategy> <task_id> <subject_file>
#
# Deterministic, idempotent merge of a task branch into <base>. stdout carries exactly one JSON
# object; diagnostics go to stderr.
#
#   squash : {"merge_sha":..., "already_merged":bool}
#   pr     : {"pr_url":..., "already_merged":false}
#   needs_human (a normal result, exit 0):
#            {"already_merged":false, "needs_human":true, "reason":...}
# Non-zero exit is for usage errors (2) and unexpected failures only.
#
# Algorithm (squash):
#   1. A commit reachable from <base> carrying the note refs/notes/dioxide == <task_id> means the
#      merge already happened -> return it.
#   2. <approved_head> (hex, full or abbreviated) is resolved to a full commit sha in <repo>
#      (unresolvable or ambiguous -> needs_human); the branch tip must equal that full sha,
#      strictly; the worktree index must match HEAD.
#      The branch ref is never moved by this script. A merge left in progress in <worktree> by a
#      killed earlier run (MERGE_HEAD, HEAD == <approved_head>) is aborted first.
#   3. In <worktree>, compute the tree of <base> merged into the branch WITHOUT committing
#      (merge --no-commit, write-tree), then restore worktree and index (merge --abort); a
#      conflict -> needs_human.
#   4. commit-tree <tree> -p <base_sha> with the message taken verbatim from <subject_file>
#      (nothing is added), then attach the note BEFORE advancing <base>, so a kill in between
#      leaves an unreachable noted commit and a rerun stays correct.
#   5. Advance <base>: ff-only merge in the worktree that has <base> checked out, otherwise an
#      atomic update-ref compare-and-set.
#   6. If advancing failed (base moved), retry from 3, at most 3 retries, then needs_human. Every
#      attempt first repeats check 1, so a concurrent run of the same task that merged meanwhile
#      is returned (already_merged) instead of being merged a second time.
# Never pushes (except the explicit "pr" strategy), never fetches, never deletes branches or
# worktrees, never touches a worktree other than <worktree> and the one that has <base> checked out.
#
# Test-only hooks (inert when unset; never set them in production):
#   DIOXIDE_TEST_KILL_AT=during_step3|after_note|after_advance
#       SIGKILL this script while the uncommitted merge of step 3 is in progress / right after the
#       note is attached / right after <base> advanced.
#   DIOXIDE_TEST_BEFORE_ADVANCE_CMD=<shell command>
#       Run once (first attempt only) after the note is attached and before <base> is advanced,
#       to simulate <base> moving between the steps.
set -euo pipefail

NOTES_REF=refs/notes/dioxide
MAX_RETRIES=3

usage() {
  echo "usage: merge.sh <repo> <base> <branch> <worktree> <approved_head> <strategy> <task_id>" \
    "<subject_file>" >&2
  exit 2
}

[ "$#" -eq 8 ] || usage
repo=$1
base=$2
branch=$3
worktree=$4
approved=$5
strategy=$6
task_id=$7
subject_file=$8

[ -n "$task_id" ] || usage
[ -s "$subject_file" ] || {
  echo "merge.sh: subject_file missing or empty: $subject_file" >&2
  exit 2
}
case "$repo" in /*) ;; *)
  echo "merge.sh: repo must be an absolute path: $repo" >&2
  exit 2
  ;;
esac
case "$worktree" in /*) ;; *)
  echo "merge.sh: worktree must be an absolute path: $worktree" >&2
  exit 2
  ;;
esac
[ -d "$worktree" ] || {
  echo "merge.sh: worktree not found: $worktree" >&2
  exit 2
}
git -C "$repo" rev-parse --git-dir >/dev/null 2>&1 || {
  echo "merge.sh: not a git repository: $repo" >&2
  exit 2
}

base_ref="refs/heads/$base"
branch_ref="refs/heads/$branch"
tmp_notes_ref="refs/notes/dioxide-tmp-$$"

cleanup() {
  git -C "$repo" update-ref -d "$tmp_notes_ref" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# JSON string escaping without jq: backslash, quote, newline, CR, tab; other control chars dropped.
json_escape() {
  local s bs dq
  bs=$'\\'
  dq='"'
  s=$(printf '%s' "$1" | LC_ALL=C tr -d '\000-\010\013\014\016-\037')
  s=${s//"$bs"/"$bs$bs"}
  s=${s//"$dq"/"$bs$dq"}
  s=${s//$'\n'/"${bs}n"}
  s=${s//$'\r'/"${bs}r"}
  s=${s//$'\t'/"${bs}t"}
  printf '%s' "$s"
}

needs_human() {
  printf '{"already_merged":false,"needs_human":true,"reason":"%s"}\n' "$(json_escape "$1")"
  exit 0
}

test_kill_at() {
  if [ "${DIOXIDE_TEST_KILL_AT:-}" = "$1" ]; then
    kill -9 $$
  fi
  return 0
}

# Prints a commit reachable from base that carries the dioxide note == task_id; returns 1 if none.
find_merged_commit() {
  local blob commit note listing
  listing=$(git -C "$repo" notes --ref="$NOTES_REF" list 2>/dev/null || true)
  [ -n "$listing" ] || return 1
  while read -r blob commit; do
    note=$(git -C "$repo" cat-file blob "$blob" 2>/dev/null || true)
    [ "$note" = "$task_id" ] || continue
    if git -C "$repo" merge-base --is-ancestor "$commit" "$base_ref" 2>/dev/null; then
      printf '%s\n' "$commit"
      return 0
    fi
  done <<END_LISTING
$listing
END_LISTING
  return 1
}

# Attach the note to $1 with a compare-and-set on the notes ref (lock-free, loses no concurrent
# note of another task): build the new notes commit on a private ref, then swap it in.
attach_note() {
  local commit=$1 attempt old new
  for attempt in $(seq 1 20); do
    old=$(git -C "$repo" rev-parse --verify -q "$NOTES_REF" || true)
    if [ -n "$old" ]; then
      git -C "$repo" update-ref "$tmp_notes_ref" "$old"
    else
      git -C "$repo" update-ref -d "$tmp_notes_ref" >/dev/null 2>&1 || true
    fi
    git -C "$repo" notes --ref="$tmp_notes_ref" add -f -m "$task_id" "$commit" >&2
    new=$(git -C "$repo" rev-parse --verify "$tmp_notes_ref")
    if git -C "$repo" update-ref "$NOTES_REF" "$new" "$old" 2>/dev/null; then
      return 0
    fi
    sleep 0.05
  done
  echo "merge.sh: could not attach note after 20 attempts" >&2
  return 1
}

# Replace $approved by its full commit sha; needs_human unless it is hex naming exactly one commit.
# (Runs in the main shell, not in a command substitution, so needs_human ends the script.)
resolve_approved() {
  local full
  case "$approved" in
    "" | *[!0-9a-f]*)
      needs_human "approved head $approved does not resolve: not a hexadecimal commit id"
      ;;
  esac
  [ "${#approved}" -ge 4 ] ||
    needs_human "approved head $approved does not resolve: shorter than 4 characters"
  full=$(git -C "$repo" rev-parse --verify -q "$approved^{commit}" 2>/dev/null) ||
    needs_human "approved head $approved does not resolve to exactly one commit in $repo"
  approved=$full
}

has_merge_in_progress() {
  git -C "$worktree" rev-parse -q --verify MERGE_HEAD >/dev/null 2>&1
}

# Path of the worktree that has base checked out, or empty.
base_worktree_path() {
  local list
  list=$(git -C "$repo" worktree list --porcelain)
  printf '%s\n' "$list" | awk -v ref="$base_ref" \
    '/^worktree /{p=substr($0,10)} $1=="branch" && $2==ref {print p; exit}'
}

merge_pr() {
  local tip remote url title body_file
  resolve_approved
  tip=$(git -C "$repo" rev-parse --verify -q "$branch_ref^{commit}") ||
    needs_human "branch not found: $branch"
  [ "$tip" = "$approved" ] ||
    needs_human "branch tip $tip differs from approved head $approved"
  remote=$(git -C "$repo" remote |
    awk 'NR==1{first=$0} $0=="origin"{o=1} END{print o ? "origin" : first}')
  [ -n "$remote" ] || needs_human "pr strategy requires a git remote; none configured"
  command -v gh >/dev/null 2>&1 || needs_human "pr strategy requires the gh CLI; not installed"
  git -C "$repo" push "$remote" "$branch_ref:$branch_ref" >&2 ||
    needs_human "git push to $remote failed"
  url=$(cd "$repo" && gh pr view "$branch" --json state,url \
    --jq 'select(.state == "OPEN") | .url' 2>/dev/null || true)
  if [ -z "$url" ]; then
    title=$(head -n 1 "$subject_file")
    body_file=$(mktemp)
    tail -n +2 "$subject_file" >"$body_file"
    url=$(cd "$repo" && gh pr create --base "$base" --head "$branch" --title "$title" \
      --body-file "$body_file") || {
      rm -f "$body_file"
      needs_human "gh pr create failed"
    }
    rm -f "$body_file"
    url=$(printf '%s\n' "$url" | tail -n 1)
  fi
  printf '{"pr_url":"%s","already_merged":false}\n' "$(json_escape "$url")"
}

merge_squash() {
  local merged tip attempt base_sha out conflicts tree commit wt last_error="" advanced

  git -C "$repo" rev-parse --verify -q "$base_ref" >/dev/null ||
    needs_human "base branch not found: $base"

  # 1. already merged?
  if merged=$(find_merged_commit); then
    printf '{"merge_sha":"%s","already_merged":true}\n' "$merged"
    return 0
  fi

  # 2. tip == approved head (strict, full shas); clean index; abort a merge left over by a killed run
  resolve_approved
  tip=$(git -C "$repo" rev-parse --verify -q "$branch_ref^{commit}") ||
    needs_human "branch not found: $branch"
  [ "$(git -C "$worktree" symbolic-ref -q HEAD || true)" = "$branch_ref" ] ||
    needs_human "worktree $worktree does not have $branch checked out"
  if [ "$tip" = "$approved" ] && has_merge_in_progress; then
    git -C "$worktree" merge --abort >&2
  fi
  [ "$tip" = "$approved" ] ||
    needs_human "branch tip $tip differs from approved head $approved"
  git -C "$worktree" diff --cached --quiet ||
    needs_human "worktree $worktree has staged, uncommitted changes; refusing to merge them"

  for attempt in $(seq 0 "$MAX_RETRIES"); do
    # 1 again: a concurrent run of the same task may have merged while this one was retrying
    if merged=$(find_merged_commit); then
      printf '{"merge_sha":"%s","already_merged":true}\n' "$merged"
      return 0
    fi
    base_sha=$(git -C "$repo" rev-parse --verify "$base_ref")

    # 3. tree of base merged into the branch, computed without committing or moving the branch
    if ! out=$(git -C "$worktree" merge --no-ff --no-commit "$base_sha" 2>&1); then
      if has_merge_in_progress; then
        conflicts=$(git -C "$worktree" diff --name-only --diff-filter=U | tr '\n' ' ')
        git -C "$worktree" merge --abort >&2 || true
        needs_human "merge conflict merging $base into $branch: ${conflicts:-unknown files}"
      fi
      needs_human "merging $base into $branch failed: $out"
    fi
    printf '%s\n' "$out" >&2
    if has_merge_in_progress; then
      tree=$(git -C "$worktree" write-tree)
      test_kill_at during_step3
      git -C "$worktree" merge --abort >&2
    else
      tree=$(git -C "$worktree" rev-parse 'HEAD^{tree}') # already up to date
    fi

    # 4. commit with the agent-written subject, note first
    commit=$(git -C "$repo" commit-tree "$tree" -p "$base_sha" -F "$subject_file")
    attach_note "$commit"
    test_kill_at after_note
    if [ "$attempt" -eq 0 ] && [ -n "${DIOXIDE_TEST_BEFORE_ADVANCE_CMD:-}" ]; then
      sh -c "$DIOXIDE_TEST_BEFORE_ADVANCE_CMD" >&2
    fi

    # 5. advance base
    advanced=false
    wt=$(base_worktree_path)
    if [ -n "$wt" ]; then
      if [ "$(git -C "$wt" rev-parse HEAD)" != "$base_sha" ]; then
        out="base moved: HEAD of $wt is no longer $base_sha"
      elif out=$(git -C "$wt" merge --ff-only "$commit" 2>&1); then
        advanced=true
      fi
    elif out=$(git -C "$repo" update-ref "$base_ref" "$commit" "$base_sha" 2>&1); then
      advanced=true
    fi
    if $advanced; then
      printf '%s\n' "$out" >&2
      test_kill_at after_advance
      printf '{"merge_sha":"%s","already_merged":false}\n' "$commit"
      return 0
    fi
    last_error=$out
    echo "merge.sh: advancing $base failed (attempt $((attempt + 1))): $out" >&2
    sleep 0.2
  done

  # 6. retries exhausted
  needs_human "could not advance $base after $MAX_RETRIES retries: $last_error"
}

case "$strategy" in
  squash) merge_squash ;;
  pr) merge_pr ;;
  *) needs_human "unknown merge strategy: $strategy" ;;
esac
