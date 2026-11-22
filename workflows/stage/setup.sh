#!/usr/bin/env bash
# setup.sh <repo> <base> <branch> <worktree>
#
# Idempotent task worktree setup. stdout: one JSON object {"head_sha":..., "reused":bool}.
# Diagnostics go to stderr. Never pushes, fetches, deletes or prunes anything, except as below.
#
#   * <worktree> registered but still locked "initializing" -> `git worktree add` was killed
#                                             mid-checkout (git leaves that lock, a partial checkout
#                                             and no index). Setup never returned, so nothing of value
#                                             is in it: that registration and its directory are
#                                             removed (`worktree remove -f -f`) and the add runs again.
#                                             The branch is never touched. Any other lock is left alone.
#   * worktree already exists on <branch>  -> reused (nothing is touched)
#   * <branch> exists, worktree missing    -> worktree add on the existing branch (its commits are
#                                             kept; -B would reset them to <base>)
#   * otherwise                            -> git worktree add -B <branch> <worktree> <base>
# If the add fails because a registered worktree's directory is gone, `git worktree prune` runs
# once (it only drops stale admin entries, never a directory or a branch) and the add is retried.
set -euo pipefail

usage() {
  echo "usage: setup.sh <repo> <base> <branch> <worktree>" >&2
  exit 2
}

[ "$#" -eq 4 ] || usage
repo=$1
base=$2
branch=$3
worktree=$4

case "$repo" in /*) ;; *)
  echo "setup.sh: repo must be an absolute path: $repo" >&2
  exit 2
  ;;
esac
case "$worktree" in /*) ;; *)
  echo "setup.sh: worktree must be an absolute path: $worktree" >&2
  exit 2
  ;;
esac

git -C "$repo" rev-parse --git-dir >/dev/null 2>&1 || {
  echo "setup.sh: not a git repository: $repo" >&2
  exit 1
}

# Registered worktree path (physical) whose checked-out branch is $1, or empty.
worktree_path_for_branch() {
  local list
  list=$(git -C "$repo" worktree list --porcelain)
  printf '%s\n' "$list" | awk -v ref="refs/heads/$1" \
    '/^worktree /{p=substr($0,10)} $1=="branch" && $2==ref {print p; exit}'
}

physical_path() {
  (cd "$1" 2>/dev/null && pwd -P) || true
}

emit() {
  [ -n "$1" ] || {
    echo "setup.sh: could not determine head_sha" >&2
    exit 1
  }
  printf '{"head_sha":"%s","reused":%s}\n' "$1" "$2"
}

# Registered worktree path for $1 (given or physical) whose lock reason is "initializing", or empty.
initializing_worktree() {
  local list
  list=$(git -C "$repo" worktree list --porcelain)
  printf '%s\n' "$list" | awk -v a="$1" -v b="$2" '
    /^worktree /{p=substr($0,10)}
    $0=="locked initializing" && (p==a || (b!="" && p==b)) {print p; exit}'
}

half=$(initializing_worktree "$worktree" "$(physical_path "$worktree")")
if [ -n "$half" ]; then
  echo "setup.sh: removing half-created worktree left by a killed add: $half" >&2
  git -C "$repo" worktree remove -f -f "$half" >&2
fi

if [ -d "$worktree" ]; then
  want=$(physical_path "$worktree")
  have=$(worktree_path_for_branch "$branch")
  if [ -n "$want" ] && [ "$want" = "$have" ]; then
    emit "$(git -C "$worktree" rev-parse HEAD)" true
    exit 0
  fi
fi

# worktree add with one prune-and-retry for a registered worktree whose directory is gone
add_worktree() {
  git -C "$repo" worktree add "$@" >&2 && return 0
  git -C "$repo" worktree prune >&2
  git -C "$repo" worktree add "$@" >&2
}

if git -C "$repo" rev-parse --verify -q "refs/heads/$branch" >/dev/null; then
  add_worktree "$worktree" "$branch"
else
  git -C "$repo" rev-parse --verify -q "$base^{commit}" >/dev/null || {
    echo "setup.sh: base not found: $base" >&2
    exit 1
  }
  add_worktree -B "$branch" "$worktree" "$base"
fi
emit "$(git -C "$worktree" rev-parse HEAD)" false
