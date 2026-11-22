"""AT-X6: every database write goes through ``write_tx()``.

A static AST check over ``src/``. A call to ``.execute(`` / ``.executemany(`` /
``.executescript(`` outside ``src/database.py`` counts as a write unless its SQL is a literal
that starts with SELECT, PRAGMA or WITH. A write is accepted when it sits lexically inside an
``async with write_tx()`` block, or inside a function that takes a ``db`` parameter and never
calls ``get_db()`` (the helper contract: helpers called inside a transaction receive its
connection).

Limits (pragmatic, not a proof):
- the SQL is read from a literal, an f-string's leading text or the left side of ``+``; SQL held
  in a variable is treated as a write, so it must live in an accepted place;
- a ``db``-parameter helper is trusted to be called only from inside a transaction; the check
  does not follow call sites;
- a ``WITH`` statement that ends in INSERT/UPDATE/DELETE passes as a read;
- calls through ``getattr`` or other aliases of ``execute`` are not seen.
"""

import ast
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src"
EXECUTE = {"execute", "executemany", "executescript"}
READS = ("SELECT", "PRAGMA", "WITH")


def _leading_sql(node: ast.expr | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr) and node.values:
        return _leading_sql(node.values[0])  # type: ignore[arg-type]
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _leading_sql(node.left)
    return None


def _is_write_tx(item: ast.withitem) -> bool:
    call = item.context_expr
    if not isinstance(call, ast.Call):
        return False
    return isinstance(call.func, ast.Name) and call.func.id == "write_tx"


def _calls_get_db(fn: ast.AST) -> bool:
    return any(
        isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "get_db"
        for n in ast.walk(fn)
    )


class _Checker(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.in_tx = 0
        self.db_helper: list[bool] = []
        self.violations: list[str] = []

    def _function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        params = {a.arg for a in node.args.args + node.args.kwonlyargs}
        self.db_helper.append("db" in params and not _calls_get_db(node))
        saved, self.in_tx = self.in_tx, 0
        self.generic_visit(node)
        self.in_tx = saved
        self.db_helper.pop()

    visit_FunctionDef = _function
    visit_AsyncFunctionDef = _function

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        tx = any(_is_write_tx(item) for item in node.items)
        self.in_tx += tx
        self.generic_visit(node)
        self.in_tx -= tx

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute) and node.func.attr in EXECUTE:
            sql = _leading_sql(node.args[0] if node.args else None)
            is_read = sql is not None and sql.lstrip().upper().startswith(READS)
            allowed = self.in_tx or (self.db_helper and self.db_helper[-1])
            if not is_read and not allowed:
                self.violations.append(f"{self.path.name}:{node.lineno}")
        self.generic_visit(node)


def violations(source: str, path: Path) -> list[str]:
    checker = _Checker(path)
    checker.visit(ast.parse(source))
    return checker.violations


def test_writes_only_inside_write_tx() -> None:
    found: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        if path.name != "database.py":
            found += violations(path.read_text(encoding="utf-8"), path)
    assert not found, f"database writes outside write_tx(): {found}"


def test_checker_flags_writes_outside_transactions() -> None:
    bad = """
async def f():
    db = get_db()
    await db.execute("UPDATE tasks SET status = 'x'")
    await db.execute(sql)
    await db.execute("SELECT 1")
    async with write_tx() as tx:
        await tx.execute("DELETE FROM tasks")

async def helper(db):
    await db.execute("INSERT INTO events VALUES (1)")

async def sneaky(db):
    await get_db().execute("INSERT INTO events VALUES (1)")
"""
    assert violations(bad, Path("x.py")) == ["x.py:4", "x.py:5", "x.py:14"]
