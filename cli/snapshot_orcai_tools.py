"""Snapshot an orcai-mcp server's tool schemas into the parity fixture.

Only `initialize` + `tools/list` are sent. Tool descriptions and schema free text
(`description`, `title`, `examples`) are dropped so the fixture stays generic. A
hand-maintained `return_keys` section already in the fixture is preserved.
"""

import asyncio
import json
from pathlib import Path
from typing import Any

import click
import httpx2
from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared._httpx_utils import create_mcp_http_client

FIXTURE_PATH = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "orcai_tools.json"
_FREE_TEXT_KEYWORDS = frozenset({"description", "title", "examples"})
_SCHEMA_MAPS = frozenset({"properties", "$defs", "definitions", "patternProperties"})


def scrub_schema(node: Any) -> Any:
    """Drop free-text keywords from a JSON schema; keep property names intact."""
    if isinstance(node, list):
        return [scrub_schema(item) for item in node]
    if not isinstance(node, dict):
        return node
    cleaned: dict[str, Any] = {}
    for key, value in node.items():
        if key in _FREE_TEXT_KEYWORDS:
            continue
        if key in _SCHEMA_MAPS and isinstance(value, dict):
            cleaned[key] = {name: scrub_schema(sub) for name, sub in value.items()}
        else:
            cleaned[key] = scrub_schema(value)
    return cleaned


async def fetch_tools(url: str, token: str | None) -> list[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"} if token else None
    http_client: httpx2.AsyncClient = create_mcp_http_client(headers=headers)
    async with http_client:
        transport = streamable_http_client(url, http_client=http_client)
        async with Client(transport, mode="legacy", cache=None) as client:
            result = await client.list_tools()
    tools = [
        {"name": tool.name, "inputSchema": scrub_schema(tool.input_schema)} for tool in result.tools
    ]
    return sorted(tools, key=lambda tool: str(tool["name"]))


def write_fixture(path: Path, tools: list[dict[str, Any]]) -> None:
    return_keys: dict[str, Any] = {}
    if path.is_file():
        return_keys = json.loads(path.read_text(encoding="utf-8")).get("return_keys", {})
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"tools": tools, "return_keys": return_keys}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


@click.command()
@click.argument("url")
@click.option("--token", default=None, help="Bearer token, if the server requires one.")
@click.option(
    "--out", "out", type=click.Path(path_type=Path), default=FIXTURE_PATH, show_default=True
)
def main(url: str, token: str | None, out: Path) -> None:
    """Write the tool schemas served at URL to the parity fixture."""
    tools = asyncio.run(fetch_tools(url, token))
    write_fixture(out, tools)
    click.echo(f"wrote {len(tools)} tools to {out}")


if __name__ == "__main__":
    main()
