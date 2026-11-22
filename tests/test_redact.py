import json
import time
from pathlib import Path

import pytest

from src.config import Settings
from src.logpager import Cursor, Source, read_page
from src.redact import Redactor

MASK = "****"

# Fake credentials, assembled from parts so this file holds no scannable secret literal.
SK = "sk-" + "ant-" + "x" * 40
SLACK_BOT = "xo" + "xb-" + "1" * 12 + "-" + "y" * 20
SLACK_APP = "xa" + "pp-" + "1-" + "Z" * 20
GH_PAT_CLASSIC = "gh" + "p_" + "q" * 36
GH_PAT_FINE = "github_" + "pat_" + "w" * 30
AWS = "AK" + "IA" + "A" * 16
JWT = "ey" + "J" + "a" * 12 + "." + "b" * 12 + "." + "c" * 12
PEM_BEGIN = "-----BEGIN " + "PRIVATE KEY-----"
PEM_END = "-----END " + "PRIVATE KEY-----"
PEM_BODY = ["M" * 64, "N" * 64, "O" * 20]
PEM_BLOCK = [PEM_BEGIN, *PEM_BODY, PEM_END]


def make(home: Path, *, extra: list[Path] | None = None) -> Redactor:
    return Redactor.from_settings(Settings(home=home, redact_env_files=extra or []))


def test_each_pattern_redacted(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    cases = {
        "anthropic": (f"key {SK} end", SK),
        "slack bot": (f"key {SLACK_BOT} end", SLACK_BOT),
        "slack app": (f"key {SLACK_APP} end", SLACK_APP),
        "github classic": (f"key {GH_PAT_CLASSIC} end", GH_PAT_CLASSIC),
        "github fine-grained": (f"key {GH_PAT_FINE} end", GH_PAT_FINE),
        "aws": (f"key {AWS} end", AWS),
        "jwt": (f"key {JWT} end", JWT),
        "bearer": ("Authorization: Bearer " + "t" * 30, "t" * 30),
        "kv secret": ("my_secret=" + "s" * 12, "s" * 12),
        "kv token": ("token: " + "k" * 12, "k" * 12),
        "kv password": ("Password = " + "p" * 12, "p" * 12),
        "kv api_key": ("API_KEY=" + "a" * 12, "a" * 12),
    }
    for name, (line, secret) in cases.items():
        out = redactor.redact_line(line)
        assert secret not in out, name
        assert MASK in out, name

    # PEM block fed line by line, as a log stream does: every line is masked.
    redactor.reset()
    outputs = [redactor.redact_line(line) for line in ["before", *PEM_BLOCK, "after"]]
    assert outputs == ["before", *[MASK] * len(PEM_BLOCK), "after"]


def test_key_and_separator_kept_for_key_value_forms(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    assert redactor.redact_line("Authorization: Bearer abc123") == "Authorization: Bearer ****"
    assert redactor.redact_line("secret=abc123") == "secret=****"
    assert redactor.redact_line("password : abc123") == "password : ****"


def test_json_ish_forms(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    assert redactor.redact_line('"password": "abc"') == '"password": "****"'
    assert redactor.redact_line('{"api_key":"abc","n":1}') == '{"api_key":"****","n":1}'
    assert redactor.redact_line('"token": "two words"') == '"token": "****"'
    # JSON nested inside a JSON string (backslash-escaped quotes).
    nested = json.dumps({"text": '{"password": "abc"}'})
    out = redactor.redact_line(nested)
    assert "abc" not in out
    assert json.loads(json.loads(out)["text"]) == {"password": MASK}


TOKEN_SAMPLES = {
    "anthropic": SK,
    "slack bot": SLACK_BOT,
    "slack app": SLACK_APP,
    "github classic": GH_PAT_CLASSIC,
    "github fine-grained": GH_PAT_FINE,
    "aws": AWS,
    "jwt": JWT,
}


def test_tokens_after_json_escapes(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for name, token in TOKEN_SAMPLES.items():
        for escape in ("\n", "\t", "\r", "\x1b", "\u00e9"):
            line = json.dumps({"content": "line1" + escape + token + escape + "tail"})
            out = redactor.redact_line(line)
            assert token not in out, (name, repr(escape))
            assert json.loads(out)["content"] == "line1" + escape + MASK + escape + "tail"


def test_values_are_quote_and_escape_safe(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    assert redactor.redact_line("password=ab'cdefgh") == "password=****"
    assert redactor.redact_line("password='y z'") == "password='****'"

    secrets = ["hunter22", 'abc"defghi', "ab'cdefgh", "back\\slash"]
    plain_lines = []
    for secret in secrets:
        plain_lines.append(json.dumps({"password": secret, "n": 1}))
        plain_lines.append(json.dumps({"msg": f"password={secret}", "n": 1}))
    # JSON nested once in a JSON string; quotes/backslashes inside would be doubly escaped
    # (deferred), so only plain secrets are used for this shape.
    for secret in ("hunter22", "two words", "ab'cdefgh"):
        plain_lines.append(json.dumps({"msg": f'x {{"api_key": {json.dumps(secret)}}} y'}))
    plain_lines.append(json.dumps({"password": "two words", "n": 1}))
    plain_lines.append(json.dumps({"text": '{"Authorization": "Bearer abcdef"}'}))
    plain_lines.append(json.dumps({"t": 'password=hunter22" more'}))
    for line in plain_lines:
        out = redactor.redact_line(line)
        parsed = json.loads(out)  # still valid JSON
        assert "hunter22" not in out and "abcdef" not in out, line
        assert "defghi" not in out and "cdefgh" not in out, line
        assert "slash" not in out and "words" not in out, line
        assert parsed.get("n", 1) == 1


def test_wider_keys_and_bare_literals(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    assert redactor.redact_line("SECRET_KEY=abcdef") == "SECRET_KEY=****"
    assert redactor.redact_line("DB_PASSWORD_PROD=abcdef") == "DB_PASSWORD_PROD=****"
    assert redactor.redact_line("passwd: abcdef") == "passwd: ****"
    assert redactor.redact_line('{"aws_secret_access_key": "abcdef"}') == (
        '{"aws_secret_access_key":"****"}'
    )
    assert redactor.redact_line('{"apikey":"abcdef"}') == '{"apikey":"****"}'

    # Usage counters and other bare JSON literals stay intact (valid JSON preserved).
    for line in (
        '{"usage": {"input_tokens": 1234, "max_tokens": 4096}}',
        '{"token": 12345678}',
        '{"token": -1.5e3, "secret": true, "password": null, "api_key": false}',
    ):
        assert json.loads(redactor.redact_line(line)) == json.loads(line)
    for line in ("max_tokens: 4096", "token=12345678", "token: true,"):
        assert redactor.redact_line(line) == line
    assert redactor.redact_line("token=12345678x") == "token=****"
    assert redactor.redact_line('"token": "12345678"') == '"token": "****"'


def test_pem_single_line_with_escaped_newlines(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    line = json.dumps({"k": "\n".join(PEM_BLOCK)})
    assert "\\n" in line
    out = redactor.redact_line(line)
    assert "MMMM" not in out
    assert PEM_BEGIN not in out
    assert json.loads(out) == {"k": MASK}


def test_pem_state_reset_and_redact_text(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    redactor.redact_line(PEM_BEGIN)  # opened, never closed
    assert redactor.redact_line("body") == MASK
    redactor.reset()
    assert redactor.redact_line("body") == "body"

    redactor.redact_line(PEM_BEGIN)
    text = "\n".join(["a", *PEM_BLOCK, "b"])
    assert redactor.redact_text(text) == "\n".join(["a", *[MASK] * len(PEM_BLOCK), "b"])
    assert redactor.redact_line("c") == "c"  # redact_text leaves no open block behind


def test_env_values_raw_and_json_escaped(tmp_path: Path) -> None:
    value = 'pa"ss\\word-12345'
    (tmp_path / ".env").write_text("OTHER=short\nSOME_KEY='" + value + "'\n")
    redactor = make(tmp_path)

    assert redactor.redact_line(f"raw {value} end") == f"raw {MASK} end"

    escaped_line = json.dumps({"msg": f"x {value} y"})
    assert json.dumps(value)[1:-1] in escaped_line
    assert json.dumps(value)[1:-1] != value
    out = redactor.redact_line(escaped_line)
    assert json.dumps(value)[1:-1] not in out
    assert json.loads(out) == {"msg": f"x {MASK} y"}


def test_env_values_unicode_escaped_forms(tmp_path: Path) -> None:
    value = 'pässwörd"-' + "9" * 6
    (tmp_path / ".env").write_text(f"APP_SECRET={value}\n")
    redactor = make(tmp_path)
    for dumped in (json.dumps(value), json.dumps(value, ensure_ascii=False)):
        assert redactor.redact_line(dumped) == f'"{MASK}"'
    assert redactor.redact_line(value) == MASK


def test_settings_secret_fields_registered(tmp_path: Path) -> None:
    auth = "auth-from-process-env-1"
    settings = Settings(home=tmp_path, auth_token=auth)
    redactor = Redactor.from_settings(settings)
    assert redactor.redact_line(f"x {auth} y") == f"x {MASK} y"
    assert make(tmp_path).redact_line(auth) == auth  # empty settings register nothing


def test_extra_env_files(tmp_path: Path) -> None:
    extra = tmp_path / "extra" / "secrets.env"
    extra.parent.mkdir()
    extra.write_text(
        "# comment\n\nexport A_KEY='quoted-value-111'\nB_KEY=\"dq-value-22222\"\nnot a pair\n"
    )
    missing = tmp_path / "does-not-exist.env"
    home = tmp_path / "home"
    home.mkdir()
    (home / ".env").write_text("HOME_KEY=home-value-3333\n")
    redactor = make(home, extra=[missing, extra])

    for secret in ("quoted-value-111", "dq-value-22222", "home-value-3333"):
        assert redactor.redact_line(f"x {secret} y") == f"x {MASK} y"
    assert redactor.redact_line("quote chars ' and \" remain") == "quote chars ' and \" remain"


def test_home_env_only_secret_named_keys(tmp_path: Path) -> None:
    token = "home-auth-" + "t" * 8
    other = "home-other-" + "s" * 8
    plain = "/srv/projects/demo"
    (tmp_path / ".env").write_text(
        f"DIOXIDE_AUTH_TOKEN={token}\nDIOXIDE_OTHER_TOKEN={other}\n"
        f"DIOXIDE_DEFAULT_PROJECT_DIR={plain}\n"
    )
    extra = tmp_path / "listed.env"
    extra.write_text(f"DIOXIDE_DEFAULT_PROJECT_DIR={plain}\n")

    home_only = make(tmp_path)
    assert home_only.redact_line(f"{token} {other}") == f"{MASK} {MASK}"
    assert home_only.redact_line(f"dir {plain}") == f"dir {plain}"

    listed = make(tmp_path, extra=[extra])
    assert listed.redact_line(f"dir {plain}") == f"dir {MASK}"


def test_env_inline_comments(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "A_TOKEN=unquoted-value-1   # trailing note\n"
        "B_TOKEN='quoted-#-value-22' # note\n"
        'C_TOKEN="dq-#-value-3333"\n'
        "D_TOKEN=hash#inside-value-4\n"
    )
    redactor = make(tmp_path)
    assert redactor.redact_line("x unquoted-value-1 y") == f"x {MASK} y"
    assert redactor.redact_line("x quoted-#-value-22 y") == f"x {MASK} y"
    assert redactor.redact_line("x dq-#-value-3333 y") == f"x {MASK} y"
    assert redactor.redact_line("x hash#inside-value-4 y") == f"x {MASK} y"
    assert redactor.redact_line("x # trailing note y") == "x # trailing note y"


def test_short_values_not_redacted(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("SHORT_KEY=1234567\nLONG_KEY=12345678\n")
    redactor = make(tmp_path)
    assert redactor.redact_line("a 1234567 b") == "a 1234567 b"
    assert redactor.redact_line("a 12345678 b") == f"a {MASK} b"


def test_longer_env_value_replaced_first(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("A_KEY=abcdefgh\nB_KEY=abcdefgh-TAIL-xyz\n")
    redactor = make(tmp_path)
    assert redactor.redact_line("v abcdefgh-TAIL-xyz v") == f"v {MASK} v"
    assert redactor.redact_line("v abcdefgh v") == f"v {MASK} v"


def test_redact_json(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    original = {
        "text": f"use {SK} now",
        "password": "hunter2",
        "empty_token": "",
        "api_key": 123,
        "nested": [{"msg": "token=" + "z" * 10}, "plain", 7, None, True],
        "SECRET_NOTE": {"inner": "kept"},
    }
    snapshot = json.loads(json.dumps(original))
    out = redactor.redact_json(original)
    assert original == snapshot  # input untouched
    assert out["text"] == f"use {MASK} now"
    assert out["password"] == MASK
    assert out["empty_token"] == ""
    assert out["api_key"] == 123
    assert out["nested"] == [{"msg": "token=****"}, "plain", 7, None, True]
    assert out["SECRET_NOTE"] == {"inner": MASK}  # leaves under a sensitive key


def test_redact_json_does_not_disturb_stream_state(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    redactor.redact_line(PEM_BEGIN)
    assert redactor.redact_json({"a": "plain"}) == {"a": "plain"}
    assert redactor.redact_line("body") == MASK  # stream still inside the block
    redactor.reset()
    assert redactor.redact_json({"a": PEM_BEGIN}) == {"a": MASK}
    assert redactor.redact_line("body") == "body"  # unterminated json string leaked no state


def test_idempotent(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("K_SECRET=env-secret-value-1\n")
    redactor = make(tmp_path)
    lines = [
        f"{SK} {SLACK_BOT} {SLACK_APP} {GH_PAT_CLASSIC} {GH_PAT_FINE} {AWS} {JWT}",
        "Authorization: Bearer abc.def",
        "token: Bearer abc",
        '{"password": "a b", "api_key":"q", "n": "env-secret-value-1"}',
        "secret=x password='y z'",
        "plain text",
        "",
    ]
    for line in lines:
        once = redactor.redact_line(line)
        assert redactor.redact_line(once) == once, line
    text = "\n".join([*PEM_BLOCK, *lines])
    once = redactor.redact_text(text)
    assert redactor.redact_text(once) == once


def test_line_based_never_splits(tmp_path: Path) -> None:
    limit = 40000
    redactor = make(tmp_path)
    lines: list[str] = []
    offset = 0
    while offset < limit - 200:
        lines.append(f"line {len(lines):05d} " + "." * 80)
        offset += len(lines[-1]) + 1
    lines.append("." * (limit - 20 - offset) + SK + " tail")  # secret starts 20 chars before 40k
    lines.extend(f"after {i:05d} " + "." * 80 for i in range(100))
    raw = "\n".join(lines)
    start = raw.index(SK)
    assert start < limit < start + len(SK)  # a raw 40k cut would land inside the secret
    path = tmp_path / "stream.log"
    path.write_text(raw + "\n")

    source = Source("s", lambda: path.open("rb"))
    pages: list[list[str]] = []
    cursor = Cursor()
    while True:  # the real pager: whole redacted lines, page capped at 40k chars
        page = read_page([source], cursor, redactor, cap=limit, limit=10_000, full=False)
        if not page.entries:
            break
        pages.append([e["line"] for e in page.entries])
        cursor = page.cursor
    assert len(pages) > 1
    for chunk in pages:
        text = "\n".join(chunk)
        assert sum(len(line) for line in chunk) <= limit
        assert "x" * 10 not in text
        assert "sk-" + "ant" not in text
    served = [line for chunk in pages for line in chunk]
    assert len(served) == len(lines) and MASK in "".join(served)


NASTY_SECRETS = [
    "Xk9Lm7",
    'Xk9"Lm7',
    "Xk9'Lm7",
    'Xk9",Lm7',
    "Xk9\\Lm7",
    "Xk9Lm7\\",
    "Xk9Lm7\\\\",
    'Xk9Lm7\\"',
    '"Xk9Lm7',
    "'Xk9Lm7",
    '"Xk9Lm7"',
    "'Xk9Lm7'",
    '"Xk9"Lm7"',
    "'Xk9'Lm7",
    "Xk9{Lm7}",
    "{Xk9Lm7",
    "Xk9[Lm7]",
    "Xk9,Lm7,",
    "Xk9:Lm7",
    "Xk9=Lm7",
    "Xk9;Lm7",
    "Xk9/Lm7+",
    "Xk9\\nLm7",
    "Xk9\\u00e9Lm7",
    "Xk9\u00e9Lm7",
    "Xk9\u65e5\u672cLm7",
    "Xk9\U0001f600Lm7",
    "Xk9#Lm7",
    "Xk9`Lm7$",
    "Xk9 Lm7 tail",
    "Xk9\tLm7",
]


def nest(obj: object, times: int) -> object:
    for _ in range(times):
        obj = json.dumps(obj)
    return obj


def test_nasty_secrets_json_shapes(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for secret in NASTY_SECRETS:
        first = secret.split()[0]
        # (1) value under a sensitive key, at depth 0, once-nested and twice-nested
        for depth in range(3):
            line = json.dumps({"id": 7, "text": nest({"password": secret, "n": 1}, depth)})
            out = redactor.redact_line(line)
            json.loads(out)
            assert secret not in out and first not in out, (secret, depth)
        line = json.dumps({"password": secret, "n": 1})
        out = redactor.redact_line(line)
        assert json.loads(out) == {"password": MASK, "n": 1}
        # (2) `password=<s>` inside a JSON string (and nested): guaranteed up to the whitespace
        for depth in range(3):
            line = json.dumps({"text": nest({"msg": f"x password={secret} y"}, depth)})
            out = redactor.redact_line(line)
            json.loads(out)
            assert first not in out, (secret, depth)
        assert redactor.redact_line(json.dumps({"t": f"password={secret}"}))
        # idempotent
        once = redactor.redact_line(line)
        assert redactor.redact_line(once) == once


def test_review_round_two_inputs(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    cases = [
        ('{"t": "password=\'abcdefgh"}', "abcdefgh"),
        ('{"t": "password: hunter2\\\\"}', "hunter2"),
        ('{"t": "token_cache=C:\\\\cache\\\\"}', "C:"),
        ('{"t": "token=ab\\\\ncdefg"}', "cdefg"),
        ('{"t": "password=Xy\\",9kLm tail"}', "9kLm"),
        (json.dumps({"text": json.dumps({"password": 'abc"defghi'})}), "defghi"),
    ]
    for line, secret in cases:
        out = redactor.redact_line(line)
        json.loads(out)
        assert secret not in out, line
    assert redactor.redact_line('DB_PASSWORD=Xy"9kLm') == "DB_PASSWORD=****"
    assert redactor.redact_line('password=Xy",9kLm') == "password=****"


def test_non_json_braces_fall_back_to_plain_text(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    assert redactor.redact_line("{not json password=abcdefgh") == "{not json password=****"
    assert redactor.redact_line("[2026-01-01] token=abcdefgh") == "[2026-01-01] token=****"
    assert redactor.redact_line("[" * 100_000) == "[" * 100_000  # too deep to parse: no raise


def test_tokens_after_literal_escapes_in_plain_lines(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for name, token in TOKEN_SAMPLES.items():
        for prefix in ("x\\n", "x\\t", "x\\r", "x\\u00e9", "x\\x1b", "x ", "x=", 'x"'):
            out = redactor.redact_line(prefix + token + " end")
            assert token not in out, (name, prefix)
    # real control characters inside JSON strings
    for name, token in TOKEN_SAMPLES.items():
        for control in ("\n", "\t", "\r"):
            out = redactor.redact_line(json.dumps({"c": control + token}))
            assert json.loads(out) == {"c": control + MASK}, (name, control)


def test_authorization_header_in_nested_json(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    line = json.dumps({"text": '{"Authorization": "Bearer abcdef"}'})
    out = redactor.redact_line(line)
    assert "abcdef" not in out
    assert json.loads(json.loads(out)["text"]) == {"Authorization": MASK}


def test_json_lines_reencoded_compactly(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    assert redactor.redact_line('  { "a" : [1, 2.5, null], "b": "é" }  ') == (
        '{"a":[1,2.5,null],"b":"é"}'
    )
    assert json.loads(redactor.redact_line('{"a": "\\ud800"}')) == {"a": "\ud800"}
    redactor.redact_line('{"a": "\\ud800"}').encode("utf-8")  # still encodable


def test_pem_in_json_string_with_real_newlines(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    block = "\n".join(["before", *PEM_BLOCK, "after"])
    out = redactor.redact_line(json.dumps({"k": block, "m": "ok"}))
    assert json.loads(out) == {"k": "before\n" + MASK + "\nafter", "m": "ok"}
    assert redactor.redact_line("next") == "next"  # json strings leave no PEM state behind


def timed(redactor: Redactor, line: str) -> float:
    redactor.reset()  # an unterminated PEM line leaves the stream state open
    start = time.perf_counter()
    out = redactor.redact_line(line)
    elapsed = time.perf_counter() - start
    assert isinstance(out, str)
    return elapsed


def test_large_inputs_are_linear(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    size = 1_000_000
    pem = "-----BEGIN RSA " + "PRIVATE KEY-----"
    lines = {
        "token": "token" * (size // 5),
        "token-": "token-" * (size // 6),
        "a_token_b": "a_token_b" * (size // 9),
        "eyJ": "eyJ" * (size // 3),
        "sk-proj-": "sk-proj-" * (size // 8),
        "sk-": "sk-" * (size // 3),
        "Cookie: ": "Cookie: " * (size // 8),
        "curl -u ": "curl -u " * (size // 8),
        "curl -u a:": "curl -u a:" * (size // 10),
        "://": "://" * (size // 3),
        "://a:": "://a:" * (size // 5),
        "A": "A" * size,
        "password=": "password=" * (size // 9),
        "pem": pem * (size // len(pem)),
        "pem+A": "x " + "A" * size + " " + pem,
        "json": json.dumps({"k": ["abc token=1 " + "w" * 90] * (size // 100)}),
        "token run then key": "A" * 100 + "token=Secret1234",
        "spaces": "token" + " " * size + "x",
        "X-API-Key: ": "X-API-Key: " * (size // 11),
        "private_key=": "private_key=" * (size // 12),
        "plain key=": "cache_key=" * (size // 10),
        "PGPASSWORD=": "PGPASSWORD=" * (size // 11),
        "OPENAI_KEY=": "OPENAI_KEY=" * (size // 11),
    }
    for name, line in lines.items():
        assert timed(redactor, line) < 1.0, name
    # Word-part keys: `AAAA…token` is one word, not the key `token`; `AAAA…_token` is.
    assert redactor.redact_line("A" * 100 + "_token=Secret1234") == "A" * 100 + "_token=****"


def test_jwt_after_url_encoded_characters(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    jwt = "ey" + "J" + "h" * 14 + "." + "p" * 20 + "." + "s" * 20
    query = "id_token%3D" + jwt
    for text in (
        "GET /login?redirect_uri=https%3A%2F%2Fapp%2Fcb%3F" + query + " HTTP/1.1",
        "state=%7B%22t%22%3A%22" + jwt + "%22%7D",
        "a%22" + jwt,
    ):
        out = redactor.redact_line(text)
        assert jwt not in out and "hhhhhhhh" not in out, text
        assert MASK in out


def test_sensitive_key_masks_every_string_leaf(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    original = {
        "password": ["x1", ["y2", 3], {"k": "z3", "n": None}],
        "secrets": {"db": "pw1", "port": 5432, "ok": True, "empty": ""},
        "api_keys": ["k1", "k2"],
        "tokens": {"input": 5, "output": 7},
        "usage": {"input_tokens": 1, "output_tokens": 2},
    }
    expected = {
        "password": [MASK, [MASK, 3], {"k": MASK, "n": None}],
        "secrets": {"db": MASK, "port": 5432, "ok": True, "empty": ""},
        "api_keys": [MASK, MASK],
        "tokens": {"input": 5, "output": 7},
        "usage": {"input_tokens": 1, "output_tokens": 2},
    }
    assert redactor.redact_json(original) == expected
    assert json.loads(redactor.redact_line(json.dumps(original))) == expected


def test_redact_json_hostile_depth(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for wrap in (lambda x: [x], lambda x: {"a": x}, lambda x: {"password": [x]}):
        deep: object = "leaf"
        for _ in range(5000):
            deep = wrap(deep)
        node = redactor.redact_json(deep)
        levels = 0
        while not isinstance(node, str):
            node = node[0] if isinstance(node, list) else next(iter(node.values()))
            levels += 1
        assert node == MASK
        assert levels <= 201
    assert redactor.redact_json([[["ok"]]]) == [[["ok"]]]
    assert redactor.redact_line("[" * 5000 + "]" * 5000)  # json.loads depth failure, no raise


def test_top_level_json_string_line(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    line = json.dumps(json.dumps({"password": "hunter22", "n": 1}))
    out = redactor.redact_line(line)
    assert "hunter22" not in out
    assert json.loads(json.loads(out)) == {"password": MASK, "n": 1}
    plain = json.dumps("see password=hunter22 now")
    out = redactor.redact_line(plain)
    assert json.loads(out) == f"see password={MASK} now"
    assert redactor.redact_line(out) == out
    assert redactor.redact_line('"just text"') == '"just text"'
    assert redactor.redact_line('"password": "abc"') == '"password": "****"'  # pretty-json line


OPENAI_PROJ = "sk-" + "proj-" + "Ab1_-" * 8
OPENAI_GENERIC = "sk-" + "Zy9" * 10


def test_openai_style_keys_masked(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    assert redactor.redact_line(f"key {OPENAI_PROJ} end") == f"key {MASK} end"
    assert redactor.redact_line(f"key={OPENAI_GENERIC}") == f"key={MASK}"
    assert redactor.redact_line("sk-proj-short") == MASK
    assert redactor.redact_line(f"\\n{OPENAI_GENERIC}") == f"\\n{MASK}"  # after a literal \n
    assert redactor.redact_line(f"key {SK} end") == f"key {MASK} end"  # sk-ant- unchanged


def test_openai_style_keys_negatives(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for text in (
        "sk-" + "a" * 19,  # too short for the generic form
        "task-" + "a" * 30,  # `sk-` inside a word
        "risk-assessment-for-the-whole-platform",
        "ask-" + "b" * 30,
    ):
        assert redactor.redact_line(text) == text, text


def test_cookie_headers_masked(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    assert redactor.redact_line("Cookie: sid=abc; theme=dark") == "Cookie: ****"
    assert redactor.redact_line("set-cookie: sid=abc; Path=/; HttpOnly") == "set-cookie: ****"
    assert redactor.redact_line("SET-COOKIE:sid=abc") == "SET-COOKIE:****"
    assert redactor.redact_line("> Cookie:   a=b") == "> Cookie:   ****"
    out = redactor.redact_text("GET / HTTP/1.1\nCookie: sid=abc\nHost: example.test")
    assert out == "GET / HTTP/1.1\nCookie: ****\nHost: example.test"


def test_cookie_negatives(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for text in ("Cookie:", "Cookie:   ", "my cookie jar", "cookies: 3", "mycookie: x"):
        assert redactor.redact_line(text) == text, text


def test_cookie_dict_key_is_sensitive(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    obj = {"Cookie": "sid=abc", "Set-Cookie": ["a=1", "b=2"], "cookies": "ok", "n": 1}
    assert redactor.redact_json(obj) == {
        "Cookie": MASK,
        "Set-Cookie": [MASK, MASK],
        "cookies": "ok",
        "n": 1,
    }
    out = redactor.redact_line(json.dumps({"headers": {"COOKIE": "sid=abc", "x": "y"}}))
    assert json.loads(out) == {"headers": {"COOKIE": MASK, "x": "y"}}


def test_curl_basic_auth_masked(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    cases = {
        "curl -u alice:hunter22 https://h.test/x": "curl -u **** https://h.test/x",
        "curl --user alice:hunter22 https://h.test/x": "curl --user **** https://h.test/x",
        "curl --user=alice:hunter22 -s": "curl --user=**** -s",
        "curl -u 'alice:hunter 22' x": "curl -u **** x",
        'curl -u "alice:hunter 22" x': "curl -u **** x",
        "curl -u alice: x": "curl -u **** x",
        "curl -u :hunter22 x": "curl -u **** x",
        "-u alice:hunter22": "-u ****",
    }
    for text, expected in cases.items():
        out = redactor.redact_line(text)
        assert "hunter" not in out, text
        assert out == expected, text


def test_curl_basic_auth_negatives(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for text in (
        "curl -u username https://h.test",
        "curl --user username",
        "sort -u file",
        "sort -u",
        "id -u",
        "id -u alice",
        "foo-u a:b",
        "curl -user a:b",
    ):
        assert redactor.redact_line(text) == text, text


def test_url_userinfo_masked(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    assert redactor.redact_line("clone https://alice:hunter22@h.test/r.git") == (
        "clone https://****@h.test/r.git"
    )
    assert redactor.redact_line("postgres://u:p%40ss@db:5432/x") == "postgres://****@db:5432/x"
    assert redactor.redact_line("ftp://:hunter22@h.test") == "ftp://****@h.test"


def test_url_userinfo_negatives(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for text in (
        "ssh://git@h.test/r.git",  # user only, no credential
        "https://h.test:8080/path?e=a@b",
        "http://[::1]:8080/x",
        "mail me at a:b@c.test",  # no scheme separator
    ):
        assert redactor.redact_line(text) == text, text


def test_new_patterns_in_json_lines(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    secrets = ("hunter22", OPENAI_PROJ, OPENAI_GENERIC)
    payloads = (
        {"msg": f"key {OPENAI_PROJ} and {OPENAI_GENERIC}"},
        {"msg": "GET /\nCookie: sid=hunter22\nHost: h.test"},
        {"cmd": "curl -u alice:hunter22 https://h.test", "n": 1},
        {"cmd": 'curl --user="alice:hunter22"', "n": 1},
        {"url": "https://alice:hunter22@h.test/x"},
        {"headers": {"Set-Cookie": "sid=hunter22"}},
        {"nested": json.dumps({"cmd": "curl -u alice:hunter22 x"})},
    )
    for payload in payloads:
        out = redactor.redact_line(json.dumps(payload))
        parsed = json.loads(out)  # still valid JSON
        assert isinstance(parsed, dict)
        assert not any(secret in out for secret in secrets), payload
        assert MASK in out, payload
    assert json.loads(redactor.redact_line(json.dumps(payloads[2])))["n"] == 1
    assert json.loads(redactor.redact_line(json.dumps(payloads[1])))["msg"] == (
        "GET /\nCookie: ****\nHost: h.test"
    )


def test_new_patterns_are_idempotent(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    texts = (
        f"key {OPENAI_PROJ} {OPENAI_GENERIC}",
        "Cookie: sid=abc",
        "Set-Cookie: sid=abc; HttpOnly",
        "curl -u alice:hunter22 --user bob:pw2 --user=c:d -u 'e:f' https://u:p@h.test",
        'curl -u "a b:c d" x',
        json.dumps({"Cookie": "a=b", "cmd": "curl -u a:b x", "u": "https://a:b@h.test"}),
    )
    for text in texts:
        once = redactor.redact_line(text)
        assert redactor.redact_line(once) == once, text


SENSITIVE_KEYS = (
    "X-API-Key", "PRIVATE_KEY", "CLIENT_CREDENTIAL", "DB_PASS", "aws_secret_access_key",
    "SECRET_KEY", "set-cookie", "GITHUB_PAT", "apiKey", "accessToken", "x_auth", "pwd",
    "signingKey", "BEARER", "client_secrets", "ssh-keys",
)  # fmt: skip
PLAIN_KEYS = ("key", "cache_key", "keyboard", "author", "passenger", "compass", "keys", "monkey")


def test_word_part_sensitive_keys_structural(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for key in SENSITIVE_KEYS:
        out = redactor.redact_json({key: "abcdefgh", "n": {key: 5, "b": True, "z": None}})
        assert out == {key: MASK, "n": {key: 5, "b": True, "z": None}}, key
        line = json.dumps({"headers": {key: "abcdefgh"}})
        assert json.loads(redactor.redact_line(line)) == {"headers": {key: MASK}}, key
    for key in PLAIN_KEYS:
        assert redactor.redact_json({key: "abcdefgh"}) == {key: "abcdefgh"}, key
        line = json.dumps({key: "abcdefgh"})
        assert json.loads(redactor.redact_line(line)) == {key: "abcdefgh"}, key
    usage = {"usage": {"input_tokens": 123, "output_tokens": 4, "cache_read_input_tokens": 9}}
    assert redactor.redact_json(usage) == usage


def test_word_part_sensitive_keys_plain_text(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for key in SENSITIVE_KEYS:
        for sep in (": ", "=", " = "):
            assert redactor.redact_line(f"{key}{sep}abcdefgh") == f"{key}{sep}{MASK}", (key, sep)
        assert redactor.redact_line(f'"{key}": "abcdefgh"') == f'"{key}": "{MASK}"', key
    for key in PLAIN_KEYS:
        for sep in (": ", "="):
            line = f"{key}{sep}abcdefgh"
            assert redactor.redact_line(line) == line, line
    assert redactor.redact_line("X-API-Key: abc123 next") == f"X-API-Key: {MASK} next"
    # A plain key's value never hides a sensitive key that follows it.
    assert redactor.redact_line("FOO=bar,token=abcdefgh") == f"FOO=bar,token={MASK}"
    assert redactor.redact_line("x\\nDB_PASS=abcdefgh") == f"x\\nDB_PASS={MASK}"
    assert redactor.redact_line("input_tokens: 123") == "input_tokens: 123"
    line = "A" * 100 + "_token=Secret1234"  # a long key: its last part still counts
    assert redactor.redact_line(line) == "A" * 100 + f"_token={MASK}"


GLUED_SENSITIVE_KEYS = (
    "PGPASSWORD", "MYSQLPASSWORD", "GITHUBTOKEN", "SLACKTOKEN", "AUTHTOKEN", "ACCESSTOKEN",
    "REFRESHTOKEN", "APITOKEN", "clientsecret", "webhooksecret", "privatekey", "DBPASS", "PGPASS",
    "OPENAI_KEY", "STRIPE_KEY", "SERVICE_ROLE_KEY", "ANON_KEY", "X-API-Key", "PRIVATE_KEY",
    "ssh_keys",
)  # fmt: skip
GLUED_PLAIN_KEYS = (
    "key", "cache_key", "keyboard", "author", "passenger", "compass", "monkey", "client_id",
    "bypass", "sort_key", "primary_key", "public_key",
)  # fmt: skip


@pytest.mark.parametrize("key", GLUED_SENSITIVE_KEYS)
def test_glued_sensitive_keys_structural(tmp_path: Path, key: str) -> None:
    redactor = make(tmp_path)
    line = json.dumps({key: "xyzsecretvalue", "n": 1})
    assert json.loads(redactor.redact_line(line)) == {key: MASK, "n": 1}
    assert redactor.redact_json({"env": {key: "xyzsecretvalue"}}) == {"env": {key: MASK}}


@pytest.mark.parametrize("key", GLUED_SENSITIVE_KEYS)
def test_glued_sensitive_keys_plain_text(tmp_path: Path, key: str) -> None:
    redactor = make(tmp_path)
    assert redactor.redact_line(f"{key}=xyz psql -h db") == f"{key}={MASK} psql -h db"
    assert redactor.redact_line(f"{key}: xyz") == f"{key}: {MASK}"


@pytest.mark.parametrize("key", GLUED_PLAIN_KEYS)
def test_glued_plain_keys_not_masked(tmp_path: Path, key: str) -> None:
    redactor = make(tmp_path)
    line = json.dumps({key: "abcdefgh"})
    assert json.loads(redactor.redact_line(line)) == {key: "abcdefgh"}
    assert redactor.redact_json({key: "abcdefgh"}) == {key: "abcdefgh"}
    for text in (f"{key}=abcdefgh next", f"{key}: abcdefgh"):
        assert redactor.redact_line(text) == text


def test_token_counters_never_masked(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    usage = {"usage": {"input_tokens": 123, "max_tokens": 4096, "total_tokens": 55}}
    assert redactor.redact_json(usage) == usage
    assert json.loads(redactor.redact_line(json.dumps(usage))) == usage
    for text in ("input_tokens: 123", "max_tokens: 4096", "total_tokens=55"):
        assert redactor.redact_line(text) == text


def test_type_annotation_not_redacted(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for text in (
        "tokens: number | null",
        "tokens_used: n/a",
        "token: string",
        "tokens: Number | null",
        "token=undefined",
        "password: None",
        "api_key: <key>",
        f"token: {MASK}",
    ):
        assert redactor.redact_line(text) == text


def test_shell_expansion_not_redacted(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for text in (
        'TOKEN="${DIOXIDE_TOKEN}"',
        "TOKEN=${DIOXIDE_TOKEN}",
        "TOKEN=${DIOXIDE_TOKEN:-}",
        "TOKEN=${DIOXIDE_TOKEN:-$OTHER_TOKEN}",
        "TOKEN=$DIOXIDE_TOKEN",
        "TOKEN=$DIOXIDE_TOKEN, next",
        "(TOKEN=$DIOXIDE_TOKEN)",
    ):
        assert redactor.redact_line(text) == text


def test_command_substitution_and_expansion_bodies_masked(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for text, masked in (
        ("TOKEN=${T:-s3cr3tvalue}", f"TOKEN={MASK}"),
        ('TOKEN="${T:-s3cr3tvalue}"', f'TOKEN="{MASK}"'),
        ("TOKEN=${t}", f"TOKEN={MASK}"),
        ("TOKEN='$(printf x)'", f"TOKEN='{MASK}'"),
        ("TOKEN=$(echo s3cr3tvalue)", f"TOKEN={MASK} s3cr3tvalue)"),
        ("DIOXIDE_TOKEN=$(grep x .env)", f"DIOXIDE_TOKEN={MASK} x .env)"),
    ):
        assert redactor.redact_line(text) == masked


def test_exempt_bare_terminator_needs_whitespace_or_eol(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for text in (
        "TOKEN=$X,s3cr3tvalue",
        "TOKEN=${X})s3cr3tvalue",
        "TOKEN=number;s3cr3tvalue",
        "TOKEN=$DIOXIDE_TOKEN,s3cr3tvalue",
        "TOKEN=${DIOXIDE_TOKEN})s3cr3tvalue",
    ):
        assert redactor.redact_line(text) == f"TOKEN={MASK}"


def test_tokens_key_with_real_value_still_redacted(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    assert redactor.redact_line("tokens: abc123def456ghi789") == f"tokens: {MASK}"
    assert redactor.redact_line("DIOXIDE_TOKEN=s3cr3tvalue") == f"DIOXIDE_TOKEN={MASK}"
    assert redactor.redact_line("token: numbers123456") == f"token: {MASK}"
    assert redactor.redact_line("token: stringabcdef123") == f"token: {MASK}"


def test_exempt_forms_must_be_the_whole_value(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for text, masked in (
        ("TOKEN=${X}s3cr3tvalue", f"TOKEN={MASK}"),
        ('TOKEN="${X}s3cr3tvalue"', f'TOKEN="{MASK}"'),
        ("TOKEN=$(a)s3cr3tvalue", f"TOKEN={MASK}"),
        ("TOKEN=$s3cr3t-value", f"TOKEN={MASK}"),
        ("TOKEN=<x>s3cr3tvalue", f"TOKEN={MASK}"),
        ("TOKEN=****s3cr3tvalue", f"TOKEN={MASK}"),
        ('TOKEN="number s3cr3t"', f'TOKEN="{MASK}"'),
        ("api_key: <" + "a" * 70 + ">", f"api_key: {MASK}"),
    ):
        assert redactor.redact_line(text) == masked


def test_name_and_placeholder_exemptions_are_narrow(tmp_path: Path) -> None:
    redactor = make(tmp_path)
    for text in ("TOKEN=$DIOXIDE_TOKEN", "token: <token>", "api_key: <api-key>"):
        assert redactor.redact_line(text) == text
    for text, masked in (
        ("password: $ecretPass1", f"password: {MASK}"),
        ("TOKEN=$Secret1", f"TOKEN={MASK}"),
        ("token: <S3cretValue>", f"token: {MASK}"),
        ("token: <Token>", f"token: {MASK}"),
    ):
        assert redactor.redact_line(text) == masked


def test_placeholder_exemption_is_a_fixed_word_list(tmp_path: Path) -> None:
    """Only the listed placeholder words, lowercase and as the whole value, are exempt: any other
    `<...>` value may be a literal credential in angle brackets."""
    redactor = make(tmp_path)
    for text, masked in (
        ("password: <hunter2>", f"password: {MASK}"),
        ("TOKEN=<s3cr3t>", f"TOKEN={MASK}"),
        ("api_key=<abc123def>", f"api_key={MASK}"),
        ("api_key: <your-key>", f"api_key: {MASK}"),
        ('password="<mypassword1>"', f'password="{MASK}"'),
        ("token: <TOKEN>", f"token: {MASK}"),
    ):
        assert redactor.redact_line(text) == masked
    words = ("token", "key", "api-key", "secret", "password", "value", "user", "name", "id")
    for word in (*words, "path", "url", "host", "port"):
        for text in (f"token: <{word}>", f'password="<{word}>"', f"TOKEN=<{word}>, next"):
            assert redactor.redact_line(text) == text
