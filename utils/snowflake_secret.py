#!/usr/bin/env python3
"""The one place that touches Snowflake credentials. It never prints them.

Two credentials, both outside the repo and mode 600:
- the PAT: `password` in ~/.config/snowflake (connections.toml format);
- an RSA key pair: private key ~/.config/snowflake_dagbench_rsa.p8
  (DAGBENCH_SF_KEY_FILE). Used instead of the PAT whenever it exists: this
  account rejects PATs without a network policy (error 390432).

Nothing exports them to the interactive shell: `run` gives a single child
process either DAGBENCH_SF_PRIVATE_KEY_PATH (key-pair; a path, not the key) or
DAGBENCH_SF_TOKEN (PAT), plus DAGBENCH_SF_WRAPPED=1, and redacts both secrets
from that child's stdout and stderr.

  python -m utils.snowflake_secret keygen           # new key pair; prints the ALTER USER statement
  python -m utils.snowflake_secret mode             # which credential `run` would use
  python -m utils.snowflake_secret run -- dbt build --target snowflake
  python -m utils.snowflake_secret scan-staged      # git pre-commit hook
  python -m utils.snowflake_secret scan-push        # git pre-push hook (stdin)
  python -m utils.snowflake_secret scan-history     # every commit, all refs
  python -m utils.snowflake_secret scan-paths logs target errors

A scan prints only paths and line numbers, never matched text. It exits 1 on
any hit.

Overrides (used by the self-test): DAGBENCH_SF_SECRET_FILE,
DAGBENCH_SF_CONNECTION.
"""
from __future__ import annotations

import os
import re
import stat
import subprocess
import sys
import tomllib
from pathlib import Path

SECRET_FILE = Path(os.environ.get("DAGBENCH_SF_SECRET_FILE", "~/.config/snowflake")).expanduser()
CONNECTION = os.environ.get("DAGBENCH_SF_CONNECTION", "my_example_connection")
ENV_NAME = "DAGBENCH_SF_TOKEN"
KEY_FILE = Path(os.environ.get("DAGBENCH_SF_KEY_FILE", "~/.config/snowflake_dagbench_rsa.p8")).expanduser()
KEY_ENV = "DAGBENCH_SF_PRIVATE_KEY_PATH"
WRAPPED_ENV = "DAGBENCH_SF_WRAPPED"
REDACTED = "***SNOWFLAKE_TOKEN_REDACTED***"
# Also catch partial copies: any WINDOW-character slice of the token.
WINDOW = 20

# Token-agnostic patterns: a long literal assigned to a password/token/secret key,
# or a JWT-looking blob. Templated values ({{ env_var(...) }}) don't match.
GENERIC = [
    re.compile(rb"""(?i)(password|passwd|token|secret|private_key)["']?\s*[:=]\s*["']?(?!\{\{)[A-Za-z0-9+/=._\-]{32,}"""),
    re.compile(rb"eyJ[A-Za-z0-9_\-]{10,}\.eyJ[A-Za-z0-9_\-]{10,}"),
    re.compile(rb"-----BEGIN (RSA |ENCRYPTED |EC )?PRIVATE KEY-----"),
]


def _die(msg: str) -> None:
    print(f"snowflake_secret: {msg}", file=sys.stderr)
    sys.exit(2)


def _check_private(path: Path) -> None:
    if path.stat().st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        _die(f"{path} is readable by group/others; run: chmod 600 {path}")


def key_mode() -> bool:
    return KEY_FILE.exists()


def load_key_lines() -> list[str]:
    """The private key's base64 body lines (for redaction and scans only)."""
    if not KEY_FILE.exists():
        return []
    _check_private(KEY_FILE)
    return [l.strip() for l in KEY_FILE.read_text().splitlines() if l.strip() and not l.startswith("-----")]


def secrets() -> list[str]:
    """Every secret string to keep out of output, artifacts and commits."""
    out = load_key_lines()
    tok = load_token(required=False)
    if tok:
        out.append(tok)
    return out


def load_token(required: bool = True) -> str | None:
    if not SECRET_FILE.exists():
        if required:
            _die(f"{SECRET_FILE} not found")
        return None
    mode = SECRET_FILE.stat().st_mode
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        _die(f"{SECRET_FILE} is readable by group/others; run: chmod 600 {SECRET_FILE}")
    with SECRET_FILE.open("rb") as fh:
        conf = tomllib.load(fh)
    token = conf.get("connections", {}).get(CONNECTION, {}).get("password", "")
    if not token or set(token) <= {"X", "x"}:
        if required:
            _die(f"no token in [connections.{CONNECTION}].password of {SECRET_FILE}")
        return None
    return token


def connection_params() -> dict:
    """Connection settings for snowflake.connector.connect(**params). May include
    the token, so never print or log the returned dict."""
    with SECRET_FILE.open("rb") as fh:
        conf = tomllib.load(fh)["connections"][CONNECTION]
    base = {k: conf[k] for k in ("account", "user", "role", "warehouse", "database") if k in conf}
    if key_mode():
        _check_private(KEY_FILE)
        return base | {"private_key_file": str(KEY_FILE), "authenticator": "SNOWFLAKE_JWT"}
    return base | {"password": load_token()}


def needles(token: str | list[str]) -> list[bytes]:
    if isinstance(token, list):
        return [n for t in token for n in needles(t)]
    raw = token.encode()
    if len(raw) <= WINDOW:
        return [raw]
    return [raw] + [raw[i : i + WINDOW] for i in range(len(raw) - WINDOW + 1)]


REDACT_WINDOW = 12  # over-redacting output is harmless, so use a smaller window


def redact(text: str, token: str | list[str]) -> str:
    """Mask every run of characters that matches the token (or any of a list of
    secrets) contiguously and is at least REDACT_WINDOW long. Each match is grown
    to its full extent, so no leftover head or tail of a fragment gets through."""
    if isinstance(token, list):
        for t in token:
            text = redact(text, t)
        return text
    spans: list[tuple[int, int]] = []
    w = min(REDACT_WINDOW, len(token))
    for i in range(len(token) - w + 1):
        part = token[i : i + w]
        start = text.find(part)
        while start != -1:
            a, b, ta, tb = start, start + w, i, i + w
            while a > 0 and ta > 0 and text[a - 1] == token[ta - 1]:
                a, ta = a - 1, ta - 1
            while b < len(text) and tb < len(token) and text[b] == token[tb]:
                b, tb = b + 1, tb + 1
            spans.append((a, b))
            start = text.find(part, start + 1)
    if not spans:
        return text
    spans.sort()
    out, pos = [], 0
    for a, b in spans:
        if a < pos:
            a = pos
        if a >= b:
            continue
        out.append(text[pos:a])
        out.append(REDACTED)
        pos = b
    out.append(text[pos:])
    return "".join(out)


def _hits(data: bytes, ndl: list[bytes]) -> list[str]:
    reasons = []
    if any(n in data for n in ndl):
        reasons.append("contains the Snowflake token (or a piece of it)")
    for pat in GENERIC:
        for m in pat.finditer(data):
            line = data.count(b"\n", 0, m.start()) + 1
            reasons.append(f"line {line}: looks like a literal secret ({pat.pattern[:24]!r}...)")
    return reasons


def _report(label: str, reasons: list[str]) -> None:
    for r in reasons:
        print(f"  BLOCKED {label}: {r}", file=sys.stderr)


def _git(*args: str) -> bytes:
    return subprocess.run(["git", *args], check=True, capture_output=True).stdout


def scan_staged(ndl: list[bytes]) -> int:
    names = _git("diff", "--cached", "--name-only", "-z", "--diff-filter=ACMR").split(b"\0")
    bad = 0
    for name in filter(None, names):
        data = _git("show", ":" + name.decode(errors="surrogateescape"))
        reasons = _hits(data, ndl)
        if reasons:
            bad += 1
            _report(name.decode(errors="replace"), reasons)
    return bad


def _scan_patch_text(label: str, patch: bytes, ndl: list[bytes]) -> int:
    added = b"\n".join(l[1:] for l in patch.splitlines() if l.startswith(b"+") and not l.startswith(b"+++"))
    reasons = _hits(added, ndl)
    # Line numbers inside a concatenated patch are meaningless; keep the reasons generic.
    reasons = [r.split(": ", 1)[-1] if r.startswith("line ") else r for r in reasons]
    if reasons:
        _report(label, sorted(set(reasons)))
    return 1 if reasons else 0


def scan_push(ndl: list[bytes]) -> int:
    zero = "0" * 40
    bad = 0
    for line in sys.stdin.read().splitlines():
        local_ref, local_sha, _remote_ref, remote_sha = line.split()
        if local_sha == zero:
            continue  # deleting a ref
        rng = [local_sha, "--not", "--remotes"] if remote_sha == zero else [f"{remote_sha}..{local_sha}"]
        for sha in _git("rev-list", *rng).split():
            patch = _git("show", "--format=", "-p", "--binary", sha.decode())
            bad += _scan_patch_text(f"commit {sha.decode()[:12]} ({local_ref})", patch, ndl)
    return bad


def scan_history(ndl: list[bytes]) -> int:
    bad = 0
    for sha in _git("rev-list", "--all").split():
        patch = _git("show", "--format=", "-p", "--binary", sha.decode())
        bad += _scan_patch_text(f"commit {sha.decode()[:12]}", patch, ndl)
    return bad


def scan_paths(paths: list[str], ndl: list[bytes]) -> int:
    bad = 0
    for root in paths:
        p = Path(root)
        files = [p] if p.is_file() else [f for f in p.rglob("*") if f.is_file()] if p.exists() else []
        for f in files:
            if f.stat().st_size > 512 * 1024 * 1024:
                continue
            data = f.read_bytes()
            # Only the exact token matters here: logs and artifacts are full of long ids.
            if any(n in data for n in ndl):
                bad += 1
                _report(str(f), ["contains the Snowflake token (or a piece of it)"])
    return bad


def run(cmd: list[str]) -> int:
    env = os.environ.copy()
    env.pop(ENV_NAME, None)
    env.pop(KEY_ENV, None)
    if key_mode():
        _check_private(KEY_FILE)
        env[KEY_ENV] = str(KEY_FILE)
    else:
        env[ENV_NAME] = load_token()
    env[WRAPPED_ENV] = "1"
    token = secrets()
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    assert proc.stdout is not None
    for line in proc.stdout:
        sys.stdout.write(redact(line, token))
        sys.stdout.flush()
    rc = proc.wait()
    # Post-check: the artifacts that dbt and the connector write to disk.
    leaked = scan_paths(["logs", "target", "errors"], needles(token))
    if leaked:
        print("snowflake_secret: token found in local artifacts; delete them before doing anything else", file=sys.stderr)
        return rc or 3
    return rc


def keygen() -> int:
    """Write a new unencrypted PKCS#8 RSA-2048 key to KEY_FILE (mode 600) and print
    the public half as an ALTER USER statement. Refuses to overwrite."""
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    import base64

    if KEY_FILE.exists():
        _die(f"{KEY_FILE} already exists; delete it first to rotate (then re-run ALTER USER)")
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption())
    KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(KEY_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as fh:
        fh.write(pem)
    der = key.public_key().public_bytes(serialization.Encoding.DER,
                                        serialization.PublicFormat.SubjectPublicKeyInfo)
    digest = hashes.Hash(hashes.SHA256())
    digest.update(der)
    fp = base64.b64encode(digest.finalize()).decode()
    user = "DREWRIPBERGER"
    try:
        with SECRET_FILE.open("rb") as fh:
            user = tomllib.load(fh)["connections"][CONNECTION].get("user", user)
    except (OSError, KeyError, tomllib.TOMLDecodeError):
        pass
    print(f"private key written to {KEY_FILE} (mode 600). The public key below is not secret.")
    print("Run this in Snowsight as yourself:\n")
    print(f"ALTER USER {user} SET RSA_PUBLIC_KEY='{base64.b64encode(der).decode()}';\n")
    print(f"Check: DESC USER {user} ->  RSA_PUBLIC_KEY_FP = SHA256:{fp}")
    return 0


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    cmd, rest = argv[0], argv[1:]
    if cmd == "run":
        if rest[:1] == ["--"]:
            rest = rest[1:]
        return run(rest)
    if cmd == "keygen":
        return keygen()
    if cmd == "mode":
        print(f"key-pair ({KEY_FILE})" if key_mode() else f"PAT ({SECRET_FILE})")
        return 0
    ndl = needles(secrets())
    if cmd == "scan-staged":
        bad = scan_staged(ndl)
    elif cmd == "scan-push":
        bad = scan_push(ndl)
    elif cmd == "scan-history":
        bad = scan_history(ndl)
    elif cmd == "scan-paths":
        bad = scan_paths(rest or ["."], ndl)
    else:
        print(__doc__)
        return 2
    if bad:
        print(f"snowflake_secret: {cmd}: {bad} offending item(s); nothing was printed from them", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
