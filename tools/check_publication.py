"""Check public source, documentation links and measured source identities."""

import hashlib
import json
import re
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
SKIP = {
    ".git",
    ".venv",
    ".local",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    "data",
    "benchmark_results",
}
PRIVATE = re.compile(
    rb"/(?:Users|home)/[^/\s\"']+/|sk-[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9]{30,}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"
)
CJK = re.compile("[\u3400-\u9fff]")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def slug(value):
    return re.sub(
        r"\s", "-", re.sub(r"[^\w\s-]", "", re.sub(r"[`*]", "", value).lower()).strip()
    )


def check():
    errors, files, links = [], [], 0
    for path in sorted(ROOT.rglob("*")):
        relative = path.relative_to(ROOT)
        if any(part in SKIP for part in relative.parts):
            continue
        name = relative.as_posix()
        if path.is_symlink():
            errors.append("symlink: " + name)
        elif path.is_file():
            files.append(path)
            if path.suffix in {
                ".aof",
                ".db",
                ".sqlite",
                ".sqlite3",
                ".env",
                ".log",
            } or path.name.startswith(".env"):
                errors.append("runtime/private file: " + name)
            if (
                path.name in {"AGENTS.md", "WORKING.md", "TRANSFER_READY.md"}
                or "DIALOGUE" in path.name
            ):
                errors.append("operator record: " + name)
            body = path.read_bytes()
            if PRIVATE.search(body):
                errors.append("private path/credential pattern: " + name)
            if path.suffix in {".py", ".md", ".txt", ".json", ".yml"} and CJK.search(
                body.decode("utf-8")
            ):
                errors.append("non-English source/document: " + name)
    for path in files:
        if path.suffix != ".md":
            continue
        plain = re.sub(r"```[\s\S]*?```", "", path.read_text())
        for target in re.findall(r"!?\[[^\]]*\]\(([^\n)]+)\)", plain):
            target = target.strip("<>")
            if re.match(r"^[a-z]+:", target):
                continue
            name, _, anchor = target.partition("#")
            destination = (path.parent / unquote(name)).resolve() if name else path
            links += 1
            if not destination.is_relative_to(ROOT) or not destination.exists():
                errors.append("missing/outside link: " + target)
            elif anchor and destination.suffix == ".md":
                headings = [
                    slug(h)
                    for h in re.findall(
                        r"^#{1,6}\s+(.+?)\s*$", destination.read_text(), re.MULTILINE
                    )
                ]
                if unquote(anchor) not in headings:
                    errors.append("missing anchor: " + target)
    study = json.loads((ROOT / "benchmarks/group_commit_20261127.json").read_text())
    for row in study["rounds"]:
        source = ROOT / "benchmarks/baseline" if row["kind"] == "baseline" else ROOT
        for name, expected in row["source_sha256"].items():
            if not (source / name).is_file() or sha(source / name) != expected:
                errors.append("measured source mismatch: " + row["kind"] + "/" + name)
    result = {
        "status": "passed" if not errors else "failed",
        "files": len(files),
        "links": links,
        "measured_cases": len(study["rounds"]),
        "errors": sorted(set(errors)),
    }
    print(json.dumps(result, indent=2))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(check())
