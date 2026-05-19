#!/usr/bin/env python3
"""Upload binaries to a CSCS shared path via FirecREST, chmod o+rx, and update
stable symlinks so consumers can reference a versionless name.

Reads SML_FIRECREST_{URL,SYSTEM,CLIENT_ID,CLIENT_SECRET,TOKEN_URI} from env.

Usage:
    upload_to_cscs.py --remote-dir DIR LOCAL:REMOTE [LOCAL:REMOTE ...]
                      [--symlink TARGET:LINK ...]

Example:
    upload_to_cscs.py \\
        --remote-dir /capstor/store/cscs/swissai/infra01/ocf-share/prod \\
        build/otela-amd64:otela-amd64-sai-v0.1.0-abc1234 \\
        build/otela-arm64:otela-arm64-sai-v0.1.0-abc1234 \\
        --symlink otela-amd64-sai-v0.1.0-abc1234:otela-amd64 \\
        --symlink otela-arm64-sai-v0.1.0-abc1234:otela-arm64

The symlink TARGET is interpreted relative to --remote-dir (so the link survives
if the share is mounted under a different path inside containers).
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path, PurePosixPath

import firecrest as f7t

# 0755 = rwxr-xr-x; covers the requested chmod o+rx for both owner and others.
_REMOTE_MODE = "755"


async def _update_symlink(
    client: f7t.v2.AsyncFirecrest,
    system_name: str,
    remote_dir: str,
    target: str,
    link_name: str,
) -> None:
    """Atomically-ish point <remote_dir>/<link_name> at <target> (target is
    relative to remote_dir). pyfirecrest exposes no `ln -sfn`, so: rm if
    present, then create. There is a tiny window where the link is missing,
    which is acceptable for our deploy cadence."""
    link_path = str(PurePosixPath(remote_dir) / link_name)
    try:
        await client.rm(system_name=system_name, path=link_path, blocking=True)
        print(f"  removed existing {link_path}")
    except f7t.FirecrestException as e:
        # Missing link is the common case on first deploy.
        print(f"  no existing {link_path} (ok: {e.__class__.__name__})")
    await client.symlink(
        system_name=system_name,
        source_path=target,
        link_path=link_path,
    )
    print(f"  symlink {link_path} -> {target}")


async def run(
    remote_dir: str,
    uploads: list[tuple[Path, str]],
    symlinks: list[tuple[str, str]],
) -> int:
    client = f7t.v2.AsyncFirecrest(
        firecrest_url=os.environ["SML_FIRECREST_URL"],
        authorization=f7t.ClientCredentialsAuth(
            client_id=os.environ["SML_FIRECREST_CLIENT_ID"],
            client_secret=os.environ["SML_FIRECREST_CLIENT_SECRET"],
            token_uri=os.environ["SML_FIRECREST_TOKEN_URI"],
        ),
    )
    system_name = os.environ["SML_FIRECREST_SYSTEM"]

    user_info = await client.userinfo(system_name)
    account = user_info["group"]["name"]

    # mkdir -p the destination so this works the first time a new
    # (e.g. ocf-share/dev or ocf-share/prod) subdir is used.
    await client.mkdir(system_name=system_name, path=remote_dir, create_parents=True)

    for local_path, remote_name in uploads:
        if not local_path.is_file():
            print(f"ERROR: local file {local_path} does not exist", file=sys.stderr)
            return 1
        local_path.chmod(0o755)
        remote_path = str(PurePosixPath(remote_dir) / remote_name)
        print(f"Uploading {local_path} ({local_path.stat().st_size} bytes) -> {remote_path}")
        await client.upload(
            system_name=system_name,
            local_file=str(local_path),
            directory=remote_dir,
            filename=remote_name,
            account=account,
            blocking=True,
        )
        await client.chmod(system_name=system_name, path=remote_path, mode=_REMOTE_MODE)
        print(f"  uploaded {remote_name} (chmod {_REMOTE_MODE})")

    for target, link_name in symlinks:
        print(f"Updating symlink {link_name} -> {target}")
        await _update_symlink(client, system_name, remote_dir, target, link_name)

    return 0


def _parse_pair(spec: str, kind: str) -> tuple[str, str]:
    if ":" not in spec:
        raise SystemExit(f"{kind} spec must be A:B, got {spec!r}")
    a, b = spec.split(":", 1)
    if not a or not b:
        raise SystemExit(f"empty side in {kind} spec {spec!r}")
    return a, b


def parse_uploads(specs: list[str]) -> list[tuple[Path, str]]:
    return [(Path(a), b) for a, b in (_parse_pair(s, "upload") for s in specs)]


def parse_symlinks(specs: list[str]) -> list[tuple[str, str]]:
    return [_parse_pair(s, "symlink") for s in specs]


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload binaries to CSCS via FirecREST.")
    parser.add_argument(
        "--remote-dir",
        required=True,
        help="Absolute remote directory on the CSCS filesystem (e.g. /capstor/.../ocf-share/dev)",
    )
    parser.add_argument(
        "--symlink",
        action="append",
        default=[],
        metavar="TARGET:LINK",
        help="Create/replace <remote-dir>/<LINK> -> <TARGET> (TARGET is relative to remote-dir). Repeatable.",
    )
    parser.add_argument(
        "uploads",
        nargs="*",
        metavar="LOCAL:REMOTE",
        help="Pairs of LOCAL_PATH:REMOTE_FILENAME — file goes to <remote-dir>/<REMOTE_FILENAME>. Optional if --symlink is given.",
    )
    args = parser.parse_args()
    if not args.uploads and not args.symlink:
        parser.error("specify at least one upload (LOCAL:REMOTE) or --symlink")
    return asyncio.run(
        run(
            args.remote_dir,
            parse_uploads(args.uploads),
            parse_symlinks(args.symlink),
        )
    )


if __name__ == "__main__":
    sys.exit(main())
