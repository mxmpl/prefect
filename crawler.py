import argparse
import io
import os
from collections.abc import Generator
from pathlib import Path

from devai_prefect.utils.hyperloop_context import hyperloop_file_transfer_context, setup_hyperloop_from_env
from prefect.runtime import flow_run


def to_exclude(path: Path) -> bool:
    if path.stem.startswith("."):
        return True
    try:
        if path.is_dir():
            if (path / "CACHEDIR.tag").is_file():
                return True
            if path.name == "__pycache__":
                return True
            if path.suffix == ".egg-info":
                return True
        if path.suffix in {".wav", ".flac", ".ogg", ".pyc"}:
            return True
    except OSError:
        return False
    return False


def bfs(root: Path, max_depth: int | None = None, _depth: int = 0) -> Generator[str]:
    to_visit = []
    try:
        for path in root.glob("*"):
            if to_exclude(path):
                continue
            yield str(path)
            try:
                if path.is_dir() and (max_depth is None or _depth < max_depth):
                    to_visit.append(path)
            except OSError:
                pass
    except OSError:
        return
    for path in to_visit:
        yield from bfs(path, max_depth, _depth + 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--max-depth", type=int, default=None, help="Max directory depth to descend into (unlimited by default)")
    args = parser.parse_args()

    transfer = setup_hyperloop_from_env()
    assert transfer is not None
    name = flow_run.get_name()
    assert name is not None
    dest_path = f"{os.environ['LOG_DEST']}/{name}.log"
    with hyperloop_file_transfer_context(dest_path, transfer, mode="w") as f:
        assert isinstance(f, io.TextIOBase)
        output = "\n".join(bfs(args.root, args.max_depth))
        f.write(output)
