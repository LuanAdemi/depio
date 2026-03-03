import datetime
import pathlib


def getmtime(f: pathlib.Path) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(f.stat().st_mtime)


def getatime(f: pathlib.Path) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(f.stat().st_atime)


def getctime(f: pathlib.Path) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(f.stat().st_ctime)


__all__ = ["getmtime", "getatime", "getctime"]
