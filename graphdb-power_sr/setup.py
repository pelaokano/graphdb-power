from setuptools import setup, Extension
import sys
import os
import subprocess


def find_sqlite_vcpkg():
    candidates = [
        r"C:\vcpkg\installed\x64-windows",
        r"C:\tools\vcpkg\installed\x64-windows",
        os.path.expanduser(r"~\vcpkg\installed\x64-windows"),
    ]
    for path in candidates:
        if os.path.isfile(os.path.join(path, "include", "sqlite3.h")):
            return path
    return None


if sys.platform == "win32":
    sources = [
        "graphdb.c",
        "graph_algorithms.c",
        "cypher_lexer.c",
        "cypher_parser.c",
        "cypher_exec.c",
        "graphdb_py.c",
    ]

    vcpkg = find_sqlite_vcpkg()

    if vcpkg:
        include_dirs = [os.path.join(vcpkg, "include")]
        library_dirs = [os.path.join(vcpkg, "lib")]
        libraries    = ["sqlite3"]
        print(f"Using SQLite from vcpkg: {vcpkg}")
    elif os.path.isfile("sqlite3.h") and os.path.isfile("sqlite3.c"):
        include_dirs = ["."]
        library_dirs = []
        libraries    = []
        sources      = ["sqlite3.c"] + sources
        print("Using SQLite amalgamation")
    else:
        raise RuntimeError(
            "\n\nSQLite not found. Two options:\n"
            "  A) vcpkg:\n"
            "       git clone https://github.com/microsoft/vcpkg\n"
            "       cd vcpkg && bootstrap-vcpkg.bat\n"
            "       vcpkg install sqlite3:x64-windows\n"
            "       vcpkg integrate install\n\n"
            "  B) amalgamation from https://sqlite.org/download.html\n"
            "     Place sqlite3.h and sqlite3.c in this directory.\n"
        )

    extra_compile = [
        "/O2", "/W3",
        "/wd4996",
        "/wd4244",
        "/D_CRT_SECURE_NO_WARNINGS",
    ]
    extra_link = []

else:
    sources = [
        "graphdb.c",
        "graph_algorithms.c",
        "cypher_lexer.c",
        "cypher_parser.c",
        "cypher_exec.c",
        "graphdb_py.c",
    ]

    def pkg_config(flag):
        try:
            return subprocess.check_output(
                ["pkg-config", flag, "sqlite3"],
                stderr=subprocess.DEVNULL
            ).decode().split()
        except Exception:
            return []

    include_dirs  = []
    library_dirs  = []
    libraries     = ["sqlite3"]
    extra_compile = [
        "-O2", "-Wall",
        "-Wno-unused-parameter",
        "-Wno-stringop-truncation",
        "-std=c11",
    ] + pkg_config("--cflags")
    extra_link = pkg_config("--libs")


graphdb_ext = Extension(
    name="graphdb",
    sources=sources,
    include_dirs=include_dirs,
    library_dirs=library_dirs,
    libraries=libraries,
    extra_compile_args=extra_compile,
    extra_link_args=extra_link,
    language="c",
)

setup(
    name="graphdb",
    version="0.3.0",
    description="Graph database with SQLite backend, Cypher queries, and graph algorithms",
    ext_modules=[graphdb_ext],
    python_requires=">=3.8",
)
