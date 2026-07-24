"""
Backward-compatible entrypoint: `python main.py <command>` still works.
Prefer the installed `gdeltforge` console script (uv sync && gdeltforge <command>).
"""
from gdeltforge.cli import main

if __name__ == "__main__":
    main()
