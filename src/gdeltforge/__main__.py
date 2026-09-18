"""
Lets `python -m gdeltforge <command>` work the same way as the installed
`gdeltforge` console script, for an environment where the script itself
isn't on PATH, or where being explicit about which interpreter runs it
matters (multiple Python installs, a venv activated in a way that
doesn't put its own Scripts/bin directory first).
"""
from gdeltforge.cli import main

if __name__ == "__main__":
    main()
