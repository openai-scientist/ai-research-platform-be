"""Render a saved research context as the `research.md` file Popper reads."""

from typing import Any

import yaml

_FENCE = "---"


def render_research_markdown(body: str, front_matter: dict[str, Any] | None) -> str:
    """Front matter between two `---` lines, then the Markdown body.

    The fence is written even when there is no front matter, so a body that itself
    starts with `---` is never mistaken for front matter.
    """
    front = (
        yaml.safe_dump(front_matter, sort_keys=False, allow_unicode=True) if front_matter else ""
    )
    return f"{_FENCE}\n{front}{_FENCE}\n{body}"
