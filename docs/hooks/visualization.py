"""Build the documentation demo from the same renderer used by notebooks."""

from pathlib import Path
import runpy


def on_page_markdown(markdown, page, config, files):
    marker = "<!-- pytred-interactive-demo -->"
    if marker not in markdown:
        return markdown
    example = Path(__file__).resolve().parents[1] / "demos" / "viewer_sample.py"
    hub = runpy.run_path(str(example))["hub"]
    return markdown.replace(marker, hub._repr_html_())
