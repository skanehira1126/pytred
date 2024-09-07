from __future__ import annotations

from typing import Literal


def get_current_env() -> Literal["Notebook", "IPython", "UnexpectedShell", "Interpreter"]:
    """
    Identify the environment in which this function is executed

    Returns
    -------
    Literal["Notebook", "IPython", "UnexpectedShell", "Interpreter"]
        A string representing the environment:
        - "Notebook": Running in a Jupyter Notebook
        - "IPython": Running in IPython terminal
        - "UnexpectedShell": Running in an unknown IPython environment
        - "Interpreter": Running in a standard Python interpreter
    """
    try:
        env = get_ipython().__class__.__name__  # type: ignore
        if env == "ZMQInteractiveShell":
            return "Notebook"
        elif env == "TerminalInteractiveShell":
            return "IPython"
        else:
            return "UnexpectedShell"
    except NameError:
        return "Interpreter"


def init_mermaid():
    """
    Initialize Mermaid.js for rendering diagrams in Jupyter Notebook.

    Raises
    -----
    RuntimeError
        If the function is not called in a Jupyter Notebook environment.

    Notes
    -----
    This function injects a script tag into the Jupyter Notebook to load Mermaid.js from a CDN.
    The diagrams are automatically initialized upon loading the script.
    """
    if get_current_env() != "Notebook":
        raise RuntimeError("This function should be used in a Jupyter Notebook.")

    from IPython.display import HTML
    from IPython.display import display

    # Inject Mermaid.js script into the notebook
    mermaid_path = "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs"

    display(
        HTML(
            f"""
    <script type="module">
        import mermaid from '{mermaid_path}';
        window.mermaid = mermaid;
        mermaid.initialize({{
            startOnLoad: true,
            theme: 'neutral',
        }});
    </script>
    """
        )
    )
