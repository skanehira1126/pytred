# pytred
Python Tools for Refinement and Enhancement of Data

## Introduction

`pytred` is a Python toolkit designed to streamline the organization and execution of data processing tasks. 
Iht facilitates a structured approach to data operations, allowing each function to be treated as an independent data processing unit, similar to how one might think of tables in a relational database.

## Features
- **Structured Data Processing**: `pytred` enables users to organize their data processing logic in a clear and modular way, with each function acting as a table in relational database.
- **Flexible and Reusable**: By encapsulating data processing tasks within functions, pytred promotes reusability and flexibility, allowing for easy modification and extension of data processing workflows.
- **Clear and Maintainable** Code: The framework promotes a clean separation of concerns, which simplifies understanding, maintaining, and debugging the data processing steps.

## Examples

1. [basic preprocessing](./examples/01_basic_preprocessing.ipynb)
1. [Preprocessing with completed data](./examples/02_use_completed_data.ipynb)
1. [visualize workflow](./examples/03_visualize_workflow.ipynb)

## Codex plugin

This repository includes a Codex plugin that teaches AI agents how to use
`DataHub`, `DataNode`, `@polars_table`, and the `pytred report` CLI without relying
on generic data-pipeline assumptions.

Install the Python package in your project's environment as usual:

```bash
pip install pytred
```

Install the Codex plugin separately from the repository marketplace:

```bash
codex plugin marketplace add skanehira1126/pytred
codex plugin add pytred@pytred
```

To use a local checkout as the marketplace source, run from the repository root:

```bash
codex plugin marketplace add .
codex plugin add pytred@pytred
```

These commands register the marketplace and plugin in your Codex environment;
running them inside a repository does not limit the installation to that repository.
For use only in a particular repository, place the `pytred-usage` skill folder under
that repository's `.agents/skills/` instead.

Start a new Codex task after installation and select `pytred-usage`, or ask for help
with a pytred pipeline. The plugin includes its own usage guide and does not require
a pytred source checkout. It does not install Python dependencies; `pip install`
does not register the Codex plugin.

The plugin lives in `plugins/pytred`, with its own version in
`plugins/pytred/.codex-plugin/plugin.json`. The repository marketplace is defined in
`.agents/plugins/marketplace.json`. Repository skill links in `.agents/skills` and
`.codex/skills` point to the same plugin skill, so edit only
`plugins/pytred/skills/pytred-usage`. If you previously installed the standalone
skill into your user skill directory, remove that old copy when switching to the
plugin to avoid duplicate skills.
