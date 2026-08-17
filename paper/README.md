# paper/

Code and raw artifacts used for writing the paper.

- `draw.py` — renders the dbt DAG of each benchmark project as an SVG figure.
- `table.py` — emits the LaTeX summary table for the 10 benchmark projects.

`figures/` holds the "compiled" output: the figures, images, and snippets that
go straight into the paper. Nothing in `figures/` is edited by hand — regenerate
it from the scripts here.

Both scripts resolve paths relative to the repo root, so they can be run from
anywhere:

```sh
python3 paper/draw.py                            # all 10 DAG figures -> paper/figures/
python3 paper/draw.py -p p01_iot                 # one benchmark project
python3 paper/draw.py paper/example-dag          # any dbt project, by path
python3 paper/table.py -o paper/figures/table.tex
```

`draw.py` takes dbt projects either way: positional arguments are paths to a
project directory (anything holding `target/manifest.json`), and `--project`/`-p`
is a shortcut for a benchmark project under `projects/`.
