# Supply Chain Graph ETL

This project converts the data behind [Resource Matters Supply Chains](https://supplychains.resourcematters.org/explore) into a graph dataset.

It does **not** scrape rendered HTML. The website is backed by static source files, and this ETL pulls those files directly:

- `data/links.csv`
- `data/countries.csv`
- `data/bhrrc-companies.csv`
- `data/bhrrc-news.json`

## What It Builds

The script exports a graph model with these main node types:

- `Company`
- `Facility`
- `Country`
- `Commodity`
- `ChainStep`
- `ChainLink`
- `Transaction`
- `Source`

And these main relationship types:

- `SUPPLIER_IN`
- `BUYER_IN`
- `SUPPLIES_TO`
- `INPUT_CHAIN_STEP`
- `OUTPUT_CHAIN_STEP`
- `INPUT_COMMODITY`
- `OUTPUT_COMMODITY`
- `OPERATES_FACILITY`
- `LOCATED_IN`
- `PARENT_OF`
- `HAS_SUBSIDIARY`
- `ASSOCIATED_WITH_JV`
- `SUPPORTED_BY`

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Build the graph CSV exports:

```bash
python main.py --refresh
```

3. Exported files will be written to `output/`:

- `output/companies.csv`
- `output/facilities.csv`
- `output/countries.csv`
- `output/commodities.csv`
- `output/chain_steps.csv`
- `output/chain_links.csv`
- `output/transactions.csv`
- `output/sources.csv`
- `output/relationships.csv`
- `output/path_matrix.csv`
- `output/path_matrix.xlsx`
- `output/graph_preview.html`
- `output/summary.json`

4. If you want a directly viewable graph preview page:

```bash
python main.py --refresh --render-html --preview-company "Kamoto Copper Company (KCC)" --preview-depth 3 --preview-limit 180
```

Then open `output/graph_preview.html` in your browser.

5. If you want a deployable static website bundle:

```bash
python main.py --refresh --render-html --publish-static --site-dir site
```

This writes:

- `site/index.html`
- `site/summary.json`
- `site/.nojekyll`

You can upload the `site/` directory to any static host, or use the included GitHub Pages workflow.

## Publish Online

This repo now includes `.github/workflows/deploy-pages.yml`.

If you push the project to a GitHub repository with a `main` branch, GitHub Actions will:

1. build the latest preview
2. copy it into `site/`
3. publish the static page to GitHub Pages

After the first successful run, enable GitHub Pages in the repository settings if needed, then your public link will usually be:

```text
https://<your-github-username>.github.io/<your-repository-name>/
```

If you prefer another static host such as Cloudflare Pages or Netlify, point the publish directory at `site/`.

## Optional: Load Into Neo4j

You can also load the graph directly into Neo4j:

```bash
python main.py --refresh --load-neo4j --neo4j-uri bolt://localhost:7687 --neo4j-user neo4j --neo4j-password your_password
```

You can also use environment variables instead:

```bash
set NEO4J_URI=bolt://localhost:7687
set NEO4J_USER=neo4j
set NEO4J_PASSWORD=your_password
python main.py --refresh --load-neo4j
```

If you want both the Neo4j load and the local preview in one run:

```bash
python main.py --refresh --render-html --preview-company "Kamoto Copper Company (KCC)" --preview-depth 3 --preview-limit 180 --load-neo4j --neo4j-uri bolt://localhost:7687 --neo4j-user neo4j --neo4j-password your_password
```

## Notes

- `countries.csv` from the source site is only a centroid helper file, not a full world country list.
- The ETL keeps country names from the transaction data even when a centroid match is unavailable.
- Amount fields are preserved as raw strings, and numeric values are parsed only when the input is cleanly numeric.
- Joint ventures, subsidiaries, and parent-company relationships are preserved as graph edges instead of being flattened into text only.
- `path_matrix.xlsx` is the sample-style export: one row per supply-chain path, one column per chain step.
- `graph_preview.html` is an original-site-inspired preview with a `chains` panel and a `map` panel. It uses the transaction links directly, keeps the page lighter than a full network graph, and lets you re-focus on different companies locally in the browser.
