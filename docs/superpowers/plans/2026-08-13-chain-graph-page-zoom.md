# Chain Graph Page Zoom Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make normal browser page zoom enlarge the upper supply-chain graph's labels, nodes, and links together while leaving the lower two-dimensional map responsive and unchanged.

**Architecture:** Keep the existing SVG layout algorithm and scroll container. Give the chain SVG the logical width and height already calculated by `renderChainPanel`, stop CSS from fitting that SVG back into the panel, and retain the existing responsive sizing only for the map SVG. A focused HTML-generation regression test will guard the CSS/JavaScript contract, and the generated static page will be rebuilt and checked in a browser at three zoom levels.

**Tech Stack:** Python 3.11, `unittest`, generated HTML/CSS/vanilla JavaScript, SVG, GitHub Pages

---

## File structure

- Create `tests/test_chain_graph_page_zoom.py`: focused regression tests for intrinsic chain sizing, empty-state cleanup, and map isolation.
- Modify `preview_classic.py`: separate chain/map SVG sizing rules and write/remove the chain SVG intrinsic dimensions.
- Regenerate `output/graph_preview.html`: local generated preview produced by the updated generator.
- Regenerate `site/index.html`: tracked GitHub Pages artifact produced by the updated generator.

### Task 1: Lock the desired sizing contract with failing tests

**Files:**
- Create: `tests/test_chain_graph_page_zoom.py`
- Test: `tests/test_chain_graph_page_zoom.py`

- [ ] **Step 1: Write the focused regression tests**

Create `tests/test_chain_graph_page_zoom.py` with this complete content:

```python
from __future__ import annotations

import re
import unittest

from preview_classic import build_classic_preview_html


class ChainGraphPageZoomTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = build_classic_preview_html({})

    def test_chain_svg_uses_intrinsic_scrollable_dimensions(self) -> None:
        chain_rule = re.search(r"#chainsSvg\s*\{(?P<body>[^}]*)\}", self.html)

        self.assertIsNotNone(chain_rule)
        css = chain_rule.group("body")
        self.assertIn("width: auto;", css)
        self.assertIn("height: auto;", css)
        self.assertIn("min-width: 100%;", css)
        self.assertIn("min-height: 100%;", css)
        self.assertIn('chainsSvg.setAttribute("width", String(width));', self.html)
        self.assertIn('chainsSvg.setAttribute("height", String(height));', self.html)

    def test_empty_chain_view_removes_stale_intrinsic_dimensions(self) -> None:
        self.assertIn('chainsSvg.removeAttribute("width");', self.html)
        self.assertIn('chainsSvg.removeAttribute("height");', self.html)

    def test_map_svg_keeps_responsive_panel_sizing(self) -> None:
        map_rule = re.search(r"#mapSvg\s*\{(?P<body>[^}]*)\}", self.html)

        self.assertIsNotNone(map_rule)
        css = map_rule.group("body")
        self.assertIn("width: 100%;", css)
        self.assertIn("height: 100%;", css)
        self.assertNotRegex(self.html, r"#chainsSvg\s*,\s*#mapSvg")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the focused test and confirm it fails for the right reason**

Run:

```powershell
python -m unittest tests.test_chain_graph_page_zoom -v
```

Expected: three failures showing that `#chainsSvg` still shares the `width: 100%; height: 100%` rule with `#mapSvg`, that intrinsic width/height assignments are absent, and that empty-state dimension cleanup is absent.

- [ ] **Step 3: Confirm the existing suite is green before implementation**

Run:

```powershell
python -m unittest discover -s tests -v
```

Expected: only the three new chain-zoom tests fail; the existing five 3D-removal tests pass.

### Task 2: Give the chain graph intrinsic dimensions without changing the map

**Files:**
- Modify: `preview_classic.py:649-660`
- Modify: `preview_classic.py:1091-1112`
- Test: `tests/test_chain_graph_page_zoom.py`

- [ ] **Step 1: Split the shared SVG sizing rule**

Replace the shared `#chainsSvg, #mapSvg` rule with these two rules:

```css
    #chainsSvg {
      display: block;
      width: auto;
      height: auto;
      min-width: 100%;
      min-height: 100%;
    }
    #mapSvg {
      display: block;
      width: 100%;
      height: 100%;
    }
```

Keep `.chains-scroll { overflow: auto; }` unchanged so the panel remains the graph viewport.

- [ ] **Step 2: Clear old intrinsic dimensions in the empty state**

Change the empty branch at the start of `renderChainPanel` to:

```javascript
      if (!stageEntries.length || !subgraph.rowsMatched) {
        chainsSvg.innerHTML = "";
        chainsSvg.removeAttribute("width");
        chainsSvg.removeAttribute("height");
        chainsEmpty.hidden = false;
        return;
      }
```

This prevents a previously rendered large graph from leaving a large blank scroll area after a filter produces no rows.

- [ ] **Step 3: Apply the calculated graph size as intrinsic SVG dimensions**

Immediately after assigning the chain `viewBox`, add the intrinsic dimensions:

```javascript
      chainsSvg.setAttribute("viewBox", `0 0 ${width} ${height}`);
      chainsSvg.setAttribute("width", String(width));
      chainsSvg.setAttribute("height", String(height));
      chainsSvg.innerHTML = "";
```

Do not change `columnWidth`, node sizes, positions, link paths, font sizes, or the map renderer.

- [ ] **Step 4: Run the focused tests and confirm they pass**

Run:

```powershell
python -m unittest tests.test_chain_graph_page_zoom -v
```

Expected: `Ran 3 tests` and `OK`.

- [ ] **Step 5: Run the complete unit-test suite**

Run:

```powershell
python -m unittest discover -s tests -v
```

Expected: `Ran 8 tests` and `OK`.

- [ ] **Step 6: Inspect the source diff and commit the tested implementation**

Run:

```powershell
git diff --check
git diff -- preview_classic.py tests/test_chain_graph_page_zoom.py
git add preview_classic.py tests/test_chain_graph_page_zoom.py
git commit -m "fix: let page zoom enlarge chain graph"
```

Expected: no whitespace errors; the diff contains only the focused CSS, chain SVG sizing, empty-state cleanup, and regression test; the commit succeeds.

### Task 3: Rebuild and verify the published static artifact

**Files:**
- Regenerate: `output/graph_preview.html`
- Regenerate: `site/index.html`
- Verify: `preview_classic.py`
- Verify: `tests/test_chain_graph_page_zoom.py`

- [ ] **Step 1: Rebuild the preview and static site using the deployment command**

Run:

```powershell
python main.py --render-html --publish-static --site-dir site
```

Expected: the command exits successfully and refreshes `output/graph_preview.html` and `site/index.html` without downloading new source data.

- [ ] **Step 2: Verify the generated page contains the sizing contract**

Run:

```powershell
rg -n -e "#chainsSvg" -e "#mapSvg" -e "chainsSvg.setAttribute\(\"width\"" -e "chainsSvg.removeAttribute\(\"width\"" site/index.html
```

Expected: separate `#chainsSvg` and `#mapSvg` rules are present; chain intrinsic sizing and empty-state cleanup are present; there is no shared `#chainsSvg, #mapSvg` selector.

- [ ] **Step 3: Re-run all unit tests against the regenerated workspace**

Run:

```powershell
python -m unittest discover -s tests -v
```

Expected: `Ran 8 tests` and `OK`.

- [ ] **Step 4: Review and commit only the generated artifacts that changed**

Run:

```powershell
git status --short
git diff --check
git diff --stat
git add output/graph_preview.html site/index.html
git commit -m "build: refresh chain graph preview"
```

Expected: only the intended generated HTML artifacts are staged; the commit succeeds. If the generator produces no tracked change in `output/graph_preview.html`, stage and commit only `site/index.html`.

### Task 4: Browser verification and GitHub Pages deployment

**Files:**
- Verify: `site/index.html`
- Verify after deployment: `https://huliutao0516.github.io/cobalt-supply-chain-map/`

- [ ] **Step 1: Serve the generated site locally**

Start a hidden local server from the repository root:

```powershell
$chainZoomServer = Start-Process python -ArgumentList '-m','http.server','8765','--directory','site' -WindowStyle Hidden -PassThru
$chainZoomServer.Id
```

Record the returned process ID so only this server is stopped after verification.

- [ ] **Step 2: Measure and inspect the chain graph at 100%, 125%, and 150% page zoom**

Open `http://127.0.0.1:8765/` in the browser. At each zoom level, inspect the same chain label and node with `getBoundingClientRect()` and record their displayed width and height.

Expected:

- the label, node, and link stroke visibly grow at every zoom step;
- the graph panel exposes horizontal and vertical scrolling instead of shrinking the graph to fit;
- all graph content remains reachable by scrolling;
- focus, filtering, and tooltips still work;
- the lower two-dimensional map keeps its existing responsive size and behavior;
- the browser console contains no application errors.

- [ ] **Step 3: Stop only the recorded local server process**

Run with the same `$chainZoomServer` process object created in Step 1:

```powershell
$chainZoomServer | Stop-Process
```

Expected: the local verification server stops and no unrelated Python process is affected.

- [ ] **Step 4: Run final pre-push verification**

Run:

```powershell
python -m unittest discover -s tests -v
git diff --check
git status -sb
```

Expected: all eight tests pass, there are no whitespace errors, and the worktree is clean with `main` ahead of `origin/main` only by the reviewed design, plan, implementation, and generated-artifact commits.

- [ ] **Step 5: Push the verified commits and monitor deployment**

Run:

```powershell
git push origin main
gh run list --repo huliutao0516/cobalt-supply-chain-map --limit 3
```

Expected: the push succeeds and a new `Deploy Static Preview` workflow run appears for the pushed commit. Wait for that run to finish successfully.

- [ ] **Step 6: Verify the public site after deployment**

Open `https://huliutao0516.github.io/cobalt-supply-chain-map/?zoom-fix=20260813-chain-page-zoom` to bypass stale caches and repeat the 100%, 125%, and 150% checks from Step 2.

Expected: the public page matches the local result, the upper graph enlarges with browser page zoom, the lower map remains unchanged, and the browser console has no application errors.
