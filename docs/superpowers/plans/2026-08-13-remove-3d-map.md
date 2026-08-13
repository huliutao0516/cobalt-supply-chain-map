# Remove the 3D Map Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish the cobalt supply-chain site with its existing two-dimensional map as the only map and with no three-dimensional UI, code, payload data, downloads, dependencies, or assets.

**Architecture:** Keep `preview_classic.py` as the single preview generator and make its SVG map path unconditional. Add a small standard-library regression test suite that asserts the rendered contract, payload contract, stale-asset cleanup, source cleanliness, and asset absence. Keep `main.py` responsible for replacing the published assets directory so an old local build cannot leak deleted files back into `site`.

**Tech Stack:** Python 3.11, standard-library `unittest`, generated HTML/CSS/JavaScript, SVG, GitHub Pages workflow.

---

## File map

- Create `tests/__init__.py`: make the regression tests importable by `unittest`.
- Create `tests/test_no_3d_map.py`: encode the no-3D contract and stale-asset behavior.
- Modify `preview_classic.py`: remove all globe generation and leave only the SVG map implementation.
- Modify `main.py`: remove a stale `site/assets` directory before optionally copying current assets.
- Modify `output/graph_preview.html` and `site/index.html`: regenerate deployable artifacts from the updated generator.
- Delete the five globe files from both `output/assets` and `site/assets`.
- Modify `docs/superpowers/specs/2026-08-13-remove-3d-map-design.md`: record that both tracked asset copies are in scope.

### Task 1: Add a failing no-3D regression contract

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/test_no_3d_map.py`
- Test: `tests/test_no_3d_map.py`

- [ ] **Step 1: Create the test package marker**

Create an empty `tests/__init__.py`.

- [ ] **Step 2: Add the regression tests**

Create `tests/test_no_3d_map.py` with this complete test module:

```python
from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from main import export_static_site
from preview_classic import build_classic_preview_html, build_world_map_payload


ROOT = Path(__file__).resolve().parents[1]
REMOVED_ASSETS = (
    "globe.gl.min.js",
    "earth_satellite_1350.jpg",
    "earth_satellite_5400.jpg",
    "earth_satellite_21600.jpg",
    "earth_topology.png",
)
FORBIDDEN_SOURCE_MARKERS = (
    "globe",
    "三维",
    "google 3d",
    "maps3d",
    "gmp-map-3d",
    "earth_satellite",
    "earth_topology",
)


class NoThreeDimensionalMapTests(unittest.TestCase):
    def test_preview_html_exposes_only_the_two_dimensional_map(self) -> None:
        html = build_classic_preview_html({})

        self.assertIn("<title>钴供应链图谱</title>", html)
        self.assertIn('<div class="panel-title">地图</div>', html)
        self.assertIn('<svg id="mapSvg"', html)
        for marker in ("三维地球", "globeViewButton", "mapViewButton", "globeCanvas"):
            self.assertNotIn(marker, html)

    def test_world_map_payload_contains_no_globe_only_data(self) -> None:
        topology = {
            "arcs": [],
            "transform": {"scale": [1, 1], "translate": [0, 0]},
            "objects": {"subunits": {"geometries": []}},
        }

        payload = build_world_map_payload(topology, [])

        self.assertEqual({"paths", "labels", "country_points"}, set(payload))

    def test_preview_generator_contains_no_3d_markers(self) -> None:
        source = (ROOT / "preview_classic.py").read_text(encoding="utf-8").casefold()

        for marker in FORBIDDEN_SOURCE_MARKERS:
            self.assertNotIn(marker.casefold(), source)

    def test_tracked_globe_assets_are_absent(self) -> None:
        for prefix in (ROOT / "output" / "assets", ROOT / "site" / "assets"):
            for filename in REMOVED_ASSETS:
                self.assertFalse(prefix.joinpath(filename).exists(), prefix / filename)

    def test_static_export_removes_stale_assets(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            output_dir = root / "output"
            site_dir = root / "site"
            output_dir.mkdir()
            stale_assets = site_dir / "assets"
            stale_assets.mkdir(parents=True)
            (output_dir / "graph_preview.html").write_text("<html></html>", encoding="utf-8")
            (stale_assets / "obsolete.bin").write_bytes(b"obsolete")

            export_static_site(output_dir, site_dir)

            self.assertFalse(stale_assets.exists())


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run the suite and confirm the intended failures**

Run: `python -m unittest tests.test_no_3d_map -v`

Expected: five tests are discovered; the HTML, payload, source-marker, tracked-asset, and stale-asset assertions fail against the current implementation.

### Task 2: Remove three-dimensional code from the preview generator

**Files:**
- Modify: `preview_classic.py:3-7,30-41,126-159,277-362,527-4141`
- Test: `tests/test_no_3d_map.py`

- [ ] **Step 1: Remove Python-only globe dependencies and payload data**

Delete `subprocess`, `urllib.request`, all `GLOBE_*` constants, and `ensure_interaction_texture`. In `build_world_map_payload`, remove `globe_rings` construction and return exactly:

```python
    return {
        "paths": country_paths,
        "labels": country_labels,
        "country_points": country_points,
    }
```

- [ ] **Step 2: Replace the page metadata and map-panel markup**

The HTML head must contain no globe preloads or library script and must use:

```html
  <title>钴供应链图谱</title>
```

Keep the existing `softHighlightButton` in the chain panel. Replace the map panel header/body with:

```html
      <section class="panel map-panel">
        <div class="panel-head">
          <div class="panel-title">地图</div>
        </div>
        <div class="stats" id="mapStats"></div>
        <div class="legend" id="mapLegend"></div>
        <div class="panel-body">
          <div id="mapView" class="geo-view">
            <svg id="mapSvg" viewBox="0 0 1400 520" preserveAspectRatio="xMidYMid meet"></svg>
          </div>
          <div id="mapEmpty" class="empty" hidden>当前焦点没有可用的地理点位。</div>
        </div>
      </section>
```

Delete globe-only CSS selectors, while retaining `.view-tab` for the chain panel's soft-highlight control and retaining `.geo-view` for the SVG map container.

- [ ] **Step 3: Make the SVG map path unconditional**

Remove globe DOM lookups, `worldCountryPoints`, `countryPointLookup`, `activeGeoView`, `googleGlobePreferred`, `globeSceneCache`, and `syncGeoView`. At the start of `renderMapPanel`, always reset the SVG:

```javascript
      mapSvg.setAttribute("viewBox", `0 0 ${width} ${height}`);
      mapSvg.innerHTML = "";
      const hasMapFocus = Boolean(mapSubgraph && mapSubgraph.rowsMatched);
      const reservedLabelBoxes = [];
```

Delete `applyGlobeBundle`, globe cache branches, `connectionCounts`, `globePoints`, `globeLines`, `globeCountries`, globe bridge updates, and the `shouldDrawMapSvg` early return. Preserve the existing point/line aggregation, stats, legend, empty state, country boundaries, country labels, map lines, map points, focus clicks, and tooltips.

- [ ] **Step 4: Remove globe event wiring and renderer scripts**

Keep only the focus bridge assignments:

```javascript
    if (window.__previewBridge) {
      window.__previewBridge.setPrimaryFocus = setPrimaryFocus;
      window.__previewBridge.toggleMapFocus = toggleMapFocus;
    }
```

Replace the final event/bootstrap block with:

```javascript
    searchButton.addEventListener("click", searchAndRender);
    resetButton.addEventListener("click", resetFocus);
    modeToggle.addEventListener("change", render);
    softHighlightButton.addEventListener("click", () => {
      softHighlightMode = !softHighlightMode;
      softHighlightButton.classList.toggle("is-active", softHighlightMode);
      render();
    });
    render();
  </script>
```

Delete all three subsequent globe-only `<script>` blocks, from the local canvas renderer through Google 3D integration and the Globe.gl renderer.

- [ ] **Step 5: Remove export-time downloads**

Make `export_original_style_preview` start directly with payload construction:

```python
    payload = build_classic_preview_payload(
        links_rows,
        matrix_rows,
        country_rows,
        world_topology,
        focus_company=focus_company,
        depth=depth,
        limit=limit,
    )
```

Retain the existing `graph_preview.html` write and summary return value.

- [ ] **Step 6: Run focused tests**

Run: `python -m unittest tests.test_no_3d_map.NoThreeDimensionalMapTests.test_preview_html_exposes_only_the_two_dimensional_map tests.test_no_3d_map.NoThreeDimensionalMapTests.test_world_map_payload_contains_no_globe_only_data tests.test_no_3d_map.NoThreeDimensionalMapTests.test_preview_generator_contains_no_3d_markers -v`

Expected: all three tests pass.

### Task 3: Prevent stale published assets

**Files:**
- Modify: `main.py:1075-1080`
- Test: `tests/test_no_3d_map.py`

- [ ] **Step 1: Move stale-directory deletion outside the source-assets condition**

Replace the asset-copy block with:

```python
    assets_dir = output_dir / "assets"
    site_assets_dir = site_dir / "assets"
    if site_assets_dir.exists():
        shutil.rmtree(site_assets_dir)
    if assets_dir.exists():
        shutil.copytree(assets_dir, site_assets_dir)
```

- [ ] **Step 2: Verify stale assets are removed**

Run: `python -m unittest tests.test_no_3d_map.NoThreeDimensionalMapTests.test_static_export_removes_stale_assets -v`

Expected: PASS and the temporary `site/assets` directory is absent after export.

### Task 4: Delete tracked globe assets and regenerate the site

**Files:**
- Delete: `output/assets/globe.gl.min.js`
- Delete: `output/assets/earth_satellite_1350.jpg`
- Delete: `output/assets/earth_satellite_5400.jpg`
- Delete: `output/assets/earth_satellite_21600.jpg`
- Delete: `output/assets/earth_topology.png`
- Delete: `site/assets/globe.gl.min.js`
- Delete: `site/assets/earth_satellite_1350.jpg`
- Delete: `site/assets/earth_satellite_5400.jpg`
- Delete: `site/assets/earth_satellite_21600.jpg`
- Delete: `site/assets/earth_topology.png`
- Modify: `output/graph_preview.html`
- Modify: `site/index.html`
- Modify: `site/summary.json` only if the deterministic build updates it

- [ ] **Step 1: Delete both tracked copies of all five globe assets**

Run `git rm` with the ten exact asset paths listed above.

Expected: Git records ten deletions; the files remain recoverable from earlier commits.

- [ ] **Step 2: Run the same static build used by GitHub Pages**

Run: `python main.py --render-html --publish-static --site-dir site`

Expected: exit code 0; `output/graph_preview.html` and `site/index.html` are regenerated; neither `output/assets` nor `site/assets` is recreated with globe resources.

- [ ] **Step 3: Run the complete regression suite**

Run: `python -m unittest tests.test_no_3d_map -v`

Expected: all five tests pass.

- [ ] **Step 4: Check syntax and whitespace**

Run: `python -m py_compile main.py preview_classic.py`

Expected: exit code 0 with no output.

Run: `git diff --check`

Expected: exit code 0 with no output.

- [ ] **Step 5: Commit the implementation**

Stage only `preview_classic.py`, `main.py`, `tests`, `output/graph_preview.html`, `site/index.html`, any deterministically changed `site/summary.json`, the ten asset deletions, the corrected design spec, and this plan. Commit with:

```text
feat: remove 3D map from supply chain site
```

### Task 5: Browser and repository verification

**Files:**
- Verify: `site/index.html`
- Verify: repository tracked file list

- [ ] **Step 1: Scan all functional surfaces for forbidden references**

Run:

```powershell
rg -n -i "globe|三维|google 3d|maps3d|gmp-map-3d|earth_satellite|earth_topology" preview_classic.py output/graph_preview.html site/index.html
```

Expected: no matches.

Run:

```powershell
git ls-files output/assets site/assets
```

Expected: no output.

- [ ] **Step 2: Serve and inspect the generated site**

Start a local HTTP server rooted at `site`, open the local page in the in-app browser, and verify:

- the panel header is `地图`;
- no `二维地图` or `三维地球` switch is visible;
- the two-dimensional SVG has country paths, supply-chain nodes, and supply-chain lines;
- changing/resetting focus still updates the map;
- the browser console has no errors.

- [ ] **Step 3: Push and monitor deployment**

Set the isolated clone's `origin` URL back to `https://github.com/huliutao0516/cobalt-supply-chain-map.git`, push `main`, and monitor the existing `Deploy Static Preview` workflow until it succeeds.

Expected: push succeeds and the GitHub Pages workflow finishes successfully.

- [ ] **Step 4: Verify the public page**

Open `https://huliutao0516.github.io/cobalt-supply-chain-map/` after deployment and repeat the UI, DOM, and console checks from Step 2.

Expected: the public site shows only the two-dimensional map and loads no globe library or globe texture assets.

