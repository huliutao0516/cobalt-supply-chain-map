# Remove the 3D Map Design

**Date:** 2026-08-13

## Goal

Remove the three-dimensional globe view from the cobalt supply-chain website without leaving UI, code, data, dependencies, or static assets related to it. Keep the existing two-dimensional supply-chain map as the only map view.

## User-facing result

- The map panel title is `地图`.
- The `二维地图` and `三维地球` view-switch buttons are removed.
- The existing two-dimensional map is shown directly and keeps its current counters, node and link rendering, focus behavior, filtering, and tooltips.
- Page metadata no longer describes the site as a three-dimensional graph.

## Source cleanup

`preview_classic.py` remains the single source for the published preview, but all globe-specific paths are removed:

- globe library and texture constants and download logic;
- globe-only payload fields, including sampled coastline rings;
- globe HTML, CSS, setup UI, labels, canvas, and tooltips;
- 2D/3D view state and switching handlers;
- local canvas globe renderer, Globe.gl renderer, and Google 3D Maps integration;
- globe data transformation, caching, resize, and update bridges.

The two-dimensional SVG map rendering path becomes unconditional.

## Published artifact cleanup

Regenerate `site/index.html` from the updated generator. Remove these tracked files because the site will no longer reference or produce them:

- `site/assets/globe.gl.min.js`
- `site/assets/earth_satellite_1350.jpg`
- `site/assets/earth_satellite_5400.jpg`
- `site/assets/earth_satellite_21600.jpg`
- `site/assets/earth_topology.png`

Update the static-site export so future local builds and GitHub Actions runs neither download nor republish three-dimensional resources.

## Verification

1. Run the static-site build used by GitHub Pages.
2. Confirm the build succeeds and the generated page contains the two-dimensional map.
3. Search the generator and published site for 3D/globe identifiers, UI labels, Google 3D integration, Globe.gl imports, and removed texture filenames; expected result is no functional three-dimensional-map references.
4. Confirm the removed asset files are absent from `site/assets` and are not recreated by a clean build.
5. Open the generated page in a browser and verify the two-dimensional map renders, the view-switch buttons are absent, and no page errors are produced.
6. Push the verified commit to `main`; the existing GitHub Pages workflow rebuilds and deploys the site. After deployment, recheck the public URL.

## Non-goals

- Redesigning the two-dimensional map.
- Changing supply-chain data or ETL output.
- Refactoring unrelated preview features.

