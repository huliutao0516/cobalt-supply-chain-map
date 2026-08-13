# Make Page Zoom Enlarge the Chain Graph

**Date:** 2026-08-13

## Goal

Make browser page zoom enlarge the upper `产业链关系` graph as a single visual unit: labels, nodes, and connecting lines must grow together. Keep the lower two-dimensional map unchanged.

## Confirmed scope

- Apply the change only to the upper supply-chain relationship SVG.
- Preserve the existing graph data, stage ordering, node placement, links, focus behavior, filtering, and tooltips.
- Use the browser's normal page zoom; do not add a separate graph zoom control.
- Allow horizontal and vertical scrolling when the enlarged graph no longer fits inside its panel.

## Current behavior and root cause

The graph renderer creates a large logical SVG canvas and assigns it a dynamic `viewBox`. CSS then forces the SVG to fill the panel with `width: 100%` and `height: 100%`. On the deployed page, an SVG label configured as `11px` is rendered at about `5px` high because the entire logical canvas is scaled down to fit the panel.

When browser zoom increases, the CSS viewport becomes narrower. The responsive SVG is consequently scaled down again to keep the complete graph inside the panel, which offsets the browser's magnification. The graph text therefore appears not to grow.

## Design

The chain graph will use its calculated logical width and height as its intrinsic rendered size instead of continuously fitting both dimensions to the panel:

- `renderChainPanel` continues calculating the existing graph width, height, view box, node coordinates, and link paths.
- The calculated width and height are also applied as the SVG's intrinsic dimensions.
- Chain-graph CSS stops overriding those dimensions with `width: 100%` and `height: 100%`.
- The existing `.chains-scroll` container remains the viewport and provides overflow scrolling in both directions.
- Minimum sizing keeps a graph that is smaller than the panel from producing unnecessary empty alignment or scroll behavior.

Because the SVG now occupies a stable number of CSS pixels, browser page zoom magnifies those pixels normally. Text, node shapes, strokes, and spacing remain proportional, while overflow is handled by the panel rather than by shrinking the graph.

The map SVG keeps its current responsive `width: 100%` and `height: 100%` behavior. The chain and map selectors will be separated so the change cannot leak into the lower map.

## Responsive and interaction behavior

- At 100% browser zoom, the graph is shown at its intrinsic layout size and may require scrolling for large datasets.
- At 125% and 150%, the graph visibly enlarges and the scrollable extent increases.
- Browser zooming out reduces the graph proportionally.
- Window resizing changes the visible portion of the graph but does not recompute a fit scale that would cancel browser zoom.
- Existing pointer, focus, tooltip, and filter behavior remains attached to the same SVG elements.

## Verification

1. Add a regression test that checks the chain SVG receives intrinsic width and height and is no longer covered by the shared responsive SVG sizing rule.
2. Confirm the map SVG retains its responsive sizing rule.
3. Rebuild `site/index.html` from `preview_classic.py` and run the existing test suite.
4. In a browser, measure the same chain label and node at 100%, 125%, and 150% page zoom; their displayed dimensions must increase at each step.
5. Confirm horizontal and vertical scrolling expose all enlarged graph content without clipping.
6. Confirm node focus, filtering, links, and tooltips still work.
7. Confirm the lower two-dimensional map has the same dimensions and behavior as before.
8. Deploy through the existing GitHub Pages workflow and repeat the zoom check on the public URL.

## Non-goals

- Adding custom zoom buttons, mouse-wheel graph zoom, panning, or a fit-to-window toggle.
- Enlarging only text while leaving nodes and links unchanged.
- Changing the lower two-dimensional map.
- Changing graph data, layout algorithms, colors, labels, or panel proportions.
