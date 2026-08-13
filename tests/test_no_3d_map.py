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
