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
        self.assertIn('chainsSvg.removeAttribute("viewBox");', self.html)

    def test_shell_has_a_definite_viewport_relative_height(self) -> None:
        shell_rule = re.search(r"\.shell\s*\{(?P<body>[^}]*)\}", self.html)

        self.assertIsNotNone(shell_rule)
        self.assertRegex(
            shell_rule.group("body"),
            r"(?m)^\s*height: max\(calc\(100vh - 36px\), 790px\);",
        )

    def test_mobile_shell_uses_fixed_graph_tracks_and_auto_height(self) -> None:
        mobile_shell_rule = re.search(
            r"@media \(max-width: 860px\)\s*\{\s*\.shell\s*\{(?P<body>[^}]*)\}",
            self.html,
        )

        self.assertIsNotNone(mobile_shell_rule)
        css = mobile_shell_rule.group("body")
        self.assertRegex(css, r"(?m)^\s*height: auto;")
        self.assertRegex(
            css,
            r"(?m)^\s*grid-template-rows: 28px auto clamp\(460px, 70vh, 600px\) 320px;",
        )

    def test_map_svg_keeps_responsive_panel_sizing(self) -> None:
        map_rule = re.search(r"#mapSvg\s*\{(?P<body>[^}]*)\}", self.html)

        self.assertIsNotNone(map_rule)
        css = map_rule.group("body")
        self.assertIn("width: 100%;", css)
        self.assertIn("height: 100%;", css)
        self.assertNotRegex(self.html, r"#chainsSvg\s*,\s*#mapSvg")


if __name__ == "__main__":
    unittest.main()
