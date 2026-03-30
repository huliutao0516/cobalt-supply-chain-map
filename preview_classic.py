from __future__ import annotations

import json
from pathlib import Path
from typing import Any
import urllib.request


STEP_ORDER = [
    "Artisanal mining",
    "Mining",
    "Recycling",
    "Artisanal processing",
    "Smelting",
    "Trading",
    "Refining",
    "Precursor manufacturing",
    "Cathode manufacturing",
    "Battery cell manufacturing",
    "Battery pack manufacturing",
    "Electric car/scooter manufacturing",
]

STEP_COLUMN_RENAMES = {
    "Electric car manufacturing": "Electric car/scooter manufacturing",
    "Electric scooter manufacturing": "Electric car/scooter manufacturing",
}

MAP_WIDTH = 1400
MAP_HEIGHT = 520
MAP_MARGIN = 24
GLOBE_TEXTURE_SOURCE_URL = "https://eoimages.gsfc.nasa.gov/images/imagerecords/73000/73909/world.topo.bathy.200412.3x21600x10800.jpg"
GLOBE_TEXTURE_RELATIVE_PATH = "assets/earth_satellite_21600.jpg"
GLOBE_TEXTURE_PREVIEW_URL = "https://neo.gsfc.nasa.gov/archive/bluemarble/bmng/world_8km/world.topo.bathy.200412.3x5400x2700.jpg"
GLOBE_TEXTURE_PREVIEW_RELATIVE_PATH = "assets/earth_satellite_5400.jpg"
GLOBE_JS_URL = "https://cdn.jsdelivr.net/npm/globe.gl"
GLOBE_JS_RELATIVE_PATH = "assets/globe.gl.min.js"
GLOBE_BUMP_URL = "https://cdn.jsdelivr.net/npm/three-globe/example/img/earth-topology.png"
GLOBE_BUMP_RELATIVE_PATH = "assets/earth_topology.png"

COUNTRY_LABELS_ZH = {
    "Australia": "澳大利亚",
    "Austria": "奥地利",
    "Belgium": "比利时",
    "Brazil": "巴西",
    "Canada": "加拿大",
    "China": "中国",
    "Cuba": "古巴",
    "Dem. Rep. Congo": "刚果（金）",
    "Democratic Republic of the Congo": "刚果（金）",
    "Finland": "芬兰",
    "France": "法国",
    "Germany": "德国",
    "Hong Kong": "中国香港",
    "Hungary": "匈牙利",
    "India": "印度",
    "Indonesia": "印度尼西亚",
    "Italy": "意大利",
    "Japan": "日本",
    "Morocco": "摩洛哥",
    "New Caledonia": "新喀里多尼亚",
    "Norway": "挪威",
    "Philippines": "菲律宾",
    "Poland": "波兰",
    "Russia": "俄罗斯",
    "Singapore": "新加坡",
    "Slovakia": "斯洛伐克",
    "South Africa": "南非",
    "South Korea": "韩国",
    "Spain": "西班牙",
    "Sweden": "瑞典",
    "Switzerland": "瑞士",
    "Taiwan": "中国台湾",
    "Tanzania": "坦桑尼亚",
    "The Netherlands": "荷兰",
    "Netherlands": "荷兰",
    "UAE": "阿联酋",
    "UK": "英国",
    "United Arab Emirates": "阿联酋",
    "United Kingdom": "英国",
    "United States": "美国",
    "USA": "美国",
    "Vietnam": "越南",
    "Zambia": "赞比亚",
    "Zimbabwe": "津巴布韦",
}

COUNTRY_CANONICAL_ALIASES = {
    "Democratic Republic of the Congo": "Dem. Rep. Congo",
    "The Netherlands": "Netherlands",
    "United Arab Emirates": "UAE",
    "United Kingdom": "United Kingdom",
    "UK": "United Kingdom",
    "United States": "United States",
    "USA": "United States",
}


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\ufeff", "").strip().split())


def parse_float(value: Any) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def normalize_country_key(name: str) -> str:
    return clean_text(name).casefold()


def localize_country_name(name: str) -> str:
    cleaned = clean_text(name)
    return COUNTRY_LABELS_ZH.get(cleaned, cleaned)


def normalize_step_name(step_name: str) -> str:
    step_name = clean_text(step_name)
    return STEP_COLUMN_RENAMES.get(step_name, step_name)


def split_matrix_cell(value: Any) -> list[str]:
    value = clean_text(value)
    if not value:
        return []
    return [clean_text(part) for part in value.split(" ; ") if clean_text(part)]


def project_coordinate(
    lon: float,
    lat: float,
    *,
    width: int = MAP_WIDTH,
    height: int = MAP_HEIGHT,
    margin: int = MAP_MARGIN,
    bounds: tuple[float, float, float, float] = (-180.0, -60.0, 180.0, 85.0),
) -> tuple[float, float]:
    min_lon, min_lat, max_lon, max_lat = bounds
    inner_width = width - margin * 2
    inner_height = height - margin * 2
    x = margin + ((lon - min_lon) / (max_lon - min_lon)) * inner_width
    y = margin + ((max_lat - lat) / (max_lat - min_lat)) * inner_height
    return x, y


def decode_topology_arcs(topology: dict[str, Any]) -> list[list[tuple[float, float]]]:
    transform = topology.get("transform") or {}
    scale_x, scale_y = transform.get("scale", [1, 1])
    translate_x, translate_y = transform.get("translate", [0, 0])
    decoded: list[list[tuple[float, float]]] = []

    for arc in topology.get("arcs", []):
        x_acc = 0
        y_acc = 0
        points: list[tuple[float, float]] = []
        for delta_x, delta_y in arc:
            x_acc += delta_x
            y_acc += delta_y
            lon = translate_x + x_acc * scale_x
            lat = translate_y + y_acc * scale_y
            points.append((lon, lat))
        decoded.append(points)

    return decoded


def arc_points(decoded_arcs: list[list[tuple[float, float]]], arc_index: int) -> list[tuple[float, float]]:
    points = decoded_arcs[arc_index if arc_index >= 0 else ~arc_index]
    if arc_index < 0:
        return list(reversed(points))
    return points[:]


def stitch_ring(decoded_arcs: list[list[tuple[float, float]]], arc_indexes: list[int]) -> list[tuple[float, float]]:
    ring: list[tuple[float, float]] = []
    for position, arc_index in enumerate(arc_indexes):
        points = arc_points(decoded_arcs, arc_index)
        if position:
            ring.extend(points[1:])
        else:
            ring.extend(points)
    return ring


def geometry_to_rings(
    decoded_arcs: list[list[tuple[float, float]]],
    geometry: dict[str, Any],
) -> list[list[tuple[float, float]]]:
    geometry_type = geometry.get("type")
    arcs = geometry.get("arcs", [])
    if geometry_type == "Polygon":
        return [stitch_ring(decoded_arcs, ring) for ring in arcs]
    if geometry_type == "MultiPolygon":
        rings: list[list[tuple[float, float]]] = []
        for polygon in arcs:
            rings.extend(stitch_ring(decoded_arcs, ring) for ring in polygon)
        return rings
    return []


def rings_to_svg_path(
    rings: list[list[tuple[float, float]]],
    *,
    width: int = MAP_WIDTH,
    height: int = MAP_HEIGHT,
    margin: int = MAP_MARGIN,
) -> str:
    commands: list[str] = []
    for ring in rings:
        if len(ring) < 2:
            continue
        segment: list[tuple[float, float]] = []
        previous_lon: float | None = None
        for lon, lat in ring:
            if previous_lon is not None and abs(lon - previous_lon) > 180:
                if len(segment) > 1:
                    start_x, start_y = segment[0]
                    commands.append(f"M {start_x} {start_y}")
                    for x, y in segment[1:]:
                        commands.append(f"L {x} {y}")
                segment = []
            segment.append(project_coordinate(lon, lat, width=width, height=height, margin=margin))
            previous_lon = lon
        if len(segment) > 1:
            start_x, start_y = segment[0]
            commands.append(f"M {start_x} {start_y}")
            for x, y in segment[1:]:
                commands.append(f"L {x} {y}")
    return " ".join(commands)


def build_world_map_payload(
    world_topology: dict[str, Any],
    country_rows: list[dict[str, str]],
) -> dict[str, Any]:
    decoded_arcs = decode_topology_arcs(world_topology)
    subunits = world_topology.get("objects", {}).get("subunits", {})
    geometries = subunits.get("geometries", [])
    country_paths: list[dict[str, str]] = []
    for geometry in geometries:
        rings = geometry_to_rings(decoded_arcs, geometry)
        path_d = rings_to_svg_path(rings)
        if not path_d:
            continue
        properties = geometry.get("properties", {})
        country_paths.append(
            {
                "name": clean_text(properties.get("name", "")),
                "path": path_d,
            }
        )

    globe_rings: list[list[list[float]]] = []
    for geometry in geometries:
        for ring in geometry_to_rings(decoded_arcs, geometry):
            if len(ring) < 3:
                continue
            step = 1
            if len(ring) > 900:
                step = 4
            elif len(ring) > 500:
                step = 3
            elif len(ring) > 220:
                step = 2
            sampled = ring[::step]
            if sampled[-1] != ring[-1]:
                sampled.append(ring[-1])
            globe_rings.append([[round(lon, 4), round(lat, 4)] for lon, lat in sampled])

    country_labels: list[dict[str, Any]] = []
    country_points: list[dict[str, Any]] = []
    seen_country_points: set[tuple[str, float, float]] = set()
    for row in country_rows:
        name = clean_text(row.get("name", ""))
        lat = parse_float(row.get("lat", ""))
        lon = parse_float(row.get("lon", ""))
        if not name or lat is None or lon is None:
            continue
        x, y = project_coordinate(lon, lat)
        country_labels.append({"name": name, "x": x, "y": y})
        country_points.append(
            {
                "name": name,
                "name_zh": localize_country_name(name),
                "lat": lat,
                "lon": lon,
            }
        )
        seen_country_points.add((normalize_country_key(name), lat, lon))

    for alias, canonical in COUNTRY_CANONICAL_ALIASES.items():
        alias_key = normalize_country_key(alias)
        canonical_key = normalize_country_key(canonical)
        canonical_point = next(
            (point for point in country_points if normalize_country_key(point["name"]) == canonical_key),
            None,
        )
        if canonical_point is None:
            continue
        alias_row = {
            "name": alias,
            "name_zh": localize_country_name(alias),
            "lat": canonical_point["lat"],
            "lon": canonical_point["lon"],
        }
        alias_identity = (alias_key, alias_row["lat"], alias_row["lon"])
        if alias_identity in seen_country_points:
            continue
        seen_country_points.add(alias_identity)
        country_points.append(alias_row)

    return {
        "paths": country_paths,
        "labels": country_labels,
        "globe_rings": globe_rings,
        "country_points": country_points,
    }


def build_classic_preview_payload(
    links_rows: list[dict[str, str]],
    matrix_rows: list[dict[str, str]],
    country_rows: list[dict[str, str]],
    world_topology: dict[str, Any],
    *,
    focus_company: str,
    depth: int,
    limit: int,
) -> dict[str, Any]:
    links: list[dict[str, Any]] = []
    companies: set[str] = set()
    pair_source_stage_counts: dict[tuple[str, str], dict[str, int]] = {}
    supplier_stage_counts: dict[str, dict[str, int]] = {}

    for row in links_rows:
        supplier = clean_text(row.get("Supplier company", ""))
        buyer = clean_text(row.get("Buyer company", ""))
        if not supplier or not buyer:
            continue

        supplier_stage = normalize_step_name(
            row.get("Input chain step", "") or row.get("Output chain step", "")
        )
        buyer_stage = normalize_step_name(row.get("Output chain step", ""))
        links.append(
            {
                "id": clean_text(row.get("ID", "")),
                "supplier": supplier,
                "buyer": buyer,
                "supplier_stage": supplier_stage,
                "buyer_stage": buyer_stage,
                "link_label": clean_text(row.get("Link in the chain", "")),
                "supplier_country": clean_text(row.get("Country of Supplier", "")),
                "buyer_country": clean_text(row.get("Country of Buyer", "")),
                "supplier_lat": parse_float(row.get("Lat supplier", "")),
                "supplier_lon": parse_float(row.get("Long supplier", "")),
                "buyer_lat": parse_float(row.get("Lat buyer", "")),
                "buyer_lon": parse_float(row.get("Long buyer", "")),
            }
        )
        companies.add(supplier)
        companies.add(buyer)
        pair_stage_bucket = pair_source_stage_counts.setdefault((supplier, buyer), {})
        pair_stage_bucket[supplier_stage] = pair_stage_bucket.get(supplier_stage, 0) + 1
        supplier_stage_bucket = supplier_stage_counts.setdefault(supplier, {})
        supplier_stage_bucket[supplier_stage] = supplier_stage_bucket.get(supplier_stage, 0) + 1

    display_columns = STEP_ORDER[:]

    def pick_source_stage(source_name: str, first_targets: list[str]) -> str:
        pair_counts: dict[str, int] = {}
        for target_name in first_targets:
            for stage_name, count in pair_source_stage_counts.get((source_name, target_name), {}).items():
                pair_counts[stage_name] = pair_counts.get(stage_name, 0) + count
        if pair_counts:
            return max(
                pair_counts,
                key=lambda stage_name: (
                    pair_counts[stage_name],
                    -display_columns.index(stage_name) if stage_name in display_columns else -999,
                    stage_name,
                ),
            )

        supplier_counts = supplier_stage_counts.get(source_name, {})
        if supplier_counts:
            return max(
                supplier_counts,
                key=lambda stage_name: (
                    supplier_counts[stage_name],
                    -display_columns.index(stage_name) if stage_name in display_columns else -999,
                    stage_name,
                ),
            )
        return display_columns[0]

    matrix_companies: set[str] = set()
    encoded_matrix_rows: list[list[list[int]]] = []
    staged_rows: list[dict[str, list[str]]] = []

    for row in matrix_rows:
        staged_row = {column: [] for column in display_columns}
        first_downstream_targets: list[str] = []
        for column in list(row.keys())[1:]:
            names = split_matrix_cell(row.get(column, ""))
            normalized_column = normalize_step_name(column)
            if names and normalized_column in staged_row:
                staged_row[normalized_column].extend(names)
                if not first_downstream_targets:
                    first_downstream_targets = names

        for source_name in split_matrix_cell(row.get("source", "")):
            source_stage = pick_source_stage(source_name, first_downstream_targets)
            if source_stage in staged_row:
                staged_row[source_stage].append(source_name)

        for names in staged_row.values():
            for name in names:
                matrix_companies.add(name)
        staged_rows.append(staged_row)

    companies.update(matrix_companies)
    sorted_companies = sorted(companies)
    company_to_index = {name: index for index, name in enumerate(sorted_companies)}

    for link in links:
        link["supplier_id"] = company_to_index.get(link["supplier"])
        link["buyer_id"] = company_to_index.get(link["buyer"])

    for staged_row in staged_rows:
        encoded_row: list[list[int]] = []
        for column in display_columns:
            unique_names = list(dict.fromkeys(staged_row[column]))
            encoded_row.append([company_to_index[name] for name in unique_names if name in company_to_index])
        encoded_matrix_rows.append(encoded_row)

    default_focus = clean_text(focus_company)
    if default_focus not in companies and companies:
        default_focus = sorted_companies[0]

    return {
        "default_focus": default_focus,
        "default_depth": depth,
        "default_limit": limit,
        "companies": sorted_companies,
        "links": links,
        "matrix_columns": display_columns,
        "matrix_rows": encoded_matrix_rows,
        "world_map": build_world_map_payload(world_topology, country_rows),
        "step_order": display_columns,
        "step_labels_zh": {
            "Artisanal mining": "手工采矿",
            "Mining": "采矿",
            "Recycling": "回收",
            "Artisanal processing": "手工加工",
            "Smelting": "冶炼",
            "Trading": "贸易",
            "Refining": "精炼",
            "Precursor manufacturing": "前驱体制造",
            "Cathode manufacturing": "正极材料制造",
            "Battery cell manufacturing": "电芯制造",
            "Battery pack manufacturing": "电池包装配",
            "Electric car/scooter manufacturing": "电动汽车/两轮车制造",
        },
        "step_colors": {
            "Artisanal mining": "#3B82F6",
            "Mining": "#84CC16",
            "Recycling": "#10B981",
            "Artisanal processing": "#EAB308",
            "Smelting": "#F97316",
            "Trading": "#EF4444",
            "Refining": "#8B5CF6",
            "Precursor manufacturing": "#06B6D4",
            "Cathode manufacturing": "#0EA5A5",
            "Battery cell manufacturing": "#6366F1",
            "Battery pack manufacturing": "#EC4899",
            "Electric car/scooter manufacturing": "#92400E",
        },
    }


def build_classic_preview_html(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>钴供应链三维图谱</title>
  <link rel="preconnect" href="https://cdn.jsdelivr.net">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link rel="preload" href="assets/earth_satellite_5400.jpg" as="image" fetchpriority="high">
  <script src="assets/globe.gl.min.js"></script>
  <style>
    @import url("https://fonts.googleapis.com/css2?family=Noto+Sans:wght@400;700&family=Noto+Sans+SC:wght@400;500;700&family=Roboto:wght@400;500;700&display=swap");
    :root {
      --ink: #355464;
      --olive: #8f9d35;
      --orange: #d77d31;
      --paper: #ffffff;
      --muted: #6e7f88;
      --surface: rgba(255,255,255,0.98);
    }
    * { box-sizing: border-box; }
    html, body {
      margin: 0;
      padding: 0;
      min-height: 100%;
      color: var(--ink);
      font-family: "Noto Sans", sans-serif;
      font-size: 0.92rem;
      letter-spacing: 0.03em;
      background:
        radial-gradient(circle at top left, rgba(143,157,53,0.16), transparent 28%),
        linear-gradient(180deg, #f7faf7 0%, #edf2ef 100%);
    }
    .page {
      min-height: 100vh;
      padding: 18px;
    }
    .shell {
      background: var(--surface);
      border-radius: 12px;
      box-shadow: 0 0 1px var(--olive), 0 20px 60px rgba(53, 84, 100, 0.08);
      min-height: calc(100vh - 36px);
      display: grid;
      grid-template-columns: auto auto;
      grid-template-rows: 40px minmax(330px, 0.95fr) minmax(360px, 1.08fr);
      grid-template-areas:
        "title filters"
        "chains chains"
        "map map";
      gap: 10px;
      padding: 20px;
    }
    .title-bar { grid-area: title; display: flex; align-items: flex-end; gap: 14px; }
    .filters { grid-area: filters; display: flex; align-items: center; justify-content: flex-end; gap: 16px; flex-wrap: wrap; }
    .panel {
      background: var(--paper);
      border-radius: 10px;
      box-shadow: 0 0 1px var(--olive);
      overflow: hidden;
      display: flex;
      flex-direction: column;
      min-width: 0;
    }
    .chains-panel { grid-area: chains; }
    .map-panel { grid-area: map; }
    .brand {
      font-family: "Roboto", sans-serif;
      font-size: 1.55rem;
      line-height: 1;
      text-transform: uppercase;
      letter-spacing: 1px;
      white-space: nowrap;
    }
    .beta {
      font-size: 0.76rem;
      color: var(--olive);
      font-weight: 700;
      position: relative;
      top: -5px;
    }
    .toolbar-link, .summary-note {
      font-size: 0.72rem;
      color: var(--muted);
    }
    .toolbar-link { cursor: pointer; text-decoration: none; }
    .toolbar-link:hover { color: var(--orange); }
    .search-wrap input, .density-wrap select {
      border: 1px solid rgba(53,84,100,0.16);
      border-radius: 18px;
      padding: 9px 14px;
      font: inherit;
      color: inherit;
      background: #fff;
      min-width: 260px;
    }
    .density-wrap select { min-width: 124px; }
    .action-wrap {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .action-wrap button {
      border: 0;
      border-radius: 999px;
      padding: 8px 14px;
      font: inherit;
      cursor: pointer;
      color: #fff;
      background: var(--ink);
      transition: opacity 0.15s ease, transform 0.15s ease;
    }
    .action-wrap button:hover {
      opacity: 0.92;
      transform: translateY(-1px);
    }
    .action-wrap button.ghost {
      color: var(--ink);
      background: rgba(53,84,100,0.08);
    }
    .search-status {
      min-width: 220px;
      text-align: right;
      color: var(--muted);
      font-size: 0.72rem;
    }
    .switch {
      position: relative;
      width: 82px;
      height: 22px;
      display: inline-flex;
      align-items: center;
      cursor: pointer;
    }
    .switch input { display: none; }
    .switch-track {
      width: 82px;
      height: 22px;
      border-radius: 999px;
      background: var(--olive);
      box-shadow: 0 1px 5px rgba(53, 84, 100, 0.22);
      position: relative;
      color: #fff;
      font-size: 0.68rem;
      line-height: 22px;
      text-align: center;
      user-select: none;
    }
    .switch-track::before {
      content: "";
      position: absolute;
      top: 1px;
      left: 1px;
      width: 20px;
      height: 20px;
      border-radius: 999px;
      background: #fff;
      transition: transform 0.18s ease;
      box-shadow: 0 1px 4px rgba(53,84,100,0.18);
    }
    .switch input:not(:checked) + .switch-track::before { transform: translateX(60px); }
    .switch-track .simple { display: inline; }
    .switch-track .detailed { display: none; }
    .switch input:not(:checked) + .switch-track .simple { display: none; }
    .switch input:not(:checked) + .switch-track .detailed { display: inline; }
    .panel-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
      padding: 14px 16px 10px;
      border-bottom: 1px solid rgba(53,84,100,0.08);
    }
    .panel-title {
      font-family: "Roboto", sans-serif;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 0.82rem;
      font-weight: 700;
    }
    .panel-body {
      position: relative;
      flex: 1;
      min-height: 0;
      background:
        linear-gradient(180deg, rgba(143,157,53,0.04), transparent 26%),
        #fff;
    }
    .chains-scroll {
      width: 100%;
      height: 100%;
      overflow: auto;
    }
    #chainsSvg, #mapSvg {
      display: block;
      width: 100%;
      height: 100%;
    }
    .stats {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      padding: 0 16px 12px;
      color: var(--muted);
      font-size: 0.75rem;
    }
    .stat-pill {
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(53,84,100,0.06);
      border: 1px solid rgba(53,84,100,0.08);
    }
    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 8px 14px;
      padding: 0 16px 12px;
      color: var(--muted);
      font-size: 0.72rem;
    }
    .legend-item { display: inline-flex; align-items: center; gap: 7px; }
    .legend-dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      display: inline-block;
    }
    .view-tabs {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .view-tab {
      border: 1px solid rgba(53,84,100,0.12);
      border-radius: 999px;
      padding: 6px 12px;
      background: rgba(53,84,100,0.05);
      color: var(--muted);
      font: inherit;
      cursor: pointer;
      transition: background 0.16s ease, color 0.16s ease, transform 0.16s ease;
    }
    .view-tab.is-active {
      background: rgba(53,84,100,0.95);
      color: #fff;
      transform: translateY(-1px);
    }
    .geo-view {
      position: absolute;
      inset: 0;
    }
    .geo-view[hidden] {
      display: none;
    }
    .globe-wrap {
      position: absolute;
      inset: 0;
      overflow: hidden;
      background:
        radial-gradient(circle at 50% 30%, rgba(94, 154, 212, 0.20), transparent 34%),
        radial-gradient(circle at 18% 20%, rgba(117, 163, 210, 0.10), transparent 28%),
        radial-gradient(circle at 82% 22%, rgba(117, 178, 226, 0.10), transparent 26%),
        linear-gradient(180deg, #eaf2f7 0%, #dce8f1 44%, #d4e0ea 100%);
    }
    .google-globe-host {
      position: absolute;
      inset: 0;
      display: none;
    }
    .google-globe-host.is-active {
      display: block;
    }
    .google-globe-host gmp-map-3d,
    .google-globe-host > div {
      width: 100%;
      height: 100%;
      display: block;
    }
    .google-globe-host .cesium-viewer,
    .google-globe-host .cesium-widget,
    .google-globe-host canvas {
      width: 100%;
      height: 100%;
      display: block;
    }
    .globe-label-layer {
      position: absolute;
      inset: 0;
      z-index: 2;
      pointer-events: none;
      overflow: hidden;
    }
    .globe-label-layer .globe-country-label {
      position: absolute;
      transform: translate(-50%, -50%);
    }
    .globe-setup {
      display: none;
      position: absolute;
      z-index: 4;
      top: 14px;
      right: 14px;
      width: 320px;
      padding: 14px 14px 12px;
      border-radius: 16px;
      background: rgba(8, 21, 34, 0.82);
      border: 1px solid rgba(123, 191, 255, 0.18);
      color: rgba(232, 245, 255, 0.94);
      box-shadow: 0 22px 60px rgba(2, 8, 18, 0.32);
      backdrop-filter: blur(12px);
    }
    .globe-setup[hidden] {
      display: none;
    }
    .globe-setup h3 {
      margin: 0 0 6px;
      font: 700 0.92rem/1.2 "Roboto", sans-serif;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    .globe-setup p {
      margin: 0 0 10px;
      font-size: 0.74rem;
      line-height: 1.45;
      color: rgba(223, 239, 252, 0.76);
    }
    .globe-setup input {
      width: 100%;
      border: 1px solid rgba(145, 203, 255, 0.24);
      border-radius: 12px;
      padding: 10px 12px;
      background: rgba(255,255,255,0.08);
      color: #fff;
      font: inherit;
      margin-bottom: 10px;
    }
    .globe-setup input::placeholder {
      color: rgba(232,245,255,0.48);
    }
    .globe-setup-actions {
      display: flex;
      gap: 8px;
      align-items: center;
    }
    .globe-setup-actions button {
      border: 0;
      border-radius: 999px;
      padding: 8px 12px;
      background: #56b8ff;
      color: #04101c;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }
    .globe-setup-actions button.ghost {
      background: rgba(255,255,255,0.10);
      color: rgba(235,246,255,0.92);
    }
    .globe-setup-status {
      margin-top: 8px;
      min-height: 1.2em;
      font-size: 0.72rem;
      color: rgba(223, 239, 252, 0.72);
    }
    .globe-stage {
      width: 100%;
      height: 100%;
    }
    .globe-canvas {
      width: 100%;
      height: 100%;
      display: block;
      cursor: grab;
    }
    .globe-canvas.is-dragging {
      cursor: grabbing;
    }
    .globe-note {
      position: absolute;
      left: 16px;
      bottom: 14px;
      z-index: 2;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(10, 27, 44, 0.72);
      color: rgba(219, 236, 251, 0.88);
      font-size: 0.7rem;
      border: 1px solid rgba(130, 183, 236, 0.16);
      box-shadow: 0 16px 40px rgba(2, 8, 18, 0.28);
      pointer-events: none;
      backdrop-filter: blur(8px);
    }
    .globe-tooltip {
      position: absolute;
      z-index: 3;
      max-width: 360px;
      min-width: 180px;
      padding: 9px 12px;
      border-radius: 10px;
      background: rgba(27, 42, 54, 0.92);
      color: #fff;
      font-size: 0.74rem;
      line-height: 1.35;
      box-shadow: 0 10px 26px rgba(24, 33, 41, 0.22);
      pointer-events: none;
      transform: translate(12px, -12px);
      white-space: normal;
      word-break: break-word;
    }
    .globe-tooltip[hidden] {
      display: none;
    }
    .globe-tooltip strong {
      display: block;
      font-size: 0.8rem;
      margin-bottom: 2px;
    }
    .globe-tooltip .meta {
      color: rgba(255,255,255,0.76);
    }
    .globe-tooltip .links {
      margin-top: 4px;
      color: rgba(255,255,255,0.92);
    }
    .globe-country-label {
      color: #eef7ff;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.04em;
      white-space: nowrap;
      text-shadow:
        0 1px 1px rgba(0, 0, 0, 0.95),
        0 0 10px rgba(43, 118, 180, 0.45);
      pointer-events: none;
      user-select: none;
      transform: translate(-50%, -50%);
      font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
    }
    .globe-country-label[hidden] {
      display: none;
    }
    .empty {
      padding: 36px 20px;
      color: var(--muted);
      text-align: center;
    }
    @media (max-width: 860px) {
      .shell {
        grid-template-columns: auto;
        grid-template-rows: 28px auto minmax(300px, 0.95fr) minmax(320px, 1fr);
        grid-template-areas:
          "title"
          "filters"
          "chains"
          "map";
      }
      .filters { justify-content: flex-start; }
      .search-wrap input { min-width: 220px; width: 100%; }
    }
  </style>
</head>
<body>
  <div class="page">
    <div class="shell">
      <div class="title-bar">
        <div class="brand">钴供应链</div>
        <div class="beta">预览版</div>
      </div>
      <div class="filters">
        <label class="switch" title="切换简洁 / 详细模式">
          <input id="modeToggle" type="checkbox">
          <span class="switch-track"><span class="simple">简洁</span><span class="detailed">详细</span></span>
        </label>
        <div class="search-wrap">
          <input id="companyInput" list="companyList" placeholder="搜索企业或矿点">
          <datalist id="companyList"></datalist>
        </div>
        <div class="action-wrap">
          <button id="searchButton" type="button">聚焦</button>
          <button id="resetButton" type="button" class="ghost">重置</button>
        </div>
        <div id="searchStatus" class="search-status">显示全局网络视图</div>
      </div>

      <section class="panel chains-panel">
        <div class="panel-head">
          <div class="panel-title">产业链关系</div>
          <div class="view-tabs">
            <button id="softHighlightButton" class="view-tab" type="button">柔和高亮</button>
          </div>
        </div>
        <div class="stats" id="chainStats"></div>
        <div class="legend" id="stepLegend"></div>
        <div class="panel-body">
          <div class="chains-scroll">
            <svg id="chainsSvg"></svg>
          </div>
          <div id="chainsEmpty" class="empty" hidden>当前没有与该焦点企业匹配的产业链路径。</div>
        </div>
      </section>

      <section class="panel map-panel">
        <div class="panel-head">
          <div class="panel-title">地图 / 三维地球</div>
          <div class="view-tabs">
            <button id="mapViewButton" class="view-tab" type="button">二维地图</button>
            <button id="globeViewButton" class="view-tab is-active" type="button">三维地球</button>
          </div>
        </div>
        <div class="stats" id="mapStats"></div>
        <div class="legend" id="mapLegend"></div>
        <div class="panel-body">
          <div id="mapView" class="geo-view" hidden>
            <svg id="mapSvg" viewBox="0 0 1400 520" preserveAspectRatio="xMidYMid meet"></svg>
          </div>
          <div id="globeView" class="geo-view">
            <div class="globe-wrap">
              <canvas id="globeCanvas" class="globe-canvas"></canvas>
              <div id="googleGlobeHost" class="google-globe-host"></div>
              <div id="globeLabelLayer" class="globe-label-layer"></div>
              <div id="globeSetup" class="globe-setup">
                <h3>Google 3D 地图</h3>
                <p>输入 Google Maps API key 后，下方将切换到官方 3D 地图；未配置时，仍保留当前本地 3D 地球作为兜底预览。</p>
                <input id="googleApiKeyInput" type="password" placeholder="输入 Google Maps API key" autocomplete="off" />
                <div class="globe-setup-actions">
                  <button id="googleApiKeyLoad" type="button">加载官方 3D</button>
                  <button id="googleApiKeyClear" type="button" class="ghost">清除 Key</button>
                </div>
                <div id="googleApiKeyStatus" class="globe-setup-status">尚未启用 Google 3D Maps API，当前显示本地 3D 地球。</div>
              </div>
              <div class="globe-note">拖动旋转，滚轮缩放</div>
              <div id="globeTooltip" class="globe-tooltip" hidden></div>
            </div>
          </div>
          <div id="mapEmpty" class="empty" hidden>当前焦点没有可用的地理点位。</div>
        </div>
      </section>
    </div>
  </div>

  <script>
    const payload = __PAYLOAD__;
    window.payload = payload;
    const companies = payload.companies;
    const links = payload.links;
    const stepOrder = payload.step_order;
    const stepColors = payload.step_colors;
    const stepLabelsZh = payload.step_labels_zh || {};
    const worldMap = payload.world_map || { paths: [], labels: [] };
    const worldCountryPoints = worldMap.country_points || [];
    const companyList = document.getElementById("companyList");
    const companyInput = document.getElementById("companyInput");
    const searchButton = document.getElementById("searchButton");
    const resetButton = document.getElementById("resetButton");
    const searchStatus = document.getElementById("searchStatus");
    const modeToggle = document.getElementById("modeToggle");
    const chainsSvg = document.getElementById("chainsSvg");
    const mapSvg = document.getElementById("mapSvg");
    const mapView = document.getElementById("mapView");
    const globeView = document.getElementById("globeView");
    const mapViewButton = document.getElementById("mapViewButton");
    const globeViewButton = document.getElementById("globeViewButton");
    const softHighlightButton = document.getElementById("softHighlightButton");
    const googleGlobeHost = document.getElementById("googleGlobeHost");
    const googleApiKeyInput = document.getElementById("googleApiKeyInput");
    const googleApiKeyLoad = document.getElementById("googleApiKeyLoad");
    const googleApiKeyClear = document.getElementById("googleApiKeyClear");
    const googleApiKeyStatus = document.getElementById("googleApiKeyStatus");
    const globeSetup = document.getElementById("globeSetup");
    const chainsEmpty = document.getElementById("chainsEmpty");
    const mapEmpty = document.getElementById("mapEmpty");
    const chainStats = document.getElementById("chainStats");
    const mapStats = document.getElementById("mapStats");
    const stepLegend = document.getElementById("stepLegend");
    const mapLegend = document.getElementById("mapLegend");
    window.__previewBridge = window.__previewBridge || {};
"""
    html += """
    companyInput.value = "";
    const matrixColumns = payload.matrix_columns || stepOrder;
    const matrixRows = payload.matrix_rows || [];
    let mapFocusOverride = null;
    let activeGeoView = "globe";
    let softHighlightMode = false;
    let googleGlobePreferred = false;
    let renderFrameHandle = 0;
    const companyIndexByNormalizedName = new Map();
    const countryPointLookup = new Map(
      worldCountryPoints.map((item) => [normalize(item.name), item])
    );
    companies.forEach((name, index) => {
      companyIndexByNormalizedName.set(normalize(name), index);
    });

    const companyRowIndex = new Map();
    const focusedSelectionCache = new Map();
    const chainViewCache = new WeakMap();
    const allChainNodesByStage = new Map(matrixColumns.map((column) => [column, new Map()]));
    matrixRows.forEach((row, rowIndex) => {
      const rowSeenCompanies = new Set();
      row.forEach((cell, columnIndex) => {
        const column = matrixColumns[columnIndex];
        const nodeMap = allChainNodesByStage.get(column);
        cell.forEach((companyId) => {
          const key = `${column}||${companyId}`;
          if (!nodeMap.has(key)) {
            nodeMap.set(key, {
              key,
              stage: column,
              companyId,
              company: companies[companyId],
              count: 0,
            });
          }
          nodeMap.get(key).count += 1;
          rowSeenCompanies.add(companyId);
        });
      });
      rowSeenCompanies.forEach((companyId) => {
        if (!companyRowIndex.has(companyId)) {
          companyRowIndex.set(companyId, []);
        }
        companyRowIndex.get(companyId).push(rowIndex);
      });
    });

    function colorWithAlpha(color, alpha) {
      if (!color || !color.startsWith("#")) return color;
      const normalized = color.slice(1);
      const expanded = normalized.length === 3
        ? normalized.split("").map((char) => `${char}${char}`).join("")
        : normalized;
      const red = parseInt(expanded.slice(0, 2), 16);
      const green = parseInt(expanded.slice(2, 4), 16);
      const blue = parseInt(expanded.slice(4, 6), 16);
      return `rgba(${red}, ${green}, ${blue}, ${alpha})`;
    }

    function localizeStep(step) {
      return stepLabelsZh[step] || step;
    }

    function localizeCountry(country) {
      return country || "";
    }

    function polarPoint(cx, cy, radius, angle) {
      return {
        x: cx + Math.cos(angle) * radius,
        y: cy + Math.sin(angle) * radius,
      };
    }

    function pieSlicePath(cx, cy, radius, startAngle, endAngle) {
      const start = polarPoint(cx, cy, radius, startAngle);
      const end = polarPoint(cx, cy, radius, endAngle);
      const largeArc = endAngle - startAngle > Math.PI ? 1 : 0;
      return [
        `M ${cx} ${cy}`,
        `L ${start.x} ${start.y}`,
        `A ${radius} ${radius} 0 ${largeArc} 1 ${end.x} ${end.y}`,
        "Z",
      ].join(" ");
    }

    function intersectsBox(a, b) {
      return !(a.x + a.width < b.x || b.x + b.width < a.x || a.y + a.height < b.y || b.y + b.height < a.y);
    }

    function createTextBox(x, y, text, fontSize, anchor = "start") {
      const width = Math.max(10, text.length * fontSize * 0.58);
      const height = fontSize * 1.25;
      let boxX = x;
      if (anchor === "middle") {
        boxX = x - width / 2;
      } else if (anchor === "end") {
        boxX = x - width;
      }
      return {
        x: boxX,
        y: y - height * 0.82,
        width,
        height,
      };
    }

    function canPlaceLabel(box, boxes, padding = 3) {
      const padded = {
        x: box.x - padding,
        y: box.y - padding,
        width: box.width + padding * 2,
        height: box.height + padding * 2,
      };
      return !boxes.some((existing) => intersectsBox(padded, existing));
    }

    function syncGeoView() {
      const showMap = activeGeoView === "map";
      mapView.hidden = !showMap;
      globeView.hidden = showMap;
      mapViewButton.classList.toggle("is-active", showMap);
      globeViewButton.classList.toggle("is-active", !showMap);
      softHighlightButton.classList.toggle("is-active", softHighlightMode);
      if (window.__previewBridge && typeof window.__previewBridge.resizeGlobeScene === "function") {
        window.requestAnimationFrame(() => window.__previewBridge.resizeGlobeScene());
      }
      if (window.__previewBridge && typeof window.__previewBridge.resizeWebGlobeScene === "function") {
        window.requestAnimationFrame(() => window.__previewBridge.resizeWebGlobeScene());
      }
      if (window.__previewBridge && typeof window.__previewBridge.resizeGoogleGlobeScene === "function") {
        window.requestAnimationFrame(() => window.__previewBridge.resizeGoogleGlobeScene());
      }
      if (showMap) {
        render();
      }
    }

    function escapeHtml(text) {
      return String(text)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }

    function normalize(text) {
      return String(text || "").trim().toLowerCase();
    }

    function findFocusCompany(term, fallbackToDefault = true) {
      const query = normalize(term);
      if (!query) return fallbackToDefault ? (payload.default_focus || companies[0] || "") : "";
      const exact = companies.find((name) => normalize(name) === query);
      if (exact) return exact;
      const partial = companies.find((name) => normalize(name).includes(query));
      return partial || query;
    }

    function updateCompanySuggestions(term) {
      const query = normalize(term);
      const matches = !query
        ? companies.slice(0, 80)
        : companies.filter((name) => normalize(name).includes(query)).slice(0, 80);
      companyList.innerHTML = "";
      matches.forEach((name) => {
        const option = document.createElement("option");
        option.value = name;
        companyList.appendChild(option);
      });
    }

    function buildFocusedSelection(focusTerm, fallbackToDefault = true) {
      const matchedFocus = findFocusCompany(focusTerm, fallbackToDefault);
      const focusId = companyIndexByNormalizedName.get(normalize(matchedFocus));
      const cacheKey = focusId === undefined ? `missing::${normalize(matchedFocus)}` : `focus::${focusId}`;
      if (focusedSelectionCache.has(cacheKey)) {
        return focusedSelectionCache.get(cacheKey);
      }
      if (focusId === undefined) {
        const emptySelection = {
          focus: matchedFocus,
          matchedFocus: "",
          focusId: null,
          companies: new Set(),
          links: [],
          activeNodeCounts: new Map(),
          activeEdgeCounts: new Map(),
          rowsMatched: 0,
        };
        focusedSelectionCache.set(cacheKey, emptySelection);
        return emptySelection;
      }

      const rowIndices = companyRowIndex.get(focusId) || [];
      const activeCompanies = new Set();
      const activeNodeCounts = new Map();
      const activeEdgeCounts = new Map();

      rowIndices.forEach((rowIndex) => {
        const row = matrixRows[rowIndex];
        const sequence = [];
        row.forEach((cell, columnIndex) => {
          if (!cell.length) return;
          const column = matrixColumns[columnIndex];
          sequence.push([column, cell]);
          cell.forEach((companyId) => {
            const companyName = companies[companyId];
            const nodeKey = `${column}||${companyId}`;
            activeCompanies.add(companyName);
            activeNodeCounts.set(nodeKey, (activeNodeCounts.get(nodeKey) || 0) + 1);
          });
        });

        for (let index = 0; index < sequence.length - 1; index += 1) {
          const [leftColumn, leftIds] = sequence[index];
          const [rightColumn, rightIds] = sequence[index + 1];
          leftIds.forEach((leftId) => {
            rightIds.forEach((rightId) => {
              const edgeKey = `${leftColumn}||${leftId}=>${rightColumn}||${rightId}`;
              activeEdgeCounts.set(edgeKey, (activeEdgeCounts.get(edgeKey) || 0) + 1);
            });
          });
        }
      });

      const relevantLinks = links.filter((link) => activeCompanies.has(link.supplier) && activeCompanies.has(link.buyer));
      const selection = {
        focus: matchedFocus,
        matchedFocus,
        focusId,
        companies: activeCompanies,
        links: relevantLinks,
        activeNodeCounts,
        activeEdgeCounts,
        rowsMatched: rowIndices.length,
      };
      focusedSelectionCache.set(cacheKey, selection);
      return selection;
    }

    function buildChainView(selection) {
      if (chainViewCache.has(selection)) {
        return chainViewCache.get(selection);
      }
      const nodesByStage = new Map();
      let nodeCount = 0;
      matrixColumns.forEach((column) => {
        const items = Array.from((allChainNodesByStage.get(column) || new Map()).values())
          .map((node) => ({
            ...node,
            activeCount: selection.activeNodeCounts.get(node.key) || 0,
            isActive: selection.activeNodeCounts.has(node.key),
            isFocus: selection.focusId === node.companyId,
          }))
          .sort((left, right) => {
            if (left.count !== right.count) return right.count - left.count;
            return left.company.localeCompare(right.company);
          });
        nodeCount += items.length;
        nodesByStage.set(column, items);
      });

      const edges = Array.from(selection.activeEdgeCounts.entries()).map(([edgeKey, count]) => {
        const [sourceKey, targetKey] = edgeKey.split("=>");
        return {
          key: edgeKey,
          source: sourceKey,
          target: targetKey,
          count,
          isActive: true,
        };
      }).sort((left, right) => right.count - left.count);

      const chainView = { nodesByStage, edges, nodeCount };
      chainViewCache.set(selection, chainView);
      return chainView;
    }
"""
    html += """
    function renderChainPanel(subgraph, simpleMode) {
      const { focus } = subgraph;
      const chainView = buildChainView(subgraph);
      const stageEntries = matrixColumns
        .map((step) => [step, chainView.nodesByStage.get(step) || []])
        .filter((entry) => entry[1].length > 0);

      stepLegend.innerHTML = stageEntries
        .map(([step]) => `<span class="legend-item"><i class="legend-dot" style="background:${stepColors[step] || "#355464"}"></i>${escapeHtml(localizeStep(step))}</span>`)
        .join("");
      chainStats.innerHTML = [
        `焦点: ${focus || "无"}`,
        `命中路径: ${subgraph.rowsMatched}`,
        `相关企业: ${subgraph.companies.size}`,
        `可见节点: ${chainView.nodeCount}`,
        `模式: ${simpleMode ? "简洁" : "详细"}`,
      ].map((item) => `<span class="stat-pill">${escapeHtml(item)}</span>`).join("");

      if (!stageEntries.length || !subgraph.rowsMatched) {
        chainsSvg.innerHTML = "";
        chainsEmpty.hidden = false;
        return;
      }
      chainsEmpty.hidden = true;

      const columnWidth = 210;
      const nodeWidth = 168;
      const nodeHeight = simpleMode ? 28 : 40;
      const topPadding = 46;
      const leftPadding = 42;
      const rowGap = simpleMode ? 12 : 16;
      const stageGap = 42;
      const maxRows = Math.max(...stageEntries.map((entry) => entry[1].length));
      const width = Math.max(960, leftPadding * 2 + stageEntries.length * columnWidth + (stageEntries.length - 1) * stageGap);
      const height = Math.max(320, topPadding + maxRows * (nodeHeight + rowGap) + 72);
      chainsSvg.setAttribute("viewBox", `0 0 ${width} ${height}`);
      chainsSvg.innerHTML = "";

      const positions = new Map();
      stageEntries.forEach(([step, nodes], stageIndex) => {
        const x = leftPadding + stageIndex * (columnWidth + stageGap);
        const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
        label.setAttribute("x", String(x + nodeWidth / 2));
        label.setAttribute("y", "24");
        label.setAttribute("text-anchor", "middle");
        label.setAttribute("font-size", "11");
        label.setAttribute("font-weight", "700");
        label.setAttribute("fill", "#355464");
        label.textContent = localizeStep(step);
        chainsSvg.appendChild(label);

        nodes.forEach((node, rowIndex) => {
          const y = topPadding + rowIndex * (nodeHeight + rowGap);
          positions.set(node.key, { x, y, node });
        });
      });

      chainView.edges.forEach((edge) => {
        const source = positions.get(edge.source);
        const target = positions.get(edge.target);
        if (!source || !target) return;
        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        const x1 = source.x + nodeWidth;
        const y1 = source.y + nodeHeight / 2;
        const x2 = target.x;
        const y2 = target.y + nodeHeight / 2;
        const bend = Math.max(40, (x2 - x1) * 0.5);
        path.setAttribute("d", `M ${x1} ${y1} C ${x1 + bend} ${y1}, ${x2 - bend} ${y2}, ${x2} ${y2}`);
        path.setAttribute("fill", "none");
        const baseColor = stepColors[source.node.stage] || "#355464";
        const strokeColor = colorWithAlpha(baseColor, source.node.isFocus || target.node.isFocus ? 0.46 : 0.28);
        path.setAttribute("stroke", simpleMode ? colorWithAlpha(baseColor, source.node.isFocus || target.node.isFocus ? 0.26 : 0.18) : strokeColor);
        path.setAttribute("stroke-width", String(Math.min(5, 1 + Math.log2(edge.count + 1))));
        path.setAttribute("stroke-linecap", "round");
        const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
        title.textContent = `${source.node.company} -> ${target.node.company} (${edge.count}) | ${localizeStep(source.node.stage)}`;
        path.appendChild(title);
        chainsSvg.appendChild(path);
      });

      positions.forEach(({ x, y, node }) => {
        const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
        group.style.cursor = "pointer";
        const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
        rect.setAttribute("x", String(x));
        rect.setAttribute("y", String(y));
        rect.setAttribute("rx", "10");
        rect.setAttribute("ry", "10");
        rect.setAttribute("width", String(nodeWidth));
        rect.setAttribute("height", String(nodeHeight));
        const stageColor = stepColors[node.stage] || "#6e7f88";
        const fill = softHighlightMode
          ? (node.isActive ? stageColor : colorWithAlpha(stageColor, 0.34))
          : (node.isActive ? stageColor : "#cfd5da");
        rect.setAttribute("fill", fill);
        rect.setAttribute("opacity", softHighlightMode ? "1" : (node.isFocus ? "1" : (node.isActive ? "0.96" : "0.9")));
        rect.setAttribute("stroke", node.isFocus ? "#355464" : (node.isActive ? "rgba(255,255,255,0.8)" : (softHighlightMode ? colorWithAlpha(stageColor, 0.48) : "rgba(255,255,255,0.55)")));
        rect.setAttribute("stroke-width", node.isFocus ? "2.4" : (softHighlightMode && node.isActive ? "1.4" : "1"));
        group.appendChild(rect);

        const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
        label.setAttribute("x", String(x + 10));
        label.setAttribute("y", String(y + (simpleMode ? 18 : 16)));
        label.setAttribute("font-size", simpleMode ? "11" : "10.5");
        label.setAttribute("font-weight", node.isFocus ? "700" : (node.isActive ? "500" : "400"));
        label.setAttribute("fill", softHighlightMode && !node.isActive ? "rgba(255,255,255,0.94)" : "#fff");
        label.textContent = node.company.length > 26 ? `${node.company.slice(0, 24)}...` : node.company;
        group.appendChild(label);

        if (!simpleMode) {
          const meta = document.createElementNS("http://www.w3.org/2000/svg", "text");
          meta.setAttribute("x", String(x + 10));
          meta.setAttribute("y", String(y + 31));
          meta.setAttribute("font-size", "9");
          meta.setAttribute("fill", softHighlightMode && !node.isActive ? "rgba(255,255,255,0.76)" : "rgba(255,255,255,0.82)");
          meta.textContent = node.isActive ? `highlighted ${node.activeCount}` : `background ${node.count}`;
          group.appendChild(meta);
        }

        const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
        title.textContent = `${node.company} | ${localizeStep(node.stage)} | 命中路径 ${node.activeCount || 0} | 全部路径 ${node.count}`;
        group.appendChild(title);
        group.addEventListener("click", () => setPrimaryFocus(node.company));
        chainsSvg.appendChild(group);
      });
    }

    function project(lon, lat, width, height) {
      const minLon = -180;
      const maxLon = 180;
      const minLat = -60;
      const maxLat = 85;
      return {
        x: ((lon - minLon) / (maxLon - minLon)) * width,
        y: ((maxLat - lat) / (maxLat - minLat)) * height,
      };
    }
"""
    html += """
    function renderMapPanel(subgraph, mapSubgraph, simpleMode) {
      const width = 1400;
      const height = 520;
      const margin = 24;
      const innerWidth = width - margin * 2;
      const innerHeight = height - margin * 2;
      const shouldDrawMapSvg = activeGeoView === "map";
      if (shouldDrawMapSvg) {
        mapSvg.setAttribute("viewBox", `0 0 ${width} ${height}`);
        mapSvg.innerHTML = "";
      }
      const hasMapFocus = Boolean(mapSubgraph && mapSubgraph.rowsMatched);
      const reservedLabelBoxes = [];

      const lineMap = new Map();
      const pointMap = new Map();
      const activeCountries = new Set();

      function ensurePoint(key, details) {
        if (!pointMap.has(key)) {
          pointMap.set(key, {
            key,
            label: details.label,
            country: details.country,
            lat: details.lat,
            lon: details.lon,
            x: details.x,
            y: details.y,
            count: 0,
            activeCount: 0,
            isFocus: false,
            isActive: false,
            stageCounts: new Map(),
            activeStageCounts: new Map(),
          });
        }
        return pointMap.get(key);
      }

      function notePointStage(point, stageName, isActive) {
        const stage = stageName || "";
        point.stageCounts.set(stage, (point.stageCounts.get(stage) || 0) + 1);
        if (isActive) {
          point.activeStageCounts.set(stage, (point.activeStageCounts.get(stage) || 0) + 1);
        }
      }

      links.forEach((link) => {
        if (
          typeof link.supplier_lat !== "number" || typeof link.supplier_lon !== "number" ||
          typeof link.buyer_lat !== "number" || typeof link.buyer_lon !== "number" ||
          link.supplier_id === undefined || link.buyer_id === undefined
        ) {
          return;
        }

        const supplierStage = link.supplier_stage || "";
        const buyerStage = link.buyer_stage || "";
        const supplierNodeKey = `${supplierStage}||${link.supplier_id}`;
        const buyerNodeKey = `${buyerStage}||${link.buyer_id}`;
        const edgeKey = `${supplierNodeKey}=>${buyerNodeKey}`;
        const supplierActive = hasMapFocus && mapSubgraph.activeNodeCounts.has(supplierNodeKey);
        const buyerActive = hasMapFocus && mapSubgraph.activeNodeCounts.has(buyerNodeKey);
        const edgeActive = hasMapFocus && mapSubgraph.activeEdgeCounts.has(edgeKey);

        const supplierPoint = project(link.supplier_lon, link.supplier_lat, innerWidth, innerHeight);
        const buyerPoint = project(link.buyer_lon, link.buyer_lat, innerWidth, innerHeight);
        supplierPoint.x += margin;
        supplierPoint.y += margin;
        buyerPoint.x += margin;
        buyerPoint.y += margin;

        const supplierKey = `${link.supplier}||${link.supplier_country}||${link.supplier_lat}||${link.supplier_lon}`;
        const buyerKey = `${link.buyer}||${link.buyer_country}||${link.buyer_lat}||${link.buyer_lon}`;
        const supplierPointNode = ensurePoint(supplierKey, {
          label: link.supplier,
          country: link.supplier_country,
          lat: link.supplier_lat,
          lon: link.supplier_lon,
          x: supplierPoint.x,
          y: supplierPoint.y,
        });
        const buyerPointNode = ensurePoint(buyerKey, {
          label: link.buyer,
          country: link.buyer_country,
          lat: link.buyer_lat,
          lon: link.buyer_lon,
          x: buyerPoint.x,
          y: buyerPoint.y,
        });

        supplierPointNode.count += 1;
        buyerPointNode.count += 1;
        notePointStage(supplierPointNode, supplierStage, supplierActive);
        notePointStage(buyerPointNode, buyerStage, buyerActive);
        if (supplierActive) {
          supplierPointNode.activeCount += 1;
          supplierPointNode.isActive = true;
          supplierPointNode.isFocus = supplierPointNode.isFocus || (link.supplier === mapSubgraph.focus);
          if (link.supplier_country) activeCountries.add(normalize(link.supplier_country));
        }
        if (buyerActive) {
          buyerPointNode.activeCount += 1;
          buyerPointNode.isActive = true;
          buyerPointNode.isFocus = buyerPointNode.isFocus || (link.buyer === mapSubgraph.focus);
          if (link.buyer_country) activeCountries.add(normalize(link.buyer_country));
        }

        const lineKey = `${supplierKey}=>${buyerKey}||${supplierStage}`;
        if (!lineMap.has(lineKey)) {
          lineMap.set(lineKey, {
            source: supplierPointNode,
            target: buyerPointNode,
            count: 0,
            activeCount: 0,
            isActive: false,
            stage: supplierStage,
          });
        }
        lineMap.get(lineKey).count += 1;
        if (edgeActive) {
          lineMap.get(lineKey).activeCount += 1;
          lineMap.get(lineKey).isActive = true;
        }
      });

      const points = Array.from(pointMap.values()).sort((left, right) => {
        if (left.isFocus !== right.isFocus) return left.isFocus ? -1 : 1;
        if (left.isActive !== right.isActive) return left.isActive ? -1 : 1;
        if (left.activeCount !== right.activeCount) return right.activeCount - left.activeCount;
        return right.count - left.count;
      });
      const lines = Array.from(lineMap.values()).sort((left, right) => {
        if (left.isActive !== right.isActive) return left.isActive ? -1 : 1;
        if (left.activeCount !== right.activeCount) return right.activeCount - left.activeCount;
        return right.count - left.count;
      });
      const activeRoutes = lines.filter((lineItem) => lineItem.isActive);
      const activePoints = points.filter((point) => point.isActive);
      const backgroundLines = lines.filter((lineItem) => !lineItem.isActive);
      const foregroundLines = lines.filter((lineItem) => lineItem.isActive);
      const backgroundPoints = points.filter((point) => !point.isActive);
      const foregroundPoints = points.filter((point) => point.isActive);
      const connectionCounts = new Map();

      function noteConnection(sourceLabel, targetLabel) {
        if (!sourceLabel || !targetLabel) return;
        if (!connectionCounts.has(sourceLabel)) {
          connectionCounts.set(sourceLabel, new Map());
        }
        const neighbors = connectionCounts.get(sourceLabel);
        neighbors.set(targetLabel, (neighbors.get(targetLabel) || 0) + 1);
      }

      (hasMapFocus ? foregroundLines : lines).forEach((lineItem) => {
        noteConnection(lineItem.source.label, lineItem.target.label);
        noteConnection(lineItem.target.label, lineItem.source.label);
      });
      const globePoints = points.map((point) => {
        const dominantStageEntries = Array.from((hasMapFocus ? point.activeStageCounts : point.stageCounts).entries())
          .filter(([stage]) => Boolean(stage))
          .sort((left, right) => right[1] - left[1]);
        const primaryStage = dominantStageEntries.length ? dominantStageEntries[0][0] : "";
        const neighborSummary = Array.from((connectionCounts.get(point.label) || new Map()).entries())
          .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
          .slice(0, 3)
          .map((entry) => entry[0]);
        return {
          label: point.label,
          country: point.country,
          lat: point.lat,
          lon: point.lon,
          isActive: point.isActive,
          isFocus: point.isFocus,
          stage: primaryStage,
          stageEntries: dominantStageEntries,
          connectionsText: neighborSummary.join(" / "),
        };
      });
      const globeLines = lines.map((lineItem) => ({
        sourceLat: lineItem.source.lat,
        sourceLon: lineItem.source.lon,
        targetLat: lineItem.target.lat,
        targetLon: lineItem.target.lon,
        isActive: lineItem.isActive,
        isFocus: lineItem.source.isFocus || lineItem.target.isFocus,
        stage: lineItem.stage,
      }));
      const globeCountries = hasMapFocus
        ? Array.from(
            new Set(
              activePoints
                .map((point) => normalize(point.country))
                .filter(Boolean)
            )
          )
            .map((key) => countryPointLookup.get(key))
            .filter(Boolean)
        : [];

      mapStats.innerHTML = [
        hasMapFocus ? `相关连线: ${activeRoutes.length}` : `总览连线: ${lines.length}`,
        hasMapFocus ? `相关点位: ${activePoints.length}` : `总览点位: ${points.length}`,
        `可见点位: ${points.length}`,
        `焦点: ${hasMapFocus ? mapSubgraph.focus : "无"}`,
      ].map((item) => `<span class="stat-pill">${escapeHtml(item)}</span>`).join("");
      mapLegend.innerHTML = hasMapFocus
        ? stepOrder
            .filter((step) => points.some((point) => (point.activeStageCounts.get(step) || 0) > 0))
            .map((step) => `<span class="legend-item"><i class="legend-dot" style="background:${stepColors[step] || "#355464"}"></i>${escapeHtml(localizeStep(step))}</span>`)
            .join("")
        : `<span class="legend-item"><i class="legend-dot" style="background:#c9d0d5"></i>总览模式</span>`;

      if (!points.length) {
        mapEmpty.hidden = false;
        if (window.__previewBridge) {
          window.__previewBridge.pendingGlobeData = null;
          if (typeof window.__previewBridge.updateWebGlobeScene === "function") {
            window.__previewBridge.updateWebGlobeScene(null);
          } else if (typeof window.__previewBridge.updateGlobeScene === "function") {
            window.__previewBridge.updateGlobeScene(null);
          }
          if (typeof window.__previewBridge.updateGoogleGlobeScene === "function") {
            window.__previewBridge.updateGoogleGlobeScene(null);
          }
        }
        return;
      }
      mapEmpty.hidden = true;
      if (window.__previewBridge) {
        window.__previewBridge.pendingGlobeData = {
          hasFocus: hasMapFocus,
          focus: hasMapFocus ? mapSubgraph.focus : "",
          points: globePoints,
          lines: globeLines,
          countries: globeCountries,
        };
        if (typeof window.__previewBridge.updateWebGlobeScene === "function") {
          window.__previewBridge.updateWebGlobeScene(window.__previewBridge.pendingGlobeData);
        } else if (typeof window.__previewBridge.updateGlobeScene === "function") {
          window.__previewBridge.updateGlobeScene(window.__previewBridge.pendingGlobeData);
        }
        if (typeof window.__previewBridge.updateGoogleGlobeScene === "function") {
          window.__previewBridge.updateGoogleGlobeScene(window.__previewBridge.pendingGlobeData);
        }
      }

      if (!shouldDrawMapSvg) {
        return;
      }

      const boundaryGroup = document.createElementNS("http://www.w3.org/2000/svg", "g");
      worldMap.paths.forEach((country) => {
        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        path.setAttribute("d", country.path);
        path.setAttribute("fill", "none");
        path.setAttribute("stroke", "rgba(83, 94, 101, 0.24)");
        path.setAttribute("stroke-width", "0.85");
        boundaryGroup.appendChild(path);
      });
      mapSvg.appendChild(boundaryGroup);

      if (hasMapFocus) {
        const labelGroup = document.createElementNS("http://www.w3.org/2000/svg", "g");
        worldMap.labels.forEach((country) => {
          if (!activeCountries.has(normalize(country.name))) return;
          const fontSize = 10.5;
          const box = createTextBox(country.x, country.y, country.name, fontSize, "middle");
          if (!canPlaceLabel(box, reservedLabelBoxes, 5)) return;
          reservedLabelBoxes.push(box);
          const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
          label.setAttribute("x", String(country.x));
          label.setAttribute("y", String(country.y));
          label.setAttribute("font-size", String(fontSize));
          label.setAttribute("font-weight", "700");
          label.setAttribute("text-anchor", "middle");
          label.setAttribute("fill", "rgba(53,84,100,0.68)");
          label.textContent = localizeCountry(country.name);
          labelGroup.appendChild(label);
        });
        mapSvg.appendChild(labelGroup);
      }

      function drawMapLine(lineItem) {
        const line = document.createElementNS("http://www.w3.org/2000/svg", "path");
        const mx = (lineItem.source.x + lineItem.target.x) / 2;
        const my = Math.min(lineItem.source.y, lineItem.target.y) - Math.abs(lineItem.source.x - lineItem.target.x) * 0.08;
        line.setAttribute("d", `M ${lineItem.source.x} ${lineItem.source.y} Q ${mx} ${my} ${lineItem.target.x} ${lineItem.target.y}`);
        line.setAttribute("fill", "none");
        const baseColor = stepColors[lineItem.stage] || "#355464";
        const strokeColor = hasMapFocus && lineItem.isActive
          ? colorWithAlpha(baseColor, lineItem.source.isFocus || lineItem.target.isFocus ? 0.38 : 0.26)
          : "rgba(160,168,174,0.16)";
        line.setAttribute("stroke", simpleMode
          ? (hasMapFocus && lineItem.isActive ? colorWithAlpha(baseColor, 0.18) : "rgba(160,168,174,0.10)")
          : strokeColor);
        line.setAttribute("stroke-width", String(hasMapFocus && lineItem.isActive ? Math.min(4, 0.8 + Math.log2(lineItem.activeCount + 1)) : 1));
        line.setAttribute("stroke-linecap", "round");
        const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
        title.textContent = `${lineItem.source.label} -> ${lineItem.target.label} (${lineItem.count})`;
        line.appendChild(title);
        mapSvg.appendChild(line);
      }

      backgroundLines.forEach(drawMapLine);
      foregroundLines.forEach(drawMapLine);

      function drawMapPoint(point) {
        const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
        group.style.cursor = "pointer";
        const radius = point.isFocus ? 5.4 : Math.min(4.8, 2.2 + Math.log2((point.isActive ? point.activeCount : point.count) + 1) * 0.72);
        const activeStageEntries = Array.from(point.activeStageCounts.entries())
          .filter(([stage]) => Boolean(stage))
          .sort((left, right) => {
            const leftIndex = stepOrder.indexOf(left[0]);
            const rightIndex = stepOrder.indexOf(right[0]);
            return leftIndex - rightIndex;
          });

        if (hasMapFocus && (point.isFocus || point.isActive) && activeStageEntries.length) {
          const halo = document.createElementNS("http://www.w3.org/2000/svg", "circle");
          halo.setAttribute("cx", String(point.x));
          halo.setAttribute("cy", String(point.y));
          halo.setAttribute("r", String(radius + 1.7));
          halo.setAttribute("fill", "#fff");
          halo.setAttribute("stroke", point.isFocus ? "#355464" : "rgba(53,84,100,0.65)");
          halo.setAttribute("stroke-width", point.isFocus ? "2" : "1");
          group.appendChild(halo);

          if (activeStageEntries.length === 1) {
            const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
            circle.setAttribute("cx", String(point.x));
            circle.setAttribute("cy", String(point.y));
            circle.setAttribute("r", String(radius));
            circle.setAttribute("fill", stepColors[activeStageEntries[0][0]] || "#355464");
            circle.setAttribute("stroke", "#fff");
            circle.setAttribute("stroke-width", "0.8");
            group.appendChild(circle);
          } else {
            const total = activeStageEntries.reduce((sum, entry) => sum + entry[1], 0);
            let startAngle = -Math.PI / 2;
            activeStageEntries.forEach(([stage, count]) => {
              const endAngle = startAngle + (count / total) * Math.PI * 2;
              const slice = document.createElementNS("http://www.w3.org/2000/svg", "path");
              slice.setAttribute("d", pieSlicePath(point.x, point.y, radius, startAngle, endAngle));
              slice.setAttribute("fill", stepColors[stage] || "#355464");
              slice.setAttribute("stroke", "#fff");
              slice.setAttribute("stroke-width", "0.7");
              group.appendChild(slice);
              startAngle = endAngle;
            });
          }
        } else {
          const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
          circle.setAttribute("cx", String(point.x));
          circle.setAttribute("cy", String(point.y));
          circle.setAttribute("r", String(radius));
          circle.setAttribute("fill", "#c9d0d5");
          circle.setAttribute("stroke", "#fff");
          circle.setAttribute("stroke-width", "0.8");
          group.appendChild(circle);
        }

        if (hasMapFocus && (point.isFocus || point.isActive)) {
          const displayLabel = point.label.length > 24 ? `${point.label.slice(0, 22)}...` : point.label;
          const labelX = point.x + 7;
          const labelY = point.y - 7;
          const labelBox = createTextBox(labelX, labelY, displayLabel, 9.5, "start");
          if (canPlaceLabel(labelBox, reservedLabelBoxes, 4)) {
            reservedLabelBoxes.push(labelBox);
            const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
            label.setAttribute("x", String(labelX));
            label.setAttribute("y", String(labelY));
            label.setAttribute("font-size", "9.5");
            label.setAttribute("fill", "#355464");
            label.textContent = displayLabel;
            group.appendChild(label);
          }
        }

        const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
        title.textContent = `${point.label}${point.country ? " | " + point.country : ""} | transactions ${point.count}`;
        group.appendChild(title);
        group.addEventListener("click", () => toggleMapFocus(point.label));
        mapSvg.appendChild(group);
      }

      backgroundPoints.forEach(drawMapPoint);
      foregroundPoints.forEach(drawMapPoint);
    }

    function performRender() {
      const focusTerm = companyInput.value.trim();
      const simpleMode = modeToggle.checked;
      const subgraph = buildFocusedSelection(focusTerm, true);
      const mapSubgraph = mapFocusOverride
        ? buildFocusedSelection(mapFocusOverride, false)
        : (focusTerm ? buildFocusedSelection(focusTerm, false) : null);

      if (focusTerm && subgraph.matchedFocus) {
        companyInput.value = subgraph.matchedFocus;
        searchStatus.textContent = `已聚焦 ${subgraph.matchedFocus} | 命中路径 ${subgraph.rowsMatched} | 相关企业 ${subgraph.companies.size}`;
      } else if (focusTerm) {
        searchStatus.textContent = `没有精确匹配 "${focusTerm}"，可尝试输入部分名称。`;
      } else if (mapSubgraph && mapSubgraph.matchedFocus) {
        searchStatus.textContent = `地图聚焦 ${mapSubgraph.matchedFocus} | 上方链路保持默认焦点 ${subgraph.focus}`;
      } else {
        searchStatus.textContent = `上方链路默认焦点 ${subgraph.focus} | 地图处于总览模式`;
      }
      renderChainPanel(subgraph, simpleMode);
      renderMapPanel(subgraph, mapSubgraph, simpleMode);
    }

    function render() {
      if (renderFrameHandle) return;
      renderFrameHandle = window.requestAnimationFrame(() => {
        renderFrameHandle = 0;
        performRender();
      });
    }

    function searchAndRender() {
      updateCompanySuggestions(companyInput.value);
      mapFocusOverride = null;
      render();
    }

    function setPrimaryFocus(label) {
      if (!label) return;
      companyInput.value = label;
      mapFocusOverride = null;
      render();
    }

    function toggleMapFocus(label) {
      if (!label) return;
      mapFocusOverride = mapFocusOverride === label ? null : label;
      render();
    }

    function resetFocus() {
      companyInput.value = "";
      mapFocusOverride = null;
      updateCompanySuggestions(companyInput.value);
      render();
    }

    if (window.__previewBridge) {
      window.__previewBridge.setPrimaryFocus = setPrimaryFocus;
      window.__previewBridge.toggleMapFocus = toggleMapFocus;
      window.__previewBridge.googleGlobeElements = {
        host: googleGlobeHost,
        input: googleApiKeyInput,
        loadButton: googleApiKeyLoad,
        clearButton: googleApiKeyClear,
        status: googleApiKeyStatus,
        setup: globeSetup,
      };
    }

    updateCompanySuggestions(companyInput.value);
    companyInput.addEventListener("change", render);
    companyInput.addEventListener("input", () => updateCompanySuggestions(companyInput.value));
    companyInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        searchAndRender();
      }
    });
    searchButton.addEventListener("click", searchAndRender);
    resetButton.addEventListener("click", resetFocus);
    modeToggle.addEventListener("change", render);
    mapViewButton.addEventListener("click", () => {
      activeGeoView = "map";
      syncGeoView();
    });
      globeViewButton.addEventListener("click", () => {
        activeGeoView = "globe";
        syncGeoView();
      });
      softHighlightButton.addEventListener("click", () => {
        softHighlightMode = !softHighlightMode;
        syncGeoView();
        render();
      });
      syncGeoView();
      render();
  </script>
  <script>
    (() => {
      const bridge = window.__previewBridge = window.__previewBridge || {};
      const canvas = document.getElementById("globeCanvas");
      const tooltip = document.getElementById("globeTooltip");
      if (!canvas) return;
      const context = canvas.getContext("2d");
      if (!context) return;

      const globeState = {
        rotationLon: -0.45,
        rotationLat: 0.18,
        zoom: 1,
        dragging: false,
        lastX: 0,
        lastY: 0,
        data: null,
        timeMs: 0,
        hoverTargets: [],
      };
      const stars = Array.from({ length: 220 }, (_, index) => {
        const angle = (index * 2.399963229728653) % (Math.PI * 2);
        const radius = 0.12 + ((index * 37) % 100) / 100 * 0.9;
        return {
          x: Math.cos(angle) * radius,
          y: Math.sin(angle * 1.7) * radius * 0.6,
          size: 0.6 + (index % 3) * 0.35,
          alpha: 0.24 + (index % 5) * 0.05,
          phase: index * 0.37,
        };
      });
      const earthRings = (window.payload?.world_map?.globe_rings || []).map((ring) => {
        const longitudes = ring.map((pair) => pair[0]);
        const latitudes = ring.map((pair) => pair[1]);
        return {
          points: ring,
          minLon: Math.min(...longitudes),
          maxLon: Math.max(...longitudes),
          minLat: Math.min(...latitudes),
          maxLat: Math.max(...latitudes),
          centerLon: longitudes.reduce((sum, value) => sum + value, 0) / Math.max(1, longitudes.length),
          centerLat: latitudes.reduce((sum, value) => sum + value, 0) / Math.max(1, latitudes.length),
        };
      });
      const earthLight = (() => {
        const vector = { x: -0.48, y: 0.26, z: 0.84 };
        const length = Math.hypot(vector.x, vector.y, vector.z) || 1;
        return {
          x: vector.x / length,
          y: vector.y / length,
          z: vector.z / length,
        };
      })();

      function clamp(value, min, max) {
        return Math.max(min, Math.min(max, value));
      }

      function mix(start, end, weight) {
        return start + (end - start) * weight;
      }

      function mixRgb(left, right, weight) {
        return [
          mix(left[0], right[0], weight),
          mix(left[1], right[1], weight),
          mix(left[2], right[2], weight),
        ];
      }

      function rgba(rgb, alpha = 1) {
        return `rgba(${Math.round(rgb[0])}, ${Math.round(rgb[1])}, ${Math.round(rgb[2])}, ${alpha})`;
      }

      function fract(value) {
        return value - Math.floor(value);
      }

      function pseudoNoise(a, b) {
        return fract(Math.sin(a * 127.1 + b * 311.7 + 78.233) * 43758.5453123);
      }

      function normalizedLongitude(lon) {
        let value = lon;
        while (value > 180) value -= 360;
        while (value < -180) value += 360;
        return value;
      }

      function pointInRing(lon, lat, ring) {
        let inside = false;
        for (let index = 0, previous = ring.length - 1; index < ring.length; previous = index, index += 1) {
          const xi = ring[index][0];
          const yi = ring[index][1];
          const xj = ring[previous][0];
          const yj = ring[previous][1];
          const intersect = ((yi > lat) !== (yj > lat)) &&
            (lon < ((xj - xi) * (lat - yi)) / ((yj - yi) || 1e-9) + xi);
          if (intersect) inside = !inside;
        }
        return inside;
      }

      function isLandCoordinate(lat, lon) {
        const normalizedLon = normalizedLongitude(lon);
        for (const ring of earthRings) {
          if (lat < ring.minLat || lat > ring.maxLat) continue;
          if (ring.maxLon - ring.minLon < 350 && (normalizedLon < ring.minLon || normalizedLon > ring.maxLon)) continue;
          if (pointInRing(normalizedLon, lat, ring.points)) {
            return true;
          }
        }
        return false;
      }

      function buildCloudSamples() {
        const clouds = [];
        for (let lat = -58; lat <= 66; lat += 7.2) {
          for (let lon = -180; lon < 180; lon += 8.4) {
            const density = pseudoNoise(lat * 0.05 + 71.3, lon * 0.07 - 19.7);
            const streak = pseudoNoise(lat * 0.11 - 42.2, lon * 0.15 + 13.4);
            if (density + streak * 0.55 < 1.14) continue;
            clouds.push({
              lat: lat + (pseudoNoise(lat * 0.17 + 8.4, lon * 0.13 - 4.2) - 0.5) * 2.6,
              lon: lon + (pseudoNoise(lat * 0.19 - 2.7, lon * 0.21 + 6.1) - 0.5) * 4.4,
              alpha: 0.06 + density * 0.11,
              sizeX: 9 + streak * 20,
              sizeY: 3.4 + density * 7.5,
              rotation: pseudoNoise(lat * 0.23, lon * 0.17) * Math.PI,
            });
          }
        }
        return clouds;
      }

      const cloudSamples = buildCloudSamples();

      function resize() {
        const rect = canvas.getBoundingClientRect();
        const dpr = Math.min(window.devicePixelRatio || 1, 2);
        canvas.width = Math.max(1, Math.round(rect.width * dpr));
        canvas.height = Math.max(1, Math.round(rect.height * dpr));
        context.setTransform(dpr, 0, 0, dpr, 0, 0);
        draw();
      }

      function stageColor(stage, fallback = "#355464") {
        const palette = window.payload?.step_colors || {};
        return palette[stage] || fallback;
      }

      function drawStars(width, height) {
        stars.forEach((star) => {
          const twinkle = 0.78 + Math.sin(globeState.timeMs * 0.0012 + star.phase) * 0.22;
          context.beginPath();
          context.arc(
            width * (0.5 + star.x * 0.86),
            height * (0.48 + star.y * 0.92),
            star.size * (0.92 + twinkle * 0.14),
            0,
            Math.PI * 2
          );
          context.fillStyle = `rgba(191, 207, 220, ${Math.max(0.08, star.alpha * twinkle)})`;
          context.fill();
        });
      }

      function drawBackdrop(width, height) {
        const topGlow = context.createRadialGradient(
          width * 0.5, height * 0.22, width * 0.02,
          width * 0.5, height * 0.22, width * 0.34
        );
        topGlow.addColorStop(0, "rgba(91, 174, 255, 0.18)");
        topGlow.addColorStop(0.45, "rgba(91, 174, 255, 0.08)");
        topGlow.addColorStop(1, "rgba(91, 174, 255, 0)");
        context.fillStyle = topGlow;
        context.fillRect(0, 0, width, height);

        const sideGlow = context.createLinearGradient(0, 0, width, height);
        sideGlow.addColorStop(0, "rgba(255,255,255,0.05)");
        sideGlow.addColorStop(0.5, "rgba(255,255,255,0)");
        sideGlow.addColorStop(1, "rgba(86, 118, 143, 0.10)");
        context.fillStyle = sideGlow;
        context.fillRect(0, 0, width, height);
      }

      function latLonToVector(lat, lon, scale = 1) {
        const latRad = lat * Math.PI / 180;
        const lonRad = lon * Math.PI / 180;
        const cosLat = Math.cos(latRad);
        return {
          x: Math.sin(lonRad) * cosLat * scale,
          y: Math.sin(latRad) * scale,
          z: Math.cos(lonRad) * cosLat * scale,
        };
      }

      function rotateVector(vector) {
        const cosLon = Math.cos(globeState.rotationLon);
        const sinLon = Math.sin(globeState.rotationLon);
        const cosLat = Math.cos(globeState.rotationLat);
        const sinLat = Math.sin(globeState.rotationLat);

        const x1 = vector.x * cosLon + vector.z * sinLon;
        const z1 = -vector.x * sinLon + vector.z * cosLon;
        const y2 = vector.y * cosLat - z1 * sinLat;
        const z2 = vector.y * sinLat + z1 * cosLat;
        return { x: x1, y: y2, z: z2 };
      }

      function projectVector(vector, radius, cx, cy) {
        const rotated = rotateVector(vector);
        return {
          x: cx + rotated.x * radius,
          y: cy - rotated.y * radius,
          z: rotated.z,
        };
      }

      function drawSphere(width, height, radius, cx, cy) {
        drawBackdrop(width, height);
        drawStars(width, height);
        const gradient = context.createRadialGradient(
          cx - radius * 0.32, cy - radius * 0.42, radius * 0.08,
          cx, cy, radius * 1.12
        );
        gradient.addColorStop(0, "#6fcfff");
        gradient.addColorStop(0.18, "#1977c2");
        gradient.addColorStop(0.45, "#0f4f8b");
        gradient.addColorStop(0.78, "#0a2b52");
        gradient.addColorStop(1, "#051221");
        context.beginPath();
        context.arc(cx, cy, radius, 0, Math.PI * 2);
        context.fillStyle = gradient;
        context.fill();

        context.save();
        context.beginPath();
        context.arc(cx, cy, radius, 0, Math.PI * 2);
        context.clip();

        const shadowOffset = Math.sin(globeState.rotationLon) * radius * 0.34;
        const shadowGradient = context.createLinearGradient(
          cx - radius - shadowOffset, cy - radius * 0.3,
          cx + radius - shadowOffset, cy + radius * 0.3
        );
        shadowGradient.addColorStop(0, "rgba(2, 8, 18, 0.72)");
        shadowGradient.addColorStop(0.33, "rgba(2, 8, 18, 0.24)");
        shadowGradient.addColorStop(0.62, "rgba(2, 8, 18, 0.03)");
        shadowGradient.addColorStop(1, "rgba(2, 8, 18, 0.38)");
        context.fillStyle = shadowGradient;
        context.fillRect(cx - radius, cy - radius, radius * 2, radius * 2);

        context.beginPath();
        context.ellipse(
          cx - radius * 0.18,
          cy - radius * 0.36,
          radius * 0.42,
          radius * 0.24,
          -0.35,
          0,
          Math.PI * 2
        );
        context.fillStyle = "rgba(233, 249, 255, 0.24)";
        context.fill();

        const oceanBloom = context.createLinearGradient(
          cx - radius * 0.94, cy - radius * 0.28,
          cx + radius * 0.62, cy + radius * 0.75
        );
        oceanBloom.addColorStop(0, "rgba(111, 206, 255, 0.20)");
        oceanBloom.addColorStop(0.32, "rgba(37, 149, 213, 0.06)");
        oceanBloom.addColorStop(1, "rgba(255,255,255,0)");
        context.fillStyle = oceanBloom;
        context.fillRect(cx - radius, cy - radius, radius * 2, radius * 2);

        const sheen = context.createLinearGradient(
          cx - radius * 0.64, cy - radius * 0.7,
          cx + radius * 0.3, cy + radius * 0.85
        );
        sheen.addColorStop(0, "rgba(227, 248, 255, 0.28)");
        sheen.addColorStop(0.38, "rgba(168, 232, 255, 0.06)");
        sheen.addColorStop(1, "rgba(255,255,255,0)");
        context.fillStyle = sheen;
        context.fillRect(cx - radius, cy - radius, radius * 2, radius * 2);
        context.restore();

        context.beginPath();
        context.arc(cx, cy, radius + 9, 0, Math.PI * 2);
        context.strokeStyle = "rgba(118, 193, 255, 0.16)";
        context.lineWidth = 18;
        context.stroke();

        context.beginPath();
        context.arc(cx, cy, radius + 2.6, 0, Math.PI * 2);
        context.strokeStyle = "rgba(219, 242, 255, 0.32)";
        context.lineWidth = 3.4;
        context.stroke();

        const shadow = context.createRadialGradient(cx, cy + radius * 0.5, radius * 0.3, cx, cy, radius * 1.4);
        shadow.addColorStop(0, "rgba(5, 12, 22, 0.34)");
        shadow.addColorStop(1, "rgba(31, 52, 70, 0)");
        context.beginPath();
        context.ellipse(cx, cy + radius * 1.06, radius * 0.84, radius * 0.16, 0, 0, Math.PI * 2);
        context.fillStyle = shadow;
        context.fill();
      }

      function climateProfile(lat, lon) {
        const absLat = Math.abs(lat);
        const moisture = pseudoNoise(lat * 0.06 - 1.8, lon * 0.08 + 2.4);
        const terrain = pseudoNoise(lat * 0.13 + 4.1, lon * 0.16 - 7.9);
        const ridges = pseudoNoise(lat * 0.22 - 6.2, lon * 0.21 + 3.7);
        const desert = absLat < 34 && moisture < 0.44 && terrain > 0.38;
        const polar = absLat > 64;
        const tropical = absLat < 22 && moisture > 0.48;
        return { absLat, moisture, terrain, ridges, desert, polar, tropical };
      }

      function ringPalette(ringMeta) {
        const profile = climateProfile(ringMeta.centerLat, ringMeta.centerLon);
        if (profile.polar) {
          return {
            top: [241, 245, 241],
            bottom: [195, 203, 197],
            highlight: [255, 255, 255],
            shadow: [132, 148, 156],
          };
        }
        if (profile.desert) {
          return {
            top: [224, 208, 163],
            bottom: [171, 142, 93],
            highlight: [250, 236, 202],
            shadow: [127, 97, 58],
          };
        }
        if (profile.tropical) {
          return {
            top: [98, 145, 86],
            bottom: [53, 97, 58],
            highlight: [166, 196, 133],
            shadow: [39, 70, 46],
          };
        }
        return {
          top: [134, 151, 103],
          bottom: [83, 100, 67],
          highlight: [192, 201, 164],
          shadow: [60, 75, 52],
        };
      }

      function buildVisibleRingProjection(ringMeta, radius, cx, cy, scale = 1.003) {
        const visiblePoints = [];
        let zSum = 0;
        for (const pair of ringMeta.points) {
          const projected = projectVector(latLonToVector(pair[1], pair[0], scale), radius, cx, cy);
          if (projected.z > -0.035) {
            visiblePoints.push(projected);
            zSum += projected.z;
          }
        }
        if (visiblePoints.length < 4 || visiblePoints.length / ringMeta.points.length < 0.44) {
          return null;
        }
        const bounds = visiblePoints.reduce((acc, point) => ({
          minX: Math.min(acc.minX, point.x),
          maxX: Math.max(acc.maxX, point.x),
          minY: Math.min(acc.minY, point.y),
          maxY: Math.max(acc.maxY, point.y),
        }), {
          minX: visiblePoints[0].x,
          maxX: visiblePoints[0].x,
          minY: visiblePoints[0].y,
          maxY: visiblePoints[0].y,
        });
        return {
          ringMeta,
          points: visiblePoints,
          avgZ: zSum / Math.max(1, visiblePoints.length),
          ...bounds,
        };
      }

      function traceProjectedPolygon(points) {
        context.beginPath();
        context.moveTo(points[0].x, points[0].y);
        for (let index = 1; index < points.length; index += 1) {
          context.lineTo(points[index].x, points[index].y);
        }
        context.closePath();
      }

      function drawOceanTexture(radius, cx, cy) {
        context.save();
        context.beginPath();
        context.arc(cx, cy, radius, 0, Math.PI * 2);
        context.clip();

        const bandCount = 18;
        for (let index = 0; index < bandCount; index += 1) {
          const t = index / Math.max(1, bandCount - 1);
          const y = cy - radius * 0.9 + t * radius * 1.8;
          const band = context.createLinearGradient(cx - radius, y, cx + radius, y + radius * 0.12);
          band.addColorStop(0, "rgba(255,255,255,0)");
          band.addColorStop(0.18, "rgba(145, 219, 255, 0.03)");
          band.addColorStop(0.5, "rgba(81, 180, 239, 0.08)");
          band.addColorStop(0.82, "rgba(145, 219, 255, 0.03)");
          band.addColorStop(1, "rgba(255,255,255,0)");
          context.fillStyle = band;
          context.fillRect(cx - radius, y - radius * 0.09, radius * 2, radius * 0.18);
        }

        const currentSeeds = [
          [-0.82, -0.18, 0.22, -0.28, 0.86, 0.14],
          [-0.76, 0.24, 0.08, 0.34, 0.82, 0.08],
          [-0.54, -0.52, -0.04, -0.26, 0.62, -0.08],
          [-0.22, 0.48, 0.16, 0.58, 0.78, 0.32],
          [-0.16, -0.16, 0.38, -0.34, 0.92, -0.08],
        ];
        currentSeeds.forEach((seed, index) => {
          context.beginPath();
          context.moveTo(cx + seed[0] * radius, cy + seed[1] * radius);
          context.bezierCurveTo(
            cx + seed[2] * radius, cy + seed[3] * radius,
            cx + seed[4] * radius, cy + seed[5] * radius,
            cx + radius * 1.02, cy + (index * 0.08 - 0.26) * radius
          );
          context.strokeStyle = index % 2 === 0
            ? "rgba(173, 232, 255, 0.06)"
            : "rgba(118, 207, 255, 0.04)";
          context.lineWidth = 1.4;
          context.stroke();
        });
        context.restore();
      }

      function drawLandTexture(radius, cx, cy) {
        const visibleRings = earthRings
          .map((ringMeta) => buildVisibleRingProjection(ringMeta, radius, cx, cy))
          .filter(Boolean)
          .sort((left, right) => left.avgZ - right.avgZ);

        visibleRings.forEach((item) => {
          const palette = ringPalette(item.ringMeta);
          const lightVector = rotateVector(latLonToVector(item.ringMeta.centerLat, item.ringMeta.centerLon, 1));
          const lighting = clamp(
            lightVector.x * earthLight.x + lightVector.y * earthLight.y + lightVector.z * earthLight.z,
            -0.28,
            1
          );
          const gradient = context.createLinearGradient(
            item.minX,
            item.minY,
            item.maxX,
            item.maxY
          );
          gradient.addColorStop(0, rgba(mixRgb(palette.top, palette.highlight, clamp(lighting * 0.38 + 0.22, 0, 0.58)), 0.98));
          gradient.addColorStop(0.48, rgba(mixRgb(palette.top, palette.bottom, 0.24), 0.98));
          gradient.addColorStop(1, rgba(mixRgb(palette.bottom, palette.shadow, clamp(0.30 - lighting * 0.16, 0.08, 0.38)), 0.98));
          traceProjectedPolygon(item.points);
          context.fillStyle = gradient;
          context.fill();

          const relief = context.createLinearGradient(
            item.minX,
            item.minY,
            item.maxX,
            item.maxY
          );
          relief.addColorStop(0, "rgba(255,255,255,0.11)");
          relief.addColorStop(0.45, "rgba(255,255,255,0.02)");
          relief.addColorStop(1, "rgba(10, 28, 22, 0.14)");
          traceProjectedPolygon(item.points);
          context.fillStyle = relief;
          context.fill();

          context.save();
          context.translate(-radius * 0.009, radius * 0.006);
          traceProjectedPolygon(item.points);
          context.strokeStyle = "rgba(10, 24, 22, 0.10)";
          context.lineWidth = 1.4;
          context.stroke();
          context.restore();

          context.save();
          traceProjectedPolygon(item.points);
          context.clip();

          const terrainBands = 10;
          for (let bandIndex = 0; bandIndex < terrainBands; bandIndex += 1) {
            const ratio = bandIndex / Math.max(1, terrainBands - 1);
            const sweep = context.createLinearGradient(
              item.minX,
              item.minY + (item.maxY - item.minY) * ratio,
              item.maxX,
              item.minY + (item.maxY - item.minY) * (ratio + 0.12)
            );
            const localClimate = climateProfile(
              item.ringMeta.centerLat + (ratio - 0.5) * 8,
              item.ringMeta.centerLon + (ratio - 0.5) * 10
            );
            const warmTint = localClimate.desert
              ? "rgba(243, 218, 170, 0.12)"
              : localClimate.tropical
                ? "rgba(128, 182, 120, 0.10)"
                : "rgba(210, 220, 188, 0.06)";
            const darkTint = localClimate.polar
              ? "rgba(124, 140, 150, 0.08)"
              : "rgba(48, 72, 54, 0.10)";
            sweep.addColorStop(0, "rgba(255,255,255,0)");
            sweep.addColorStop(0.28, warmTint);
            sweep.addColorStop(0.72, darkTint);
            sweep.addColorStop(1, "rgba(255,255,255,0)");
            context.fillStyle = sweep;
            context.fillRect(item.minX, item.minY, item.maxX - item.minX, item.maxY - item.minY);
          }

          const ridgeCount = Math.max(4, Math.round((item.maxX - item.minX + item.maxY - item.minY) / 120));
          for (let ridgeIndex = 0; ridgeIndex < ridgeCount; ridgeIndex += 1) {
            const startX = mix(item.minX, item.maxX, pseudoNoise(item.ringMeta.centerLon * 0.17 + ridgeIndex * 1.7, item.ringMeta.centerLat * 0.23));
            const startY = mix(item.minY, item.maxY, pseudoNoise(item.ringMeta.centerLat * 0.19 - ridgeIndex * 1.1, item.ringMeta.centerLon * 0.11));
            const endX = startX + (pseudoNoise(ridgeIndex * 0.41, item.ringMeta.centerLon * 0.07) - 0.5) * (item.maxX - item.minX) * 0.48;
            const endY = startY + (pseudoNoise(ridgeIndex * 0.33, item.ringMeta.centerLat * 0.09) - 0.5) * (item.maxY - item.minY) * 0.24;
            context.beginPath();
            context.moveTo(startX, startY);
            context.quadraticCurveTo(
              mix(startX, endX, 0.48) + (item.maxX - item.minX) * 0.06,
              mix(startY, endY, 0.42) - (item.maxY - item.minY) * 0.04,
              endX,
              endY
            );
            context.strokeStyle = "rgba(255,255,255,0.05)";
            context.lineWidth = 0.9;
            context.stroke();
          }

          const snowChance = Math.abs(item.ringMeta.centerLat) > 46 ? 6 : 0;
          for (let patchIndex = 0; patchIndex < snowChance; patchIndex += 1) {
            const patchX = mix(item.minX, item.maxX, pseudoNoise(item.ringMeta.centerLon * 0.13 + patchIndex, item.ringMeta.centerLat * 0.07));
            const patchY = mix(item.minY, item.maxY, pseudoNoise(item.ringMeta.centerLat * 0.17 - patchIndex, item.ringMeta.centerLon * 0.05));
            const patch = context.createRadialGradient(patchX, patchY, 0, patchX, patchY, 16);
            patch.addColorStop(0, "rgba(255,255,255,0.18)");
            patch.addColorStop(0.55, "rgba(255,255,255,0.06)");
            patch.addColorStop(1, "rgba(255,255,255,0)");
            context.beginPath();
            context.arc(patchX, patchY, 16, 0, Math.PI * 2);
            context.fillStyle = patch;
            context.fill();
          }

          context.restore();
        });
      }

      function drawCloudLayer(radius, cx, cy) {
        context.save();
        context.filter = "blur(1.1px)";
        cloudSamples.forEach((cloud) => {
          const projected = projectVector(latLonToVector(cloud.lat, cloud.lon, 1.03), radius, cx, cy);
          if (projected.z <= 0.02) return;
          context.save();
          context.translate(projected.x, projected.y);
          context.rotate(cloud.rotation);
          context.scale(1, 0.7 + projected.z * 0.16);
          context.beginPath();
          context.ellipse(
            0,
            0,
            cloud.sizeX * (0.42 + projected.z * 0.28),
            cloud.sizeY * (0.36 + projected.z * 0.22),
            0,
            0,
            Math.PI * 2
          );
          context.fillStyle = `rgba(255,255,255,${clamp(cloud.alpha * (0.42 + projected.z * 0.5), 0.02, 0.22)})`;
          context.fill();
          context.restore();
        });
        context.filter = "none";
        context.restore();
      }

      function drawCoastline(ring, radius, cx, cy) {
        let drawing = false;
        context.beginPath();
        for (const pair of ring) {
          const projected = projectVector(latLonToVector(pair[1], pair[0]), radius, cx, cy);
          if (projected.z <= 0) {
            drawing = false;
            continue;
          }
          if (!drawing) {
            context.moveTo(projected.x, projected.y);
            drawing = true;
          } else {
            context.lineTo(projected.x, projected.y);
          }
        }
        context.stroke();
      }

      function buildArcPoints(lineItem, segments = 40) {
        const start = latLonToVector(lineItem.sourceLat, lineItem.sourceLon, 1);
        const end = latLonToVector(lineItem.targetLat, lineItem.targetLon, 1);
        const points = [];
        for (let index = 0; index <= segments; index += 1) {
          const t = index / segments;
          const lift = 1 + Math.sin(Math.PI * t) * 0.22;
          const vector = {
            x: start.x * (1 - t) + end.x * t,
            y: start.y * (1 - t) + end.y * t,
            z: start.z * (1 - t) + end.z * t,
          };
          const length = Math.hypot(vector.x, vector.y, vector.z) || 1;
          points.push({
            x: (vector.x / length) * lift,
            y: (vector.y / length) * lift,
            z: (vector.z / length) * lift,
          });
        }
        return points;
      }

      function drawArc(lineItem, radius, cx, cy) {
        if ([lineItem.sourceLat, lineItem.sourceLon, lineItem.targetLat, lineItem.targetLon].some((value) => typeof value !== "number")) {
          return;
        }
        const color = lineItem.isActive
          ? stageColor(lineItem.stage, "#56b8ff")
          : "rgba(155, 174, 192, 0.18)";
        const points = lineItem._arcPoints || (lineItem._arcPoints = buildArcPoints(lineItem));
        let drawing = false;
        context.beginPath();
        points.forEach((point) => {
          const projected = projectVector(point, radius, cx, cy);
          if (projected.z <= -0.02) {
            drawing = false;
            return;
          }
          if (!drawing) {
            context.moveTo(projected.x, projected.y);
            drawing = true;
          } else {
            context.lineTo(projected.x, projected.y);
          }
        });
        context.strokeStyle = color;
        context.lineWidth = lineItem.isActive ? (lineItem.isFocus ? 2.4 : 1.45) : 0.7;
        context.globalAlpha = lineItem.isActive ? (lineItem.isFocus ? 0.96 : 0.66) : 0.26;
        context.stroke();
        context.globalAlpha = 1;
      }

      function sampleArcPoint(points, progress) {
        if (!points || points.length === 0) return null;
        const scaled = Math.max(0, Math.min(points.length - 1, progress * (points.length - 1)));
        const leftIndex = Math.floor(scaled);
        const rightIndex = Math.min(points.length - 1, leftIndex + 1);
        const mix = scaled - leftIndex;
        const left = points[leftIndex];
        const right = points[rightIndex];
        return {
          x: left.x + (right.x - left.x) * mix,
          y: left.y + (right.y - left.y) * mix,
          z: left.z + (right.z - left.z) * mix,
        };
      }

      function drawArcPulse(lineItem, radius, cx, cy) {
        if (!lineItem.isActive) return;
        const points = lineItem._arcPoints || (lineItem._arcPoints = buildArcPoints(lineItem));
        const seed = (lineItem.stage || "").length * 0.061 + (lineItem.isFocus ? 0.14 : 0.03);
        const pulseCount = lineItem.isFocus ? 2 : 1;
        for (let pulse = 0; pulse < pulseCount; pulse += 1) {
          const progress = (seed + pulse * 0.38 + globeState.timeMs * 0.00005) % 1;
          const point = sampleArcPoint(points, progress);
          if (!point) continue;
          const projected = projectVector(point, radius, cx, cy);
          if (projected.z <= -0.02) continue;
          const color = stageColor(lineItem.stage, "#56b8ff");
          context.beginPath();
          context.arc(projected.x, projected.y, lineItem.isFocus ? 3.1 : 2.2, 0, Math.PI * 2);
          context.fillStyle = color;
          context.globalAlpha = lineItem.isFocus ? 0.95 : 0.72;
          context.fill();
          context.beginPath();
          context.arc(projected.x, projected.y, lineItem.isFocus ? 5.7 : 4.2, 0, Math.PI * 2);
          context.fillStyle = color;
          context.globalAlpha = 0.16;
          context.fill();
          context.globalAlpha = 1;
        }
      }

      function drawPoint(point, radius, cx, cy) {
        if ([point.lat, point.lon].some((value) => typeof value !== "number")) {
          return;
        }
        if (!point.isActive && !point.isFocus) {
          return;
        }
        const projected = projectVector(latLonToVector(point.lat, point.lon, 1.018), radius, cx, cy);
        if (projected.z <= 0) return;

        const pointRadius = point.isFocus ? 3.6 : 2.2;
        const baseColor = point.isFocus ? "#f7fbff" : stageColor(point.stage || "", "#56b8ff");
        const glow = context.createRadialGradient(
          projected.x,
          projected.y,
          0,
          projected.x,
          projected.y,
          pointRadius + (point.isFocus ? 12 : 8)
        );
        glow.addColorStop(0, point.isFocus ? "rgba(255,255,255,0.96)" : "rgba(255,255,255,0.68)");
        glow.addColorStop(0.22, point.isFocus ? "rgba(255,255,255,0.72)" : rgba([255, 255, 255], 0.26));
        glow.addColorStop(0.5, point.isFocus ? rgba([255, 255, 255], 0.12) : rgba([86, 184, 255], 0.10));
        glow.addColorStop(1, "rgba(255,255,255,0)");
        context.beginPath();
        context.arc(projected.x, projected.y, pointRadius + (point.isFocus ? 12 : 8), 0, Math.PI * 2);
        context.fillStyle = glow;
        context.fill();

        context.beginPath();
        context.arc(projected.x, projected.y, pointRadius + 0.8, 0, Math.PI * 2);
        context.fillStyle = "rgba(255,255,255,0.88)";
        context.fill();

        context.beginPath();
        context.arc(projected.x, projected.y, pointRadius, 0, Math.PI * 2);
        context.fillStyle = baseColor;
        context.fill();
        globeState.hoverTargets.push({
          x: projected.x,
          y: projected.y,
          radius: pointRadius + 7,
          label: point.label,
          country: point.country || "",
          stage: point.stage || "",
          connectionsText: point.connectionsText || "",
        });
      }

      function updateTooltip(clientX, clientY) {
        if (!tooltip) return;
        const rect = canvas.getBoundingClientRect();
        const localX = clientX - rect.left;
        const localY = clientY - rect.top;
        let hit = null;
        for (let index = globeState.hoverTargets.length - 1; index >= 0; index -= 1) {
          const target = globeState.hoverTargets[index];
          const distance = Math.hypot(localX - target.x, localY - target.y);
          if (distance <= target.radius) {
            hit = target;
            break;
          }
        }
        if (!hit) {
          tooltip.hidden = true;
          return;
        }
        tooltip.hidden = false;
        const tooltipX = Math.min(localX, rect.width - 280);
        const tooltipY = Math.min(localY, rect.height - 96);
        tooltip.style.left = `${Math.max(8, tooltipX)}px`;
        tooltip.style.top = `${Math.max(8, tooltipY)}px`;
        const meta = [hit.stage, hit.country].filter(Boolean).join(" | ");
        const links = hit.connectionsText
          ? `<div class="links">关联节点: ${hit.connectionsText}</div>`
          : "";
        tooltip.innerHTML = `<strong>${hit.label}</strong>${meta ? `<div class="meta">${localizeStep(hit.stage || "")}${hit.stage && hit.country ? " | " : ""}${localizeCountry(hit.country || "")}</div>` : ""}${links}`;
      }

      function draw() {
        const width = canvas.clientWidth || 1;
        const height = canvas.clientHeight || 1;
        const cx = width / 2;
        const cy = height / 2;
        const radius = Math.min(width, height) * 0.29 * globeState.zoom;
        globeState.hoverTargets = [];

        context.clearRect(0, 0, width, height);
        drawSphere(width, height, radius, cx, cy);

        context.save();
        context.beginPath();
        context.arc(cx, cy, radius + 0.5, 0, Math.PI * 2);
        context.clip();

        drawOceanTexture(radius, cx, cy);
        drawLandTexture(radius, cx, cy);
        drawCloudLayer(radius, cx, cy);
        context.strokeStyle = "rgba(238, 247, 232, 0.24)";
        context.lineWidth = 0.72;
        (window.payload?.world_map?.globe_rings || []).forEach((ring) => drawCoastline(ring, radius * 1.001, cx, cy));

        const data = globeState.data;
        if (data) {
          const backgroundLines = data.lines.filter((line) => !line.isActive);
          const activeLines = data.lines.filter((line) => line.isActive);
          const activePoints = data.points.filter((point) => point.isActive);

          backgroundLines.forEach((line) => drawArc(line, radius, cx, cy));
          context.save();
          context.globalCompositeOperation = "lighter";
          activeLines.forEach((line) => drawArc(line, radius, cx, cy));
          activeLines.forEach((line) => drawArcPulse(line, radius, cx, cy));
          context.restore();
          activePoints.forEach((point) => drawPoint(point, radius, cx, cy));
        }

        context.restore();
      }

      bridge.resizeGlobeScene = resize;
      bridge.updateGlobeScene = (data) => {
        globeState.data = data;
        draw();
      };

      canvas.addEventListener("pointerdown", (event) => {
        globeState.dragging = true;
        globeState.lastX = event.clientX;
        globeState.lastY = event.clientY;
        canvas.classList.add("is-dragging");
        canvas.setPointerCapture(event.pointerId);
      });

      canvas.addEventListener("pointermove", (event) => {
        if (!globeState.dragging) return;
        const dx = event.clientX - globeState.lastX;
        const dy = event.clientY - globeState.lastY;
        globeState.lastX = event.clientX;
        globeState.lastY = event.clientY;
        globeState.rotationLon += dx * 0.0085;
        globeState.rotationLat += dy * 0.0062;
        globeState.rotationLat = Math.max(-1.15, Math.min(1.15, globeState.rotationLat));
        draw();
      });

      function endDrag(event) {
        globeState.dragging = false;
        canvas.classList.remove("is-dragging");
        if (event && canvas.hasPointerCapture(event.pointerId)) {
          canvas.releasePointerCapture(event.pointerId);
        }
      }

      canvas.addEventListener("pointerup", endDrag);
      canvas.addEventListener("pointerleave", (event) => {
        endDrag(event);
        if (tooltip) tooltip.hidden = true;
      });
      canvas.addEventListener("pointermove", (event) => {
        if (globeState.dragging) return;
        updateTooltip(event.clientX, event.clientY);
      });
      canvas.addEventListener("wheel", (event) => {
        event.preventDefault();
        globeState.zoom *= event.deltaY < 0 ? 1.06 : 0.94;
        globeState.zoom = Math.max(0.78, Math.min(1.85, globeState.zoom));
        draw();
      }, { passive: false });

      window.addEventListener("resize", resize);
      if (bridge.pendingGlobeData) {
        globeState.data = bridge.pendingGlobeData;
      }
      resize();

      function animate(timeMs) {
        globeState.timeMs = timeMs || 0;
        if (canvas.style.visibility === "hidden") {
          window.setTimeout(() => window.requestAnimationFrame(animate), 240);
          return;
        }
        draw();
        window.requestAnimationFrame(animate);
      }
      window.requestAnimationFrame(animate);
    })();
  </script>
  <script>
    (() => {
      const bridge = window.__previewBridge = window.__previewBridge || {};
      const elements = bridge.googleGlobeElements || {};
      const host = elements.host || document.getElementById("googleGlobeHost");
      const input = elements.input || document.getElementById("googleApiKeyInput");
      const loadButton = elements.loadButton || document.getElementById("googleApiKeyLoad");
      const clearButton = elements.clearButton || document.getElementById("googleApiKeyClear");
      const status = elements.status || document.getElementById("googleApiKeyStatus");
      const setup = elements.setup || document.getElementById("globeSetup");
      const canvas = document.getElementById("globeCanvas");
      const tooltip = document.getElementById("globeTooltip");
      const note = document.querySelector(".globe-note");
      if (!host || !input || !loadButton || !clearButton || !status) return;

      const STORAGE_KEY = "preview.google.maps.api.key";
      try {
        window.localStorage.removeItem(STORAGE_KEY);
      } catch (_error) {
      }
      if (host) host.innerHTML = "";
      if (status) status.textContent = "当前版本使用本地三维地球渲染，不再启用 Google 3D 地图。";
      if (setup) setup.hidden = true;
      if (note) note.textContent = "拖动旋转，滚轮缩放";
      return;

      const googleState = {
        scriptPromise: null,
        scriptKey: "",
        libraries: null,
        mapElement: null,
        usingGoogle: false,
      };

      function escapeHtmlLite(text) {
        return String(text || "")
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/"/g, "&quot;");
      }

      function setStatus(message, tone = "neutral") {
        status.textContent = message;
        status.dataset.tone = tone;
        status.style.color = tone === "error"
          ? "#ffd1d1"
          : tone === "success"
            ? "#d7f6df"
            : tone === "warn"
              ? "#ffe7ba"
              : "rgba(233, 241, 247, 0.88)";
      }

      function setGoogleMode(active) {
        googleState.usingGoogle = active;
        host.classList.toggle("is-active", active);
        if (canvas) {
          canvas.style.visibility = active ? "hidden" : "visible";
          canvas.style.pointerEvents = active ? "none" : "auto";
        }
        if (tooltip) {
          tooltip.hidden = true;
        }
        if (setup) {
          setup.style.opacity = active ? "0.9" : "1";
        }
        if (note) {
          note.textContent = active
            ? "Google 官方 3D 地图已启用，可拖动、缩放和倾斜。"
            : "拖动旋转，滚轮缩放";
        }
      }

      function stageColor(stage, alpha = 1) {
        const palette = window.payload?.step_colors || {};
        const base = palette[stage] || "#91a3b0";
        if (!base.startsWith("#")) return base;
        if (alpha >= 0.999) return base;
        const hex = base.slice(1);
        const full = hex.length === 3 ? hex.split("").map((char) => `${char}${char}`).join("") : hex;
        const red = Number.parseInt(full.slice(0, 2), 16);
        const green = Number.parseInt(full.slice(2, 4), 16);
        const blue = Number.parseInt(full.slice(4, 6), 16);
        return `rgba(${red}, ${green}, ${blue}, ${alpha})`;
      }

      function pointStage(point) {
        if (point.stage) return point.stage;
        if (Array.isArray(point.stageEntries) && point.stageEntries.length) {
          return point.stageEntries[0][0];
        }
        return "";
      }

      function shortLabel(name, limit = 16) {
        return name.length > limit ? `${name.slice(0, limit - 1)}…` : name;
      }

      function toRadians(value) {
        return value * Math.PI / 180;
      }

      function toDegrees(value) {
        return value * 180 / Math.PI;
      }

      function normalizeLongitude(lon) {
        let value = Number(lon);
        while (value > 180) value -= 360;
        while (value < -180) value += 360;
        return value;
      }

      function longitudeDelta(fromLon, toLon) {
        const start = normalizeLongitude(fromLon);
        const end = normalizeLongitude(toLon);
        let delta = end - start;
        while (delta > 180) delta -= 360;
        while (delta < -180) delta += 360;
        return delta;
      }

      function geographicAverage(points) {
        if (!points.length) {
          return { lat: 18, lng: 12 };
        }
        let x = 0;
        let y = 0;
        let z = 0;
        points.forEach((point) => {
          const lat = toRadians(point.lat);
          const lon = toRadians(point.lon);
          x += Math.cos(lat) * Math.cos(lon);
          y += Math.cos(lat) * Math.sin(lon);
          z += Math.sin(lat);
        });
        const count = points.length || 1;
        x /= count;
        y /= count;
        z /= count;
        const hyp = Math.sqrt(x * x + y * y);
        return {
          lat: toDegrees(Math.atan2(z, hyp)),
          lng: normalizeLongitude(toDegrees(Math.atan2(y, x))),
        };
      }

      function estimateRange(points, hasFocus) {
        if (!points.length) return hasFocus ? 5800000 : 16000000;
        const center = geographicAverage(points);
        const latitudes = points.map((point) => point.lat);
        const longitudes = points.map((point) => longitudeDelta(center.lng, point.lon));
        const latSpan = Math.max(...latitudes) - Math.min(...latitudes);
        const lonSpan = Math.max(...longitudes) - Math.min(...longitudes);
        const span = Math.max(latSpan, lonSpan);
        const adaptive = hasFocus
          ? 1800000 + span * 105000
          : 9000000 + span * 70000;
        return Math.max(hasFocus ? 1800000 : 9000000, Math.min(hasFocus ? 9200000 : 18500000, adaptive));
      }

      function buildSceneSubset(data) {
        const activePoints = data.points.filter((point) => point.isActive || point.isFocus);
        const backgroundPoints = data.points.filter((point) => !point.isActive && !point.isFocus);
        const activeLines = data.lines.filter((line) => line.isActive || line.isFocus);
        const backgroundLines = data.lines.filter((line) => !line.isActive && !line.isFocus);
        const maxBackgroundPoints = data.hasFocus ? 220 : 520;
        const maxBackgroundLines = data.hasFocus ? 360 : 900;
        return {
          points: activePoints.concat(backgroundPoints.slice(0, maxBackgroundPoints)),
          lines: activeLines.concat(backgroundLines.slice(0, maxBackgroundLines)),
          truncated: backgroundPoints.length > maxBackgroundPoints || backgroundLines.length > maxBackgroundLines,
        };
      }

      function buildArcCoordinates(line) {
        const deltaLon = longitudeDelta(line.sourceLon, line.targetLon);
        const deltaLat = line.targetLat - line.sourceLat;
        const midLon = normalizeLongitude(line.sourceLon + deltaLon * 0.5);
        const midLat = Math.max(-72, Math.min(78, (line.sourceLat + line.targetLat) * 0.5 + Math.sign(deltaLat || 1) * Math.min(18, Math.abs(deltaLon) * 0.08 + Math.abs(deltaLat) * 0.06)));
        const arcHeight = Math.min(
          line.isActive ? 1600000 : 720000,
          (line.isActive ? 240000 : 90000) + (Math.abs(deltaLon) + Math.abs(deltaLat)) * (line.isActive ? 11500 : 6200)
        );
        return [
          { lat: line.sourceLat, lng: normalizeLongitude(line.sourceLon), altitude: 1800 },
          { lat: midLat, lng: midLon, altitude: arcHeight },
          { lat: line.targetLat, lng: normalizeLongitude(line.targetLon), altitude: 1800 },
        ];
      }

      function markerColor(point) {
        if (!point.isActive && !point.isFocus) return "#c8d1d9";
        return window.payload?.step_colors?.[pointStage(point)] || "#6fa8dc";
      }

      function labelForPoint(point, hasFocus) {
        if (point.isFocus) return shortLabel(point.label, 24);
        if (hasFocus && point.isActive) return shortLabel(point.label, 16);
        return "";
      }

      function updateGoogleCamera(mapElement, data, points) {
        const focusPoints = data.hasFocus
          ? points.filter((point) => point.isActive || point.isFocus)
          : points;
        const cameraPoints = focusPoints.length ? focusPoints : points;
        const center = geographicAverage(cameraPoints);
        mapElement.center = { lat: center.lat, lng: center.lng, altitude: 0 };
        mapElement.range = estimateRange(cameraPoints, data.hasFocus);
        mapElement.tilt = data.hasFocus ? 58 : 40;
        mapElement.heading = data.hasFocus ? 22 : 0;
        mapElement.roll = 0;
      }

      function clearGoogleOverlays() {
        if (!googleState.mapElement) return;
        while (googleState.mapElement.firstChild) {
          googleState.mapElement.removeChild(googleState.mapElement.firstChild);
        }
      }

      function ensureGoogleScript(key) {
        if (window.google?.maps?.importLibrary) {
          return Promise.resolve();
        }
        if (googleState.scriptPromise && googleState.scriptKey === key) {
          return googleState.scriptPromise;
        }
        googleState.scriptKey = key;
        googleState.scriptPromise = new Promise((resolve, reject) => {
          const callbackName = "__googleMaps3DPreviewReady";
          window[callbackName] = () => {
            resolve();
            try {
              delete window[callbackName];
            } catch (_error) {
              window[callbackName] = undefined;
            }
          };
          const existing = document.querySelector('script[data-google-maps-3d="preview"]');
          if (existing) {
            existing.remove();
          }
          const script = document.createElement("script");
          script.async = true;
          script.defer = true;
          script.dataset.googleMaps3d = "preview";
          script.src = `https://maps.googleapis.com/maps/api/js?key=${encodeURIComponent(key)}&v=beta&loading=async&language=zh-CN&region=CN&callback=${callbackName}`;
          script.onerror = () => {
            googleState.scriptPromise = null;
            reject(new Error("Google Maps JavaScript API 加载失败"));
          };
          document.head.appendChild(script);
        });
        return googleState.scriptPromise;
      }

      async function ensureGoogleLibraries(key) {
        if (googleState.libraries) return googleState.libraries;
        await ensureGoogleScript(key);
        const maps3d = await google.maps.importLibrary("maps3d");
        const marker = await google.maps.importLibrary("marker");
        googleState.libraries = { maps3d, marker };
        return googleState.libraries;
      }

      async function ensureGoogleMapElement(key) {
        const libraries = await ensureGoogleLibraries(key);
        const maps3d = libraries.maps3d || {};
        const Map3DElement = maps3d.Map3DElement;
        if (!Map3DElement) {
          throw new Error("当前 Google Maps API 未返回 3D 地图组件");
        }
        if (!googleState.mapElement) {
          const mapElement = new Map3DElement();
          mapElement.mode = maps3d.MapMode?.HYBRID || "HYBRID";
          mapElement.defaultUIDisabled = true;
          mapElement.gestureHandling = "GREEDY";
          mapElement.center = { lat: 18, lng: 12, altitude: 0 };
          mapElement.range = 16000000;
          mapElement.tilt = 38;
          mapElement.heading = 0;
          host.innerHTML = "";
          host.appendChild(mapElement);
          googleState.mapElement = mapElement;
        }
        return googleState.mapElement;
      }

      async function renderGoogleGlobeScene(data) {
        const key = input.value.trim();
        if (!key) {
          setGoogleMode(false);
          setStatus("尚未配置 Google Maps API key，当前继续显示本地 3D 地球。", "warn");
          return;
        }

        try {
          const libraries = await ensureGoogleLibraries(key);
          const mapElement = await ensureGoogleMapElement(key);
          const maps3d = libraries.maps3d || {};
          const markerLib = libraries.marker || {};
          const Polyline3DElement = maps3d.Polyline3DElement;
          const Marker3DInteractiveElement = maps3d.Marker3DInteractiveElement;
          const AltitudeMode = maps3d.AltitudeMode || {};
          const PinElement = markerLib.PinElement;
          if (!Polyline3DElement || !Marker3DInteractiveElement || !PinElement) {
            throw new Error("Google 3D 地图依赖组件未完整加载");
          }

          setGoogleMode(true);
          clearGoogleOverlays();

          if (!data || !Array.isArray(data.points) || !data.points.length) {
            setStatus("Google 3D 已连接，但当前没有可视化的地理节点。", "success");
            return;
          }

          const scene = buildSceneSubset(data);
          const scenePoints = scene.points.slice().sort((left, right) => {
            if (left.isFocus !== right.isFocus) return left.isFocus ? 1 : -1;
            if (left.isActive !== right.isActive) return left.isActive ? 1 : -1;
            return String(left.label).localeCompare(String(right.label));
          });
          const sceneLines = scene.lines.slice().sort((left, right) => {
            if (left.isActive !== right.isActive) return left.isActive ? 1 : -1;
            if (left.isFocus !== right.isFocus) return left.isFocus ? 1 : -1;
            return 0;
          });

          updateGoogleCamera(mapElement, data, scenePoints);

          sceneLines.forEach((line) => {
            const stage = line.stage || "";
            const polyline = new Polyline3DElement();
            polyline.coordinates = buildArcCoordinates(line);
            polyline.altitudeMode = AltitudeMode.ABSOLUTE || "ABSOLUTE";
            polyline.strokeColor = line.isActive
              ? stageColor(stage, line.isFocus ? 0.92 : 0.76)
              : "rgba(196, 207, 218, 0.34)";
            polyline.outerColor = line.isActive
              ? stageColor(stage, line.isFocus ? 0.34 : 0.18)
              : "rgba(255,255,255,0.08)";
            polyline.strokeWidth = line.isActive ? (line.isFocus ? 4.8 : 3.4) : 1.05;
            polyline.outerWidth = line.isActive ? 1.2 : 0.45;
            polyline.drawsOccludedSegments = Boolean(line.isActive);
            polyline.zIndex = line.isFocus ? 240 : (line.isActive ? 180 : 40);
            mapElement.appendChild(polyline);
          });

          scenePoints.forEach((point) => {
            const pin = new PinElement({
              background: markerColor(point),
              borderColor: point.isFocus ? "#ffffff" : "#e7eef4",
              glyphColor: "#ffffff",
              scale: point.isFocus ? 0.9 : (point.isActive ? 0.76 : 0.58),
            });
            const marker = new Marker3DInteractiveElement();
            marker.position = { lat: point.lat, lng: normalizeLongitude(point.lon), altitude: point.isFocus ? 1200 : 0 };
            marker.altitudeMode = AltitudeMode.RELATIVE_TO_GROUND || "RELATIVE_TO_GROUND";
            marker.drawsWhenOccluded = true;
            marker.sizePreserved = true;
            marker.extruded = Boolean(point.isFocus);
            marker.label = labelForPoint(point, data.hasFocus);
            marker.title = point.label;
            marker.zIndex = point.isFocus ? 360 : (point.isActive ? 240 : 80);
            marker.append(pin.element || pin);
            marker.addEventListener("gmp-click", () => {
              if (typeof bridge.toggleMapFocus === "function") {
                bridge.toggleMapFocus(point.label);
              }
            });
            mapElement.appendChild(marker);
          });

          setStatus(
            `Google 3D 已启用 | 点位 ${scenePoints.length} | 连线 ${sceneLines.length}${scene.truncated ? " | 背景关系已适度抽样以保证流畅度" : ""}`,
            "success"
          );
        } catch (error) {
          setGoogleMode(false);
          setStatus(`Google 3D 加载失败：${error?.message || error}`, "error");
        }
      }

      bridge.updateGoogleGlobeScene = (data) => {
        if (!input.value.trim()) return;
        renderGoogleGlobeScene(data);
      };
      bridge.resizeGoogleGlobeScene = () => {
        if (googleState.usingGoogle && googleState.mapElement) {
          googleState.mapElement.center = googleState.mapElement.center;
        }
      };

      loadButton.addEventListener("click", () => {
        const key = input.value.trim();
        if (!key) {
          setStatus("请先输入 Google Maps API key。", "warn");
          input.focus();
          return;
        }
        window.localStorage.setItem(STORAGE_KEY, key);
        renderGoogleGlobeScene(bridge.pendingGlobeData || null);
      });

      clearButton.addEventListener("click", () => {
        window.localStorage.removeItem(STORAGE_KEY);
        input.value = "";
        host.innerHTML = "";
        googleState.mapElement = null;
        googleState.libraries = null;
        setGoogleMode(false);
        setStatus("已清除 Google Maps API key，已切回本地 3D 地球。", "warn");
      });

      const savedKey = window.localStorage.getItem(STORAGE_KEY);
      if (savedKey) {
        input.value = savedKey;
        renderGoogleGlobeScene(bridge.pendingGlobeData || null);
      } else {
        setGoogleMode(false);
      }
    })();
  </script>
  <script>
    (() => {
      const bridge = window.__previewBridge = window.__previewBridge || {};
      const host = document.getElementById("googleGlobeHost");
      const canvas = document.getElementById("globeCanvas");
      const tooltip = document.getElementById("globeTooltip");
      const note = document.querySelector(".globe-note");
      const SATELLITE_PREVIEW_IMAGE = "assets/earth_satellite_5400.jpg";
      const SATELLITE_FALLBACK_IMAGE = "assets/earth_satellite_21600.jpg";
      const GLOBE_BUMP_IMAGE = "assets/earth_topology.png";
      const COUNTRY_LABEL_ALTITUDE = 1.72;
      const MAX_RENDER_PIXEL_RATIO = 1.85;
      const INTERACTION_RENDER_PIXEL_RATIO = 1.0;
      if (!host || typeof window.Globe !== "function") {
        return;
      }

      let mouseX = 24;
      let mouseY = 24;
      let hoverPayload = null;
      let globeInstance = null;
      let latestGlobeData = null;
      let resizeObserver = null;
      let resizeFrameHandle = 0;
      let updateFrameHandle = 0;
      let restoreQualityHandle = 0;
      let highResTextureRequested = false;
      let highResTextureApplied = false;
      let currentRenderPixelRatio = MAX_RENDER_PIXEL_RATIO;
      let pendingSceneData = null;
      function showTooltip(content) {
        if (!tooltip) return;
        if (!content) {
          tooltip.hidden = true;
          return;
        }
        tooltip.hidden = false;
        const rect = host.getBoundingClientRect();
        tooltip.style.left = `${Math.max(12, Math.min(mouseX, rect.width - 280))}px`;
        tooltip.style.top = `${Math.max(12, Math.min(mouseY, rect.height - 110))}px`;
        tooltip.innerHTML = content;
      }

      function setSatelliteMode(active) {
        host.classList.toggle("is-active", active);
        const labelLayer = document.getElementById("globeLabelLayer");
        if (labelLayer) {
          labelLayer.hidden = true;
          labelLayer.innerHTML = "";
        }
        if (canvas) {
          canvas.style.visibility = active ? "hidden" : "visible";
          canvas.style.pointerEvents = active ? "none" : "auto";
        }
        if (note) {
          note.textContent = active
            ? "卫星地球模式，可拖动旋转、滚轮缩放"
            : "拖动旋转，滚轮缩放";
        }
      }

      function lineColor(stage, active) {
        if (!active) return "rgba(150, 170, 190, 0.18)";
        return stepColors[stage] || "#7fd0ff";
      }

      function pointColor(point) {
        return point.isFocus ? "#f6fbff" : (stepColors[point.stage] || "#7fd0ff");
      }

      function radians(value) {
        return value * Math.PI / 180;
      }

      function degrees(value) {
        return value * 180 / Math.PI;
      }

      function angularDistanceRadians(line) {
        const startLat = radians(line.sourceLat);
        const endLat = radians(line.targetLat);
        const deltaLat = endLat - startLat;
        const deltaLng = radians(line.targetLon - line.sourceLon);
        const sinLat = Math.sin(deltaLat / 2);
        const sinLng = Math.sin(deltaLng / 2);
        const haversine = sinLat * sinLat + Math.cos(startLat) * Math.cos(endLat) * sinLng * sinLng;
        return 2 * Math.atan2(Math.sqrt(haversine), Math.sqrt(Math.max(0, 1 - haversine)));
      }

      function arcPeakAltitude(line) {
        const distance = angularDistanceRadians(line);
        const adaptive = distance * (line.isFocus ? 0.7 : 0.56);
        return Math.max(line.isFocus ? 0.08 : 0.055, Math.min(line.isFocus ? 0.2 : 0.16, adaptive));
      }

      function normalizeVector(vector) {
        const length = Math.hypot(vector.x, vector.y, vector.z) || 1;
        return {
          x: vector.x / length,
          y: vector.y / length,
          z: vector.z / length,
        };
      }

      function latLonToVector(lat, lon) {
        const latRad = radians(lat);
        const lonRad = radians(lon);
        const cosLat = Math.cos(latRad);
        return {
          x: Math.cos(latRad) * Math.cos(lonRad),
          y: Math.sin(latRad),
          z: Math.cos(latRad) * Math.sin(lonRad),
        };
      }

      function vectorToLatLon(vector) {
        const unit = normalizeVector(vector);
        return {
          lat: degrees(Math.asin(unit.y)),
          lng: degrees(Math.atan2(unit.z, unit.x)),
        };
      }

      function bearingDegrees(fromLat, fromLng, toLat, toLng) {
        const phi1 = radians(fromLat);
        const phi2 = radians(toLat);
        const lambda1 = radians(fromLng);
        const lambda2 = radians(toLng);
        const y = Math.sin(lambda2 - lambda1) * Math.cos(phi2);
        const x = Math.cos(phi1) * Math.sin(phi2) - Math.sin(phi1) * Math.cos(phi2) * Math.cos(lambda2 - lambda1);
        return degrees(Math.atan2(y, x));
      }

      function slerpVectors(start, end, t) {
        const dot = Math.max(-1, Math.min(1, start.x * end.x + start.y * end.y + start.z * end.z));
        const omega = Math.acos(dot);
        if (omega < 1e-6) {
          return normalizeVector({
            x: start.x + (end.x - start.x) * t,
            y: start.y + (end.y - start.y) * t,
            z: start.z + (end.z - start.z) * t,
          });
        }
        const sinOmega = Math.sin(omega) || 1;
        const scaleStart = Math.sin((1 - t) * omega) / sinOmega;
        const scaleEnd = Math.sin(t * omega) / sinOmega;
        return normalizeVector({
          x: start.x * scaleStart + end.x * scaleEnd,
          y: start.y * scaleStart + end.y * scaleEnd,
          z: start.z * scaleStart + end.z * scaleEnd,
        });
      }

      function buildOverlayElements(data, altitude) {
        const overlays = [];
        if (data?.hasFocus && Number.isFinite(altitude) && altitude <= COUNTRY_LABEL_ALTITUDE) {
          (data.countries || []).forEach((country) => {
            overlays.push({
              type: "country",
              lat: country.lat,
              lng: country.lon,
              altitude: 0.028,
              text: country.name || "",
            });
          });
        }
        return overlays;
      }

      function currentAltitude() {
        if (!globeInstance || typeof globeInstance.pointOfView !== "function") return Infinity;
        const view = globeInstance.pointOfView();
        return typeof view?.altitude === "number" ? view.altitude : Infinity;
      }

      function refreshCountryLabels(altitudeOverride) {
        if (!globeInstance) return;
        const altitude = Number.isFinite(altitudeOverride) ? altitudeOverride : currentAltitude();
        globeInstance.htmlElementsData(buildOverlayElements(latestGlobeData, altitude));
      }

      function flowLineColor(stage, isFocus) {
        const baseColor = stepColors[stage] || "#7fd0ff";
        const expanded = baseColor.startsWith("#")
          ? (baseColor.length === 4
              ? baseColor.slice(1).split("").map((char) => `${char}${char}`).join("")
              : baseColor.slice(1))
          : "7fd0ff";
        const red = parseInt(expanded.slice(0, 2), 16);
        const green = parseInt(expanded.slice(2, 4), 16);
        const blue = parseInt(expanded.slice(4, 6), 16);
        const mix = isFocus ? 0.62 : 0.5;
        const alpha = isFocus ? 0.98 : 0.94;
        const flowRed = Math.round(red + (255 - red) * mix);
        const flowGreen = Math.round(green + (255 - green) * mix);
        const flowBlue = Math.round(blue + (255 - blue) * mix);
        return `rgba(${flowRed}, ${flowGreen}, ${flowBlue}, ${alpha})`;
      }

      function buildRenderLines(lines) {
        return lines.flatMap((line, lineIndex) => {
          const base = { ...line, renderKind: "base" };
          if (!(line.isActive || line.isFocus)) {
            return [base];
          }
          const pulse = {
            ...line,
            renderKind: "pulse",
            pulseOffset: ((lineIndex * 0.173) + Math.abs(line.sourceLat + line.targetLon) * 0.0037) % 1,
          };
          return [base, pulse];
        });
      }

      function syncRendererQuality(pixelRatio = currentRenderPixelRatio) {
        if (!globeInstance || typeof globeInstance.renderer !== "function") return;
        const renderer = globeInstance.renderer();
        if (!renderer) return;
        if (typeof renderer.setPixelRatio === "function") {
          renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, pixelRatio));
        }
        if (typeof renderer.setSize === "function") {
          renderer.setSize(host.clientWidth || 1200, host.clientHeight || 620, false);
        }
        const maxAnisotropy = renderer.capabilities?.getMaxAnisotropy?.() || 1;
        if (typeof globeInstance.globeMaterial === "function") {
          const material = globeInstance.globeMaterial();
          if (material?.map) {
            material.map.anisotropy = Math.max(4, Math.min(maxAnisotropy, 16));
            material.map.needsUpdate = true;
          }
          if (material?.bumpMap) {
            material.bumpMap.anisotropy = Math.max(4, Math.min(maxAnisotropy, 16));
            material.bumpMap.needsUpdate = true;
          }
          if (typeof material?.specular?.set === "function") {
            material.specular.set("#0d1520");
          }
          if (typeof material?.emissive?.set === "function") {
            material.emissive.set("#06101a");
            material.emissiveIntensity = 0.05;
          }
          material.bumpScale = 3.2;
          material.shininess = 2.1;
          material.needsUpdate = true;
        }
      }

      function setInteractiveQuality() {
        currentRenderPixelRatio = INTERACTION_RENDER_PIXEL_RATIO;
        syncRendererQuality();
        if (restoreQualityHandle) {
          window.clearTimeout(restoreQualityHandle);
        }
        restoreQualityHandle = window.setTimeout(() => {
          currentRenderPixelRatio = MAX_RENDER_PIXEL_RATIO;
          syncRendererQuality();
        }, 180);
      }

      function scheduleGlobeSizeSync() {
        if (resizeFrameHandle) return;
        resizeFrameHandle = window.requestAnimationFrame(() => {
          resizeFrameHandle = 0;
          syncGlobeSize();
        });
      }

      function syncGlobeSize() {
        if (!globeInstance) return;
        globeInstance.width(host.clientWidth || 1200).height(host.clientHeight || 620);
        syncRendererQuality();
        refreshCountryLabels();
      }

      function ensureHighResTexture() {
        if (highResTextureRequested || highResTextureApplied || !globeInstance) return;
        highResTextureRequested = true;
        const image = new Image();
        image.decoding = "async";
        image.loading = "eager";
        image.onload = () => {
          if (!globeInstance) return;
          globeInstance.globeImageUrl(SATELLITE_FALLBACK_IMAGE);
          highResTextureApplied = true;
          currentRenderPixelRatio = MAX_RENDER_PIXEL_RATIO;
          syncRendererQuality();
        };
        image.src = SATELLITE_FALLBACK_IMAGE;
      }

      function createGlobe() {
        if (globeInstance) return globeInstance;
        globeInstance = new window.Globe(host, {
          rendererConfig: {
            antialias: true,
            alpha: true,
            powerPreference: "high-performance",
          },
        })
          .width(host.clientWidth || 1200)
          .height(host.clientHeight || 620)
          .backgroundColor("rgba(0,0,0,0)")
          .globeImageUrl(SATELLITE_PREVIEW_IMAGE)
          .bumpImageUrl(GLOBE_BUMP_IMAGE)
          .showAtmosphere(true)
          .atmosphereColor("#9fd6ff")
          .atmosphereAltitude(0.17)
          .globeCurvatureResolution(2)
          .arcAltitudeAutoScale(0.22)
          .arcStroke((d) => {
            if (d.renderKind === "pulse") return d.isFocus ? 0.42 : 0.32;
            return d.isFocus ? 0.24 : 0.16;
          })
          .arcStartAltitude((d) => d.isFocus ? 0.028 : 0.018)
          .arcEndAltitude((d) => d.isFocus ? 0.028 : 0.018)
          .arcAltitude((d) => arcPeakAltitude(d))
          .arcCurveResolution(96)
          .arcCircularResolution(10)
          .arcDashLength((d) => d.renderKind === "pulse" ? (d.isFocus ? 0.16 : 0.13) : 1)
          .arcDashGap((d) => d.renderKind === "pulse" ? 1.15 : 0)
          .arcDashInitialGap((d) => d.renderKind === "pulse" ? (d.pulseOffset || 0) : 0)
          .arcDashAnimateTime((d) => d.renderKind === "pulse" ? (d.isFocus ? 2600 : 3200) : 0)
          .arcsTransitionDuration(0)
          .pointAltitude((d) => d.isFocus ? 0.028 : 0.018)
          .pointRadius((d) => d.isFocus ? 0.16 : 0.1)
          .pointsTransitionDuration(0)
          .htmlTransitionDuration(0)
          .onPointHover((point) => {
            hoverPayload = point
              ? `<strong>${point.label}</strong><div class="meta">${localizeStep(point.stage || "")}${point.country ? " | " : ""}${localizeCountry(point.country || "")}</div>${point.connectionsText ? `<div class="links">关联节点: ${point.connectionsText}</div>` : ""}`
              : null;
            showTooltip(hoverPayload);
          })
          .onPointClick((point) => {
            if (point && typeof bridge.toggleMapFocus === "function") {
              bridge.toggleMapFocus(point.label);
            }
          })
          .onZoom((view) => {
            refreshCountryLabels(typeof view?.altitude === "number" ? view.altitude : currentAltitude());
          })
          .htmlLat("lat")
          .htmlLng("lng")
          .htmlAltitude((item) => item.altitude ?? 0.028)
          .htmlElement((item) => {
            const label = document.createElement("div");
            label.className = "globe-country-label";
            label.lang = "en";
            label.textContent = item.text;
            return label;
          });

        const controls = globeInstance.controls?.();
        if (controls) {
          controls.autoRotate = false;
          controls.enablePan = false;
          controls.minDistance = 180;
          controls.maxDistance = 420;
          controls.enableDamping = true;
          controls.dampingFactor = 0.08;
          if (typeof controls.addEventListener === "function") {
            controls.addEventListener("start", setInteractiveQuality);
            controls.addEventListener("end", () => {
              if (restoreQualityHandle) {
                window.clearTimeout(restoreQualityHandle);
              }
              restoreQualityHandle = window.setTimeout(() => {
                currentRenderPixelRatio = MAX_RENDER_PIXEL_RATIO;
                syncRendererQuality();
              }, 140);
            });
          }
        }
        syncRendererQuality();
        ensureHighResTexture();
        if (typeof ResizeObserver !== "undefined") {
          resizeObserver = new ResizeObserver(() => {
            scheduleGlobeSizeSync();
          });
          resizeObserver.observe(host);
        }
        host.addEventListener("pointerdown", () => {
          setInteractiveQuality();
        }, { passive: true });
        host.addEventListener("wheel", () => {
          setInteractiveQuality();
        }, { passive: true });
        host.addEventListener("mousemove", (event) => {
          const rect = host.getBoundingClientRect();
          mouseX = event.clientX - rect.left + 16;
          mouseY = event.clientY - rect.top + 16;
          if (hoverPayload) {
            showTooltip(hoverPayload);
          }
        });
        host.addEventListener("mouseleave", () => {
          hoverPayload = null;
          showTooltip(null);
        });
        return globeInstance;
      }

      function updateWebGlobe(data) {
        if (!data) {
          latestGlobeData = null;
          setSatelliteMode(false);
          return;
        }
        const globe = createGlobe();
        setSatelliteMode(true);
        latestGlobeData = data;

        const activePoints = (data.points || []).filter((point) => point.isActive || point.isFocus);
        const activeLines = (data.lines || []).filter((line) => line.isActive || line.isFocus);
        const renderLines = buildRenderLines(activeLines);

        globe
          .pointsData(activePoints)
          .pointLat("lat")
          .pointLng("lon")
          .pointColor((point) => pointColor(point))
          .arcsData(renderLines)
          .arcStartLat("sourceLat")
          .arcStartLng("sourceLon")
          .arcEndLat("targetLat")
          .arcEndLng("targetLon")
          .arcColor((line) => line.renderKind === "pulse"
            ? flowLineColor(line.stage, line.isFocus)
            : colorWithAlpha(stepColors[line.stage] || "#7fd0ff", line.isFocus ? 0.42 : 0.28));

        const focusPoints = activePoints.length ? activePoints : (data.points || []);
        if (focusPoints.length) {
          const avgLat = focusPoints.reduce((sum, point) => sum + point.lat, 0) / focusPoints.length;
          const avgLon = focusPoints.reduce((sum, point) => sum + point.lon, 0) / focusPoints.length;
          globe.pointOfView({
            lat: avgLat,
            lng: avgLon,
            altitude: data.hasFocus ? 1.65 : 2.05,
          }, 900);
        }
        scheduleGlobeSizeSync();
        window.setTimeout(() => scheduleGlobeSizeSync(), 180);
        refreshCountryLabels();
      }

      bridge.updateWebGlobeScene = (data) => {
        pendingSceneData = data;
        if (updateFrameHandle) return;
        updateFrameHandle = window.requestAnimationFrame(() => {
          updateFrameHandle = 0;
          updateWebGlobe(pendingSceneData);
        });
      };
      bridge.resizeWebGlobeScene = () => {
        scheduleGlobeSizeSync();
      };

      if (bridge.pendingGlobeData) {
        updateWebGlobe(bridge.pendingGlobeData);
      }
    })();
  </script>
</body>
</html>
"""
    return html.replace("__PAYLOAD__", payload_json)


def export_original_style_preview(
    links_rows: list[dict[str, str]],
    matrix_rows: list[dict[str, str]],
    country_rows: list[dict[str, str]],
    world_topology: dict[str, Any],
    output_dir: Path,
    *,
    focus_company: str,
    depth: int,
    limit: int,
) -> dict[str, int]:
    texture_path = output_dir / GLOBE_TEXTURE_RELATIVE_PATH
    texture_preview_path = output_dir / GLOBE_TEXTURE_PREVIEW_RELATIVE_PATH
    globe_js_path = output_dir / GLOBE_JS_RELATIVE_PATH
    bump_path = output_dir / GLOBE_BUMP_RELATIVE_PATH
    texture_path.parent.mkdir(parents=True, exist_ok=True)
    if not texture_path.exists() or texture_path.stat().st_size < 5_000_000:
        urllib.request.urlretrieve(GLOBE_TEXTURE_SOURCE_URL, texture_path)
    if not texture_preview_path.exists() or texture_preview_path.stat().st_size < 1_000_000:
        urllib.request.urlretrieve(GLOBE_TEXTURE_PREVIEW_URL, texture_preview_path)
    if not globe_js_path.exists() or globe_js_path.stat().st_size < 100_000:
        urllib.request.urlretrieve(GLOBE_JS_URL, globe_js_path)
    if not bump_path.exists() or bump_path.stat().st_size < 50_000:
        urllib.request.urlretrieve(GLOBE_BUMP_URL, bump_path)
    payload = build_classic_preview_payload(
        links_rows,
        matrix_rows,
        country_rows,
        world_topology,
        focus_company=focus_company,
        depth=depth,
        limit=limit,
    )
    output_path = output_dir / "graph_preview.html"
    output_path.write_text(build_classic_preview_html(payload), encoding="utf-8")
    return {
        "preview_total_companies": len(payload["companies"]),
        "preview_total_transactions": len(payload["links"]),
        "preview_default_depth": payload["default_depth"],
    }
