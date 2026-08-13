from __future__ import annotations

import json
from pathlib import Path
from typing import Any


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
  <title>钴供应链图谱</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
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
      height: max(calc(100vh - 36px), 790px);
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
    .empty {
      padding: 36px 20px;
      color: var(--muted);
      text-align: center;
    }
    @media (max-width: 860px) {
      .shell {
        height: auto;
        grid-template-columns: auto;
        grid-template-rows: 28px auto clamp(460px, 70vh, 600px) 320px;
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
    const companyList = document.getElementById("companyList");
    const companyInput = document.getElementById("companyInput");
    const searchButton = document.getElementById("searchButton");
    const resetButton = document.getElementById("resetButton");
    const searchStatus = document.getElementById("searchStatus");
    const modeToggle = document.getElementById("modeToggle");
    const chainsSvg = document.getElementById("chainsSvg");
    const mapSvg = document.getElementById("mapSvg");
    const softHighlightButton = document.getElementById("softHighlightButton");
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
    let softHighlightMode = false;
    let renderFrameHandle = 0;
    const companyIndexByNormalizedName = new Map();
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
        chainsSvg.removeAttribute("width");
        chainsSvg.removeAttribute("height");
        chainsSvg.removeAttribute("viewBox");
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
      chainsSvg.setAttribute("width", String(width));
      chainsSvg.setAttribute("height", String(height));
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
      mapSvg.setAttribute("viewBox", `0 0 ${width} ${height}`);
      mapSvg.innerHTML = "";
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
        return;
      }
      mapEmpty.hidden = true;

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
    softHighlightButton.addEventListener("click", () => {
      softHighlightMode = !softHighlightMode;
      softHighlightButton.classList.toggle("is-active", softHighlightMode);
      render();
    });
    render();
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
