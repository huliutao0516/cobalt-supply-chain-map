#!/usr/bin/env python3
"""
Build a graph dataset from https://supplychains.resourcematters.org/explore.

The site is a JavaScript app backed by static data files. This script downloads
those source files, normalizes the entities/relations, exports Neo4j-friendly
CSV files, and can optionally load everything into Neo4j directly.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import sys
import unicodedata
import zipfile
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError
from xml.sax.saxutils import escape
from pathlib import Path
from typing import Any

from preview_classic import export_original_style_preview

BASE_URL = "https://supplychains.resourcematters.org"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": f"{BASE_URL}/explore",
}
SOURCE_FILES = {
    "links": "data/links.csv",
    "countries": "data/countries.csv",
    "world": "data/world.json",
    "bhrrc_companies": "data/bhrrc-companies.csv",
    "bhrrc_news": "data/bhrrc-news.json",
}

ENTITY_EXPORTS = {
    "companies.csv": "companies",
    "facilities.csv": "facilities",
    "countries.csv": "countries",
    "commodities.csv": "commodities",
    "chain_steps.csv": "chain_steps",
    "chain_links.csv": "chain_links",
    "transactions.csv": "transactions",
    "sources.csv": "sources",
}

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
    "Electric car manufacturing",
    "Electric scooter manufacturing",
]

STEP_COLUMN_RENAMES = {
    "Electric car manufacturing": "Electric car/scooter manufacturing",
    "Electric scooter manufacturing": "Electric car/scooter manufacturing",
}


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\ufeff", "").strip()
    return re.sub(r"\s+", " ", text)


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_value).strip("-").lower()
    return slug or "unknown"


def stable_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]


def parse_float(value: str) -> float | None:
    value = clean_text(value)
    if not value:
        return None
    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None


def parse_number(value: str) -> float | None:
    value = clean_text(value)
    if not value:
        return None
    if re.fullmatch(r"[-+]?\d+(?:\.\d+)?", value.replace(",", "")):
        return float(value.replace(",", ""))
    return None


def split_multi_value(value: str) -> list[str]:
    value = clean_text(value)
    if not value:
        return []
    parts = re.split(r"\s*(?:/|;|\|)\s*", value)
    return [clean_text(part) for part in parts if clean_text(part)]


def sanitize_properties(properties: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in properties.items():
        if value is None:
            continue
        if isinstance(value, str):
            value = clean_text(value)
            if not value:
                continue
        cleaned[key] = value
    return cleaned


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def download_file(url: str, destination: Path) -> None:
    ensure_directory(destination.parent)
    request = urllib.request.Request(url, headers=DEFAULT_HEADERS)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            destination.write_bytes(response.read())
    except HTTPError as exc:
        raise RuntimeError(
            f"Failed to download {url} (HTTP {exc.code}). "
            "The site may be blocking non-browser requests."
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"Failed to download {url}: {exc.reason}") from exc


def ensure_source_files(data_dir: Path, refresh: bool) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for key, relative_path in SOURCE_FILES.items():
        destination = data_dir / Path(relative_path).name
        paths[key] = destination
        if refresh or not destination.exists():
            download_file(f"{BASE_URL}/{relative_path}", destination)
    return paths


def read_delimited_rows(path: Path, delimiter: str) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=delimiter)
        raw_headers = next(reader)
        headers: list[str] = []
        seen_headers: dict[str, int] = {}

        for index, raw_header in enumerate(raw_headers):
            header = clean_text(raw_header) or f"_blank_{index}"
            count = seen_headers.get(header, 0)
            seen_headers[header] = count + 1
            if count:
                header = f"{header}_{count + 1}"
            headers.append(header)

        rows: list[dict[str, str]] = []
        for raw_row in reader:
            padded = list(raw_row) + [""] * max(0, len(headers) - len(raw_row))
            row = {
                headers[index]: clean_text(padded[index])
                for index in range(len(headers))
            }
            rows.append(row)
        return rows


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def alias_key(value: str) -> str:
    return slugify(clean_text(value))


class GraphBuilder:
    def __init__(
        self,
        bhrrc_index: dict[str, dict[str, str]],
        country_centroids: dict[str, dict[str, str]],
    ) -> None:
        self.bhrrc_index = bhrrc_index
        self.country_centroids = country_centroids

        self.companies: dict[str, dict[str, Any]] = {}
        self.facilities: dict[str, dict[str, Any]] = {}
        self.countries: dict[str, dict[str, Any]] = {}
        self.commodities: dict[str, dict[str, Any]] = {}
        self.chain_steps: dict[str, dict[str, Any]] = {}
        self.chain_links: dict[str, dict[str, Any]] = {}
        self.transactions: dict[str, dict[str, Any]] = {}
        self.sources: dict[str, dict[str, Any]] = {}

        self.relationships: list[dict[str, Any]] = []
        self.relationship_keys: set[tuple[Any, ...]] = set()

    def add_company(
        self,
        name: str,
        *,
        company_type: str = "",
        role_hint: str = "",
    ) -> str:
        name = clean_text(name)
        if not name:
            raise ValueError("Company name cannot be empty.")

        key = alias_key(name)
        existing = self.companies.get(key)
        if existing is None:
            bhrrc = self.bhrrc_index.get(key, {})
            existing = {
                "node_id": f"company::{slugify(name)}",
                "name": name,
                "bhrrc_link": bhrrc.get("BHRRC link", ""),
                "bhrrc_id": bhrrc.get("ID", ""),
                "role_tags": set(),
                "company_type_tags": set(),
                "_coordinate_counts": {},
                "_country_counts": {},
            }
            self.companies[key] = existing

        if role_hint:
            existing["role_tags"].add(role_hint)
        if company_type:
            existing["company_type_tags"].add(company_type)
        return existing["node_id"]

    def add_company_location(
        self,
        company_name: str,
        *,
        lat: str,
        lon: str,
        country_name: str,
    ) -> None:
        company_name = clean_text(company_name)
        if not company_name:
            return

        existing = self.companies.get(alias_key(company_name))
        if existing is None:
            return

        country_name = clean_text(country_name)
        if country_name:
            country_counts = existing["_country_counts"]
            country_counts[country_name] = country_counts.get(country_name, 0) + 1

        lat_value = parse_float(lat)
        lon_value = parse_float(lon)
        if lat_value is None or lon_value is None:
            return

        coordinate_key = (lat_value, lon_value, country_name)
        coordinate_counts = existing["_coordinate_counts"]
        coordinate_counts[coordinate_key] = coordinate_counts.get(coordinate_key, 0) + 1

    def add_facility(
        self,
        *,
        name: str,
        clean_name: str,
        facility_type: str,
        place: str,
        country_name: str,
        lat: str,
        lon: str,
    ) -> str | None:
        name = clean_text(name)
        clean_name = clean_text(clean_name)
        place = clean_text(place)
        country_name = clean_text(country_name)
        facility_type = clean_text(facility_type)

        if not any([name, clean_name, place, country_name]):
            return None

        display_name = clean_name or name or place
        key = alias_key("|".join([display_name, place, country_name, facility_type]))
        existing = self.facilities.get(key)
        if existing is None:
            existing = {
                "node_id": f"facility::{stable_hash(key)}",
                "name": name or display_name,
                "display_name": display_name,
                "facility_type": facility_type,
                "place": place,
                "country_name": country_name,
                "lat": parse_float(lat),
                "lon": parse_float(lon),
            }
            self.facilities[key] = existing
        return existing["node_id"]

    def add_country(self, name: str) -> str | None:
        name = clean_text(name)
        if not name:
            return None

        key = alias_key(name)
        existing = self.countries.get(key)
        if existing is None:
            centroid = self.country_centroids.get(key, {})
            existing = {
                "node_id": f"country::{slugify(name)}",
                "name": name,
                "centroid_name": centroid.get("name", ""),
                "lat": parse_float(centroid.get("lat", "")),
                "lon": parse_float(centroid.get("lon", "")),
            }
            self.countries[key] = existing
        return existing["node_id"]

    def add_commodity(self, name: str, *, commodity_type: str, direction: str) -> str | None:
        name = clean_text(name)
        if not name:
            return None

        key = alias_key(name)
        existing = self.commodities.get(key)
        if existing is None:
            existing = {
                "node_id": f"commodity::{slugify(name)}",
                "name": name,
                "commodity_types": set(),
                "direction_tags": set(),
            }
            self.commodities[key] = existing

        if commodity_type:
            existing["commodity_types"].add(commodity_type)
        if direction:
            existing["direction_tags"].add(direction)
        return existing["node_id"]

    def add_chain_step(self, name: str) -> str | None:
        name = clean_text(name)
        if not name:
            return None

        key = alias_key(name)
        existing = self.chain_steps.get(key)
        if existing is None:
            existing = {
                "node_id": f"chain_step::{slugify(name)}",
                "name": name,
            }
            self.chain_steps[key] = existing
        return existing["node_id"]

    def add_chain_link(self, name: str) -> str | None:
        name = clean_text(name)
        if not name:
            return None

        key = alias_key(name)
        existing = self.chain_links.get(key)
        if existing is None:
            existing = {
                "node_id": f"chain_link::{slugify(name)}",
                "name": name,
            }
            self.chain_links[key] = existing
        return existing["node_id"]

    def add_transaction(self, row: dict[str, str]) -> str:
        transaction_id = clean_text(row["ID"])
        node_id = f"transaction::{transaction_id}"
        if node_id not in self.transactions:
            self.transactions[node_id] = {
                "node_id": node_id,
                "transaction_id": transaction_id,
                "link_in_chain": clean_text(row["Link in the chain"]),
                "transaction_realised": clean_text(row["Transaction realised?"]),
                "transaction_notes": clean_text(row["Transaction notes"]),
                "date_of_transaction": clean_text(row["Date of transaction"]),
                "expected_date_of_transaction": clean_text(row["Expected date of transaction"]),
                "amount_usd_raw": clean_text(row["Amount (USD)"]),
                "amount_usd_value": parse_number(row["Amount (USD)"]),
                "amount_yuan_raw": clean_text(row["Amount (Yuan)"]),
                "amount_yuan_value": parse_number(row["Amount (Yuan)"]),
                "amount_tonnes_raw": clean_text(row["Amount (tonnes)"]),
                "amount_tonnes_value": parse_number(row["Amount (tonnes)"]),
                "amount_energy_units_raw": clean_text(row["Amount (energy units)"]),
                "amount_energy_units_value": parse_number(row["Amount (energy units)"]),
                "amount_units_raw": clean_text(row["Amount (units)"]),
                "amount_units_value": parse_number(row["Amount (units)"]),
                "notes": clean_text(row["Notes"]),
                "notes1": clean_text(row["Notes1"]),
            }
        return node_id

    def add_source(self, url: str) -> str | None:
        url = clean_text(url)
        if not url:
            return None

        key = stable_hash(url)
        existing = self.sources.get(key)
        if existing is None:
            existing = {
                "node_id": f"source::{key}",
                "url": url,
                "host": urllib.parse.urlparse(url).netloc,
            }
            self.sources[key] = existing
        return existing["node_id"]

    def add_relationship(
        self,
        start_id: str | None,
        end_id: str | None,
        rel_type: str,
        **properties: Any,
    ) -> None:
        if not start_id or not end_id:
            return
        cleaned = sanitize_properties(properties)
        dedupe_key = (
            start_id,
            end_id,
            rel_type,
            tuple(sorted((key, json.dumps(value, sort_keys=True)) for key, value in cleaned.items())),
        )
        if dedupe_key in self.relationship_keys:
            return
        self.relationship_keys.add(dedupe_key)
        self.relationships.append(
            {
                "start_id": start_id,
                "end_id": end_id,
                "type": rel_type,
                **cleaned,
            }
        )

    def finalize(self) -> dict[str, list[dict[str, Any]]]:
        companies = [self._finalize_company(record) for record in self.companies.values()]
        facilities = list(self.facilities.values())
        countries = list(self.countries.values())
        commodities = [self._finalize_commodity(record) for record in self.commodities.values()]
        chain_steps = list(self.chain_steps.values())
        chain_links = list(self.chain_links.values())
        transactions = list(self.transactions.values())
        sources = list(self.sources.values())

        return {
            "companies": sorted(companies, key=lambda row: row["node_id"]),
            "facilities": sorted(facilities, key=lambda row: row["node_id"]),
            "countries": sorted(countries, key=lambda row: row["node_id"]),
            "commodities": sorted(commodities, key=lambda row: row["node_id"]),
            "chain_steps": sorted(chain_steps, key=lambda row: row["node_id"]),
            "chain_links": sorted(chain_links, key=lambda row: row["node_id"]),
            "transactions": sorted(transactions, key=lambda row: row["node_id"]),
            "sources": sorted(sources, key=lambda row: row["node_id"]),
            "relationships": sorted(
                self.relationships,
                key=lambda row: (row["type"], row["start_id"], row["end_id"]),
            ),
        }

    def _finalize_company(self, record: dict[str, Any]) -> dict[str, Any]:
        country_name = ""
        location_precision = ""
        lat: float | None = None
        lon: float | None = None

        country_counts: dict[str, int] = record.get("_country_counts", {})
        if country_counts:
            country_name = max(
                country_counts,
                key=lambda item: (country_counts[item], item),
            )

        coordinate_counts: dict[tuple[float, float, str], int] = record.get("_coordinate_counts", {})
        if coordinate_counts:
            lat, lon, coordinate_country = max(
                coordinate_counts,
                key=lambda item: (coordinate_counts[item], item[2], item[0], item[1]),
            )
            if coordinate_country:
                country_name = coordinate_country
            location_precision = "exact"
        elif country_name:
            centroid = self.country_centroids.get(alias_key(country_name), {})
            lat = parse_float(centroid.get("lat", ""))
            lon = parse_float(centroid.get("lon", ""))
            if lat is not None and lon is not None:
                location_precision = "country_centroid"

        return {
            key: value
            for key, value in {
                **record,
                "country_name": country_name,
                "lat": lat,
                "lon": lon,
                "location_precision": location_precision,
            }.items()
            if key not in {"_coordinate_counts", "_country_counts"}
        } | {
            "role_tags": ";".join(sorted(record["role_tags"])),
            "company_type_tags": ";".join(sorted(record["company_type_tags"])),
        }

    @staticmethod
    def _finalize_commodity(record: dict[str, Any]) -> dict[str, Any]:
        return {
            **record,
            "commodity_types": ";".join(sorted(record["commodity_types"])),
            "direction_tags": ";".join(sorted(record["direction_tags"])),
        }


def build_graph(
    links_rows: list[dict[str, str]],
    country_rows: list[dict[str, str]],
    bhrrc_rows: list[dict[str, str]],
) -> dict[str, list[dict[str, Any]]]:
    country_index = {alias_key(row["name"]): row for row in country_rows if clean_text(row.get("name", ""))}
    bhrrc_index = {
        alias_key(row["RM name"]): row
        for row in bhrrc_rows
        if clean_text(row.get("RM name", ""))
    }

    graph = GraphBuilder(bhrrc_index=bhrrc_index, country_centroids=country_index)

    for row in links_rows:
        transaction_id = graph.add_transaction(row)
        source_id = graph.add_source(row["Source"])
        if source_id:
            graph.add_relationship(
                transaction_id,
                source_id,
                "SUPPORTED_BY",
                notes_reference=clean_text(row["Notes1"]),
            )

        supplier_company_id = graph.add_company(
            row["Supplier company"],
            company_type=row["Type of supplier company clean"],
            role_hint="supplier",
        )
        graph.add_company_location(
            row["Supplier company"],
            lat=row["Lat supplier"],
            lon=row["Long supplier"],
            country_name=row["Country of Supplier"],
        )
        buyer_company_id = graph.add_company(
            row["Buyer company"],
            company_type=row["Type of buyer company clean"],
            role_hint="buyer",
        )
        graph.add_company_location(
            row["Buyer company"],
            lat=row["Lat buyer"],
            lon=row["Long buyer"],
            country_name=row["Country of Buyer"],
        )

        graph.add_relationship(supplier_company_id, transaction_id, "SUPPLIER_IN")
        graph.add_relationship(buyer_company_id, transaction_id, "BUYER_IN")

        input_step_id = graph.add_chain_step(row["Input chain step"])
        output_step_id = graph.add_chain_step(row["Output chain step"])
        chain_link_id = graph.add_chain_link(row["Link in the chain"])

        graph.add_relationship(transaction_id, input_step_id, "INPUT_CHAIN_STEP")
        graph.add_relationship(transaction_id, output_step_id, "OUTPUT_CHAIN_STEP")
        graph.add_relationship(transaction_id, chain_link_id, "CHAIN_LINK")

        input_commodity_id = graph.add_commodity(
            row["Input commodity"] or row["Type of input commodity"],
            commodity_type=row["Type of input commodity"],
            direction="input",
        )
        output_commodity_id = graph.add_commodity(
            row["Output commodity clean"] or row["Type of output commodity"],
            commodity_type=row["Type of output commodity"],
            direction="output",
        )

        graph.add_relationship(transaction_id, input_commodity_id, "INPUT_COMMODITY")
        graph.add_relationship(transaction_id, output_commodity_id, "OUTPUT_COMMODITY")

        supplier_country_id = graph.add_country(row["Country of Supplier"])
        buyer_country_id = graph.add_country(row["Country of Buyer"])

        supplier_facility_id = graph.add_facility(
            name=row["Supplier facility name"],
            clean_name=row["Supplier_facility_clean"],
            facility_type=row["Type of supplier company clean"],
            place=row["Place of supplier"],
            country_name=row["Country of Supplier"],
            lat=row["Lat supplier"],
            lon=row["Long supplier"],
        )
        buyer_facility_id = graph.add_facility(
            name=row["Buyer facility name"],
            clean_name=row["Buyer facility type clean"] or row["Buyer facility name"],
            facility_type=row["Type of buyer company clean"],
            place=row["Place of buyer"],
            country_name=row["Country of Buyer"],
            lat=row["Lat buyer"],
            lon=row["Long buyer"],
        )

        graph.add_relationship(supplier_company_id, supplier_facility_id, "OPERATES_FACILITY", side="supplier")
        graph.add_relationship(buyer_company_id, buyer_facility_id, "OPERATES_FACILITY", side="buyer")
        graph.add_relationship(supplier_facility_id, supplier_country_id, "LOCATED_IN")
        graph.add_relationship(buyer_facility_id, buyer_country_id, "LOCATED_IN")
        graph.add_relationship(transaction_id, supplier_facility_id, "SUPPLIER_FACILITY")
        graph.add_relationship(transaction_id, buyer_facility_id, "BUYER_FACILITY")

        for parent_name in split_multi_value(row["Supplier parent company(ies)"]):
            parent_id = graph.add_company(parent_name, role_hint="parent")
            graph.add_relationship(parent_id, supplier_company_id, "PARENT_OF")

        for parent_name in split_multi_value(row["Buyer parent company(ies)"]):
            parent_id = graph.add_company(parent_name, role_hint="parent")
            graph.add_relationship(parent_id, buyer_company_id, "PARENT_OF")

        if clean_text(row["Supplier subsidiary company"]):
            subsidiary_id = graph.add_company(row["Supplier subsidiary company"], role_hint="subsidiary")
            graph.add_relationship(supplier_company_id, subsidiary_id, "HAS_SUBSIDIARY")

        if clean_text(row["Buyer subsidiary company"]):
            subsidiary_id = graph.add_company(row["Buyer subsidiary company"], role_hint="subsidiary")
            graph.add_relationship(buyer_company_id, subsidiary_id, "HAS_SUBSIDIARY")

        if clean_text(row["Supplier joint venture"]):
            joint_venture_id = graph.add_company(row["Supplier joint venture"], role_hint="joint_venture")
            graph.add_relationship(
                supplier_company_id,
                joint_venture_id,
                "ASSOCIATED_WITH_JV",
                composition=clean_text(row["Supplier joint venture composition"]),
                side="supplier",
            )

        if clean_text(row["Buyer joint venture"]):
            joint_venture_id = graph.add_company(row["Buyer joint venture"], role_hint="joint_venture")
            graph.add_relationship(
                buyer_company_id,
                joint_venture_id,
                "ASSOCIATED_WITH_JV",
                composition=clean_text(row["Buyer joint venture composition"]),
                side="buyer",
            )

        graph.add_relationship(
            supplier_company_id,
            buyer_company_id,
            "SUPPLIES_TO",
            transaction_id=clean_text(row["ID"]),
            link_in_chain=clean_text(row["Link in the chain"]),
        )

    return graph.finalize()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_directory(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    priority = [
        "node_id",
        "transaction_id",
        "name",
        "display_name",
        "url",
        "start_id",
        "end_id",
        "type",
    ]
    keys = {key for row in rows for key in row.keys()}
    ordered_keys = [key for key in priority if key in keys]
    ordered_keys.extend(sorted(key for key in keys if key not in ordered_keys))

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ordered_keys)
        writer.writeheader()
        for row in rows:
            serialized = {
                key: json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value
                for key, value in row.items()
            }
            writer.writerow(serialized)


def write_ordered_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    ensure_directory(path.parent)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def normalize_step_name(step_name: str) -> str:
    step_name = clean_text(step_name)
    return STEP_COLUMN_RENAMES.get(step_name, step_name)


def step_sort_key(step_name: str) -> tuple[int, str]:
    normalized = normalize_step_name(step_name)
    index_map = {name: idx for idx, name in enumerate(STEP_ORDER)}
    reverse_map = {normalize_step_name(name): idx for idx, name in enumerate(STEP_ORDER)}
    return reverse_map.get(normalized, index_map.get(step_name, 999)), normalized


def build_path_matrix(links_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    edges: list[dict[str, str]] = []
    buyers = set()
    suppliers = set()

    for row in links_rows:
        supplier = clean_text(row["Supplier company"])
        buyer = clean_text(row["Buyer company"])
        output_step = clean_text(row["Output chain step"])
        if not supplier or not buyer or not output_step:
            continue

        edge = {
            "transaction_id": clean_text(row["ID"]),
            "supplier": supplier,
            "buyer": buyer,
            "input_step": clean_text(row["Input chain step"]),
            "output_step": output_step,
            "link_in_chain": clean_text(row["Link in the chain"]),
        }
        edges.append(edge)
        suppliers.add(supplier)
        buyers.add(buyer)

    adjacency: dict[str, list[dict[str, str]]] = {}
    for edge in edges:
        adjacency.setdefault(edge["supplier"], []).append(edge)

    for edge_list in adjacency.values():
        edge_list.sort(key=lambda item: (step_sort_key(item["output_step"]), item["buyer"], item["transaction_id"]))

    start_step_candidates = {"Artisanal mining", "Mining", "Recycling", "Artisanal processing"}
    start_edges = [
        edge for edge in edges
        if edge["input_step"] in start_step_candidates or edge["supplier"] not in buyers
    ]
    if not start_edges:
        start_edges = list(edges)

    sink_steps = {"Electric car manufacturing", "Electric scooter manufacturing"}
    # The path matrix is intentionally wider than the graph export, so a small
    # fixed traversal cap can silently truncate valid downstream chains.
    max_expansions = max(200000, len(edges) * 1000)
    frontier: list[dict[str, Any]] = []
    completed: list[dict[str, str]] = []

    columns = ["source"]
    seen_columns = set(columns)
    for edge in edges:
        column = normalize_step_name(edge["output_step"])
        if column not in seen_columns:
            columns.append(column)
            seen_columns.add(column)
    columns[1:] = sorted(columns[1:], key=step_sort_key)

    seen_states: set[tuple[Any, ...]] = set()

    def row_signature(row: dict[str, str]) -> tuple[str, ...]:
        return tuple(row[column] for column in columns)

    def register_state(current_company: str, last_rank: int, row: dict[str, str]) -> bool:
        key = (current_company, last_rank, row_signature(row))
        if key in seen_states:
            return False
        seen_states.add(key)
        return True

    for start_edge in start_edges:
        row = {column: "" for column in columns}
        row["source"] = start_edge["supplier"]
        row[normalize_step_name(start_edge["output_step"])] = start_edge["buyer"]
        last_rank = step_sort_key(start_edge["output_step"])[0]
        if register_state(start_edge["buyer"], last_rank, row):
            frontier.append(
                {
                    "row": row,
                    "current_company": start_edge["buyer"],
                    "last_rank": last_rank,
                    "used_transactions": {start_edge["transaction_id"]},
                    "visited_companies": {start_edge["supplier"], start_edge["buyer"]},
                    "path_length": 1,
                    "ended": start_edge["output_step"] in sink_steps,
                }
            )

    expansions = 0
    truncated = False
    while frontier:
        state = frontier.pop()
        row = state["row"]
        current_company = state["current_company"]
        last_rank = state["last_rank"]
        used_transactions = state["used_transactions"]
        visited_companies = state["visited_companies"]

        if state["ended"]:
            completed.append(row)
            continue

        candidates = []
        for candidate in adjacency.get(current_company, []):
            if candidate["transaction_id"] in used_transactions:
                continue
            next_rank = step_sort_key(candidate["output_step"])[0]
            if next_rank < last_rank:
                continue
            if candidate["buyer"] in visited_companies and candidate["buyer"] != current_company:
                continue
            candidates.append(candidate)

        if not candidates:
            completed.append(row)
            continue

        for candidate in candidates:
            if expansions >= max_expansions:
                truncated = True
                completed.append(row)
                break

            new_row = dict(row)
            column = normalize_step_name(candidate["output_step"])
            existing = new_row.get(column, "")
            buyer = candidate["buyer"]
            if not existing:
                new_row[column] = buyer
            elif buyer not in existing.split(" ; "):
                new_row[column] = f"{existing} ; {buyer}"

            next_rank = step_sort_key(candidate["output_step"])[0]
            if not register_state(candidate["buyer"], next_rank, new_row):
                continue

            frontier.append(
                {
                    "row": new_row,
                    "current_company": candidate["buyer"],
                    "last_rank": next_rank,
                    "used_transactions": set(used_transactions) | {candidate["transaction_id"]},
                    "visited_companies": set(visited_companies) | {candidate["buyer"]},
                    "path_length": state["path_length"] + 1,
                    "ended": candidate["output_step"] in sink_steps,
                }
            )
            expansions += 1

    if truncated:
        print(
            f"Warning: path matrix traversal stopped after {max_expansions} expansions; "
            "some downstream chains may be incomplete.",
            file=sys.stderr,
        )

    matrix_rows: list[dict[str, str]] = []
    row_keys: set[tuple[str, ...]] = set()

    for row in completed:
        dedupe_key = tuple(row[column] for column in columns)
        if dedupe_key in row_keys:
            continue
        row_keys.add(dedupe_key)
        matrix_rows.append(row)

    matrix_rows.sort(
        key=lambda row: (
            -sum(1 for value in row.values() if clean_text(value)),
            row["source"],
            *(row[column] for column in columns[1:]),
        )
    )
    return matrix_rows


def excel_column_name(index: int) -> str:
    name = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def write_simple_xlsx(path: Path, rows: list[dict[str, Any]], sheet_name: str = "Sheet1") -> None:
    ensure_directory(path.parent)
    if not rows:
        rows = [{"empty": ""}]

    headers = list(rows[0].keys())

    def cell_xml(row_idx: int, col_idx: int, value: Any) -> str:
        ref = f"{excel_column_name(col_idx)}{row_idx}"
        text = "" if value is None else str(value)
        return f'<c r="{ref}" t="inlineStr"><is><t>{escape(text)}</t></is></c>'

    sheet_rows: list[str] = []
    header_cells = "".join(cell_xml(1, index + 1, header) for index, header in enumerate(headers))
    sheet_rows.append(f'<row r="1">{header_cells}</row>')
    for row_number, row in enumerate(rows, start=2):
        cells = "".join(cell_xml(row_number, index + 1, row.get(header, "")) for index, header in enumerate(headers))
        sheet_rows.append(f'<row r="{row_number}">{cells}</row>')

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{''.join(sheet_rows)}</sheetData>"
        "</worksheet>"
    )

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<sheets><sheet name="{escape(sheet_name)}" sheetId="1" r:id="rId1"/></sheets>'
        "</workbook>"
    )

    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
        "</Relationships>"
    )

    rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        "</Relationships>"
    )

    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>'
        '<borders count="1"><border/></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>'
        "</styleSheet>"
    )

    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        "</Types>"
    )

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", content_types_xml)
        workbook.writestr("_rels/.rels", rels_xml)
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        workbook.writestr("xl/styles.xml", styles_xml)


def export_graph(graph: dict[str, list[dict[str, Any]]], output_dir: Path) -> dict[str, int]:
    ensure_directory(output_dir)
    summary: dict[str, int] = {}

    for file_name, key in ENTITY_EXPORTS.items():
        write_csv(output_dir / file_name, graph[key])
        summary[key] = len(graph[key])

    write_csv(output_dir / "relationships.csv", graph["relationships"])
    summary["relationships"] = len(graph["relationships"])
    return summary


def export_path_matrix(
    matrix_rows: list[dict[str, str]],
    coordinate_rows: list[dict[str, Any]],
    output_dir: Path,
) -> dict[str, int]:
    fieldnames = list(matrix_rows[0].keys()) if matrix_rows else ["source", "source longitude", "source latitude"]
    write_ordered_csv(output_dir / "path_matrix.csv", matrix_rows, fieldnames)
    xlsx_path = output_dir / "path_matrix.xlsx"
    try:
        write_simple_xlsx(xlsx_path, matrix_rows, sheet_name="PathMatrix")
    except PermissionError:
        fallback_path = output_dir / "path_matrix.new.xlsx"
        write_simple_xlsx(fallback_path, matrix_rows, sheet_name="PathMatrix")
        print(
            f"Warning: {xlsx_path} is in use, wrote {fallback_path.name} instead.",
            file=sys.stderr,
        )
    if coordinate_rows:
        write_ordered_csv(
            output_dir / "node_coordinates.csv",
            coordinate_rows,
            ["company", "stage", "country", "longitude", "latitude", "precision"],
        )
    return {
        "path_matrix_rows": len(matrix_rows),
        "path_matrix_columns": len(matrix_rows[0]) if matrix_rows else 0,
        "node_coordinates": len(coordinate_rows),
    }


def export_static_site(output_dir: Path, site_dir: Path) -> dict[str, int]:
    ensure_directory(site_dir)
    preview_path = output_dir / "graph_preview.html"
    if not preview_path.exists():
        raise FileNotFoundError(
            f"Static site export needs {preview_path.name}. Run with --render-html (or --publish-static)."
        )

    shutil.copyfile(preview_path, site_dir / "index.html")
    (site_dir / ".nojekyll").write_text("", encoding="utf-8")

    summary_path = output_dir / "summary.json"
    if summary_path.exists():
        shutil.copyfile(summary_path, site_dir / "summary.json")

    return {
        "site_files": sum(1 for path in site_dir.iterdir() if path.is_file()),
    }


def split_matrix_cell(value: str) -> list[str]:
    value = clean_text(value)
    if not value:
        return []
    return [clean_text(part) for part in value.split(" ; ") if clean_text(part)]


def format_coordinate(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.6f}".rstrip("0").rstrip(".")


def build_company_location_index(
    links_rows: list[dict[str, str]],
    country_rows: list[dict[str, str]],
) -> dict[str, Any]:
    country_index = {
        alias_key(row.get("name", "")): row
        for row in country_rows
        if clean_text(row.get("name", ""))
    }
    company_coordinate_counts: dict[str, dict[tuple[float, float, str], int]] = {}
    company_country_counts: dict[str, dict[str, int]] = {}
    stage_coordinate_counts: dict[tuple[str, str], dict[tuple[float, float, str], int]] = {}
    stage_country_counts: dict[tuple[str, str], dict[str, int]] = {}

    def note(company: str, stage: str, lat: str, lon: str, country: str) -> None:
        company = clean_text(company)
        stage = normalize_step_name(stage)
        country = clean_text(country)
        if not company:
            return

        if country:
            company_country_bucket = company_country_counts.setdefault(company, {})
            company_country_bucket[country] = company_country_bucket.get(country, 0) + 1
            if stage:
                stage_country_bucket = stage_country_counts.setdefault((company, stage), {})
                stage_country_bucket[country] = stage_country_bucket.get(country, 0) + 1

        lat_value = parse_float(lat)
        lon_value = parse_float(lon)
        if lat_value is None or lon_value is None:
            return

        coordinate_key = (lat_value, lon_value, country)
        company_bucket = company_coordinate_counts.setdefault(company, {})
        company_bucket[coordinate_key] = company_bucket.get(coordinate_key, 0) + 1
        if stage:
            stage_bucket = stage_coordinate_counts.setdefault((company, stage), {})
            stage_bucket[coordinate_key] = stage_bucket.get(coordinate_key, 0) + 1

    for row in links_rows:
        note(
            row.get("Supplier company", ""),
            row.get("Input chain step", "") or row.get("Output chain step", ""),
            row.get("Lat supplier", ""),
            row.get("Long supplier", ""),
            row.get("Country of Supplier", ""),
        )
        note(
            row.get("Buyer company", ""),
            row.get("Output chain step", ""),
            row.get("Lat buyer", ""),
            row.get("Long buyer", ""),
            row.get("Country of Buyer", ""),
        )

    def choose_best(
        company: str,
        stage: str = "",
    ) -> dict[str, Any]:
        company = clean_text(company)
        stage = normalize_step_name(stage)
        if not company:
            return {"lat": None, "lon": None, "country": "", "precision": ""}

        coordinate_counts = stage_coordinate_counts.get((company, stage), {}) if stage else {}
        if coordinate_counts:
            lat, lon, country = max(
                coordinate_counts,
                key=lambda item: (coordinate_counts[item], item[2], item[0], item[1]),
            )
            return {"lat": lat, "lon": lon, "country": country, "precision": "exact_stage"}

        coordinate_counts = company_coordinate_counts.get(company, {})
        if coordinate_counts:
            lat, lon, country = max(
                coordinate_counts,
                key=lambda item: (coordinate_counts[item], item[2], item[0], item[1]),
            )
            return {"lat": lat, "lon": lon, "country": country, "precision": "exact_company"}

        country_counts = stage_country_counts.get((company, stage), {}) if stage else {}
        if not country_counts:
            country_counts = company_country_counts.get(company, {})
        if country_counts:
            country_name = max(
                country_counts,
                key=lambda item: (country_counts[item], item),
            )
            centroid = country_index.get(alias_key(country_name), {})
            lat = parse_float(centroid.get("lat", ""))
            lon = parse_float(centroid.get("lon", ""))
            return {"lat": lat, "lon": lon, "country": country_name, "precision": "country_centroid"}

        return {"lat": None, "lon": None, "country": "", "precision": ""}

    return {
        "lookup": choose_best,
        "companies": sorted(company_coordinate_counts),
    }


def enrich_path_matrix_with_coordinates(
    matrix_rows: list[dict[str, str]],
    links_rows: list[dict[str, str]],
    country_rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, Any]], dict[str, int]]:
    location_index = build_company_location_index(links_rows, country_rows)
    lookup = location_index["lookup"]
    enriched_rows: list[dict[str, str]] = []
    coordinate_rows: list[dict[str, Any]] = []
    exact_count = 0
    centroid_count = 0
    missing_count = 0

    for row in matrix_rows:
        enriched_row: dict[str, str] = {}
        for column, value in row.items():
            enriched_row[column] = value
            company_names = split_matrix_cell(value)
            stage_name = "" if column == "source" else column
            locations = [lookup(company_name, stage_name) for company_name in company_names]
            longitude_values = [format_coordinate(location["lon"]) for location in locations]
            latitude_values = [format_coordinate(location["lat"]) for location in locations]
            enriched_row[f"{column} longitude"] = " ; ".join(longitude_values)
            enriched_row[f"{column} latitude"] = " ; ".join(latitude_values)

            for company_name, location in zip(company_names, locations):
                precision = location.get("precision", "")
                if precision.startswith("exact"):
                    exact_count += 1
                elif precision == "country_centroid":
                    centroid_count += 1
                else:
                    missing_count += 1
                coordinate_rows.append(
                    {
                        "company": company_name,
                        "stage": stage_name or "source",
                        "longitude": format_coordinate(location.get("lon")),
                        "latitude": format_coordinate(location.get("lat")),
                        "country": location.get("country", ""),
                        "precision": precision,
                    }
                )
        enriched_rows.append(enriched_row)

    deduped_coordinate_rows: list[dict[str, Any]] = []
    seen_coordinate_keys: set[tuple[str, str, str, str]] = set()
    for row in coordinate_rows:
        key = (row["company"], row["stage"], row["longitude"], row["latitude"])
        if key in seen_coordinate_keys:
            continue
        seen_coordinate_keys.add(key)
        deduped_coordinate_rows.append(row)

    return enriched_rows, deduped_coordinate_rows, {
        "path_matrix_exact_coordinates": exact_count,
        "path_matrix_centroid_coordinates": centroid_count,
        "path_matrix_missing_coordinates": missing_count,
    }


def pick_preview_focus_company(matrix_rows: list[dict[str, str]], requested_company: str) -> str:
    requested_company = clean_text(requested_company)
    if requested_company:
        return requested_company

    preferred = "Kamoto Copper Company (KCC)"
    for row in matrix_rows:
        if preferred in row.values():
            return preferred

    counts: dict[str, int] = {}
    for row in matrix_rows:
        for value in row.values():
            for company in split_matrix_cell(value):
                counts[company] = counts.get(company, 0) + 1
    if counts:
        return max(counts, key=counts.get)
    return preferred


def build_preview_payload(
    matrix_rows: list[dict[str, str]],
    *,
    focus_company: str,
    limit: int,
) -> dict[str, Any]:
    if not matrix_rows:
        return {
            "focus_company": focus_company,
            "columns": [],
            "rows_used": 0,
            "nodes": [],
            "edges": [],
        }

    columns = list(matrix_rows[0].keys())
    stage_columns = columns[1:]
    filtered_rows: list[dict[str, str]] = []
    for row in matrix_rows:
        if not focus_company:
            filtered_rows.append(row)
            continue
        if any(focus_company in split_matrix_cell(value) for value in row.values()):
            filtered_rows.append(row)

    if not filtered_rows:
        filtered_rows = matrix_rows[:limit]
    else:
        filtered_rows = filtered_rows[:limit]

    palette = [
        "#204c6f",
        "#0f766e",
        "#9a3412",
        "#1d4ed8",
        "#6d28d9",
        "#065f46",
        "#92400e",
        "#be123c",
        "#334155",
    ]
    color_map = {
        column: palette[index % len(palette)]
        for index, column in enumerate(["source", *stage_columns])
    }

    nodes_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    edges_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    for row in filtered_rows:
        sequence: list[tuple[str, list[str]]] = [("source", split_matrix_cell(row.get("source", "")))]
        for column in stage_columns:
            names = split_matrix_cell(row.get(column, ""))
            if names:
                sequence.append((column, names))

        sequence = [(column, names) for column, names in sequence if names]
        if len(sequence) < 2:
            continue

        for stage_index, (column, names) in enumerate(sequence):
            for name in names:
                key = (column, name)
                node = nodes_by_key.get(key)
                if node is None:
                    node = {
                        "id": f"{slugify(column)}::{stable_hash(name)}",
                        "label": name,
                        "column": column,
                        "stage_index": stage_index,
                        "count": 0,
                        "color": color_map[column],
                    }
                    nodes_by_key[key] = node
                node["count"] += 1

        for index in range(len(sequence) - 1):
            left_column, left_names = sequence[index]
            right_column, right_names = sequence[index + 1]
            for left_name in left_names:
                left_node = nodes_by_key[(left_column, left_name)]
                for right_name in right_names:
                    right_node = nodes_by_key[(right_column, right_name)]
                    key = (left_node["id"], right_node["id"])
                    edge = edges_by_key.get(key)
                    if edge is None:
                        edge = {
                            "source": left_node["id"],
                            "target": right_node["id"],
                            "source_column": left_column,
                            "target_column": right_column,
                            "count": 0,
                        }
                        edges_by_key[key] = edge
                    edge["count"] += 1

    nodes = sorted(
        nodes_by_key.values(),
        key=lambda node: (node["stage_index"], -node["count"], node["label"]),
    )
    edges = sorted(
        edges_by_key.values(),
        key=lambda edge: (-edge["count"], edge["source"], edge["target"]),
    )

    return {
        "focus_company": focus_company,
        "columns": ["source", *stage_columns],
        "rows_used": len(filtered_rows),
        "nodes": nodes,
        "edges": edges,
    }


def build_preview_html(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Supply Chain Graph Preview</title>
  <style>
    :root {{
      --bg: #f4efe7;
      --panel: rgba(255, 252, 247, 0.84);
      --ink: #1f2937;
      --muted: #6b7280;
      --line: rgba(31, 41, 55, 0.12);
      --accent: #b45309;
      --shadow: 0 24px 60px rgba(15, 23, 42, 0.12);
      --node-width: 190px;
      --node-height: 58px;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(180, 83, 9, 0.16), transparent 32%),
        radial-gradient(circle at top right, rgba(15, 118, 110, 0.16), transparent 28%),
        linear-gradient(180deg, #f8f4ec 0%, #efe6da 100%);
    }}
    .page {{
      max-width: 1600px;
      margin: 0 auto;
      padding: 28px 20px 40px;
    }}
    .hero {{
      background: var(--panel);
      border: 1px solid rgba(255,255,255,0.7);
      border-radius: 24px;
      padding: 24px 26px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
    }}
    h1 {{
      margin: 0;
      font-size: 30px;
      line-height: 1.1;
      letter-spacing: -0.03em;
    }}
    .sub {{
      margin-top: 10px;
      color: var(--muted);
      max-width: 900px;
      line-height: 1.6;
    }}
    .stats {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 18px;
    }}
    .chip {{
      padding: 10px 14px;
      border-radius: 999px;
      background: rgba(255,255,255,0.72);
      border: 1px solid rgba(31, 41, 55, 0.08);
      font-size: 13px;
    }}
    .legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 14px;
      margin-top: 18px;
      font-size: 12px;
      color: var(--muted);
    }}
    .legend span {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }}
    .legend i {{
      width: 12px;
      height: 12px;
      border-radius: 999px;
      display: inline-block;
    }}
    .board {{
      margin-top: 22px;
      background: rgba(255,255,255,0.68);
      border: 1px solid rgba(255,255,255,0.72);
      border-radius: 28px;
      box-shadow: var(--shadow);
      overflow: auto;
      position: relative;
      min-height: 540px;
    }}
    .canvas {{
      position: relative;
      min-width: 1180px;
    }}
    #edges {{
      position: absolute;
      inset: 0;
      overflow: visible;
    }}
    .stage-label {{
      position: absolute;
      top: 18px;
      width: var(--node-width);
      text-align: center;
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #7c5a2d;
    }}
    .node {{
      position: absolute;
      width: var(--node-width);
      min-height: var(--node-height);
      padding: 12px 14px;
      border-radius: 18px;
      color: white;
      box-shadow: 0 16px 30px rgba(15, 23, 42, 0.18);
      border: 1px solid rgba(255,255,255,0.18);
    }}
    .node-title {{
      font-size: 13px;
      line-height: 1.35;
      font-weight: 700;
    }}
    .node-meta {{
      margin-top: 6px;
      font-size: 11px;
      opacity: 0.86;
    }}
    .empty {{
      padding: 48px 24px;
      color: var(--muted);
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>Supply Chain Graph Preview</h1>
      <div class="sub">
        Layered graph preview built from the path-matrix export. It highlights how the selected company connects across downstream chain steps, so you can quickly inspect the rendered effect before moving into Neo4j Browser or Bloom.
      </div>
      <div class="stats" id="stats"></div>
      <div class="legend" id="legend"></div>
    </section>
    <section class="board">
      <div class="canvas" id="canvas">
        <svg id="edges"></svg>
      </div>
      <div class="empty" id="empty" hidden>No preview rows matched the requested company.</div>
    </section>
  </div>
  <script>
    const payload = {payload_json};
    const canvas = document.getElementById("canvas");
    const edgesSvg = document.getElementById("edges");
    const stats = document.getElementById("stats");
    const legend = document.getElementById("legend");
    const empty = document.getElementById("empty");
    const stageWidth = 230;
    const nodeWidth = 190;
    const nodeHeight = 58;
    const marginLeft = 38;
    const marginTop = 78;
    const marginBottom = 42;
    const rowGap = 18;

    function escapeHtml(text) {{
      return String(text)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/\"/g, "&quot;");
    }}

    function renderStats() {{
      const items = [
        `Focus company: ${{payload.focus_company || "auto"}}`,
        `Rows used: ${{payload.rows_used}}`,
        `Nodes: ${{payload.nodes.length}}`,
        `Edges: ${{payload.edges.length}}`
      ];
      stats.innerHTML = items.map((item) => `<div class="chip">${{escapeHtml(item)}}</div>`).join("");
    }}

    function renderLegend() {{
      legend.innerHTML = payload.columns.map((column) => {{
        const sample = payload.nodes.find((node) => node.column === column);
        const color = sample ? sample.color : "#64748b";
        return `<span><i style="background:${{color}}"></i>${{escapeHtml(column)}}</span>`;
      }}).join("");
    }}

    function render() {{
      renderStats();
      renderLegend();

      if (!payload.nodes.length) {{
        empty.hidden = false;
        canvas.style.display = "none";
        return;
      }}

      const nodesByColumn = new Map();
      payload.columns.forEach((column) => nodesByColumn.set(column, []));
      payload.nodes.forEach((node) => {{
        if (!nodesByColumn.has(node.column)) {{
          nodesByColumn.set(node.column, []);
        }}
        nodesByColumn.get(node.column).push(node);
      }});

      const maxColumnSize = Math.max(...Array.from(nodesByColumn.values()).map((items) => items.length));
      const width = marginLeft * 2 + Math.max(payload.columns.length, 1) * stageWidth;
      const height = marginTop + maxColumnSize * (nodeHeight + rowGap) + marginBottom;
      canvas.style.width = `${{width}}px`;
      canvas.style.height = `${{height}}px`;
      edgesSvg.setAttribute("width", width);
      edgesSvg.setAttribute("height", height);
      edgesSvg.innerHTML = "";

      const positions = new Map();
      payload.columns.forEach((column, columnIndex) => {{
        const label = document.createElement("div");
        label.className = "stage-label";
        label.style.left = `${{marginLeft + columnIndex * stageWidth}}px`;
        label.textContent = column;
        canvas.appendChild(label);

        const nodes = nodesByColumn.get(column) || [];
        nodes.forEach((node, rowIndex) => {{
          const x = marginLeft + columnIndex * stageWidth;
          const y = marginTop + rowIndex * (nodeHeight + rowGap);
          positions.set(node.id, {{ x, y }});
          const el = document.createElement("div");
          el.className = "node";
          el.style.left = `${{x}}px`;
          el.style.top = `${{y}}px`;
          el.style.background = `linear-gradient(145deg, ${{node.color}}, rgba(15, 23, 42, 0.92))`;
          el.innerHTML = `
            <div class="node-title">${{escapeHtml(node.label)}}</div>
            <div class="node-meta">${{escapeHtml(node.column)}} · paths ${{node.count}}</div>
          `;
          canvas.appendChild(el);
        }});
      }});

      payload.edges.forEach((edge) => {{
        const from = positions.get(edge.source);
        const to = positions.get(edge.target);
        if (!from || !to) {{
          return;
        }}
        const startX = from.x + nodeWidth;
        const startY = from.y + nodeHeight / 2;
        const endX = to.x;
        const endY = to.y + nodeHeight / 2;
        const bend = Math.max(48, (endX - startX) * 0.45);
        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        path.setAttribute("d", `M ${{startX}} ${{startY}} C ${{startX + bend}} ${{startY}}, ${{endX - bend}} ${{endY}}, ${{endX}} ${{endY}}`);
        path.setAttribute("fill", "none");
        path.setAttribute("stroke", "rgba(31, 41, 55, 0.18)");
        path.setAttribute("stroke-width", String(Math.min(8, 1 + Math.log2(edge.count + 1))));
        path.setAttribute("stroke-linecap", "round");
        edgesSvg.appendChild(path);
      }});
    }}

    render();
  </script>
</body>
</html>
"""


def export_preview_html(
    matrix_rows: list[dict[str, str]],
    output_dir: Path,
    *,
    focus_company: str,
    limit: int,
) -> dict[str, int]:
    focus_company = pick_preview_focus_company(matrix_rows, focus_company)
    payload = build_preview_payload(matrix_rows, focus_company=focus_company, limit=limit)
    output_path = output_dir / "graph_preview.html"
    ensure_directory(output_path.parent)
    output_path.write_text(build_preview_html(payload), encoding="utf-8")
    return {
        "preview_rows": payload["rows_used"],
        "preview_nodes": len(payload["nodes"]),
        "preview_edges": len(payload["edges"]),
    }


def pick_full_graph_preview_focus_company(
    graph: dict[str, list[dict[str, Any]]],
    requested_company: str,
) -> str:
    requested_company = clean_text(requested_company)
    company_names = [
        clean_text(row.get("name", ""))
        for row in graph["companies"]
        if clean_text(row.get("name", ""))
    ]
    if requested_company:
        return requested_company
    preferred = "Kamoto Copper Company (KCC)"
    if preferred in company_names:
        return preferred
    return company_names[0] if company_names else preferred


def preview_graph_node_label(kind: str, row: dict[str, Any]) -> str:
    for key in ("name", "display_name", "transaction_id", "url", "host", "node_id"):
        value = clean_text(row.get(key, ""))
        if value:
            return value
    return row["node_id"]


def preview_graph_node_subtitle(kind: str, row: dict[str, Any]) -> str:
    if kind == "Company":
        return " | ".join(
            value
            for value in [
                clean_text(row.get("company_type_tags", "")),
                clean_text(row.get("role_tags", "")),
            ]
            if value
        )
    if kind == "Facility":
        return " | ".join(
            value
            for value in [
                clean_text(row.get("facility_type", "")),
                clean_text(row.get("place", "")),
                clean_text(row.get("country_name", "")),
            ]
            if value
        )
    if kind == "Country":
        return clean_text(row.get("centroid_name", ""))
    if kind == "Commodity":
        return " | ".join(
            value
            for value in [
                clean_text(row.get("commodity_types", "")),
                clean_text(row.get("direction_tags", "")),
            ]
            if value
        )
    if kind == "Transaction":
        return " | ".join(
            value
            for value in [
                clean_text(row.get("link_in_chain", "")),
                clean_text(row.get("date_of_transaction", "")),
            ]
            if value
        )
    if kind == "Source":
        return clean_text(row.get("host", ""))
    return ""


def build_full_graph_preview_payload(
    graph: dict[str, list[dict[str, Any]]],
    *,
    focus_company: str,
    depth: int,
    max_nodes: int,
) -> dict[str, Any]:
    entity_groups = {
        "Company": graph["companies"],
        "Facility": graph["facilities"],
        "Country": graph["countries"],
        "Commodity": graph["commodities"],
        "ChainStep": graph["chain_steps"],
        "ChainLink": graph["chain_links"],
        "Transaction": graph["transactions"],
        "Source": graph["sources"],
    }
    kind_colors = {
        "Company": "#1d4ed8",
        "Facility": "#0f766e",
        "Country": "#b45309",
        "Commodity": "#7c3aed",
        "ChainStep": "#be123c",
        "ChainLink": "#7c2d12",
        "Transaction": "#334155",
        "Source": "#15803d",
    }

    nodes: list[dict[str, Any]] = []
    node_ids: set[str] = set()
    company_names: list[str] = []

    for kind, rows in entity_groups.items():
        for row in rows:
            node_id = clean_text(row.get("node_id", ""))
            if not node_id:
                continue
            label = preview_graph_node_label(kind, row)
            nodes.append(
                {
                    "id": node_id,
                    "label": label,
                    "kind": kind,
                    "subtitle": preview_graph_node_subtitle(kind, row),
                    "color": kind_colors[kind],
                }
            )
            node_ids.add(node_id)
            if kind == "Company":
                company_names.append(label)

    edge_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    relation_types: set[str] = set()
    for rel in graph["relationships"]:
        start_id = clean_text(rel.get("start_id", ""))
        end_id = clean_text(rel.get("end_id", ""))
        rel_type = clean_text(rel.get("type", ""))
        if not start_id or not end_id or not rel_type:
            continue
        if start_id not in node_ids or end_id not in node_ids:
            continue
        key = (start_id, end_id, rel_type)
        edge = edge_map.get(key)
        if edge is None:
            edge = {
                "source": start_id,
                "target": end_id,
                "type": rel_type,
                "count": 0,
            }
            edge_map[key] = edge
        edge["count"] += 1
        relation_types.add(rel_type)

    return {
        "default_focus": pick_full_graph_preview_focus_company(graph, focus_company),
        "default_depth": depth,
        "default_max_nodes": max_nodes,
        "company_names": sorted(set(company_names)),
        "nodes": sorted(nodes, key=lambda row: (row["kind"], row["label"], row["id"])),
        "edges": sorted(edge_map.values(), key=lambda row: (row["type"], row["source"], row["target"])),
        "relation_types": sorted(relation_types),
        "kind_colors": kind_colors,
    }


def build_full_graph_preview_html(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Supply Chain Graph Preview</title>
  <style>
    :root {
      --panel: rgba(255, 252, 246, 0.88);
      --ink: #1f2937;
      --muted: #6b7280;
      --shadow: 0 28px 70px rgba(15, 23, 42, 0.14);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(180, 83, 9, 0.18), transparent 30%),
        radial-gradient(circle at top right, rgba(29, 78, 216, 0.15), transparent 28%),
        linear-gradient(180deg, #f8f3ea 0%, #efe5d7 100%);
    }
    .page {
      max-width: 1680px;
      margin: 0 auto;
      padding: 28px 20px 42px;
    }
    .hero {
      background: var(--panel);
      border: 1px solid rgba(255,255,255,0.78);
      border-radius: 26px;
      padding: 24px 26px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
    }
    h1 {
      margin: 0;
      font-size: 32px;
      line-height: 1.1;
      letter-spacing: -0.03em;
    }
    .sub {
      margin-top: 10px;
      color: var(--muted);
      max-width: 980px;
      line-height: 1.6;
    }
    .controls {
      display: grid;
      grid-template-columns: minmax(240px, 1.8fr) 0.7fr 0.7fr auto;
      gap: 12px;
      margin-top: 18px;
    }
    .controls input,
    .controls select,
    .controls button {
      width: 100%;
      border: 1px solid rgba(31, 41, 55, 0.12);
      background: rgba(255,255,255,0.82);
      border-radius: 14px;
      padding: 12px 14px;
      font: inherit;
      color: inherit;
    }
    .controls button {
      cursor: pointer;
      background: linear-gradient(135deg, #1d4ed8, #0f766e);
      color: white;
      font-weight: 700;
      border: 0;
    }
    .stats {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 18px;
    }
    .chip {
      padding: 10px 14px;
      border-radius: 999px;
      background: rgba(255,255,255,0.72);
      border: 1px solid rgba(31, 41, 55, 0.08);
      font-size: 13px;
    }
    .legends {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-top: 18px;
    }
    .legend-box {
      background: rgba(255,255,255,0.6);
      border: 1px solid rgba(31, 41, 55, 0.08);
      border-radius: 18px;
      padding: 14px 16px;
    }
    .legend-title {
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: #7c5a2d;
      margin-bottom: 10px;
    }
    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 10px 14px;
      font-size: 12px;
      color: var(--muted);
    }
    .legend span {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .legend i {
      width: 12px;
      height: 12px;
      border-radius: 999px;
      display: inline-block;
    }
    .board {
      margin-top: 22px;
      background: rgba(255,255,255,0.7);
      border: 1px solid rgba(255,255,255,0.78);
      border-radius: 30px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .canvas-wrap {
      position: relative;
      min-height: 860px;
      overflow: auto;
      border-top: 1px solid rgba(31, 41, 55, 0.08);
    }
    #graph {
      display: block;
      width: 100%;
      min-height: 860px;
      background:
        radial-gradient(circle at 20% 20%, rgba(255,255,255,0.86), transparent 24%),
        linear-gradient(180deg, rgba(255,255,255,0.54), rgba(255,255,255,0.28));
    }
    .footer-note {
      padding: 14px 18px 18px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.6;
    }
    .empty {
      padding: 42px 24px;
      color: var(--muted);
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>Supply Chain Graph Preview</h1>
      <div class="sub">
        This preview is built from the full graph export instead of the path-matrix sample. It keeps companies, facilities, countries, commodities, chain steps, transactions, sources, and all exported relationship types, then renders a focused neighborhood around the selected entity.
      </div>
      <form class="controls" id="controls">
        <input id="focusInput" list="companyList" placeholder="Search a company or node label">
        <select id="depthSelect">
          <option value="1">Depth 1</option>
          <option value="2">Depth 2</option>
          <option value="3">Depth 3</option>
          <option value="4">Depth 4</option>
        </select>
        <select id="limitSelect">
          <option value="120">120 nodes</option>
          <option value="180">180 nodes</option>
          <option value="260">260 nodes</option>
          <option value="360">360 nodes</option>
        </select>
        <button type="submit">Render</button>
        <datalist id="companyList"></datalist>
      </form>
      <div class="stats" id="stats"></div>
      <div class="legends">
        <div class="legend-box">
          <div class="legend-title">Node Types</div>
          <div class="legend" id="kindLegend"></div>
        </div>
        <div class="legend-box">
          <div class="legend-title">Relation Types In Current View</div>
          <div class="legend" id="relationLegend"></div>
        </div>
      </div>
    </section>
    <section class="board">
      <div class="canvas-wrap">
        <svg id="graph" viewBox="0 0 1500 920" preserveAspectRatio="xMidYMid meet"></svg>
        <div class="empty" id="empty" hidden>No matching node was found for this focus term.</div>
      </div>
      <div class="footer-note">
        Click a node label to re-center on it. The default view is a neighborhood around the focus company; it keeps all relation types, but still uses a node cap so the browser stays responsive.
      </div>
    </section>
  </div>
  <script>
    const payload = __PAYLOAD_JSON__;
    const graphSvg = document.getElementById("graph");
    const stats = document.getElementById("stats");
    const kindLegend = document.getElementById("kindLegend");
    const relationLegend = document.getElementById("relationLegend");
    const focusInput = document.getElementById("focusInput");
    const depthSelect = document.getElementById("depthSelect");
    const limitSelect = document.getElementById("limitSelect");
    const controls = document.getElementById("controls");
    const companyList = document.getElementById("companyList");
    const empty = document.getElementById("empty");
    const width = 1500;
    const height = 920;
    const centerX = width / 2;
    const centerY = height / 2;
    const nodeById = new Map(payload.nodes.map((node) => [node.id, node]));
    const adjacency = new Map();
    const relationPriority = {
      SUPPLIES_TO: 1,
      SUPPLIER_IN: 2,
      BUYER_IN: 2,
      OUTPUT_CHAIN_STEP: 3,
      INPUT_CHAIN_STEP: 3,
      CHAIN_LINK: 4,
      OUTPUT_COMMODITY: 5,
      INPUT_COMMODITY: 5,
      OPERATES_FACILITY: 6,
      SUPPLIER_FACILITY: 6,
      BUYER_FACILITY: 6,
      LOCATED_IN: 7,
      PARENT_OF: 8,
      HAS_SUBSIDIARY: 8,
      ASSOCIATED_WITH_JV: 9,
      SUPPORTED_BY: 10
    };
    payload.company_names.forEach((name) => {
      const option = document.createElement("option");
      option.value = name;
      companyList.appendChild(option);
    });

    payload.edges.forEach((edge, index) => {
      if (!adjacency.has(edge.source)) adjacency.set(edge.source, []);
      if (!adjacency.has(edge.target)) adjacency.set(edge.target, []);
      adjacency.get(edge.source).push({ edge, index, other: edge.target });
      adjacency.get(edge.target).push({ edge, index, other: edge.source });
    });

    function escapeHtml(text) {
      return String(text)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }

    function normalize(value) {
      return String(value || "").trim().toLowerCase();
    }

    function findStartNodes(term) {
      const query = normalize(term);
      const allNodes = payload.nodes;
      if (!query) {
        return allNodes.filter((node) => node.kind === "Company" && node.label === payload.default_focus);
      }
      const companyExact = allNodes.filter((node) => node.kind === "Company" && normalize(node.label) === query);
      if (companyExact.length) return companyExact;
      const companyPartial = allNodes.filter((node) => node.kind === "Company" && normalize(node.label).includes(query));
      if (companyPartial.length) return companyPartial.slice(0, 6);
      const exact = allNodes.filter((node) => normalize(node.label) === query);
      if (exact.length) return exact;
      return allNodes.filter((node) => normalize(node.label).includes(query)).slice(0, 6);
    }

    function buildSubgraph(term, maxDepth, maxNodes) {
      const starts = findStartNodes(term);
      if (!starts.length) {
        return { nodes: [], edges: [], starts: [], relationCounts: new Map() };
      }

      const queue = [];
      const depthMap = new Map();
      starts.forEach((node) => {
        depthMap.set(node.id, 0);
        queue.push(node.id);
      });

      while (queue.length && depthMap.size < maxNodes) {
        const current = queue.shift();
        const currentDepth = depthMap.get(current) || 0;
        if (currentDepth >= maxDepth) continue;
        const neighbors = (adjacency.get(current) || []).slice().sort((left, right) => {
          const leftPriority = relationPriority[left.edge.type] || 99;
          const rightPriority = relationPriority[right.edge.type] || 99;
          if (leftPriority !== rightPriority) return leftPriority - rightPriority;
          return right.edge.count - left.edge.count;
        });
        for (const item of neighbors) {
          if (depthMap.size >= maxNodes) break;
          if (!depthMap.has(item.other)) {
            depthMap.set(item.other, currentDepth + 1);
            queue.push(item.other);
          }
        }
      }

      const selectedNodeIds = new Set(depthMap.keys());
      const nodes = Array.from(selectedNodeIds).map((id) => ({
        ...nodeById.get(id),
        depth: depthMap.get(id) || 0,
      }));
      const edges = payload.edges.filter((edge) => selectedNodeIds.has(edge.source) && selectedNodeIds.has(edge.target));
      const relationCounts = new Map();
      edges.forEach((edge) => {
        relationCounts.set(edge.type, (relationCounts.get(edge.type) || 0) + edge.count);
      });
      return { nodes, edges, starts, relationCounts };
    }

    function updateStats(subgraph, focusTerm, maxDepth, maxNodes) {
      const items = [
        `Focus: ${focusTerm || payload.default_focus}`,
        `Depth: ${maxDepth}`,
        `Node cap: ${maxNodes}`,
        `Rendered nodes: ${subgraph.nodes.length}`,
        `Rendered edges: ${subgraph.edges.length}`,
        `Full graph nodes: ${payload.nodes.length}`,
        `Full graph edges: ${payload.edges.length}`
      ];
      stats.innerHTML = items.map((item) => `<div class="chip">${escapeHtml(item)}</div>`).join("");
    }

    function updateLegends(relationCounts) {
      kindLegend.innerHTML = Object.entries(payload.kind_colors).map(([kind, color]) => {
        return `<span><i style="background:${color}"></i>${escapeHtml(kind)}</span>`;
      }).join("");

      const relationItems = Array.from(relationCounts.entries())
        .sort((left, right) => right[1] - left[1])
        .slice(0, 14);
      relationLegend.innerHTML = relationItems.map(([type, count]) => {
        return `<span><i style="background:rgba(31,41,55,0.28)"></i>${escapeHtml(type)} (${count})</span>`;
      }).join("");
    }

    function nodeRadius(kind) {
      return {
        Company: 18,
        Facility: 14,
        Country: 12,
        Commodity: 12,
        ChainStep: 13,
        ChainLink: 10,
        Transaction: 11,
        Source: 9
      }[kind] || 11;
    }

    function initializeLayout(nodes, focusedIds) {
      const depthGroups = new Map();
      nodes.forEach((node) => {
        if (!depthGroups.has(node.depth)) depthGroups.set(node.depth, []);
        depthGroups.get(node.depth).push(node);
      });

      Array.from(depthGroups.entries()).forEach(([depth, group]) => {
        const radius = depth === 0 ? 0 : 120 + (depth - 1) * 160;
        group.forEach((node, index) => {
          const angle = group.length === 1 ? 0 : (Math.PI * 2 * index) / group.length;
          const jitter = focusedIds.has(node.id) ? 0 : 24;
          node.x = centerX + Math.cos(angle) * radius + (Math.random() - 0.5) * jitter;
          node.y = centerY + Math.sin(angle) * radius + (Math.random() - 0.5) * jitter;
          node.vx = 0;
          node.vy = 0;
        });
      });
    }

    function stepSimulation(nodes, edges) {
      const repulsion = 5400;
      for (let i = 0; i < nodes.length; i += 1) {
        const a = nodes[i];
        for (let j = i + 1; j < nodes.length; j += 1) {
          const b = nodes[j];
          let dx = b.x - a.x;
          let dy = b.y - a.y;
          let distSq = dx * dx + dy * dy;
          if (distSq < 0.01) distSq = 0.01;
          const force = repulsion / distSq;
          const dist = Math.sqrt(distSq);
          dx /= dist;
          dy /= dist;
          a.vx -= dx * force;
          a.vy -= dy * force;
          b.vx += dx * force;
          b.vy += dy * force;
        }
      }

      edges.forEach((edge) => {
        const source = edge.sourceNode;
        const target = edge.targetNode;
        let dx = target.x - source.x;
        let dy = target.y - source.y;
        let dist = Math.sqrt(dx * dx + dy * dy) || 1;
        const desired = edge.type === "SUPPLIES_TO" ? 160 : edge.type.includes("CHAIN_STEP") ? 120 : 105;
        const strength = 0.002 + Math.min(edge.count, 6) * 0.0007;
        const pull = (dist - desired) * strength;
        dx /= dist;
        dy /= dist;
        source.vx += dx * pull;
        source.vy += dy * pull;
        target.vx -= dx * pull;
        target.vy -= dy * pull;
      });

      nodes.forEach((node) => {
        const ring = node.depth === 0 ? 0 : 120 + (node.depth - 1) * 160;
        let dx = node.x - centerX;
        let dy = node.y - centerY;
        const dist = Math.sqrt(dx * dx + dy * dy) || 1;
        const pull = (dist - ring) * 0.0035;
        node.vx -= (dx / dist) * pull;
        node.vy -= (dy / dist) * pull;
        node.vx *= 0.84;
        node.vy *= 0.84;
        node.x = Math.max(40, Math.min(width - 40, node.x + node.vx));
        node.y = Math.max(40, Math.min(height - 40, node.y + node.vy));
      });
    }

    function renderGraph(subgraph, focusTerm, maxDepth, maxNodes) {
      updateStats(subgraph, focusTerm, maxDepth, maxNodes);
      updateLegends(subgraph.relationCounts);

      if (!subgraph.nodes.length) {
        empty.hidden = false;
        graphSvg.innerHTML = "";
        return;
      }

      empty.hidden = true;
      graphSvg.setAttribute("viewBox", `0 0 ${width} ${height}`);
      graphSvg.innerHTML = "";

      const focusedIds = new Set(subgraph.starts.map((node) => node.id));
      const nodes = subgraph.nodes.map((node) => ({ ...node }));
      const nodeMap = new Map(nodes.map((node) => [node.id, node]));
      const edges = subgraph.edges.map((edge) => ({
        ...edge,
        sourceNode: nodeMap.get(edge.source),
        targetNode: nodeMap.get(edge.target),
      }));

      initializeLayout(nodes, focusedIds);

      const edgeElements = edges.map((edge) => {
        const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("stroke", "rgba(31, 41, 55, 0.18)");
        line.setAttribute("stroke-width", String(Math.min(5, 0.7 + Math.log2(edge.count + 1))));
        line.setAttribute("stroke-linecap", "round");
        const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
        title.textContent = `${edge.type} (${edge.count})`;
        line.appendChild(title);
        graphSvg.appendChild(line);
        return { edge, line };
      });

      const nodeElements = nodes.map((node) => {
        const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
        group.style.cursor = "pointer";
        const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
        circle.setAttribute("r", String(nodeRadius(node.kind)));
        circle.setAttribute("fill", node.color);
        circle.setAttribute("stroke", focusedIds.has(node.id) ? "#111827" : "rgba(255,255,255,0.8)");
        circle.setAttribute("stroke-width", focusedIds.has(node.id) ? "3" : "1.5");
        const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
        text.setAttribute("y", String(nodeRadius(node.kind) + 16));
        text.setAttribute("text-anchor", "middle");
        text.setAttribute("font-size", "11");
        text.setAttribute("font-weight", focusedIds.has(node.id) ? "700" : "500");
        text.setAttribute("fill", "#1f2937");
        text.textContent = node.label.length > 34 ? `${node.label.slice(0, 31)}...` : node.label;
        const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
        title.textContent = `${node.label}${node.subtitle ? " | " + node.subtitle : ""}`;
        group.appendChild(circle);
        group.appendChild(text);
        group.appendChild(title);
        group.addEventListener("click", () => {
          focusInput.value = node.label;
          renderFromControls();
        });
        graphSvg.appendChild(group);
        return { node, group };
      });

      let ticks = 0;
      function frame() {
        stepSimulation(nodes, edges);
        edgeElements.forEach(({ edge, line }) => {
          line.setAttribute("x1", edge.sourceNode.x.toFixed(1));
          line.setAttribute("y1", edge.sourceNode.y.toFixed(1));
          line.setAttribute("x2", edge.targetNode.x.toFixed(1));
          line.setAttribute("y2", edge.targetNode.y.toFixed(1));
        });
        nodeElements.forEach(({ node, group }) => {
          group.setAttribute("transform", `translate(${node.x.toFixed(1)} ${node.y.toFixed(1)})`);
        });
        ticks += 1;
        if (ticks < 140) {
          requestAnimationFrame(frame);
        }
      }
      frame();
    }

    function renderFromControls() {
      const focusTerm = focusInput.value.trim() || payload.default_focus;
      const maxDepth = Number(depthSelect.value || payload.default_depth);
      const maxNodes = Number(limitSelect.value || payload.default_max_nodes);
      const subgraph = buildSubgraph(focusTerm, maxDepth, maxNodes);
      renderGraph(subgraph, focusTerm, maxDepth, maxNodes);
    }

    focusInput.value = payload.default_focus;
    depthSelect.value = String(payload.default_depth);
    limitSelect.value = String(payload.default_max_nodes);
    controls.addEventListener("submit", (event) => {
      event.preventDefault();
      renderFromControls();
    });
    renderFromControls();
  </script>
</body>
</html>
"""
    return html.replace("__PAYLOAD_JSON__", payload_json)


def export_full_graph_preview_html(
    graph: dict[str, list[dict[str, Any]]],
    output_dir: Path,
    *,
    focus_company: str,
    depth: int,
    max_nodes: int,
) -> dict[str, int]:
    payload = build_full_graph_preview_payload(
        graph,
        focus_company=focus_company,
        depth=depth,
        max_nodes=max_nodes,
    )
    output_path = output_dir / "graph_preview.html"
    ensure_directory(output_path.parent)
    output_path.write_text(build_full_graph_preview_html(payload), encoding="utf-8")
    return {
        "preview_total_nodes": len(payload["nodes"]),
        "preview_total_edges": len(payload["edges"]),
        "preview_default_depth": payload["default_depth"],
    }


def load_into_neo4j(
    graph: dict[str, list[dict[str, Any]]],
    uri: str,
    user: str,
    password: str,
) -> None:
    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise RuntimeError("neo4j package is required for --load-neo4j") from exc

    entity_groups = {
        "Company": graph["companies"],
        "Facility": graph["facilities"],
        "Country": graph["countries"],
        "Commodity": graph["commodities"],
        "ChainStep": graph["chain_steps"],
        "ChainLink": graph["chain_links"],
        "Transaction": graph["transactions"],
        "Source": graph["sources"],
    }

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            session.run("CREATE CONSTRAINT entity_node_id IF NOT EXISTS FOR (n:Entity) REQUIRE n.node_id IS UNIQUE")

            for label, rows in entity_groups.items():
                for row in rows:
                    node_id = row["node_id"]
                    props = sanitize_properties({key: value for key, value in row.items() if key != "node_id"})
                    session.run(
                        f"MERGE (n:Entity:{label} {{node_id: $node_id}}) "
                        "SET n += $props",
                        node_id=node_id,
                        props=props,
                    )

            for rel in graph["relationships"]:
                start_id = rel["start_id"]
                end_id = rel["end_id"]
                rel_type = rel["type"]
                props = sanitize_properties(
                    {key: value for key, value in rel.items() if key not in {"start_id", "end_id", "type"}}
                )
                session.run(
                    f"MATCH (a:Entity {{node_id: $start_id}}) "
                    f"MATCH (b:Entity {{node_id: $end_id}}) "
                    f"MERGE (a)-[r:{rel_type}]->(b) "
                    "SET r += $props",
                    start_id=start_id,
                    end_id=end_id,
                    props=props,
                )
    finally:
        driver.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a graph dataset from Resource Matters supply-chain data.")
    parser.add_argument("--data-dir", default="data/raw", help="Where raw downloaded source files are stored.")
    parser.add_argument("--output-dir", default="output", help="Where the graph CSV exports are written.")
    parser.add_argument("--site-dir", default="site", help="Where the deployable static site bundle is written.")
    parser.add_argument("--refresh", action="store_true", help="Redownload all source files before building the graph.")
    parser.add_argument("--load-neo4j", action="store_true", help="Load the generated graph directly into Neo4j.")
    parser.add_argument("--render-html", action="store_true", help="Write an original-style HTML preview with chains and map panels.")
    parser.add_argument("--publish-static", action="store_true", help="Copy the generated HTML preview into a deployable static site bundle.")
    parser.add_argument("--preview-company", default="Kamoto Copper Company (KCC)", help="Focus company for the HTML preview.")
    parser.add_argument("--preview-depth", type=int, default=3, help="Company-neighborhood depth used by the HTML preview.")
    parser.add_argument("--preview-limit", type=int, default=180, help="Maximum number of companies shown in the HTML preview.")
    parser.add_argument("--neo4j-uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--neo4j-user", default=os.getenv("NEO4J_USER", "neo4j"))
    parser.add_argument("--neo4j-password", default=os.getenv("NEO4J_PASSWORD", "password"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    site_dir = Path(args.site_dir)
    should_render_html = args.render_html or args.publish_static

    source_paths = ensure_source_files(data_dir, refresh=args.refresh)
    links_rows = read_delimited_rows(source_paths["links"], delimiter="\t")
    country_rows = read_delimited_rows(source_paths["countries"], delimiter="\t")
    world_topology = read_json(source_paths["world"])
    bhrrc_rows = read_delimited_rows(source_paths["bhrrc_companies"], delimiter="\t")

    graph = build_graph(links_rows, country_rows, bhrrc_rows)
    matrix_rows = build_path_matrix(links_rows)
    matrix_rows_with_coordinates, coordinate_rows, coordinate_summary = enrich_path_matrix_with_coordinates(
        matrix_rows,
        links_rows,
        country_rows,
    )
    summary = export_graph(graph, output_dir)
    summary.update(export_path_matrix(matrix_rows_with_coordinates, coordinate_rows, output_dir))
    summary.update(coordinate_summary)
    if should_render_html:
        summary.update(
            export_original_style_preview(
                links_rows,
                matrix_rows,
                country_rows,
                world_topology,
                output_dir,
                focus_company=args.preview_company,
                depth=max(1, args.preview_depth),
                limit=max(40, args.preview_limit),
            )
        )
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    if args.publish_static:
        summary.update(export_static_site(output_dir, site_dir))

    print("Graph export complete.")
    for key in sorted(summary):
        print(f"  {key}: {summary[key]}")

    if args.load_neo4j:
        load_into_neo4j(
            graph,
            uri=args.neo4j_uri,
            user=args.neo4j_user,
            password=args.neo4j_password,
        )
        print(f"Loaded graph into Neo4j at {args.neo4j_uri}")

    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    if args.publish_static:
        shutil.copyfile(output_dir / "summary.json", site_dir / "summary.json")

    return 0


if __name__ == "__main__":
    sys.exit(main())
