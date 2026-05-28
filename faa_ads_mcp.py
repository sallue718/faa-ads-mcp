#!/usr/bin/env python3
"""
FAA Airworthiness Directives (AD) MCP Server — faa_ads_mcp

Exposes six tools that together cover the full FAA AD corpus:

  Modern database  (1998 – present)
    Primary:  av-info.faa.gov/adrecords/api  (JSON REST)
    Fallback: av-info.faa.gov/adportal       (ASP.NET WebForms scraper)

  Historical database  (inception – ~1998)
    Source:   rgl.faa.gov                    (Lotus-Domino full-text search)

Tools
─────
  faa_get_makes            – list all FAA aircraft makes
  faa_get_models           – list models for a given make
  faa_search_ads_modern    – query the 1998+ database
  faa_search_ads_historical– query the pre-1998 database
  faa_search_ads           – unified search across both (use for compliance reports)
  faa_get_ad_detail        – fetch full document text for a specific AD

Run
───
  python faa_ads_mcp.py              # streamable-http on port 8081
  PORT=9000 python faa_ads_mcp.py   # custom port
"""

import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from typing import Any, Optional
from urllib.parse import quote

import httpx
from bs4 import BeautifulSoup
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

mcp = FastMCP("faa_ads_mcp")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FAA_API_BASE    = "https://av-info.faa.gov/adrecords/api"
FAA_PORTAL_BASE = "https://av-info.faa.gov"
FAA_PORTAL_PATH = "/adportal/AdSearch.aspx"
RGL_BASE        = "https://rgl.faa.gov"
RGL_SEARCH_PATH = "/Regulatory_and_Guidance_Library/rgAD.nsf/0/$SearchView"
RGL_DOC_URL     = (
    "https://rgl.faa.gov/Regulatory_and_Guidance_Library/"
    "rgAD.nsf/0/{ad_encoded}?OpenDocument"
)

PORTAL_URL      = f"{FAA_PORTAL_BASE}{FAA_PORTAL_PATH}"
REQUEST_DELAY   = 0.5   # seconds between sequential FAA requests
REQUEST_TIMEOUT = 20.0  # seconds

HEADERS = {
    "User-Agent": (
        "ClearedMX/1.0 (aviation maintenance platform; contact@clearedmx.dev)"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

TTL_MAKES   = timedelta(hours=168)  # makes list changes rarely
TTL_MODELS  = timedelta(hours=168)
TTL_ADS     = timedelta(hours=1)
TTL_DETAIL  = timedelta(hours=6)

# ---------------------------------------------------------------------------
# In-memory cache
# ---------------------------------------------------------------------------

_cache: dict[str, tuple[Any, datetime]] = {}


def _cache_get(key: str, ttl: timedelta) -> Any | None:
    entry = _cache.get(key)
    if entry and datetime.utcnow() - entry[1] < ttl:
        return entry[0]
    return None


def _cache_set(key: str, value: Any) -> None:
    _cache[key] = (value, datetime.utcnow())


# ---------------------------------------------------------------------------
# Date normalisation
# ---------------------------------------------------------------------------

def _norm_date(raw: str) -> str:
    """Normalise FAA date strings → 'YYYY-MM-DD' or ''."""
    s = (raw or "").strip()
    if not s or s in ("N/A", "--", "None", "null"):
        return ""
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    return s


def _ad_year(ad_number: str) -> int | None:
    """Extract year from AD number like '98-23-04' or '2023-16-01'."""
    m = re.match(r"^(\d{2,4})", ad_number.strip())
    if not m:
        return None
    yr = int(m.group(1))
    if yr < 100:
        return 1900 + yr if yr >= 40 else 2000 + yr
    return yr


def _is_historical(ad_number: str) -> bool:
    yr = _ad_year(ad_number)
    return yr is not None and yr < 1998


def _rgl_doc_url(ad_number: str) -> str:
    return RGL_DOC_URL.format(ad_encoded=quote(ad_number, safe=""))


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=HEADERS, timeout=REQUEST_TIMEOUT, follow_redirects=True
    )


async def _get(url: str, params: dict | None = None) -> httpx.Response:
    async with _http_client() as client:
        return await client.get(url, params=params)


async def _post_form(url: str, data: dict, referer: str = "") -> httpx.Response:
    hdrs = {
        **HEADERS,
        "Content-Type": "application/x-www-form-urlencoded",
        **({"Referer": referer} if referer else {}),
    }
    async with httpx.AsyncClient(
        headers=hdrs, timeout=REQUEST_TIMEOUT, follow_redirects=True
    ) as client:
        return await client.post(url, data=data)


# ---------------------------------------------------------------------------
# Shared AD record builder
# ---------------------------------------------------------------------------

def _build_ad(
    ad_number: str,
    title: str = "",
    subject: str = "",
    eff_date: str = "",
    comp_date: str = "",
    applicability: str = "",
    action: str = "",
    doc_url: str = "",
    source: str = "",
) -> dict:
    return {
        "adNumber":      ad_number,
        "title":         title or subject,
        "subject":       subject or title,
        "effectiveDate": _norm_date(eff_date),
        "complianceDate": _norm_date(comp_date),
        "applicability": applicability,
        "action":        action,
        "documentUrl":   doc_url or _rgl_doc_url(ad_number),
        "source":        source,
    }


# ---------------------------------------------------------------------------
# Source 1 — FAA JSON REST API  (modern, 1998+)
# ---------------------------------------------------------------------------

def _parse_api_record(raw: dict, source: str = "modern_api") -> dict | None:
    ad_num = str(
        raw.get("AdNumber") or raw.get("adNumber") or raw.get("AD_Number") or ""
    ).strip()
    if not ad_num:
        return None

    title  = str(raw.get("Title") or raw.get("Subject") or raw.get("SubjectText") or "").strip()
    subj   = str(raw.get("SubjectText") or raw.get("Subject") or raw.get("subject") or title).strip()
    applic = str(raw.get("ApplicabilityText") or raw.get("Applicability") or "").strip()
    action = str(raw.get("ActionText") or raw.get("Action") or "").strip()
    eff    = str(raw.get("EffDate") or raw.get("EffectiveDate") or "")
    comp   = str(raw.get("CompDate") or raw.get("ComplianceDate") or "")
    raw_url = str(raw.get("DocumentUrl") or raw.get("DocUrl") or raw.get("Url") or "")
    doc_url = raw_url if raw_url.startswith("http") else _rgl_doc_url(ad_num)

    return _build_ad(ad_num, title, subj, eff, comp, applic, action, doc_url, source)


async def _api_get_makes(q: str | None) -> list[str]:
    cached = _cache_get("faa:makes", TTL_MAKES)
    if cached is not None:
        makes: list[str] = cached
    else:
        try:
            r = await _get(f"{FAA_API_BASE}/Makes")
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                makes = [
                    str(item.get("Make") or item.get("Name") or item.get("Value") or item or "")
                    for item in data
                ]
                makes = [m for m in makes if m]
            else:
                makes = []
            if makes:
                _cache_set("faa:makes", makes)
        except Exception:
            return []

    return [m for m in makes if q.lower() in m.lower()] if q else makes


async def _api_get_models(make: str, q: str | None) -> list[str]:
    ck = f"faa:models:{make.lower().replace(' ', '_')}"
    cached = _cache_get(ck, TTL_MODELS)
    if cached is not None:
        models: list[str] = cached
    else:
        try:
            r = await _get(f"{FAA_API_BASE}/Models", params={"make": make})
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                models = [
                    str(item.get("Model") or item.get("Name") or item.get("Value") or item or "")
                    for item in data
                ]
                models = [m for m in models if m]
            else:
                models = []
            if models:
                _cache_set(ck, models)
        except Exception:
            return []

    return [m for m in models if q.lower() in m.lower()] if q else models


async def _api_search(make: str, model: str) -> list[dict]:
    ck = f"faa:api:{make.lower()}:{model.lower()}"
    cached = _cache_get(ck, TTL_ADS)
    if cached is not None:
        return cached

    records: list[dict] = []
    try:
        r = await _get(f"{FAA_API_BASE}/AdRecords", params={"make": make, "model": model})
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            records = [rec for raw in data if (rec := _parse_api_record(raw)) is not None]
    except Exception:
        pass

    if records:
        _cache_set(ck, records)
    return records


# ---------------------------------------------------------------------------
# Source 2 — FAA Portal (ASP.NET WebForms scraper, modern fallback)
# ---------------------------------------------------------------------------

def _form_state(soup: BeautifulSoup) -> dict:
    state = {}
    for field in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
        el = soup.find("input", {"name": field})
        if el:
            state[field] = el.get("value", "")
    return state


def _dropdown(soup: BeautifulSoup, pattern: str) -> list[dict]:
    sel = soup.find("select", id=re.compile(pattern, re.I)) or \
          soup.find("select", attrs={"name": re.compile(pattern, re.I)})
    if not sel:
        return []
    return [
        {"id": o.get("value", ""), "text": o.get_text(strip=True)}
        for o in sel.find_all("option")
        if o.get("value") not in (None, "", "0", "-1")
    ]


def _fuzzy(needle: str, options: list[dict]) -> dict | None:
    n = needle.strip().lower()
    first_word = n.split()[0] if n.split() else n
    for opt in options:
        t = opt["text"].lower()
        if t == n:
            return opt
    for opt in options:
        t = opt["text"].lower()
        if t.startswith(n) or n.startswith(t.split()[0] if t.split() else t):
            return opt
    for opt in options:
        t = opt["text"].lower()
        if first_word in t or t.split()[0] in n:
            return opt
    return None


def _parse_table(soup: BeautifulSoup, base: str, source: str) -> list[dict]:
    table = soup.find("table", id=re.compile(r"Grid|Result|AdList|grid", re.I)) or \
            soup.find("table", class_=re.compile(r"grid", re.I))
    rows = table.find_all("tr") if table else soup.find_all("tr")
    ads: list[dict] = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        link = cells[0].find("a")
        ad_num = (link.get_text(strip=True) if link else cells[0].get_text(strip=True))
        if not re.search(r"\d{2,4}-\d{2}-\d{2,3}", ad_num):
            continue
        href = link.get("href", "") if link else ""
        doc_url = (href if href.startswith("http")
                   else f"{base}{href if href.startswith('/') else '/' + href}" if href
                   else _rgl_doc_url(ad_num))
        subj     = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        eff_date = cells[2].get_text() if len(cells) > 2 else ""
        comp_date= cells[3].get_text() if len(cells) > 3 else ""
        ads.append(_build_ad(ad_num, subj, subj, eff_date, comp_date, "", "", doc_url, source))
    return ads


async def _portal_search(make: str, model: str, source: str = "modern_portal") -> list[dict]:
    try:
        r1 = await _get(PORTAL_URL)
        r1.raise_for_status()
        s1 = BeautifulSoup(r1.text, "html.parser")
        st1 = _form_state(s1)

        make_opts = _dropdown(s1, r"ddMake|Make")
        make_opt  = _fuzzy(make, make_opts)
        if not make_opt:
            return []

        make_el = s1.find("select", id=re.compile(r"ddMake|Make", re.I))
        make_name = (make_el.get("name") if make_el else None) or \
                    "ctl00$cphContent$AdSearch1$ddMake"

        await asyncio.sleep(REQUEST_DELAY)
        r2 = await _post_form(
            PORTAL_URL,
            {**st1, "__EVENTTARGET": make_name, "__EVENTARGUMENT": "", make_name: make_opt["id"]},
            referer=PORTAL_URL,
        )
        r2.raise_for_status()
        s2 = BeautifulSoup(r2.text, "html.parser")
        st2 = _form_state(s2)

        model_opts = _dropdown(s2, r"ddModel|Model")
        model_opt  = _fuzzy(model, model_opts)
        if not model_opt:
            return []

        model_el   = s2.find("select", id=re.compile(r"ddModel|Model", re.I))
        model_name = (model_el.get("name") if model_el else None) or \
                     "ctl00$cphContent$AdSearch1$ddModel"
        btn        = s2.find("input", {"type": "submit"})
        btn_name   = (btn.get("name") if btn else None) or \
                     "ctl00$cphContent$AdSearch1$btnSearch"

        await asyncio.sleep(REQUEST_DELAY)
        r3 = await _post_form(
            PORTAL_URL,
            {**st2, "__EVENTTARGET": "", "__EVENTARGUMENT": "",
             make_name: make_opt["id"], model_name: model_opt["id"], btn_name: "Search"},
            referer=PORTAL_URL,
        )
        r3.raise_for_status()
        return _parse_table(BeautifulSoup(r3.text, "html.parser"), FAA_PORTAL_BASE, source)

    except Exception:
        return []


# ---------------------------------------------------------------------------
# Source 3 — RGL full-text search (covers both modern and historical)
# ---------------------------------------------------------------------------

async def _rgl_search(
    make: str, model: str, max_results: int = 500, source: str = "rgl"
) -> list[dict]:
    try:
        r = await _get(
            f"{RGL_BASE}{RGL_SEARCH_PATH}",
            params={
                "SearchOrder": "4",
                "SearchWV":    "TRUE",
                "SearchMax":   str(max_results),
                "Query":       f"{make} {model}",
            },
        )
        r.raise_for_status()
    except Exception:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    make_tok  = make.lower().split()[0]
    model_tok = model.lower().split()[0]
    ads: list[dict] = []

    for row in soup.find_all("tr", class_=re.compile(r"viewrow", re.I)):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        link   = cells[0].find("a")
        ad_num = link.get_text(strip=True) if link else cells[0].get_text(strip=True)
        if not re.search(r"\d{2,4}-\d{2}-\d{2,3}", ad_num):
            continue
        subj   = cells[1].get_text(strip=True)
        subj_l = subj.lower()
        if make_tok not in subj_l and model_tok not in subj_l:
            continue
        href    = link.get("href", "") if link else ""
        doc_url = (href if href.startswith("http") else f"{RGL_BASE}{href}") if href \
                  else _rgl_doc_url(ad_num)
        eff  = cells[2].get_text() if len(cells) > 2 else ""
        comp = cells[3].get_text() if len(cells) > 3 else ""
        ads.append(_build_ad(ad_num, subj, subj, eff, comp, "", "", doc_url, source))

    # Fallback: bare link scan when Lotus table structure is absent
    if not ads:
        for link in soup.find_all("a"):
            txt = link.get_text(strip=True)
            if not re.search(r"\d{2,4}-\d{2}-\d{2,3}", txt):
                continue
            href    = link.get("href", "")
            doc_url = (href if href.startswith("http") else f"{RGL_BASE}{href}") if href \
                      else _rgl_doc_url(txt)
            row    = link.find_parent("tr")
            cells  = row.find_all("td") if row else []
            subj   = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            eff    = cells[2].get_text() if len(cells) > 2 else ""
            ads.append(_build_ad(txt, subj, subj, eff, "", "", "", doc_url, source))

    return ads


# ---------------------------------------------------------------------------
# Pydantic input models
# ---------------------------------------------------------------------------


class GetMakesInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    q: Optional[str] = Field(
        default=None,
        description=(
            "Optional case-insensitive filter substring, e.g. 'cessna'. "
            "Omit to return all makes."
        ),
    )


class GetModelsInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    make: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description=(
            "Aircraft make, exactly as returned by faa_get_makes, e.g. 'CESSNA'. "
            "Case-insensitive."
        ),
    )
    q: Optional[str] = Field(
        default=None,
        description="Optional filter substring, e.g. '172'.",
    )


class SearchAdsInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    make: str = Field(
        ..., min_length=1, max_length=100, description="Aircraft make, e.g. 'CESSNA'."
    )
    model: str = Field(
        ..., min_length=1, max_length=100, description="Aircraft model, e.g. '172S'."
    )
    year: Optional[int] = Field(
        default=None,
        ge=1900,
        le=2100,
        description=(
            "Year of manufacture. When provided, modern ADs issued more than "
            "5 years before manufacture are filtered out."
        ),
    )


class MakeModelInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    make:  str = Field(..., min_length=1, max_length=100, description="Aircraft make.")
    model: str = Field(..., min_length=1, max_length=100, description="Aircraft model.")


class AdDetailInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    adNumber: str = Field(
        ...,
        min_length=4,
        max_length=20,
        description="FAA AD number, e.g. '2023-16-01' or '98-23-04'.",
    )


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool(
    name="faa_get_makes",
    annotations={
        "title": "Get FAA Aircraft Makes",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    },
)
async def faa_get_makes(params: GetMakesInput) -> str:
    """List all aircraft makes available in the FAA AD database.

    Fetches the authoritative manufacturer list from av-info.faa.gov/adrecords/api.
    Values returned here should be passed verbatim to faa_get_models and
    faa_search_ads_* tools.

    Args:
        params (GetMakesInput):
            - q (Optional[str]): Case-insensitive filter substring.

    Returns:
        str: JSON object:
        {
          "count": int,
          "makes": ["CESSNA", "PIPER", "BEECH", ...]
        }
        On failure: {"error": "..."}

    Examples:
        - Use when: "What makes does the FAA AD database have?"
        - Use when: "Find all makes matching 'piper'"  → q="piper"
    """
    makes = await _api_get_makes(params.q)
    if not makes:
        return json.dumps({
            "error": "Could not retrieve makes from FAA API. "
                     "The service may be temporarily unavailable — try again shortly."
        })
    return json.dumps({"count": len(makes), "makes": makes}, indent=2)


@mcp.tool(
    name="faa_get_models",
    annotations={
        "title": "Get FAA Aircraft Models for a Make",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    },
)
async def faa_get_models(params: GetModelsInput) -> str:
    """List aircraft models for a given make from the FAA AD database.

    Args:
        params (GetModelsInput):
            - make (str): Aircraft make exactly as returned by faa_get_makes.
            - q (Optional[str]): Optional filter substring.

    Returns:
        str: JSON object:
        {
          "make": str,
          "count": int,
          "models": ["172", "172S", "172R", ...]
        }
        On failure: {"error": "..."} — verify the make with faa_get_makes first.

    Examples:
        - Use when: "What Cessna models are in the FAA AD database?"
          → make="CESSNA"
        - Use when: "List Cessna 172 variants"
          → make="CESSNA", q="172"
    """
    models = await _api_get_models(params.make, params.q)
    if not models:
        return json.dumps({
            "error": (
                f"No models found for make '{params.make}'. "
                "Verify the make string with faa_get_makes first."
            )
        })
    return json.dumps({"make": params.make, "count": len(models), "models": models}, indent=2)


@mcp.tool(
    name="faa_search_ads_modern",
    annotations={
        "title": "Search FAA Modern AD Database (1998 – present)",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    },
)
async def faa_search_ads_modern(params: SearchAdsInput) -> str:
    """Search the FAA modern AD database (roughly 1998 to present).

    Strategy:
      1. Queries av-info.faa.gov/adrecords/api (JSON REST, fastest).
      2. Falls back to the av-info.faa.gov/adportal WebForms scraper
         when the JSON API returns nothing.

    Use faa_search_ads for a complete compliance report that also includes
    historical (pre-1998) ADs.

    Args:
        params (SearchAdsInput):
            - make  (str): Aircraft make, e.g. 'CESSNA'.
            - model (str): Aircraft model, e.g. '172S'.
            - year  (Optional[int]): Year of manufacture; prunes modern ADs
              issued > 5 years before the aircraft was built.

    Returns:
        str: JSON object:
        {
          "make": str,
          "model": str,
          "source": "modern_api" | "modern_portal",
          "count": int,
          "ads": [
            {
              "adNumber":      str,   // e.g. "2023-16-01"
              "title":         str,
              "subject":       str,
              "effectiveDate": str,   // "YYYY-MM-DD" or ""
              "complianceDate":str,
              "applicability": str,
              "action":        str,
              "documentUrl":   str,
              "source":        str
            }, ...
          ]
        }
    """
    ck = f"faa:modern:{params.make.lower()}:{params.model.lower()}:{params.year or 'x'}"
    cached = _cache_get(ck, TTL_ADS)
    if cached:
        return json.dumps({**cached, "_cached": True}, indent=2)

    ads = await _api_search(params.make, params.model)
    used_source = "modern_api"

    if not ads:
        ads = await _portal_search(params.make, params.model)
        used_source = "modern_portal"

    if params.year and ads:
        cutoff = params.year - 5
        ads = [
            a for a in ads
            if not a["effectiveDate"] or int(a["effectiveDate"][:4]) >= cutoff
        ]

    result = {
        "make": params.make, "model": params.model,
        "source": used_source, "count": len(ads), "ads": ads,
    }
    if ads:
        _cache_set(ck, result)
    return json.dumps(result, indent=2)


@mcp.tool(
    name="faa_search_ads_historical",
    annotations={
        "title": "Search FAA Historical AD Database (inception – ~1998)",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    },
)
async def faa_search_ads_historical(params: MakeModelInput) -> str:
    """Search the FAA historical AD database — ADs issued before 1998.

    Queries the FAA Regulatory Guidance Library (rgl.faa.gov) via full-text
    search, which is the FAA's authoritative archive for legacy ADs going back
    to the 1950s. Results are filtered to AD numbers whose year prefix indicates
    issue before 1998.

    Args:
        params (MakeModelInput):
            - make  (str): Aircraft make, e.g. 'CESSNA'.
            - model (str): Aircraft model, e.g. '172'.

    Returns:
        str: JSON object:
        {
          "make": str,
          "model": str,
          "source": "rgl_historical",
          "count": int,
          "ads": [ { same schema as faa_search_ads_modern } ... ]
        }

    Note:
        Some historical ADs may lack effectiveDate or complianceDate because
        the RGL full-text index does not always contain structured date fields
        for older documents.
    """
    ck = f"faa:hist:{params.make.lower()}:{params.model.lower()}"
    cached = _cache_get(ck, TTL_ADS)
    if cached:
        return json.dumps({**cached, "_cached": True}, indent=2)

    all_rgl = await _rgl_search(params.make, params.model, max_results=500, source="rgl_historical")
    ads = [a for a in all_rgl if _is_historical(a["adNumber"])]

    result = {
        "make": params.make, "model": params.model,
        "source": "rgl_historical", "count": len(ads), "ads": ads,
    }
    if ads:
        _cache_set(ck, result)
    return json.dumps(result, indent=2)


@mcp.tool(
    name="faa_search_ads",
    annotations={
        "title": "Search FAA ADs — Full History (Modern + Historical)",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    },
)
async def faa_search_ads(params: SearchAdsInput) -> str:
    """Search ALL FAA ADs for a make/model across both modern and historical databases.

    This is the primary tool for generating complete AD compliance reports.
    It combines results from three sources and deduplicates by AD number:

      1. av-info.faa.gov/adrecords/api  — modern ADs, structured JSON (1998+)
      2. av-info.faa.gov/adportal       — modern fallback via HTML scraping
      3. rgl.faa.gov                    — historical ADs via full-text search

    Sources 1 and 3 are queried in parallel. The modern API takes precedence
    for any AD number that appears in both modern and RGL results.

    Args:
        params (SearchAdsInput):
            - make  (str): Aircraft make, e.g. 'CESSNA'.
            - model (str): Aircraft model, e.g. '172S'.
            - year  (Optional[int]): Year of manufacture. When provided,
              modern ADs issued > 5 years before manufacture are excluded.

    Returns:
        str: JSON object:
        {
          "make":            str,
          "model":           str,
          "totalCount":      int,   // deduplicated total
          "modernCount":     int,   // ADs from modern database
          "historicalCount": int,   // ADs from pre-1998 database
          "ads": [
            {
              "adNumber":      str,
              "title":         str,
              "subject":       str,
              "effectiveDate": str,   // "YYYY-MM-DD" or ""
              "complianceDate":str,
              "applicability": str,
              "action":        str,
              "documentUrl":   str,
              "source":        str   // which database the record came from
            }, ...
          ]   // sorted newest → oldest by effectiveDate
        }
    """
    ck = f"faa:unified:{params.make.lower()}:{params.model.lower()}:{params.year or 'x'}"
    cached = _cache_get(ck, TTL_ADS)
    if cached:
        return json.dumps({**cached, "_cached": True}, indent=2)

    # Query modern API and RGL simultaneously
    api_task, rgl_task = await asyncio.gather(
        _api_search(params.make, params.model),
        _rgl_search(params.make, params.model, max_results=500),
    )
    modern_ads: list[dict] = api_task
    rgl_all:    list[dict] = rgl_task

    # Portal fallback only when JSON API returned nothing
    if not modern_ads:
        modern_ads = await _portal_search(params.make, params.model)

    # Separate historical slice from RGL results
    historical_ads = [a for a in rgl_all if _is_historical(a["adNumber"])]

    # Year-filter modern ADs
    if params.year and modern_ads:
        cutoff = params.year - 5
        modern_ads = [
            a for a in modern_ads
            if not a["effectiveDate"] or int(a["effectiveDate"][:4]) >= cutoff
        ]

    # Deduplicate: modern API record wins over RGL for the same AD number
    seen: set[str] = set()
    combined: list[dict] = []
    for ad in modern_ads:
        if ad["adNumber"] and ad["adNumber"] not in seen:
            seen.add(ad["adNumber"])
            combined.append(ad)
    for ad in historical_ads:
        if ad["adNumber"] and ad["adNumber"] not in seen:
            seen.add(ad["adNumber"])
            combined.append(ad)

    # Sort newest → oldest; ADs with no effective date go to the end
    combined.sort(
        key=lambda a: (
            "1" if a.get("effectiveDate") else "0",
            a.get("effectiveDate", ""),
        ),
        reverse=True,
    )

    result = {
        "make":            params.make,
        "model":           params.model,
        "totalCount":      len(combined),
        "modernCount":     len(modern_ads),
        "historicalCount": len(historical_ads),
        "ads":             combined,
    }
    if combined:
        _cache_set(ck, result)
    return json.dumps(result, indent=2)


@mcp.tool(
    name="faa_get_ad_detail",
    annotations={
        "title": "Get FAA AD Full Document Details",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    },
)
async def faa_get_ad_detail(params: AdDetailInput) -> str:
    """Fetch full document text and structured fields for a specific FAA AD.

    Retrieves the complete AD from the FAA Regulatory Guidance Library
    (rgl.faa.gov), including applicability, required action, and compliance
    deadline. Works for both modern (1998+) and historical ADs.

    Args:
        params (AdDetailInput):
            - adNumber (str): FAA AD number, e.g. '2023-16-01' or '82-14-02'.

    Returns:
        str: JSON object:
        {
          "adNumber":       str,
          "title":          str,
          "effectiveDate":  str,   // "YYYY-MM-DD" or ""
          "complianceDate": str,
          "applicability":  str,
          "action":         str,
          "documentUrl":    str,
          "fullText":       str,   // first 4 000 chars of the document body
          "source":         "rgl_detail"
        }
        On not-found: {"error": "AD '...' not found in RGL. Verify the AD number."}
        On timeout:   {"error": "Request timed out. Please try again."}

    Examples:
        - Use when: You have an AD number from faa_search_ads and need
          full compliance text, e.g. adNumber="2023-16-01"
        - Use when: Checking an historical AD's applicability statement,
          e.g. adNumber="82-14-02"
    """
    ad = params.adNumber.strip()
    ck = f"faa:detail:{ad}"
    cached = _cache_get(ck, TTL_DETAIL)
    if cached:
        return json.dumps({**cached, "_cached": True}, indent=2)

    doc_url = _rgl_doc_url(ad)
    try:
        r = await _get(doc_url)
        if r.status_code == 404:
            return json.dumps({
                "error": f"AD '{ad}' not found in RGL. "
                         "Verify the AD number (use faa_search_ads to look it up)."
            })
        r.raise_for_status()
    except httpx.TimeoutException:
        return json.dumps({"error": "Request timed out fetching AD document. Please try again."})
    except httpx.HTTPStatusError as exc:
        return json.dumps({
            "error": f"RGL returned HTTP {exc.response.status_code} for AD '{ad}'."
        })
    except Exception as exc:
        return json.dumps({"error": f"Failed to fetch AD '{ad}': {type(exc).__name__}"})

    soup = BeautifulSoup(r.text, "html.parser")

    def _td_after(label: str) -> str:
        for td in soup.find_all("td"):
            if label.lower() in td.get_text(strip=True).lower():
                nxt = td.find_next_sibling("td")
                if nxt:
                    return nxt.get_text(separator=" ", strip=True)
        return ""

    title        = soup.title.get_text(strip=True) if soup.title else ad
    eff_date     = _norm_date(_td_after("effective date") or _td_after("effdate"))
    comp_date    = _norm_date(_td_after("compliance date") or _td_after("compdate"))
    applicability= (_td_after("applicability") or
                    _td_after("applies to") or
                    _td_after("affected airplanes"))
    action       = (_td_after("required actions") or
                    _td_after("corrective action") or
                    _td_after("action"))
    full_text    = soup.get_text(separator="\n", strip=True)[:4000]

    detail = {
        "adNumber":       ad,
        "title":          title,
        "effectiveDate":  eff_date,
        "complianceDate": comp_date,
        "applicability":  applicability,
        "action":         action,
        "documentUrl":    doc_url,
        "fullText":       full_text,
        "source":         "rgl_detail",
    }
    _cache_set(ck, detail)
    return json.dumps(detail, indent=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    mcp.run(transport="streamable-http", host="0.0.0.0", port=port)
