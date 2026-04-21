#!/usr/bin/env python3
"""
HTML report generation — data-driven from results_a / results_b / coverage pickles.

Structure follows specs/output/report_structure.md:
  1. Report metadata
  2. Coverage snapshot
  3. Definitions
  4. Data-quality caveats
  5. Problem A findings (funnel, A5/A6, segments, A7 non-activator deep-dive)
  6. Problem B findings (reason codes, overwrite split, B4/B5/B6)  — skipped in pulse
  7. Ranked root causes (auto-generated from reason-code totals)
  8. Unexplained remainder
  9. (Recurring) longitudinal comparison — stub
  10. Recommendations (static generic; specific wins belong in result tables)

Usage:
    REPORT_MODE=oneoff|pulse|deep python -m analysis.report
"""

import os
import pickle
from datetime import datetime, timezone
from pathlib import Path

from src.config import (
    CACHE_DIR, CACHE_SUFFIX, REPORT_ID, REPORT_MODE, PROBLEM_B_ENABLED,
    A_START, A_END, B_START, B_END,
)

REPORTS_DIR = Path("./reports")
REPORTS_DIR.mkdir(exist_ok=True)


# ── Data loading ─────────────────────────────────────────────────────────────

def _load_pkl(name: str, required: bool = True):
    path = CACHE_DIR / f"{name}__{CACHE_SUFFIX}.pkl"
    if not path.exists():
        legacy = CACHE_DIR / f"{name}.pkl"
        if legacy.exists():
            path = legacy
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Missing results file: {path}")
        return None
    with open(path, "rb") as f:
        return pickle.load(f)


# ── Formatting helpers ───────────────────────────────────────────────────────

def _fmt(n):
    if isinstance(n, (int, float)):
        return f"{n:,.0f}"
    return str(n)


def _pct(num, denom, decimals=1):
    if denom in (0, None):
        return "N/A"
    return f"{100 * num / denom:.{decimals}f}%"


# ── HTML building blocks ─────────────────────────────────────────────────────

def _rate_cell_class(val_str: str) -> str:
    try:
        v = float(str(val_str).rstrip("%"))
        if v >= 80:
            return "rate-good"
        if v >= 50:
            return "rate-mid"
        return "rate-bad"
    except (ValueError, AttributeError):
        return ""


def _table(headers, rows, caption: str = "", rate_cols=None) -> str:
    html = ""
    if caption:
        html += f"<h4 class='tbl-caption'>{caption}</h4>\n"
    html += "<div class='tbl-wrap'><table>\n<thead><tr>"
    for h in headers:
        html += f"<th>{h}</th>"
    html += "</tr></thead>\n<tbody>\n"
    for row in rows:
        html += "<tr>"
        for ci, cell in enumerate(row):
            cls = ""
            if rate_cols and ci in rate_cols:
                cls = _rate_cell_class(cell)
            cell_cls = f' class="{cls}"' if cls else ""
            html += f"<td{cell_cls}>{cell}</td>"
        html += "</tr>\n"
    html += "</tbody></table></div>\n"
    return html


def _section(title: str, content: str, level: int = 2) -> str:
    return f"<h{level}>{title}</h{level}>\n{content}\n"


def _callout(kind: str, text: str) -> str:
    styles = {
        "critical": ("callout-critical", "❗"),
        "warning":  ("callout-warning",  "⚠️"),
        "info":     ("callout-info",     "ℹ️"),
        "finding":  ("callout-finding",  "✅"),
    }
    cls, icon = styles.get(kind, styles["info"])
    return f'<div class="callout {cls}"><span class="callout-icon">{icon}</span>{text}</div>\n'


def _label_html(label: str) -> str:
    css = {
        "GLOBAL_DIRECT":    "lbl-global",
        "CIS_DIRECT":       "lbl-cis-direct",
        "CIS_DIRECT_AF":    "lbl-cis-af",
        "CIS_DIRECT_UTM":   "lbl-cis-utm",
        "CIS_PARTIAL_UTM":  "lbl-cis-partial",
        "CIS_PROXY":        "lbl-cis-proxy",
    }
    cls = css.get(label, "lbl-neutral")
    return f'<span class="lbl {cls}">{label}</span>'


def _kpi_cards(cards) -> str:
    html = '<div class="kpi-grid">'
    for label, value, sentiment in cards:
        html += (
            f'<div class="kpi-card">'
            f'<div class="kpi-value kpi-{sentiment}">{value}</div>'
            f'<div class="kpi-label">{label}</div>'
            f'</div>'
        )
    html += '</div>\n'
    return html


# ── Funnel visualization ─────────────────────────────────────────────────────

def funnel_visual(funnel: dict) -> str:
    steps = [
        ("Total users",               "total_users"),
        ("Eligible (product pages)",  "eligible_users"),
        ("+ usable config",           "with_usable_config"),
        ("Reached hub (AC)",          "reached_hub"),
        ("Direct return signal",      "direct_return"),
        ("Any return (direct+proxy)", "any_return"),
    ]
    regions = [r for r in ["Global", "CIS", "All"] if r in funnel]
    html = '<div class="funnel-section">'
    for reg in regions:
        f = funnel[reg]
        base = f.get("total_users", 1) or 1
        html += (f'<h4 class="funnel-region-title">{reg} '
                 f'({_fmt(f.get("total_users",0))} users)</h4>')
        html += '<div class="funnel-bars">'
        for step_label, key in steps:
            v = f.get(key, 0)
            pct_width = min(100, 100 * v / base)
            if key == "total_users":
                bar_cls = "bar-neutral"
            elif key in ("eligible_users", "with_usable_config"):
                bar_cls = "bar-good"
            elif key == "reached_hub":
                conv = 100 * v / (f.get("eligible_users", 1) or 1)
                bar_cls = "bar-good" if conv >= 80 else ("bar-mid" if conv >= 50 else "bar-bad")
            else:
                conv = 100 * v / (f.get("reached_hub", 1) or 1)
                bar_cls = "bar-good" if conv >= 80 else ("bar-mid" if conv >= 50 else "bar-bad")
            html += (
                f'<div class="funnel-row">'
                f'<div class="funnel-label">{step_label}</div>'
                f'<div class="funnel-bar-wrap">'
                f'<div class="funnel-bar {bar_cls}" style="width:{pct_width:.1f}%"></div>'
                f'</div>'
                f'<div class="funnel-count">{_fmt(v)}</div>'
                f'</div>'
            )
        html += '</div>'
    html += '</div>\n'
    return html


def funnel_table(funnel: dict) -> str:
    regions = [r for r in ["Global", "CIS", "All"] if r in funnel]
    headers = ["Step"] + regions
    steps = [
        ("1. Total users",                  "total_users"),
        ("2. Eligible (product pages)",     "eligible_users"),
        ("3. + usable config",              "with_usable_config"),
        ("4. Reached hub (Affiliate Click)", "reached_hub"),
        ("5. Direct return signal",         "direct_return"),
        ("6. Any return (direct+proxy)",    "any_return"),
    ]
    rows = []
    for label, key in steps:
        row = [label] + [_fmt(funnel[reg].get(key, 0)) for reg in regions]
        rows.append(row)

    rate_col_indices = list(range(1, len(regions) + 1))
    for label, num_key, den_key in [
        ("Eligible rate",                 "eligible_users", "total_users"),
        ("Hub reach rate (of eligible)",  "reached_hub",    "eligible_users"),
        ("Return rate (of hub)",          "any_return",     "reached_hub"),
    ]:
        row = [f"<em>{label}</em>"]
        for reg in regions:
            n = funnel[reg].get(num_key, 0)
            d = funnel[reg].get(den_key, 0)
            row.append(_pct(n, d))
        rows.append(row)

    html = funnel_visual(funnel)
    html += _table(headers, rows, "Funnel — numeric detail", rate_cols=rate_col_indices)
    return html


# ── Reason code / segment tables ─────────────────────────────────────────────

def reason_code_table(reason_codes: dict) -> str:
    html = ""
    for reg, codes in reason_codes.items():
        headers = ["Reason Code", "Count", "%", ""]
        rows = []
        for rc in codes:
            pct_val = rc["pct"]
            bar = (f'<div class="rc-bar-wrap">'
                   f'<div class="rc-bar" style="width:{min(pct_val,100):.1f}%"></div>'
                   f'</div>')
            rows.append([
                f'<code>{rc["reason_code"]}</code>',
                _fmt(rc["count"]),
                f'{pct_val:.1f}%',
                bar,
            ])
        html += _table(headers, rows, f"Reason codes — {reg}", rate_cols=[2])
    return html


_RATE_COL_KEYWORDS = ("rate", "pct", "loss", "return", "hub", "elig")


def segment_tables(segments: dict, problem: str) -> str:
    html = ""
    for seg_name, data in segments.items():
        if not data:
            continue
        headers = list(data[0].keys())
        rate_cols = [
            i for i, h in enumerate(headers)
            if any(kw in str(h).lower() for kw in _RATE_COL_KEYWORDS)
        ]
        rows = []
        for row in data[:20]:
            cells = []
            for h in headers:
                v = row.get(h, "")
                if isinstance(v, float):
                    cells.append(f"{v:.1f}")
                else:
                    cells.append(_fmt(v) if isinstance(v, (int,)) else str(v))
            rows.append(cells)
        html += _table(headers, rows, f"{problem} — by {seg_name}",
                       rate_cols=rate_cols if rate_cols else None)
    return html


# ── Coverage snapshot (section 2) ────────────────────────────────────────────

def coverage_section(coverage: dict | None) -> str:
    if not coverage:
        return _callout("info", "Coverage snapshot not available — run "
                                "<code>analysis.extract</code> first.")
    rows = []
    if "events_params_count" in coverage:
        rows.append(["events.params (new query-param object)",
                     _fmt(coverage['events_params_count']),
                     _fmt(coverage['events_total']),
                     f"{coverage['events_params_pct']:.1f}%"])
    if "build_app_count" in coverage:
        rows.append(["clients.build_app",
                     _fmt(coverage['build_app_count']),
                     _fmt(coverage['clients_total']),
                     f"{coverage['build_app_pct']:.1f}%"])
    if "pc_new_fields_count" in coverage:
        rows.append(["Purchase Completed new fields",
                     _fmt(coverage['pc_new_fields_count']),
                     _fmt(coverage['pc_total']),
                     f"{coverage['pc_new_fields_pct']:.1f}%"])
    html = _table(["Instrumentation field", "Present", "Total", "Coverage"],
                  rows, "New-field coverage", rate_cols=[3])

    if "build_app_breakdown" in coverage:
        bd = coverage["build_app_breakdown"]
        rows2 = [[k or "<missing>", _fmt(v)] for k, v in
                 sorted(bd.items(), key=lambda kv: -kv[1])[:10]]
        html += _table(["build_app value", "Clients"], rows2,
                       "build_app breakdown")

    # Attribution source tiers (events.params → querySk → url_parse).
    # Produced by analysis.extract._source_tier_counts as:
    #   {"sample_size": N, "total_events": N,
    #    "by_kind": {"sk": {params:.., querySk:.., url_parse:.., none:..},
    #                "af": {params:.., url_parse:.., none:..},
    #                "utm":{params:.., url_parse:.., none:..}}}
    tiers = coverage.get("source_tiers") or {}
    by_kind = tiers.get("by_kind") or {}
    if by_kind:
        rows3 = []
        for kind in ("sk", "af", "utm"):
            tier = by_kind.get(kind) or {}
            if not tier:
                continue
            denom = sum(tier.values()) or 1
            rows3.append([
                kind,
                f"{_fmt(tier.get('params', 0))} ({100*tier.get('params',0)/denom:.1f}%)",
                (f"{_fmt(tier.get('querySk', 0))} "
                 f"({100*tier.get('querySk',0)/denom:.1f}%)"
                 if "querySk" in tier else "—"),
                f"{_fmt(tier.get('url_parse', 0))} ({100*tier.get('url_parse',0)/denom:.1f}%)",
                f"{_fmt(tier.get('none', 0))} ({100*tier.get('none',0)/denom:.1f}%)",
            ])
        if rows3:
            caption = (
                "Attribution source tiers — where each param came from "
                f"(sampled {_fmt(tiers.get('sample_size', 0))} of "
                f"{_fmt(tiers.get('total_events', 0))} events)"
            )
            html += _table(
                ["param", "events.params", "querySk", "url_parse", "none"],
                rows3, caption=caption,
            )

    # Flow lineage split — produced by analysis.extract._lineage_split as:
    #   {"total_clients": N, "counts": {...}, "pcts": {...}}
    lin = coverage.get("lineage_split") or {}
    counts = lin.get("counts") or {}
    pcts = lin.get("pcts") or {}
    if counts:
        order = ("dogi", "auto_redirect", "edge_ambiguous_build", "unknown_build")
        rows4 = [
            [k, _fmt(counts.get(k, 0)), f"{pcts.get(k, 0.0):.1f}%"]
            for k in order if k in counts
        ]
        html += _table(["lineage", "clients", "share"], rows4,
                       caption="Flow lineage split",
                       rate_cols=[2])
    return html


# ── A7 rendering ─────────────────────────────────────────────────────────────

def a7_section(a7: dict | None, mode: str) -> str:
    if not a7:
        return ""
    html = '<h3 id="a7">A7 — Non-Activator Deep-Dive</h3>\n'

    # Table 1 — cohort sizing
    t1 = a7.get("table1_cohort_sizing", {})
    html += _kpi_cards([
        ("Non-activators",
         f"{_fmt(t1.get('non_activators', 0))} ({t1.get('non_pct_of_total', 0):.1f}%)",
         "warn"),
        ("Non-activators with eligible opp.",
         _fmt(t1.get("non_with_eligible", 0)), "bad"),
        ("Non-activators, no eligible opp.",
         _fmt(t1.get("non_no_eligible", 0)), "neutral"),
        ("Never-activator (in-period)",
         _fmt(t1.get("never_activator_in_period", 0)), "neutral"),
    ])

    # Table 2 — profile distribution (skipped in pulse; volume too thin
    # per-dimension to be meaningful on a 7-day window).
    if mode != "pulse":
        t2 = a7.get("table2_profile", {})
        for dim, cohorts in t2.items():
            rows = []
            na = {r["value"]: r for r in cohorts.get("non_activator", [])}
            ac = {r["value"]: r for r in cohorts.get("activator", [])}
            values = list(na.keys()) + [v for v in ac.keys() if v not in na]
            for v in values[:10]:
                na_r = na.get(v, {})
                ac_r = ac.get(v, {})
                rows.append([
                    v,
                    f'{_fmt(na_r.get("count", 0))} ({na_r.get("pct", 0):.1f}%)',
                    f'{_fmt(ac_r.get("count", 0))} ({ac_r.get("pct", 0):.1f}%)',
                ])
            if rows:
                html += _table(
                    [dim, "non-activator", "activator"],
                    rows,
                    caption=f"A7.2 Profile distribution — {dim}",
                )

    # Table 3 — non-activator rate by segment.
    # Per specs/problems/problem_a_non_activator.md "Weekly pulse subset":
    # restrict Table 3 to browser / country / flow-lineage only in pulse.
    t3 = a7.get("table3_non_activator_rate", {})
    t3_dims = (("browser_fam", "country", "lineage") if mode == "pulse"
               else tuple(t3.keys()))
    for seg_name in t3_dims:
        rows = t3.get(seg_name) or []
        if not rows:
            continue
        html += _table(
            [seg_name, "users", "activators", "non_activators", "non_activator_rate"],
            [[str(r[seg_name]) if seg_name in r else "<missing>",
              _fmt(r["users"]), _fmt(r["activators"]),
              _fmt(r["non_activators"]),
              f"{r['non_activator_rate']:.1f}%"] for r in rows],
            caption=f"A7.3 Non-activator rate × {seg_name}",
            rate_cols=[4],
        )

    # Table 5 — top-N non-activator cohorts. Shown in BOTH pulse (top-5)
    # and monthly deep (top-10), per spec.
    t5 = a7.get("table5_top_cohorts", [])
    if t5:
        t5_limit = 5 if mode == "pulse" else 10
        rows = [[r["cohort"], _fmt(r["users"]), f"{r['share_pct']:.1f}%"]
                for r in t5[:t5_limit]]
        html += _table(["cohort (browser / country)", "users", "% of non-activators"],
                       rows,
                       caption=f"A7.5 Top-{t5_limit} non-activator cohorts",
                       rate_cols=[2])

    # Weekly pulse stops here — no session metrics, no hypothesis proxies.
    if mode == "pulse":
        return html

    # Table 4 — session metrics
    t4 = a7.get("table4_session_metrics", {})
    if t4:
        rows = []
        for cohort in ("non_activator", "activator"):
            s = t4.get(cohort, {})
            rows.append([
                cohort,
                _fmt(s.get("sessions", 0)),
                f"{s.get('median_duration_s', 0):.0f}",
                f"{s.get('median_events', 0):.1f}",
                f"{s.get('median_eligible_hits', 0):.1f}",
                f"{s.get('bounce_rate_pct', 0):.1f}%",
            ])
        html += _table(
            ["cohort", "sessions", "median_duration_s", "median_events",
             "median_eligible_hits", "bounce_rate"],
            rows, caption="A7.4 Session metrics — non-activator vs activator",
        )

    # (Table 5 already rendered above the pulse cut-off.)

    # Table 6 — hypothesis proxies
    t6 = a7.get("table6_hypothesis_proxies", {})
    if t6:
        rows = [[k.replace("_", " "), _fmt(v)] for k, v in t6.items()]
        html += _table(["hypothesis proxy", "affected non-activators"],
                       rows, caption="A7.6 Hypothesis proxy counts")
    return html


# ── Ranked root causes (auto-generated) ──────────────────────────────────────

_REASON_LABEL = {
    # Global
    "NO_OUR_SK_IN_72H":       "GLOBAL_DIRECT",
    "FOREIGN_SK_AFTER_OUR_SK":"GLOBAL_DIRECT",
    "AF_AFTER_OUR_SK":        "GLOBAL_DIRECT",
    "CASHBACK_TRACE":         "GLOBAL_DIRECT",
    "UNKNOWN":                "GLOBAL_DIRECT",
    # CIS
    "CIS_NO_OUR_SIGNAL_IN_72H": "CIS_DIRECT_AF",
    "CIS_FOREIGN_AF_AFTER_OURS": "CIS_DIRECT_AF",
    "CIS_FOREIGN_UTM_AFTER_OURS":"CIS_DIRECT_UTM",
    "CIS_NO_HUB_REACH_OBSERVED": "CIS_DIRECT_AF",
    "CIS_HUB_REACHED_NO_RETURN": "CIS_DIRECT_AF",
    "CIS_PARTIAL_UTM_ONLY":      "CIS_PARTIAL_UTM",
    "CIS_PROXY_ONLY":            "CIS_PROXY",
    "CIS_CASHBACK_TRACE":        "CIS_DIRECT_AF",
    "CIS_UNKNOWN":               "CIS_DIRECT_AF",
}


def ranked_root_causes(results_b: dict) -> str:
    rc = (results_b or {}).get("reason_codes", {}).get("All", [])
    if not rc:
        return _callout("info", "Problem B not available — ranked root causes skipped.")
    rc_sorted = sorted(rc, key=lambda r: r["count"], reverse=True)[:10]
    rows = []
    for i, r in enumerate(rc_sorted, start=1):
        label = _REASON_LABEL.get(r["reason_code"], "")
        rows.append([
            str(i),
            f'<code>{r["reason_code"]}</code>',
            _label_html(label) if label else "",
            _fmt(r["count"]),
            f'{r["pct"]:.1f}%',
        ])
    return _table(
        ["#", "Reason code", "Label", "Unmatched PC", "Share"],
        rows, caption="Ranked root causes — Problem B reason codes",
        rate_cols=[4],
    )


# ── Static sections ──────────────────────────────────────────────────────────

def definitions_section() -> str:
    return """
<ol>
<li><strong>Canonical user identity:</strong> <code>guests._id</code> =
    Mixpanel <code>$user_id</code>. Join events / clients / guestStateHistory
    via <code>guest_id</code>. <em>Never</em> use <code>clients._id</code>.</li>
<li><strong>Global direct affiliate state (sk-based):</strong> events whose
    attribution is an AliHelper-owned <code>sk</code> from the whitelist.
    Source priority: <code>events.params.sk</code> →
    <code>events.payload.querySk</code> → parse <code>events.payload.url</code>.</li>
<li><strong>CIS direct affiliate state (two mutually exclusive URL patterns
    on <code>aliexpress.ru</code>):</strong>
    Pattern A <code>af=*_7685</code> (+ typically <code>utm_medium=cpa</code>);
    Pattern B <code>utm_source=aerkol</code> + <code>utm_medium=cpa</code> +
    <code>utm_campaign=*_7685</code>. Source priority:
    <code>events.params.&lt;name&gt;</code> → parse URL.</li>
<li><strong>CIS proxy-return fallback:</strong> when no owned CIS marker is
    present but an <code>aliexpress.ru</code> event lands within
    ≤120 s after an <code>Affiliate Click</code> inside the 72 h window — label
    <code>CIS_PROXY</code>. Weakest evidence tier; never overrides a direct
    marker.</li>
<li><strong>Eligible opportunity:</strong> product pages only —
    DOGI: page carries <code>productId</code>; auto-redirect: URL matches
    <code>clients.checkListUrls</code>. Homepages / category pages do not
    generate affiliate activation.</li>
<li><strong>Attribution window:</strong> 72 h before
    <code>Purchase Completed</code>, server-side <code>events</code> only.
    Client-side <code>last_sk</code> / <code>last_af</code> /
    <code>last_utm_*</code> have no 72 h limit and are NOT authoritative.</li>
<li><strong>Client enrichment rule:</strong> use <code>clients</code> only
    to add context (browser, user_agent, os, city, country, client_version)
    to <code>events</code>. Treat as client-state history, not a canonical
    user table.</li>
<li><strong>Global overwrite rule:</strong> within the 72 h window, a foreign
    <code>sk</code> OR a foreign <code>af</code> (third-party CIS marker on
    a Global AliExpress host) observed <em>after</em> the latest owned
    <code>sk</code> timestamp counts as overwrite.</li>
<li><strong>CIS overwrite rule:</strong> within the 72 h window, a foreign
    <code>af</code> or foreign <code>utm_*</code> observed <em>after</em>
    the latest owned CIS marker counts as overwrite. <code>foreign_af_after</code>
    and <code>foreign_utm_after</code> are tracked separately.</li>
<li><strong>Mature purchase cohort:</strong> for Problem B (deep mode), the
    window ends 7 days before today so postbacks have a chance to arrive
    (post­backs can lag 3–7 days). See
    <code>specs/workflows/recurring_reports.md</code>.</li>
<li><strong>Purchase matching rule:</strong> same user, ±10 min proximity
    (no <code>order_id</code> available). Sensitivity run at 5 / 10 / 15 / 20 min.</li>
<li><strong>Global direct-return evidence rule:</strong> an owned <code>sk</code>
    marker (by source priority above) inside the 72 h window.</li>
<li><strong>CIS direct-return evidence rule (UTM in URL):</strong> Pattern A
    <code>af=*_7685</code> or Pattern B full UTM triple
    (<code>aerkol</code> + <code>cpa</code> + <code>*_7685</code>) inside
    the 72 h window.</li>
<li><strong>CIS proxy-return rule:</strong> fallback ≤120 s; used only when
    no direct CIS marker is present.</li>
<li><strong>Latest delivered config rule:</strong> for any enrichment that
    depends on config (hub domain, cashback list, checkListUrls), use the
    most recent <code>guestStateHistory</code> entry before the event
    timestamp — never the live config.</li>
<li><strong>Routing-based regional split:</strong> classify by URL domain —
    <code>aliexpress.ru</code> = CIS, all other AliExpress hosts = Global.
    <em>UA is routed as Global/Portals</em>, not CIS. An EPN suffix
    <code>_7685</code> on a Global host is an anomaly, not a CIS classification.</li>
</ol>

<h3>Labels</h3>
<p>Every finding carries one of:
<code>GLOBAL_DIRECT</code> / <code>CIS_DIRECT_AF</code> /
<code>CIS_DIRECT_UTM</code> / <code>CIS_PARTIAL_UTM</code> / <code>CIS_PROXY</code>.
<code>CIS_DIRECT_AF</code> + <code>CIS_DIRECT_UTM</code> may be aggregated
as <code>CIS_DIRECT</code> in summaries.</p>
"""


def caveats_section() -> str:
    return """
<ul>
<li><strong>Cashback observability:</strong> client-side only; partial evidence via
    <code>Purchase Completed.cashback_list</code> (current session only).</li>
<li><strong>Auto-redirect attempts:</strong> no backend log; reconstructed from eligible
    visits + lineage + cooldown + config + later signals.</li>
<li><strong>noLogUrls exclusions:</strong> absence of events near checkout is not
    necessarily absence of activity.</li>
<li><strong>Short-window (pulse) limitations:</strong> Problem B is NOT reliable on 7 d —
    postbacks lag 3–7 d, per-reason-code volume too thin. Monthly deep only.</li>
<li><strong>New-field coverage:</strong> <code>events.params</code>, <code>build_app</code>,
    and <code>Purchase Completed</code> last_* fields rolled out mid-April 2026 — coverage
    is near zero in earlier windows.</li>
<li><strong>PC client-side fields:</strong> <code>last_sk</code> / <code>last_af</code> /
    <code>last_utm_*</code> have no 72 h limit and are NOT authoritative. Used only for
    validation (B6) / coverage.</li>
<li><strong>MongoDB indexing:</strong> <code>events</code> has no index on <code>created</code> —
    use <code>_id</code>-based filtering + <code>allowDiskUse=true</code>.</li>
<li><strong>Mixpanel timezone:</strong> <code>Europe/Moscow</code> (UTC+3) — explicit
    conversion for all joins.</li>
</ul>
"""


def longitudinal_section() -> str:
    """Section 9 — recurring reports only. Populated when LONG_BASELINE env
    var is set to a prior report_id; otherwise renders a placeholder."""
    baseline = os.getenv("LONG_BASELINE", "").strip()
    prev_same = os.getenv("LONG_PREVIOUS", "").strip()
    if REPORT_MODE == "oneoff":
        return _callout("info",
                        "Longitudinal comparison is not applicable for "
                        "one-off reports — see sections 5/6 for the primary "
                        "findings.")
    if not baseline and not prev_same:
        return _callout("info",
                        "No baseline configured. Set <code>LONG_BASELINE</code> "
                        "and/or <code>LONG_PREVIOUS</code> env vars to a prior "
                        "<code>report_id</code> to enable current-vs-previous / "
                        "current-vs-baseline / 4-week-trailing comparison. "
                        "See <code>specs/workflows/recurring_reports.md</code>.")
    rows = []
    if prev_same:
        rows.append(["Previous same-type", prev_same,
                     "<em>Δ computation pending wiring</em>"])
    if baseline:
        rows.append(["Baseline", baseline,
                     "<em>Δ computation pending wiring</em>"])
    rows.append(["4-week trailing avg", "<em>derived from history store</em>",
                 "<em>Δ computation pending wiring</em>"])
    return _table(["Reference", "Report id", "Delta"],
                  rows, caption="Longitudinal comparison")


def reproducible_code_section() -> str:
    return f"""
<p>Every figure in this report is rebuildable from the raw extracts in
<code>cache/</code>. Cache namespace for this run:
<code>{CACHE_SUFFIX}</code>.</p>

<h3>Pipeline (run in order)</h3>
<ol>
<li><code>python -m analysis.extract</code> — MongoDB + Mixpanel export
    into <code>cache/</code> (incremental; reuses existing extracts).</li>
<li><code>python -m analysis.problem_a</code> — Problem A funnel, label
    breakdown, A5/A6, segments, A7 non-activator deep-dive. Writes
    <code>cache/results_a__{CACHE_SUFFIX}.pkl</code>.</li>
<li><code>python -m analysis.problem_b</code> — Problem B attribution
    reconstruction, reason codes, B3 delayed postback, B4/B5/B6. Writes
    <code>cache/results_b__{CACHE_SUFFIX}.pkl</code> (skipped in pulse).</li>
<li><code>python -m analysis.report</code> — this HTML report.</li>
</ol>

<h3>Mode selection</h3>
<p>Set <code>REPORT_MODE</code> to one of <code>oneoff</code> /
<code>pulse</code> / <code>deep</code> before running any step. Windows
are derived from the mode in <code>src/config.py</code>; cache files are
keyed by <code>mode__A_START__A_END</code> to avoid cross-contamination.</p>

<h3>Files</h3>
<ul>
<li><code>analysis/extract.py</code> — extraction + coverage snapshot.</li>
<li><code>analysis/problem_a.py</code> — Problem A analysis + A7.</li>
<li><code>analysis/problem_b.py</code> — Problem B analysis + B6 validation.</li>
<li><code>analysis/report.py</code> — HTML assembly from cached results.</li>
<li><code>src/utils.py</code> — classification, labeling, region / lineage /
    subtype helpers.</li>
<li><code>src/config.py</code> — windows, OUR_SKS, EPN cabinet id,
    attribution constants.</li>
</ul>
"""


def recommendations_section() -> str:
    return """
<h3>Quick wins</h3>
<ol>
<li>Push <code>clients.build_app</code> completion monotonically — force updates on old
    clients to shrink <code>edge_ambiguous_build</code> and <code>unknown_build</code>.</li>
<li>Backfill <code>events.params</code> coverage monitoring as an alert (drops =
    instrumentation regression).</li>
<li>For any label-level finding with <code>CIS_PARTIAL_UTM</code> share &gt; 1 %, trace
    the creative source — these are typically misconfigured landing pages.</li>
</ol>

<h3>Medium-term product changes</h3>
<ol>
<li>Reduce time between AliHelper activation and purchase (checkout-time re-activation
    when a foreign <code>sk</code> / <code>af</code> is detected).</li>
<li>Improve DOGI activation rate where DOGI hub-reach is materially below auto-redirect.</li>
<li>Add normalised affiliate metadata to <code>events</code>
    (<code>affiliate_provider</code>, <code>is_alihelper_owned</code>,
    <code>affiliate_marker_type</code>).</li>
</ol>

<h3>Tracking &amp; instrumentation</h3>
<ol>
<li>Log an <code>Affiliate Overwrite Detected</code> event when our owned marker is
    replaced — real-time overwrite telemetry.</li>
<li>Log auto-redirect attempts (success/failure) as backend events.</li>
<li>Backend-log cashback exposure where detectable (currently client local storage only).</li>
</ol>
"""


# ── B-specific rendering helpers ─────────────────────────────────────────────

def problem_b_findings(results_b: dict) -> str:
    if not results_b:
        return _callout("info", "Problem B not available in this mode (pulse).")
    html = ""
    s = results_b.get("summary", {})
    if s:
        total_pc = s.get("total_pc", 0) or 1
        matched = s.get("matched", 0)
        unmatched = s.get("unmatched", 0)
        html += _kpi_cards([
            ("Total Purchase Completed", _fmt(total_pc), "neutral"),
            ("Matched to Purchase",
             f"{_fmt(matched)} ({100*matched/total_pc:.1f}%)",
             "good" if 100*matched/total_pc >= 60 else "warn"),
            ("Unmatched (gap)",
             f"{_fmt(unmatched)} ({100*unmatched/total_pc:.1f}%)",
             "bad"),
        ])

    # B1
    attr = results_b.get("attribution", {})
    if attr:
        rows = []
        for reg, v in attr.items():
            rows.append([
                reg, _fmt(v["total"]), _fmt(v["any_owned"]),
                _pct(v["any_owned"], v["total"]),
                _fmt(v.get("owned_sk", 0)),
                _fmt(v.get("owned_af", 0)),
                _fmt(v.get("owned_utm_full", 0)),
                _fmt(v.get("owned_utm_partial", 0)),
            ])
        html += _table(
            ["Region", "Total PC", "Any owned marker", "Any %",
             "owned sk", "owned af (A)", "owned UTM full (B)", "partial UTM"],
            rows, caption="B1 — Attribution evidence by effective region",
            rate_cols=[3],
        )

    # B2
    ow = results_b.get("overwrite", {})
    if ow:
        rows = []
        for reg, v in ow.items():
            den = v["with_owned"] or 1
            rows.append([
                reg, _fmt(v["with_owned"]),
                f"{_fmt(v['foreign_sk_after'])} ({100*v['foreign_sk_after']/den:.1f}%)",
                f"{_fmt(v['af_on_global_after'])} ({100*v['af_on_global_after']/den:.1f}%)",
                f"{_fmt(v['foreign_af_after'])} ({100*v['foreign_af_after']/den:.1f}%)",
                f"{_fmt(v['foreign_utm_after'])} ({100*v['foreign_utm_after']/den:.1f}%)",
            ])
        html += _table(
            ["Region", "With owned", "foreign sk after",
             "af on Global after", "foreign af after", "foreign UTM after"],
            rows, caption="B2 — Overwrite (split: foreign-af vs foreign-utm)",
        )

    # Reason codes
    rc = results_b.get("reason_codes", {})
    if rc:
        html += reason_code_table(rc)

    # B4
    b4 = results_b.get("matching_sensitivity", [])
    if b4:
        rows = [[f"{r['window_min']} min", _fmt(r["matched"]),
                 f"{r['pct']:.1f}%"] for r in b4]
        html += _table(["Window", "Matched", "Match rate"],
                       rows, caption="B4 — Matching window sensitivity",
                       rate_cols=[2])

    # B5
    if "segments" in results_b:
        html += segment_tables(results_b["segments"], "Problem B")

    # B6
    b6 = results_b.get("pc_field_validation", {})
    if b6:
        rows = []
        for kind, v in b6.items():
            rows.append([kind, _fmt(v["checked"]), _fmt(v["agree"]),
                         f"{v['agree_pct']:.1f}%"])
        html += _table(["PC field", "checked", "agree", "agreement %"],
                       rows, caption="B6 — PC field vs events reconstruction",
                       rate_cols=[3])
    return html


# ── CSS ──────────────────────────────────────────────────────────────────────

_CSS = """
*, *::before, *::after { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  font-size: 15px; line-height: 1.65; color: #374151;
  margin: 0; background: #fff;
}
a { color: #2563eb; text-decoration: none; }
a:hover { text-decoration: underline; }
code {
  font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
  font-size: 12.5px; background: #f3f4f6; padding: 1px 5px;
  border-radius: 3px; color: #1e40af;
}
em { color: #6b7280; font-style: normal; }
ol, ul { margin: 8px 0 8px 0; padding-left: 22px; }
li { margin: 5px 0; }
hr { border: none; border-top: 1px solid #e5e7eb; margin: 32px 0; }

.layout { display: flex; min-height: 100vh; }
.sidebar {
  width: 240px; flex-shrink: 0;
  position: sticky; top: 0; height: 100vh; overflow-y: auto;
  background: #f9fafb; border-right: 1px solid #e5e7eb;
  padding: 24px 0;
}
.sidebar-logo {
  padding: 0 18px 16px; font-weight: 700; font-size: 14px;
  color: #1e40af; border-bottom: 1px solid #e5e7eb; margin-bottom: 12px;
}
.sidebar nav a {
  display: block; padding: 5px 18px; font-size: 13px;
  color: #6b7280; border-left: 3px solid transparent;
  transition: all .15s;
}
.sidebar nav a:hover, .sidebar nav a.active {
  color: #2563eb; background: #eff6ff;
  border-left-color: #2563eb; text-decoration: none;
}
.main { flex: 1; max-width: 1160px; padding: 32px 48px; overflow-x: hidden; }

h1 {
  font-size: 26px; font-weight: 700; color: #111827;
  border-bottom: 3px solid #2563eb; padding-bottom: 10px; margin-top: 0;
}
h2 {
  font-size: 20px; font-weight: 700; color: #1e40af;
  margin-top: 48px; border-bottom: 2px solid #e5e7eb; padding-bottom: 6px;
}
h3 { font-size: 16px; font-weight: 600; color: #374151; margin-top: 28px; }
h4, .tbl-caption {
  font-size: 13px; font-weight: 600; color: #6b7280;
  margin: 20px 0 6px; text-transform: uppercase; letter-spacing: .4px;
}
.meta { color: #9ca3af; font-size: 13px; margin-top: -8px; }

.kpi-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(180px,1fr));
  gap: 14px; margin: 20px 0 28px;
}
.kpi-card {
  background: #f9fafb; border: 1px solid #e5e7eb;
  border-radius: 10px; padding: 16px 18px; text-align: center;
  box-shadow: 0 1px 3px rgba(0,0,0,.06);
}
.kpi-value { font-size: 28px; font-weight: 800; line-height: 1.1; }
.kpi-label { font-size: 12px; color: #6b7280; margin-top: 4px; }
.kpi-good  { color: #059669; }
.kpi-warn  { color: #d97706; }
.kpi-bad   { color: #dc2626; }
.kpi-neutral { color: #2563eb; }

.callout {
  display: flex; gap: 12px; align-items: flex-start;
  padding: 13px 16px; margin: 14px 0;
  border-radius: 6px; border-left: 4px solid;
  font-size: 14px;
}
.callout-icon { font-size: 16px; flex-shrink: 0; margin-top: 1px; }
.callout-critical { background:#fef2f2; border-color:#f87171; color:#7f1d1d; }
.callout-warning  { background:#fffbeb; border-color:#fbbf24; color:#78350f; }
.callout-info     { background:#eff6ff; border-color:#60a5fa; color:#1e3a8a; }
.callout-finding  { background:#f0fdf4; border-color:#34d399; color:#14532d; }

.lbl {
  display: inline-block; padding: 2px 8px; border-radius: 99px;
  font-size: 11.5px; font-weight: 700; font-family: monospace;
  margin-right: 4px; letter-spacing: .2px;
}
.lbl-global        { background: #dbeafe; color: #1d4ed8; }
.lbl-cis-direct    { background: #fef3c7; color: #92400e; }
.lbl-cis-af        { background: #fed7aa; color: #9a3412; }
.lbl-cis-utm       { background: #fde68a; color: #92400e; }
.lbl-cis-partial   { background: #fef9c3; color: #854d0e; }
.lbl-cis-proxy     { background: #e5e7eb; color: #374151; }
.lbl-neutral       { background: #e5e7eb; color: #374151; }

.tbl-wrap { overflow-x: auto; margin: 0 0 28px; border-radius: 8px;
            border: 1px solid #e5e7eb; box-shadow: 0 1px 4px rgba(0,0,0,.05); }
table { border-collapse: collapse; width: 100%; font-size: 13.5px; }
thead tr { background: #f3f4f6; }
th {
  text-align: left; padding: 9px 13px;
  border-bottom: 2px solid #d1d5db;
  font-weight: 600; font-size: 12.5px;
  color: #374151; white-space: nowrap;
  position: sticky; top: 0; background: #f3f4f6;
}
td { padding: 7px 13px; border-bottom: 1px solid #f3f4f6; vertical-align: top; }
tr:last-child td { border-bottom: none; }
tbody tr:hover td { background: #eff6ff; }
tbody tr:nth-child(even) td { background: #fafafa; }
tbody tr:nth-child(even):hover td { background: #eff6ff; }

.rate-good { background: #d1fae5 !important; color: #065f46; font-weight: 600; }
.rate-mid  { background: #fef3c7 !important; color: #78350f; font-weight: 600; }
.rate-bad  { background: #fee2e2 !important; color: #7f1d1d; font-weight: 600; }

.funnel-section { margin: 20px 0 28px; }
.funnel-region-title {
  font-size: 13px; font-weight: 700; color: #374151;
  margin: 16px 0 8px; text-transform: uppercase; letter-spacing: .4px;
}
.funnel-bars { display: flex; flex-direction: column; gap: 7px; }
.funnel-row { display: flex; align-items: center; gap: 10px; }
.funnel-label { width: 220px; flex-shrink: 0; font-size: 13px; color: #4b5563; }
.funnel-bar-wrap {
  flex: 1; background: #f3f4f6; border-radius: 4px; height: 22px; overflow: hidden;
}
.funnel-bar { height: 100%; border-radius: 4px; transition: width .3s; min-width: 2px; }
.bar-good    { background: #34d399; }
.bar-mid     { background: #fbbf24; }
.bar-bad     { background: #f87171; }
.bar-neutral { background: #93c5fd; }
.funnel-count { width: 80px; text-align: right; font-size: 13px;
                font-weight: 600; color: #374151; }

.rc-bar-wrap { width: 120px; background: #f3f4f6; border-radius: 3px; height: 10px; overflow: hidden; }
.rc-bar { height: 100%; background: #6366f1; border-radius: 3px; }

@media (max-width: 860px) {
  .sidebar { display: none; }
  .main { padding: 20px; }
}
"""

_SIDEBAR_JS = """
<script>
const sections = document.querySelectorAll('h2[id]');
const navLinks = document.querySelectorAll('.sidebar nav a[href^="#"]');
const obs = new IntersectionObserver(entries => {
  entries.forEach(e => {
    if (e.isIntersecting) {
      navLinks.forEach(l => l.classList.remove('active'));
      const active = document.querySelector('.sidebar nav a[href="#' + e.target.id + '"]');
      if (active) active.classList.add('active');
    }
  });
}, { rootMargin: '-20% 0px -75% 0px' });
sections.forEach(s => obs.observe(s));
</script>
"""


# ── Full report ──────────────────────────────────────────────────────────────

def build_report(results_a: dict, results_b: dict | None,
                 coverage: dict | None) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    a_period = f"{A_START.strftime('%Y-%m-%d')} → {A_END.strftime('%Y-%m-%d')}"
    b_period = (f"{B_START.strftime('%Y-%m-%d')} → {B_END.strftime('%Y-%m-%d')}"
                if B_START and B_END else "n/a (pulse mode)")

    nav_items = [
        ('meta',      '1. Metadata'),
        ('coverage',  '2. Coverage'),
        ('defs',      '3. Definitions'),
        ('caveats',   '4. Data quality'),
        ('pa',        '5. Problem A'),
    ]
    if PROBLEM_B_ENABLED:
        nav_items.append(('pb', '6. Problem B'))
    nav_items += [
        ('ranked',      '7. Ranked root causes'),
        ('unexplained', '8. Unexplained remainder'),
        ('longitudinal','9. Longitudinal'),
        ('recs',        '10. Recommendations'),
        ('repro',       '11. Reproducible code'),
    ]
    nav_html = "\n".join(f'<a href="#{i}">{t}</a>' for i, t in nav_items)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AliHelper Research — {REPORT_ID}</title>
<style>{_CSS}</style>
</head>
<body>
<div class="layout">

<aside class="sidebar">
  <div class="sidebar-logo">📊 AliHelper Research</div>
  <nav>{nav_html}</nav>
</aside>

<main class="main">
<h1>AliHelper — Root-Cause Research Report</h1>
<p class="meta">
  Generated {now} &nbsp;·&nbsp;
  Mode <code>{REPORT_MODE}</code> &nbsp;·&nbsp; Report id <code>{REPORT_ID}</code>
</p>

<h2 id="meta">1. Report metadata</h2>
{_table(
    ["Field", "Value"],
    [
        ["report_id", REPORT_ID],
        ["report_mode", REPORT_MODE],
        ["problem_a_period_utc", a_period],
        ["problem_b_period_utc", b_period],
        ["generated_at_utc", now],
        ["baseline_report_id", os.getenv("LONG_BASELINE", "—") or "—"],
        ["previous_same_type_report_id", os.getenv("LONG_PREVIOUS", "—") or "—"],
    ],
    caption="Report metadata",
)}

<h2 id="coverage">2. Coverage snapshot</h2>
{coverage_section(coverage)}

<h2 id="defs">3. Definitions</h2>
{definitions_section()}

<h2 id="caveats">4. Data-quality caveats</h2>
{caveats_section()}

<h2 id="pa">5. Problem A — Missing Affiliate Click</h2>
"""

    if results_a and "funnel" in results_a:
        f = results_a["funnel"]
        all_f = f.get("All", {})
        total = all_f.get("total_users", 1) or 1
        hub = all_f.get("reached_hub", 0)
        ret = all_f.get("any_return", 0)
        hub_rate = 100 * hub / total
        ret_rate = 100 * ret / (hub or 1)
        html += _kpi_cards([
            ("Total users", _fmt(total), "neutral"),
            ("Eligible", _fmt(all_f.get("eligible_users", 0)), "good"),
            ("Reached hub", f"{_fmt(hub)} ({hub_rate:.0f}%)",
             "good" if hub_rate >= 70 else "warn"),
            ("Any return", f"{_fmt(ret)} ({ret_rate:.0f}%)",
             "good" if ret_rate >= 80 else ("warn" if ret_rate >= 50 else "bad")),
        ])
        html += funnel_table(f)

    lbl = (results_a or {}).get("label_breakdown", {})
    if lbl:
        rows = [[_label_html(str(k)), _fmt(int(v))] for k, v in lbl.items()]
        html += _table(["Primary label", "Users"], rows,
                       caption="Label breakdown (per-user primary label)")

    ma = (results_a or {}).get("missing_ac", {})
    if ma:
        html += _section("A5 — Missing Mixpanel click tracking",
                         f"<p>{_label_html('GLOBAL_DIRECT')} Global: "
                         f"{_fmt(ma.get('global', 0))} users with our sk but no Affiliate Click<br>"
                         f"{_label_html('CIS_DIRECT_AF')}{_label_html('CIS_DIRECT_UTM')} CIS: "
                         f"{_fmt(ma.get('cis', 0))} users with our af/UTM but no Affiliate Click</p>", 3)

    hn = (results_a or {}).get("hub_no_return", {})
    if hn:
        html += _section("A6 — Hub reached, no return",
                         f"<p>{_label_html('GLOBAL_DIRECT')} Global: "
                         f"{_fmt(hn.get('global', 0))} users reached hub, no owned sk<br>"
                         f"{_label_html('CIS_DIRECT_AF')}{_label_html('CIS_DIRECT_UTM')} CIS: "
                         f"{_fmt(hn.get('cis', 0))} users reached hub, no af/UTM or proxy return</p>", 3)

    if (results_a or {}).get("segments"):
        html += segment_tables(results_a["segments"], "Problem A")

    html += a7_section((results_a or {}).get("a7"), REPORT_MODE)

    if PROBLEM_B_ENABLED:
        html += '<h2 id="pb">6. Problem B — Purchase Completed without Purchase</h2>\n'
        html += problem_b_findings(results_b)

    html += '<h2 id="ranked">7. Ranked root causes</h2>\n'
    html += ranked_root_causes(results_b)

    html += """<h2 id="unexplained">8. Unexplained remainder</h2>
<p>Everything under reason code <code>UNKNOWN</code> / <code>CIS_UNKNOWN</code>
remains unexplained after attribution reconstruction — likely a mix of delayed
postback, client-side cashback exposure (not observable server-side), partner-program
exclusions, or af-parameter overwrite on Global URLs (new-field coverage dependent).
See the Coverage snapshot above to judge how much of this residual is explained by
fallback methodology.</p>
"""

    html += '<h2 id="longitudinal">9. Longitudinal comparison</h2>\n'
    html += longitudinal_section()

    html += '<h2 id="recs">10. Recommended fixes</h2>\n'
    html += recommendations_section()

    html += '<h2 id="repro">11. Reproducible code</h2>\n'
    html += reproducible_code_section()

    html += f"""
<hr>
<p class="meta">End of report · {REPORT_ID}</p>
</main>
</div>
{_SIDEBAR_JS}
</body>
</html>"""

    return html


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    results_a = _load_pkl("results_a")
    results_b = _load_pkl("results_b", required=False) if PROBLEM_B_ENABLED else None
    coverage  = _load_pkl("coverage", required=False)

    html = build_report(results_a, results_b, coverage)
    # Organise by year/month of the A window end, per
    # specs/workflows/recurring_reports.md ("Store all under reports/YYYY/MM/…")
    out_dir = REPORTS_DIR / A_END.strftime("%Y") / A_END.strftime("%m")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{REPORT_ID}.html"
    with open(out_path, "w") as f:
        f.write(html)
    print(f"Report saved to {out_path}")


if __name__ == "__main__":
    run()
