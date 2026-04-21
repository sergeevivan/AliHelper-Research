#!/usr/bin/env python3
"""
HTML report generation from Problem A & Problem B results,
including root-cause analysis narrative, ranked hypotheses,
impact tables, and instrumentation recommendations.

Usage:
    python -m analysis.report
"""

import pickle
from datetime import datetime, timezone
from pathlib import Path

from src.config import CACHE_DIR, A_START, A_END, B_START, B_END


REPORTS_DIR = Path("./reports")
REPORTS_DIR.mkdir(exist_ok=True)


def _load_pkl(name):
    with open(CACHE_DIR / f"{name}.pkl", "rb") as f:
        return pickle.load(f)


def _pct(num, denom):
    if denom == 0:
        return "N/A"
    return f"{100 * num / denom:.1f}%"


# ── HTML building blocks ─────────────────────────────────────────────────────

def _rate_cell_class(val_str: str) -> str:
    """Return CSS class for a rate cell based on its numeric value."""
    try:
        v = float(str(val_str).rstrip("%"))
        if v >= 80:
            return "rate-good"
        if v >= 50:
            return "rate-mid"
        return "rate-bad"
    except (ValueError, AttributeError):
        return ""


def _table(headers: list[str], rows: list[list], caption: str = "",
           rate_cols: list[int] | None = None) -> str:
    """Generate an HTML table with optional rate-colored columns."""
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
    tag = f"h{level}"
    return f"<{tag}>{title}</{tag}>\n{content}\n"


def _fmt(n):
    if isinstance(n, (int, float)):
        return f"{n:,.0f}"
    return str(n)


def _callout(kind: str, text: str) -> str:
    """Generate a styled callout box with icon."""
    styles = {
        "critical": ("callout-critical", "❗"),
        "warning":  ("callout-warning",  "⚠️"),
        "info":     ("callout-info",     "ℹ️"),
        "finding":  ("callout-finding",  "✅"),
    }
    cls, icon = styles.get(kind, styles["info"])
    return f'<div class="callout {cls}"><span class="callout-icon">{icon}</span>{text}</div>\n'


def _label_html(label: str) -> str:
    """Render an attribution label badge."""
    css = {
        "GLOBAL_DIRECT": "lbl-global",
        "CIS_DIRECT":    "lbl-cis-direct",
        "CIS_PROXY":     "lbl-cis-proxy",
    }
    cls = css.get(label, "lbl-global")
    return f'<span class="lbl {cls}">{label}</span>'


def _kpi_cards(cards: list[tuple]) -> str:
    """Render KPI cards. cards = [(label, value, sentiment), ...] sentiment: good/warn/bad/neutral"""
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


# ── Funnel visualization ──────────────────────────────────────────────────────

def funnel_visual(funnel: dict) -> str:
    """Render a visual funnel with horizontal bars for each region."""
    steps = [
        ("Total users",               "total_users"),
        ("Eligible (product pages)",  "eligible_users"),
        ("+ usable config",           "with_usable_config"),
        ("Reached hub (AC)",          "reached_hub"),
        ("Direct return signal",      "direct_return"),
        ("Any return (direct+proxy)", "any_return"),
    ]
    regions = [r for r in ["Global", "CIS", "All"] if r in funnel]
    colors = {"Global": "#3b82f6", "CIS": "#f59e0b", "All": "#8b5cf6"}

    html = '<div class="funnel-section">'
    for reg in regions:
        f = funnel[reg]
        base = f.get("total_users", 1) or 1
        html += f'<h4 class="funnel-region-title">{reg} ({_fmt(f.get("total_users",0))} users)</h4>'
        html += '<div class="funnel-bars">'
        for step_label, key in steps:
            v = f.get(key, 0)
            pct_width = min(100, 100 * v / base)
            # color bar by conversion from previous step
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
    """Build funnel comparison table across regions."""
    regions = [r for r in ["Global", "CIS", "All"] if r in funnel]
    headers = ["Step"] + regions

    steps = [
        ("1. Total users",               "total_users"),
        ("2. Eligible (product pages)",  "eligible_users"),
        ("3. + usable config",           "with_usable_config"),
        ("4. Reached hub (Affiliate Click)", "reached_hub"),
        ("5. Direct return signal",      "direct_return"),
        ("6. Any return (direct+proxy)", "any_return"),
    ]

    rows = []
    for label, key in steps:
        row = [label]
        for reg in regions:
            v = funnel[reg].get(key, 0)
            row.append(_fmt(v))
        rows.append(row)

    rate_col_indices = list(range(1, len(regions) + 1))
    for label, num_key, den_key in [
        ("Eligible rate",              "eligible_users",    "total_users"),
        ("Hub reach rate (of eligible)", "reached_hub",    "eligible_users"),
        ("Return rate (of hub)",        "any_return",       "reached_hub"),
    ]:
        row = [f"<em>{label}</em>"]
        for reg in regions:
            n = funnel[reg].get(num_key, 0)
            d = funnel[reg].get(den_key, 0)
            row.append(_pct(n, d))
        rows.append(row)

    html = funnel_visual(funnel)
    html += _table(headers, rows, "Funnel — numeric detail",
                   rate_cols=rate_col_indices)
    return html


# ── Reason code table ────────────────────────────────────────────────────────

def reason_code_table(reason_codes: dict) -> str:
    """Build reason code breakdown with inline bar charts."""
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


# ── Segment tables ───────────────────────────────────────────────────────────

# Columns whose names suggest they are rate/percentage values
_RATE_COL_KEYWORDS = ("rate", "pct", "loss", "return", "hub", "elig")


def segment_tables(segments: dict, problem: str) -> str:
    """Build segmentation tables with rate-colored cells."""
    html = ""
    for seg_name, data in segments.items():
        if not data:
            continue
        headers = list(data[0].keys())
        # Detect rate columns by name
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


# ══════════════════════════════════════════════════════════════════════════════
# ROOT-CAUSE ANALYSIS NARRATIVE
# ══════════════════════════════════════════════════════════════════════════════

def build_executive_summary() -> str:
    """Build the executive summary identifying the shared root cause."""
    return """
<div class="exec-box">
<h3>Executive Summary</h3>

<p><strong>Shared root cause:</strong> The dominant cause of purchase commission loss is
<strong>foreign affiliate overwrite</strong> &mdash; third-party affiliates (cashback services,
coupon extensions, price comparison tools) replace AliHelper's <code>sk</code> before
the user completes their purchase.</p>

<ul>
<li><strong>Problem A</strong> &mdash; The Global affiliate flow works: 93.7% of users who reach
the hub return with our <code>sk</code>. CIS return rate is 95.6%+ (100% including proxy).
The main funnel gap is <strong>hub reach</strong>: 31.1% of eligible users never generate
<code>Affiliate Click</code>, driven by DOGI activation UX (64% vs auto-redirect 87%) and
older extension versions.</li>
<li><strong>Problem B</strong> &mdash; Of 240,100 unmatched Purchase Completed events (49.6% gap),
<strong>65.2% (156,550)</strong> are Global users whose AliHelper <code>sk</code> was overwritten
by a foreign <code>sk</code> within the 72h attribution window. An additional 24.6% (59,140)
had no AliHelper <code>sk</code> at all in 72h. CIS losses are smaller: 3.4% unknown,
2.2% no UTM, 1.9% foreign UTM overwrite.</li>
</ul>

<p><strong>Key numbers:</strong> 85.3% of Global Purchase Completed events had our <code>sk</code>
in the 72h window, but 94.1% of those were subsequently overwritten by a foreign <code>sk</code>.
For CIS, 83.7% had our UTM markers, with 18.8% overwritten.</p>

<p><strong>Estimated revenue impact:</strong> Addressing affiliate overwrite for the 156,550 monthly
Global purchases lost to foreign <code>sk</code> overwrite represents the dominant recovery
opportunity. Reducing overwrite by even 30% would recover ~47,000 attributed purchases per month.</p>
</div>
"""


def build_problem_a_analysis() -> str:
    """Build the root-cause analysis for Problem A."""
    html = ""

    # ── Root cause 1: Hub reach gap ───────────────────────────────────────
    html += _section("Root Cause #1: 31.1% of eligible users never reach the hub",
                     "", 3)

    html += _callout("warning",
        f"{_label_html('GLOBAL_DIRECT')} {_label_html('CIS_DIRECT')} &mdash; "
        "Of 65,001 eligible users with usable config, 20,241 (31.1%) never generated "
        "an <code>Affiliate Click</code>. This is the largest funnel gap in Problem A.")

    html += """<p><strong>Segmentation breakdown:</strong></p>
<ul>
<li><strong>By lineage:</strong> DOGI users reach the hub at 64.0% vs auto-redirect at 87.1%.
    The DOGI flow requires user interaction (clicking DOGI coin/product thumbnail), while
    auto-redirect fires automatically. The 23pp gap represents ~11,900 missed activations
    from DOGI users.</li>
<li><strong>By version:</strong> Older versions (3.0.x and 2.30.x) have dramatically lower hub
    reach rates: 3.0.5 at 6.7%, 3.0.3 at 3.2%, 2.30.8 at 14.3%. These collectively affect
    ~3,400 users. Version 3.1.0 (24,890 users) reaches 62.1% vs 3.1.2 at 89.8% &mdash;
    a 27.7pp gap affecting ~6,900 users within v3.1.0.</li>
<li><strong>By browser:</strong> The "other" browser category (3,964 users) has only 31.2% hub
    reach &mdash; these browsers may not support the required extension APIs.
    Safari (4 users) is negligible.</li>
</ul>
"""

    # ── Root cause 2: Modest Global return gap ────────────────────────────
    html += _section("Root Cause #2: 6.3% of Global hub users have no sk return",
                     "", 3)

    html += _callout("info",
        f"{_label_html('GLOBAL_DIRECT')} &mdash; "
        "32,531 of 34,729 Global users who reached the hub (93.7%) returned to AliExpress "
        "with an AliHelper-owned <code>sk</code>. The remaining 2,198 (6.3%) show no "
        "<code>sk</code> in subsequent events.")

    html += """<p><strong>Evidence:</strong></p>
<ul>
<li>Global <code>sk</code> return is functional across all versions, browsers, and hubs
    (all ~95% return rate). This is not a systemic failure.</li>
<li>The 6.3% gap likely represents: redirect timeouts, user cancellations (closing tab
    before return), ad-blockers or privacy extensions stripping query parameters,
    or users navigating away from AliExpress before the return event fires.</li>
<li>A5 check: 404 Global users had our <code>sk</code> without <code>Affiliate Click</code>
    in Mixpanel &mdash; a minor Mixpanel event-delivery loss (~1.2% of Global hub users).</li>
<li>By browser: Edge shows highest return rate at 99.1%, while Firefox shows 90.9% &mdash;
    the auto-redirect flow in Firefox may occasionally lose the sk during its redirect chain.</li>
</ul>
"""

    # ── Root cause 3: CIS has specific issues ────────────────────────────
    html += _section("Root Cause #3: CIS-specific issues (smaller but actionable)", "", 3)

    html += _callout("info",
        f"{_label_html('CIS_DIRECT')} {_label_html('CIS_PROXY')} &mdash; "
        "CIS performs well: 95.6% direct UTM return rate and ~100% "
        "total return (including proxy). But 213 CIS users reached the hub with no return, "
        "and 210 had UTM markers but no Affiliate Click (tracking gap).")

    html += """<p><strong>CIS details:</strong></p>
<ul>
<li>210 CIS users show AliHelper UTM markers without <code>Affiliate Click</code> in
    Mixpanel &mdash; a minor Mixpanel event-delivery loss (~2.1% of CIS hub users).</li>
<li>213 CIS users reached hub but had neither UTM nor proxy return &mdash; possible
    hub-side failures or abandoned redirects.</li>
<li>CIS hub reach rate is 64.5% (vs Global 70.2%), partly because BY (Belarus) shows
    57.3% hub reach.</li>
<li>Yandex browser shows 95.4% return rate &mdash; consistent with CIS-heavy
    user base (Yandex is predominantly Russian).</li>
</ul>
"""

    # ── Impact quantification ────────────────────────────────────────────
    html += _section("Impact Quantification &mdash; Problem A", "", 3)
    html += _table(
        ["Cause", "Label", "Affected Users", "% of Total", "Severity", "Fixability"],
        [
            ["Hub-reach gap: DOGI vs auto-redirect",
             _label_html("GLOBAL_DIRECT") + " " + _label_html("CIS_DIRECT"),
             "~11,900", "~17.9%", "High &mdash; missed activation opportunities",
             "Medium &mdash; product UX, DOGI prominence, auto-trigger exploration"],
            ["Version 3.1.0 underperformance vs 3.1.2",
             _label_html("GLOBAL_DIRECT") + " " + _label_html("CIS_DIRECT"),
             "~6,900", "~10.4%", "Medium &mdash; fixable with version upgrade push",
             "High &mdash; push users to 3.1.2+"],
            ["Legacy versions (< 3.1.0) very low hub reach",
             _label_html("GLOBAL_DIRECT") + " " + _label_html("CIS_DIRECT"),
             "~3,400", "~5.1%", "Medium", "High &mdash; force-update or deprecate"],
            ["Global sk return gap (6.3%)",
             _label_html("GLOBAL_DIRECT"),
             "2,198", "3.3%", "Low &mdash; 93.7% return rate is healthy",
             "Low &mdash; diminishing returns to optimize further"],
            ["CIS Mixpanel tracking loss",
             _label_html("CIS_DIRECT"),
             "210 + 213", "0.6%", "Low", "Low priority"],
        ],
        "Problem A &mdash; Impact by Cause"
    )

    return html


def build_problem_b_analysis() -> str:
    """Build the root-cause analysis for Problem B."""
    html = ""

    # ── Root cause 1: Foreign sk overwrite (Global) ───────────────────────
    html += _section("Root Cause #1: Foreign affiliate overwrite (65.2% of gap)",
                     "", 3)

    html += _callout("critical",
        f"{_label_html('GLOBAL_DIRECT')} &mdash; 156,550 of 240,100 unmatched Purchase Completed "
        "events (65.2%) are Global users with reason code <code>FOREIGN_SK_AFTER_OUR_SK</code>. "
        "AliHelper's <code>sk</code> was present in the 72h attribution window but was subsequently "
        "overwritten by a third-party <code>sk</code> before purchase.")

    html += """<p><strong>This is a last-click attribution problem.</strong> The affiliate activation
flow works correctly &mdash; our <code>sk</code> is set. But between activation and purchase,
the user encounters another affiliate source (cashback extension, coupon site, price comparison
tool, or another browser extension) that replaces AliHelper's <code>sk</code> with its own.</p>

<p><strong>Scale of the overwrite problem:</strong></p>
<ul>
<li>85.3% of Global Purchase Completed (377,286 / 442,237) had our <code>sk</code> in the 72h window</li>
<li>Of those, <strong>94.1%</strong> (355,158) were subsequently overwritten by a foreign <code>sk</code></li>
<li>156,550 ended up unmatched (the overwrite caused commission loss)</li>
<li>The remaining ~198,000 with overwrite still matched to a <code>Purchase</code> &mdash;
    these may be cases where the overwrite happened after the commission was already locked in,
    or where the time-based matching is coincidental</li>
</ul>

<p><strong>Causal chain:</strong></p>
<ol>
<li>User visits AliExpress product page &rarr; extension activates &rarr; hub redirect</li>
<li>User returns with our <code>sk</code> (e.g., <code>_c36PoUEj</code>)</li>
<li>Before purchasing, user visits a cashback site / coupon extension / other affiliate link</li>
<li>Third-party affiliate sets a different <code>sk</code> (e.g., <code>_ePNSNV</code>)</li>
<li>User completes purchase &rarr; commission goes to the last <code>sk</code> owner (not us)</li>
<li><code>Purchase Completed</code> fires, but no AliHelper <code>Purchase</code> is generated</li>
</ol>
"""

    # ── Root cause 2: No sk in 72h ────────────────────────────────────────
    html += _section("Root Cause #2: No AliHelper sk in 72h window (24.6% of gap)", "", 3)

    html += _callout("warning",
        f"{_label_html('GLOBAL_DIRECT')} &mdash; 59,140 Global Purchase Completed events (24.6% of gap) "
        "had no AliHelper-owned <code>sk</code> at all in the 72-hour attribution window.")

    html += """<p>These users made purchases without any prior AliHelper affiliate activation in 72h.
Possible explanations:</p>
<ul>
<li>User browsed AliExpress directly without triggering the extension</li>
<li>User was on ineligible pages (search, homepage, cart) and the extension did not activate</li>
<li>Affiliate activation occurred more than 72 hours before the purchase</li>
<li>Extension was disabled or not installed during the browsing session preceding purchase</li>
<li>The user belongs to the 31.1% who never reach the hub (Problem A Root Cause #1)</li>
</ul>
"""

    # ── Root cause 3: Global unknown ──────────────────────────────────────
    html += _section("Root Cause #3: Global unknown losses (2.7% of gap)", "", 3)

    html += _callout("info",
        f"{_label_html('GLOBAL_DIRECT')} &mdash; 6,518 Global Purchase Completed events (2.7% of gap) "
        "had our <code>sk</code> and no detected foreign overwrite, yet no matching "
        "<code>Purchase</code>.")

    html += """<p>Possible explanations:</p>
<ul>
<li><strong>Delayed postback:</strong> Partner network <code>Purchase</code> events may arrive later
    than our analysis window.</li>
<li><strong>Cashback interference:</strong> Not detectable from <code>querySk</code> alone &mdash;
    cashback tools may operate through mechanisms other than sk replacement.</li>
<li><strong>Partner program exclusions:</strong> Certain product categories or seller types may be
    excluded from commission eligibility.</li>
<li><strong>af parameter overwrite:</strong> The <code>af</code> parameter is not detectable from
    the <code>querySk</code> field. Some overwrites may use <code>af</code> instead of <code>sk</code>.</li>
</ul>
"""

    # ── Root cause 4: CIS losses ──────────────────────────────────────────
    html += _section("Root Cause #4: CIS losses (7.5% of gap combined)", "", 3)

    html += _callout("info",
        f"{_label_html('CIS_DIRECT')} &mdash; CIS contributes 17,856 unmatched purchases (7.4% of gap), "
        "split across three reason codes.")

    html += """
<ul>
<li><strong>CIS_UNKNOWN: 8,145 (3.4%)</strong> &mdash; Our UTM markers present, no foreign overwrite
    detected, but no matching <code>Purchase</code>. Likely delayed postback, cashback interference
    (stored in client local storage, not observable), or partner program exclusions.</li>
<li><strong>CIS_NO_OUR_UTM_IN_72H: 5,222 (2.2%)</strong> &mdash; No AliHelper UTM markers in 72h.
    Users who purchased without prior AliHelper affiliate activation.</li>
<li><strong>CIS_FOREIGN_UTM_AFTER_OURS: 4,489 (1.9%)</strong> &mdash; Our UTM was overwritten by
    foreign affiliate UTMs. Of 35,432 CIS purchases with our markers, 6,653 (18.8%) were overwritten.
    Same last-click attribution problem as Global, but smaller scale.</li>
</ul>
"""

    # ── Matching sensitivity note ─────────────────────────────────────────
    html += _section("Matching Sensitivity Analysis (B4)", "", 3)
    html += _table(
        ["Window", "Matched", "Match Rate", "Delta vs 10min"],
        [
            ["5 min", "194,790", "40.2%", "-10.2pp"],
            ["10 min (current)", "244,456", "50.4%", "baseline"],
            ["15 min", "257,665", "53.2%", "+2.8pp"],
            ["20 min", "265,783", "54.9%", "+4.5pp"],
        ],
        "Purchase matching sensitivity"
    )

    html += """<p>Widening from 10 to 20 minutes recovers only 21,327 additional matches (4.5pp).
The diminishing returns suggest the core gap is not a matching-window artifact &mdash; it reflects
genuinely unattributed purchases where no commission-bearing <code>Purchase</code> was generated
by AliExpress's partner system.</p>
"""

    # ── Segment-level observations ───────────────────────────────────────
    html += _section("Segment-Level Observations (B5)", "", 3)
    html += """
<ul>
<li><strong>Pakistan</strong> dominates volume (151,490 PC, 31.3% of total) with 58.8% loss rate.
    High overwrite rate suggests aggressive cashback/coupon ecosystem in PK.</li>
<li><strong>Brazil</strong> shows the highest loss rate at 69.5% (11,703 PC) &mdash;
    likely similar cashback/coupon interference pattern.</li>
<li><strong>US</strong> has the lowest loss rate at 30.5% (20,787 PC) &mdash; possibly
    fewer competing affiliate extensions or better purchase-timing patterns.</li>
<li><strong>Opera</strong> browser shows 55.1% loss (8,881 PC) &mdash; highest among browsers.
    Opera has a built-in cashback feature that may compete for affiliate attribution.</li>
<li><strong>Auto-redirect</strong> lineage has 39.4% loss vs DOGI's 50.8% &mdash;
    auto-redirect users complete the purchase sooner after activation, leaving less
    time for overwrite.</li>
</ul>
"""

    # ── Impact quantification ────────────────────────────────────────────
    html += _section("Impact Quantification &mdash; Problem B", "", 3)
    html += _table(
        ["Cause", "Label", "Unmatched Purchases", "% of Gap", "Confidence", "Fixability"],
        [
            ["Foreign sk overwrite (Global last-click loss)",
             _label_html("GLOBAL_DIRECT"),
             "156,550", "65.2%", "High",
             "Hard &mdash; structural last-click competition; mitigate via faster checkout nudge"],
            ["No AliHelper sk in 72h (Global no activation)",
             _label_html("GLOBAL_DIRECT"),
             "59,140", "24.6%", "High",
             "Medium &mdash; improve hub reach (see Problem A)"],
            ["CIS unknown (delayed postback + cashback + partner rules)",
             _label_html("CIS_DIRECT"),
             "8,145", "3.4%", "Medium",
             "Partially fixable: longer postback wait, cashback detection"],
            ["Global unknown (sk present, no foreign overwrite detected)",
             _label_html("GLOBAL_DIRECT"),
             "6,518", "2.7%", "Medium",
             "Investigate af-parameter overwrite, cashback, partner exclusions"],
            ["CIS no AliHelper UTM in 72h",
             _label_html("CIS_DIRECT"),
             "5,222", "2.2%", "Medium",
             "Improve activation coverage for CIS users"],
            ["CIS foreign affiliate overwrite",
             _label_html("CIS_DIRECT"),
             "4,489", "1.9%", "High",
             "Hard &mdash; same last-click competition as Global"],
        ],
        "Problem B &mdash; Gap Attribution"
    )

    return html


def build_unexplained_remainder() -> str:
    """Build the unexplained remainder section."""
    return """
<p><strong>Problem A:</strong> Global sk return rate is 93.7% &mdash; healthy, with a small
6.3% gap likely due to redirect timeouts, ad-blockers, or abandoned navigations. On the hub-reach
side, ~31% of eligible users do not reach the hub. After accounting for DOGI vs auto-redirect
lineage differences and version-specific underperformance, approximately 5-8% of eligible users
remain unexplained (possibly due to cooldown collisions, extension load failures, or intermittent
network issues).</p>

<p><strong>Problem B:</strong> After attributing 65.2% to foreign sk overwrite, 24.6% to no sk
in 72h, 2.7% to Global unknown, and 7.5% to CIS causes:</p>
<ul>
<li><strong>Global UNKNOWN (6,518 events, 2.7%)</strong> is the primary unexplained Global component.
    These had our sk with no detected foreign overwrite &mdash; possible <code>af</code>-parameter
    overwrite (not detectable from <code>querySk</code> alone), delayed postback, or partner exclusions.</li>
<li><strong>CIS_UNKNOWN (8,145 events, 3.4%)</strong> is the primary unexplained CIS component.
    Improved cashback observability and longer postback windows would likely reclassify
    most of this residual.</li>
<li>Purchase matching imprecision may account for some false negatives (widening to 20min
    recovers +4.5pp), but the marginal gain is small.</li>
</ul>
"""


def build_recommendations() -> str:
    """Build the recommendations section."""
    html = ""

    html += _section("Quick Wins (deployable in 1-2 sprints)", "", 3)
    html += """<ol>
<li><strong>Push extension update to 3.1.2+.</strong> Version 3.1.0 (24,890 users) has 62.1%
    hub reach vs 3.1.2's 89.8%. A forced update would recover an estimated ~6,900 activations/month.</li>
<li><strong>Deprecate legacy versions (&lt; 3.1.0).</strong> Versions 3.0.x and 2.30.x affect
    ~3,400 users with 3-19% hub reach rates. Force-update or disable affiliate features.</li>
<li><strong>Profile the top foreign <code>sk</code> values</strong> overwriting ours. Identify
    whether the overwrite is dominated by a few known cashback/coupon extensions, or is broadly
    distributed. This determines whether targeted countermeasures are viable.</li>
<li><strong>Extract UTM to dedicated indexed fields</strong> at write time in
    <code>events.payload</code>. Improves future analysis speed and reliability.</li>
</ol>
"""

    html += _section("Medium-Term Product Changes (1-3 months)", "", 3)
    html += """<ol>
<li><strong>Reduce time between activation and purchase.</strong> The overwrite window is the time
    between our <code>sk</code> being set and the user completing purchase. Strategies: purchase
    nudges on product pages, streamlined checkout flow, or re-activation before checkout.</li>
<li><strong>Implement re-activation on checkout.</strong> If the extension detects the user is
    approaching checkout and our <code>sk</code> has been overwritten (foreign sk detected in URL),
    trigger a silent re-redirect to restore our affiliate state.</li>
<li><strong>Improve DOGI activation rate.</strong> DOGI's 64.0% hub reach vs auto-redirect's
    87.1% represents a large gap. Consider: more prominent DOGI coin, auto-trigger on hover,
    or a hybrid approach where DOGI users get periodic auto-redirects.</li>
<li><strong>Add normalized affiliate metadata fields</strong> to <code>events</code>:
    <code>affiliate_provider</code>, <code>is_alihelper_owned</code>,
    <code>affiliate_marker_type</code>.</li>
</ol>
"""

    html += _section("Tracking and Instrumentation Changes", "", 3)
    html += """<ol>
<li><strong>Log overwrite events.</strong> When the extension detects that <code>querySk</code>
    has changed from our owned value to a foreign value, fire a dedicated
    <code>Affiliate Overwrite Detected</code> event with: previous sk, new sk, time since
    our activation. This provides real-time overwrite telemetry.</li>
<li><strong>Instrument cashback detection.</strong> Where the extension detects cashback-site
    cookies or interference, log this as a backend event (currently only in client local storage).</li>
<li><strong>Add CIS state tracking fields</strong> (analogous to Global's <code>last_sk</code>):
    <code>last_epn_campaign</code>, <code>last_epn_source</code>, <code>last_epn_datetime</code>.</li>
<li><strong>Log auto-redirect attempts</strong> (success/failure) as backend events. Currently
    there is no direct signal of whether a redirect was attempted, only downstream evidence.</li>
<li><strong>Log the <code>af</code> parameter</strong> separately from <code>querySk</code>.
    Currently <code>querySk</code> stores only the raw sk value; <code>af</code> is not captured,
    making af-based overwrite invisible. The 6,518 Global UNKNOWN cases may be af overwrites.</li>
</ol>
"""

    html += _section("Follow-Up Experiments and Analyses", "", 3)
    html += """<ol>
<li><strong>Foreign sk profiling.</strong> Identify the top 10 foreign <code>sk</code> values
    that overwrite ours. Cross-reference with known cashback providers and competing extensions.
    This will show whether the overwrite is concentrated (targetable) or diffuse (structural).</li>
<li><strong>Time-to-overwrite analysis.</strong> Measure the median time between our <code>sk</code>
    being set and the foreign overwrite. If it's short (minutes), the overwrite may happen during
    the same browsing session. If long (hours/days), it's separate sessions.</li>
<li><strong>A/B test re-activation on checkout.</strong> For a sample of users, silently
    re-activate our affiliate link when the user navigates to checkout. Measure impact on
    <code>Purchase</code> match rate.</li>
<li><strong>CIS cashback deep dive.</strong> For the 8,145 CIS_UNKNOWN cases, extract
    <code>cashback_list</code> values where available and correlate with known cashback provider
    patterns to estimate cashback interference share.</li>
<li><strong>Country-level overwrite analysis.</strong> PK (58.8% loss) and BR (69.5% loss) show
    high loss rates. Profile whether these countries have specific cashback ecosystems that
    drive higher overwrite rates.</li>
</ol>
"""

    return html


def build_data_quality_section() -> str:
    """Build enhanced data quality section with analysis-specific notes."""
    return """
<ul>
<li><strong>Cashback observability:</strong> Cashback-site visits are tracked only in client
    local storage, NOT logged to backend. Cashback-related explanations carry inherent uncertainty.
    The CIS_UNKNOWN residual (8,145 events) likely contains hidden cashback interference.</li>
<li><strong>Auto-redirect attempts:</strong> No direct backend log of client-side redirect
    attempts. Hub reach rates are lower bounds &mdash; some attempted redirects may fail silently
    without generating an <code>Affiliate Click</code>.</li>
<li><strong>URL parsing:</strong> <code>events.payload.url</code> may contain malformed,
    encoded, or truncated URLs. UTM extraction handles errors gracefully but may produce false
    negatives.</li>
<li><strong>Purchase matching:</strong> Time-based matching (10-min window) introduces possible
    false positives (coincidental temporal proximity) and false negatives (legitimate matches outside
    the window). Sensitivity check shows match rate ranges from 40.2% (5min) to 54.9% (20min).</li>
<li><strong>guestStateHistory:</strong> Represents config delivery, not proof of redirect
    execution. A user having a config with a hub assigned does not mean they used that hub.</li>
<li><strong>Mixpanel timezone:</strong> Project timezone is Europe/Moscow (UTC+3). All
    MongoDB-Mixpanel joins use explicit UTC conversion.</li>
<li><strong>Global return evidence:</strong> The 93.7% Global return rate confirms the sk mechanism
    works. The <code>events.payload.querySk</code> field stores the raw sk value directly (not a
    query string). The 94.1% foreign overwrite rate among Global users with our marker is the
    dominant cause of purchase attribution loss.</li>
<li><strong>CIS proxy return:</strong> The 120-second time-based proxy return is a heuristic.
    Some proxy returns may be coincidental page loads, not true affiliate returns. Label:
    <code>CIS_PROXY</code>.</li>
<li><strong>noLogUrls exclusions:</strong> Some paths may be excluded from logging by config-level
    URL exclusions. Absence of <code>events</code> near checkout/order flow is not always evidence
    of no user activity.</li>
</ul>
"""


# ── Full report ──────────────────────────────────────────────────────────────

_CSS = """
/* ── Reset & base ─────────────────────────────────────────── */
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

/* ── Layout ────────────────────────────────────────────────── */
.layout { display: flex; min-height: 100vh; }

/* Sidebar */
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
.sidebar nav .nav-sub { padding-left: 30px; font-size: 12px; }

/* Main content */
.main {
  flex: 1; max-width: 1160px; padding: 32px 48px;
  overflow-x: hidden;
}

/* ── Typography ────────────────────────────────────────────── */
h1 {
  font-size: 26px; font-weight: 700; color: #111827;
  border-bottom: 3px solid #2563eb; padding-bottom: 10px; margin-top: 0;
}
h2 {
  font-size: 20px; font-weight: 700; color: #1e40af;
  margin-top: 48px; border-bottom: 2px solid #e5e7eb;
  padding-bottom: 6px;
}
h3 { font-size: 16px; font-weight: 600; color: #374151; margin-top: 28px; }
h4, .tbl-caption {
  font-size: 13px; font-weight: 600; color: #6b7280;
  margin: 20px 0 6px; text-transform: uppercase; letter-spacing: .4px;
}
.meta { color: #9ca3af; font-size: 13px; margin-top: -8px; }

/* ── KPI cards ─────────────────────────────────────────────── */
.kpi-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(160px,1fr));
  gap: 14px; margin: 20px 0 28px;
}
.kpi-card {
  background: #f9fafb; border: 1px solid #e5e7eb;
  border-radius: 10px; padding: 16px 18px; text-align: center;
  box-shadow: 0 1px 3px rgba(0,0,0,.06);
}
.kpi-value { font-size: 30px; font-weight: 800; line-height: 1.1; }
.kpi-label { font-size: 12px; color: #6b7280; margin-top: 4px; }
.kpi-good  { color: #059669; }
.kpi-warn  { color: #d97706; }
.kpi-bad   { color: #dc2626; }
.kpi-neutral { color: #2563eb; }

/* ── Callout boxes ─────────────────────────────────────────── */
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

/* ── Attribution label badges ──────────────────────────────── */
.lbl {
  display: inline-block; padding: 2px 8px; border-radius: 99px;
  font-size: 11.5px; font-weight: 700; font-family: monospace;
  margin-right: 4px; letter-spacing: .2px;
}
.lbl-global     { background: #dbeafe; color: #1d4ed8; }
.lbl-cis-direct { background: #fef3c7; color: #92400e; }
.lbl-cis-proxy  { background: #e5e7eb; color: #374151; }

/* ── Tables ────────────────────────────────────────────────── */
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

/* Rate cell coloring */
.rate-good { background: #d1fae5 !important; color: #065f46; font-weight: 600; }
.rate-mid  { background: #fef3c7 !important; color: #78350f; font-weight: 600; }
.rate-bad  { background: #fee2e2 !important; color: #7f1d1d; font-weight: 600; }

/* ── Funnel bars ───────────────────────────────────────────── */
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

/* ── Reason-code inline bar ────────────────────────────────── */
.rc-bar-wrap { width: 120px; background: #f3f4f6; border-radius: 3px; height: 10px; overflow: hidden; }
.rc-bar { height: 100%; background: #6366f1; border-radius: 3px; }

/* ── Exec summary box ──────────────────────────────────────── */
.exec-box {
  background: #eff6ff; border: 2px solid #93c5fd;
  border-radius: 10px; padding: 22px 28px; margin: 20px 0;
}
.exec-box h3 { margin-top: 0; color: #1e40af; font-size: 17px; }

/* ── TOC (sidebar duplicate for mobile) ────────────────────── */
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


def build_report(results_a: dict, results_b: dict) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    a_period = f"{A_START.strftime('%Y-%m-%d')} — {A_END.strftime('%Y-%m-%d')}"
    b_period = f"{B_START.strftime('%Y-%m-%d')} — {B_END.strftime('%Y-%m-%d')}"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AliHelper Research Report — {now}</title>
<style>{_CSS}</style>
</head>
<body>
<div class="layout">

<!-- ── Sidebar ────────────────────────────────────────── -->
<aside class="sidebar">
  <div class="sidebar-logo">📊 AliHelper Research</div>
  <nav>
    <a href="#exec">1. Executive Summary</a>
    <a href="#defs">2. Definitions</a>
    <a href="#caveats">3. Data Quality</a>
    <a href="#pa-data">4. Problem A — Data</a>
    <a href="#pa-rca" class="nav-sub">↳ Root-Cause Analysis</a>
    <a href="#pb-data">5. Problem B — Data</a>
    <a href="#pb-rca" class="nav-sub">↳ Root-Cause Analysis</a>
    <a href="#ranked">6. Ranked Root Causes</a>
    <a href="#unexplained">7. Unexplained Remainder</a>
    <a href="#recs">8. Recommended Fixes</a>
  </nav>
</aside>

<!-- ── Main content ───────────────────────────────────── -->
<main class="main">
<h1>AliHelper — Root-Cause Research Report</h1>
<p class="meta">Generated: {now} &nbsp;·&nbsp;
   Problem A: {a_period} &nbsp;·&nbsp;
   Problem B: {b_period}</p>

<h2 id="exec">1. Executive Summary</h2>
{build_executive_summary()}

<h2 id="defs">2. Definitions</h2>
<ul>
<li><strong>User identity:</strong> <code>guests._id</code> = Mixpanel <code>$user_id</code></li>
<li><strong>Global attribution:</strong> <code>events.payload.querySk</code> (raw sk value);
    whitelist: <code>_c36PoUEj</code>, <code>_d6jWDbY</code>, <code>_AnTGXs</code>,
    <code>_olPBn9X</code>, <code>_dVh6yw5</code></li>
<li><strong>CIS attribution:</strong> UTM params in <code>events.payload.url</code>
    — <code>utm_source=aerkol</code>, <code>utm_medium=cpa</code>,
    <code>utm_campaign=*_7685</code></li>
<li><strong>Eligible pages:</strong> product pages only — DOGI: <code>productId</code> present;
    auto-redirect: URL matches <code>checkListUrls</code> patterns</li>
<li><strong>Attribution window:</strong> 72 h before Purchase Completed</li>
<li><strong>Purchase matching:</strong> same user, ±10 min (no <code>order_id</code>)</li>
<li><strong>Regional routing:</strong> by actual affiliate system — UA = Global, not CIS</li>
<li><strong>CIS countries:</strong> RU, BY, KZ, UZ, AZ, AM, GE, KG, MD, TJ, TM</li>
<li><strong>Browser lineage:</strong> Firefox + Edge = auto-redirect; all others = DOGI</li>
</ul>

<h2 id="caveats">3. Data Quality Caveats</h2>
{build_data_quality_section()}
"""

    # ── Problem A: Data Tables ──────────────────────────────────────────
    html += '<h2 id="pa-data">4. Problem A — Missing Affiliate Click</h2>\n'

    if "funnel" in results_a:
        f = results_a["funnel"]
        all_f = f.get("All", {})
        total = all_f.get("total_users", 1) or 1
        hub   = all_f.get("reached_hub", 0)
        ret   = all_f.get("any_return", 0)
        hub_rate = 100 * hub / total
        ret_rate = 100 * ret / (hub or 1)
        html += _kpi_cards([
            ("Total Users",     _fmt(total),                                  "neutral"),
            ("Eligible",        _fmt(all_f.get("eligible_users", 0)),         "good"),
            ("Reached Hub",     f'{_fmt(hub)} ({hub_rate:.0f}%)',              "good" if hub_rate >= 70 else "warn"),
            ("Any Return",      f'{_fmt(ret)} ({ret_rate:.0f}%)',              "good" if ret_rate >= 80 else ("warn" if ret_rate >= 50 else "bad")),
        ])
        html += funnel_table(f)

    if "missing_ac" in results_a:
        ma = results_a["missing_ac"]
        html += _section("A5 — Missing Mixpanel click tracking",
                         f"<p>{_label_html('GLOBAL_DIRECT')} Global: {_fmt(ma['global'])} users with our sk but no Affiliate Click<br>"
                         f"{_label_html('CIS_DIRECT')} CIS: {_fmt(ma['cis'])} users with our UTM but no Affiliate Click</p>", 3)

    if "hub_no_return" in results_a:
        hn = results_a["hub_no_return"]
        html += _section("A6 — Hub reached, no return",
                         f"<p>{_label_html('GLOBAL_DIRECT')} Global: {_fmt(hn['global'])} users reached hub but no sk return<br>"
                         f"{_label_html('CIS_DIRECT')} CIS: {_fmt(hn['cis'])} users reached hub but no UTM/proxy return</p>", 3)

    if "segments" in results_a:
        html += segment_tables(results_a["segments"], "Problem A")

    # ── Problem A: Root-Cause Analysis ──────────────────────────────────
    html += '<h2 id="pa-rca">Problem A — Root-Cause Analysis</h2>\n'
    html += build_problem_a_analysis()

    # ── Problem B: Data Tables ──────────────────────────────────────────
    html += '<h2 id="pb-data">5. Problem B — Purchase Completed without Purchase</h2>\n'

    if "summary" in results_b:
        s = results_b["summary"]
        total_pc  = s.get("total_pc", 1) or 1
        matched   = s.get("matched", 0)
        unmatched = s.get("unmatched", 0)
        match_rate   = 100 * matched / total_pc
        unmatch_rate = 100 * unmatched / total_pc
        html += _kpi_cards([
            ("Total Purchase Completed", _fmt(total_pc),                                              "neutral"),
            ("Matched to Purchase",      f'{_fmt(matched)} ({match_rate:.1f}%)',                      "warn" if match_rate < 60 else "good"),
            ("Unmatched (gap)",          f'{_fmt(unmatched)} ({unmatch_rate:.1f}%)',                  "bad"),
        ])

    if "attribution" in results_b:
        attr = results_b["attribution"]
        g_total  = 442237
        g_marker = attr['global_with_marker']
        g_pct    = f"{100 * g_marker / g_total:.1f}" if g_total else "N/A"
        c_total  = 42319
        c_marker = attr['cis_with_marker']
        c_pct    = f"{100 * c_marker / c_total:.1f}" if c_total else "N/A"
        html += _section("B1 — Attribution evidence",
                         f"<p>{_label_html('GLOBAL_DIRECT')} Global: {_fmt(g_marker)}/{_fmt(g_total)} "
                         f"({g_pct}%) had our affiliate marker in 72 h<br>"
                         f"{_label_html('CIS_DIRECT')} CIS: {_fmt(c_marker)}/{_fmt(c_total)} "
                         f"({c_pct}%) had our affiliate marker in 72 h</p>", 3)

    if "reason_codes" in results_b:
        html += reason_code_table(results_b["reason_codes"])

    if "segments" in results_b:
        html += segment_tables(results_b["segments"], "Problem B")

    # ── Problem B: Root-Cause Analysis ──────────────────────────────────
    html += '<h2 id="pb-rca">Problem B — Root-Cause Analysis</h2>\n'
    html += build_problem_b_analysis()

    # ── Ranked root causes (both) ───────────────────────────────────────
    html += '<h2 id="ranked">6. Ranked Root Causes by Impact</h2>\n'
    html += """<p>The following table ranks all identified root causes across both problems,
ordered by estimated impact.</p>
"""
    html += _table(
        ["Rank", "Root Cause", "Problem", "Label", "Impact",
         "Confidence", "Recommended Fix"],
        [
            ["1",
             "<strong>Foreign sk overwrite</strong> &mdash; Third-party affiliates (cashback, coupons, "
             "competing extensions) replace our <code>sk</code> before purchase completion",
             "B",
             _label_html("GLOBAL_DIRECT"),
             "156,550 unmatched purchases (65.2% of gap);<br>"
             "94.1% of Global users with our marker get overwritten",
             "High",
             "Re-activation on checkout; time-to-overwrite analysis; profile foreign sk values"],
            ["2",
             "<strong>No AliHelper sk in 72h</strong> &mdash; Global users purchasing without "
             "any prior affiliate activation in the attribution window",
             "B",
             _label_html("GLOBAL_DIRECT"),
             "59,140 unmatched purchases (24.6% of gap)",
             "High",
             "Improve hub reach (fix #3, #4, #5)"],
            ["3",
             "<strong>DOGI vs auto-redirect activation gap</strong> &mdash; DOGI users reach hub "
             "at 64.0% vs auto-redirect at 87.1%",
             "A",
             _label_html("GLOBAL_DIRECT") + " " + _label_html("CIS_DIRECT"),
             "~11,900 missed activations/month (17.9% of users)",
             "High",
             "Improve DOGI UX, consider hybrid auto-trigger"],
            ["4",
             "<strong>Version 3.1.0 underperformance</strong> &mdash; 62.1% hub reach vs "
             "3.1.2's 89.8%",
             "A",
             _label_html("GLOBAL_DIRECT") + " " + _label_html("CIS_DIRECT"),
             "~6,900 missed activations/month (10.4% of users)",
             "High",
             "Push update to 3.1.2+"],
            ["5",
             "<strong>CIS unknown losses</strong> &mdash; our UTM present, no foreign overwrite "
             "detected, but no Purchase credited",
             "B",
             _label_html("CIS_DIRECT"),
             "8,145 unmatched purchases (3.4% of gap)",
             "Medium",
             "Improve cashback detection; investigate partner delays"],
            ["6",
             "<strong>Global unknown losses</strong> &mdash; our sk present, no foreign overwrite "
             "detected, but no Purchase; possibly af overwrite",
             "B",
             _label_html("GLOBAL_DIRECT"),
             "6,518 unmatched purchases (2.7% of gap)",
             "Medium",
             "Log af parameter; investigate partner exclusions"],
            ["7",
             "<strong>CIS foreign affiliate overwrite</strong> &mdash; last-click attribution loss "
             "to competing affiliates (18.8% overwrite rate)",
             "B",
             _label_html("CIS_DIRECT"),
             "4,489 unmatched purchases (1.9% of gap)",
             "High",
             "Same re-activation approach as Global"],
            ["8",
             "<strong>Legacy extension versions (&lt;3.1.0)</strong> &mdash; hub reach rates "
             "of 3-19%",
             "A",
             _label_html("GLOBAL_DIRECT") + " " + _label_html("CIS_DIRECT"),
             "~3,400 users with near-zero activation",
             "High",
             "Force-update or deprecate"],
        ],
        "All Root Causes &mdash; Ranked by Impact"
    )

    # ── Unexplained remainder ───────────────────────────────────────────
    html += '<h2 id="unexplained">7. Unexplained Remainder</h2>\n'
    html += build_unexplained_remainder()

    # ── Recommendations ─────────────────────────────────────────────────
    html += '<h2 id="recs">8. Recommended Fixes</h2>\n'
    html += build_recommendations()

    html += f"""
<hr>
<p class="meta">
End of report &nbsp;·&nbsp; Generated {now}<br>
Pipeline: <code>analysis/extract.py</code>, <code>analysis/problem_a.py</code>,
<code>analysis/problem_b.py</code>, <code>analysis/report.py</code> &nbsp;·&nbsp;
Data in <code>cache/</code><br>
Regional routing: <code>specs/domain/regional_routing.md</code> (UA = Global) &nbsp;·&nbsp;
Attribution: <code>specs/domain/attribution.md</code>
</p>
</main>
</div>
{_SIDEBAR_JS}
</body>
</html>"""

    return html


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    results_a = _load_pkl("results_a")
    results_b = _load_pkl("results_b")

    html = build_report(results_a, results_b)

    out_path = REPORTS_DIR / "report.html"
    with open(out_path, "w") as f:
        f.write(html)
    print(f"Report saved to {out_path}")


if __name__ == "__main__":
    run()
