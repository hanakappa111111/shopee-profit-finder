#!/usr/bin/env python3
"""Streamlit Web Dashboard for the Shopee Profit Research Engine.

Launch with::

    pip install streamlit
    streamlit run web_dashboard.py

Provides a browser-based UI for:
  - Keyword-driven on-demand research
  - Results table with sorting/filtering
  - CSV export
  - Database statistics
  - Research history from past runs
"""

from __future__ import annotations

import json
import time
from io import StringIO

import streamlit as st

# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Shopee Profit Research",
    page_icon="🔍",
    layout="wide",
)


# ── Lazy imports (avoid crashing if deps missing) ────────────────────────────

@st.cache_resource
def _init_pipeline():
    """Import and return pipeline function (cached to avoid re-import)."""
    from src.research_pipeline.pipeline import run_research_pipeline
    return run_research_pipeline


@st.cache_resource
def _init_db():
    """Return database singleton."""
    from src.database.database import db
    db.initialize()
    return db


# ── Sidebar ──────────────────────────────────────────────────────────────────

st.sidebar.title("Shopee Profit Research")
page = st.sidebar.radio(
    "Navigate",
    ["Research", "Database Stats", "Settings Info"],
)


# ── Research page ────────────────────────────────────────────────────────────

if page == "Research":
    st.title("Keyword Research")
    st.markdown(
        "Enter a Shopee search keyword to find profitable Japan → Shopee "
        "arbitrage opportunities."
    )

    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        keyword = st.text_input(
            "Search keyword",
            placeholder="e.g. pokemon card, nendoroid, one piece figure",
        )
    with col2:
        max_pages = st.number_input("Max pages", min_value=1, max_value=10, value=2)
    with col3:
        top_n = st.number_input("Top N results", min_value=5, max_value=100, value=20)

    if st.button("Run Research", type="primary", disabled=not keyword):
        run_pipeline = _init_pipeline()

        with st.spinner(f"Researching '{keyword}' — this may take a few minutes..."):
            t0 = time.time()
            report = run_pipeline(keyword, max_pages=max_pages, top_n=top_n)
            elapsed = time.time() - t0

        # ── Summary metrics ──────────────────────────────────────────────
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Products Scraped", report.products_scraped)
        c2.metric("Japan Sources", report.japan_sources_found)
        c3.metric("Matches", report.matches_found)
        c4.metric("Profitable", report.profitable_count)
        c5.metric("Time", f"{elapsed:.1f}s")

        if not report.results:
            st.warning("No profitable opportunities found for this keyword.")
        else:
            # ── Results table ────────────────────────────────────────────
            import pandas as pd

            rows = []
            for r in report.results:
                rows.append({
                    "Product": r.product_name[:60],
                    "Shopee Price": r.shopee_price,
                    "Japan Price (JPY)": r.japan_supplier_price,
                    "Profit (JPY)": round(r.estimated_profit_jpy),
                    "ROI %": round(r.roi_percent, 1),
                    "Match": r.match_confidence,
                    "Source": r.japan_source,
                    "Supplier URL": r.supplier_url,
                    "Shopee URL": r.shopee_url,
                })

            df = pd.DataFrame(rows)

            st.dataframe(
                df,
                use_container_width=True,
                column_config={
                    "Supplier URL": st.column_config.LinkColumn("Supplier"),
                    "Shopee URL": st.column_config.LinkColumn("Shopee"),
                    "Profit (JPY)": st.column_config.NumberColumn(format="¥%d"),
                    "ROI %": st.column_config.NumberColumn(format="%.1f%%"),
                },
            )

            # ── CSV download ─────────────────────────────────────────────
            csv_buf = StringIO()
            df.to_csv(csv_buf, index=False)
            st.download_button(
                "Download CSV",
                csv_buf.getvalue(),
                file_name=f"research_{keyword.replace(' ', '_')}.csv",
                mime="text/csv",
            )

            # ── JSON download ────────────────────────────────────────────
            json_str = json.dumps(report.to_dict(), indent=2, ensure_ascii=False)
            st.download_button(
                "Download JSON",
                json_str,
                file_name=f"research_{keyword.replace(' ', '_')}.json",
                mime="application/json",
            )

        # Store in session state for history
        if "history" not in st.session_state:
            st.session_state.history = []
        st.session_state.history.append({
            "keyword": keyword,
            "profitable": report.profitable_count,
            "elapsed": f"{elapsed:.1f}s",
        })

    # ── Research history ─────────────────────────────────────────────────
    if "history" in st.session_state and st.session_state.history:
        st.markdown("---")
        st.subheader("Session History")
        import pandas as pd

        hist_df = pd.DataFrame(st.session_state.history)
        st.dataframe(hist_df, use_container_width=True)


# ── Database Stats page ──────────────────────────────────────────────────────

elif page == "Database Stats":
    st.title("Database Statistics")

    db = _init_db()

    try:
        stats = db.get_stats()

        col1, col2, col3 = st.columns(3)
        col1.metric("Products", stats.get("products", 0))
        col2.metric("Japan Sources", stats.get("sources", 0))
        col3.metric("Matches", stats.get("matches", 0))

        col4, col5, col6 = st.columns(3)
        col4.metric("Listings", stats.get("listings", 0))
        col5.metric("Profitable Matches", stats.get("profitable_matches", 0))
        col6.metric("Research Candidates", stats.get("research_candidates", 0))

    except Exception as e:
        st.error(f"Could not fetch stats: {e}")
        st.info("The database may not be initialized yet. Run a research first.")


# ── Settings Info page ───────────────────────────────────────────────────────

elif page == "Settings Info":
    st.title("Current Configuration")

    from src.config.settings import settings

    config_items = {
        "Market": settings.SHOPEE_MARKET,
        "OpenAI Model": settings.OPENAI_MODEL,
        "Min Profit (JPY)": settings.MIN_PROFIT_YEN,
        "Min ROI": f"{settings.MIN_ROI * 100:.0f}%",
        "Shopee Fee Rate": f"{settings.SHOPEE_FEE_RATE * 100:.0f}%",
        "Match Similarity Threshold": settings.MIN_MATCH_SIMILARITY,
        "Request Delay": f"{settings.REQUEST_DELAY_SECONDS}s",
        "Automation Enabled": settings.AUTOMATION_ENABLED,
        "Rakuten API": "Configured" if settings.RAKUTEN_APP_ID else "Not set",
        "Proxy": "Configured" if settings.SCRAPER_PROXY else "Not set",
    }

    for label, value in config_items.items():
        st.text(f"{label}: {value}")

    st.markdown("---")
    st.caption(
        "Settings are loaded from .env file. "
        "Restart the dashboard after changing .env values."
    )
