"""
Generate the main architecture diagram for the platform:
    "Heterogeneous Realtime Financial Data Management and Integration
     for AI/LLM Models"

Visual style: mirrors the reference diagram convention (Azure Cloud outer,
Docker inner, labelled blocks for DAGs / databases / processing modules,
arrows for data flow, separate row for visualisation / CI-CD).

Output: architecture_diagram.png (1600x1000, 150 DPI, PNG)

Usage:
    python scripts/generate_architecture_diagram.py
"""

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from matplotlib.lines import Line2D

OUT_PATH = Path(__file__).resolve().parents[1] / "architecture_diagram.png"

# ---------------------------------------------------------------------------
# Palette (close to the reference: muted blues, accent colours per layer)
# ---------------------------------------------------------------------------
C_AZURE       = "#E8F4FB"
C_DOCKER      = "#F4F9FD"
C_L0_MAINT    = "#FFF3E0"
C_L1_INGEST   = "#E3F2FD"
C_L2_FUSION   = "#E8F5E9"
C_L3_SENT     = "#F3E5F5"
C_DB_PG       = "#DDEEFF"
C_DB_MONGO    = "#E1F5E1"
C_API         = "#FFE0B2"
C_EXTERNAL    = "#FAFAFA"
C_BORDER      = "#5A6F8C"
C_ARROW       = "#3D5A80"
C_ACCENT      = "#0277BD"


def box(ax, x, y, w, h, label, fc, fontsize=10, fontweight="bold",
        ec=C_BORDER, lw=1.2, italic=False, multiline=False, subtitle=None):
    """Draw a rounded box with a label."""
    patch = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.02,rounding_size=0.08",
        facecolor=fc, edgecolor=ec, linewidth=lw,
    )
    ax.add_patch(patch)

    style = "italic" if italic else "normal"
    if subtitle:
        ax.text(x + w / 2, y + h * 0.62, label,
                ha="center", va="center",
                fontsize=fontsize, fontweight=fontweight, style=style)
        ax.text(x + w / 2, y + h * 0.28, subtitle,
                ha="center", va="center",
                fontsize=fontsize - 2, style="italic", color="#555555")
    else:
        ax.text(x + w / 2, y + h / 2, label,
                ha="center", va="center",
                fontsize=fontsize, fontweight=fontweight, style=style)


def arrow(ax, x1, y1, x2, y2, color=C_ARROW, lw=1.4, style="->"):
    """Draw a flow arrow."""
    arr = FancyArrowPatch(
        (x1, y1), (x2, y2),
        arrowstyle=style, mutation_scale=14,
        color=color, linewidth=lw,
        connectionstyle="arc3,rad=0",
    )
    ax.add_patch(arr)


def cylinder(ax, cx, cy, w, h, label, fc=C_DB_PG, sublabel=None):
    """Draw a database cylinder (top ellipse + body + bottom ellipse curve)."""
    body = mpatches.Rectangle((cx - w / 2, cy - h / 2 + 0.05),
                              w, h - 0.1, facecolor=fc, edgecolor=C_BORDER, lw=1.2)
    top  = mpatches.Ellipse((cx, cy + h / 2 - 0.05), w, 0.18,
                            facecolor=fc, edgecolor=C_BORDER, lw=1.2)
    bot  = mpatches.Ellipse((cx, cy - h / 2 + 0.05), w, 0.18,
                            facecolor=fc, edgecolor=C_BORDER, lw=1.2)
    ax.add_patch(body)
    ax.add_patch(bot)
    ax.add_patch(top)

    if sublabel:
        ax.text(cx, cy + 0.04, label, ha="center", va="center",
                fontsize=9, fontweight="bold")
        ax.text(cx, cy - 0.18, sublabel, ha="center", va="center",
                fontsize=8, style="italic", color="#444444")
    else:
        ax.text(cx, cy, label, ha="center", va="center",
                fontsize=9, fontweight="bold")


def main():
    fig, ax = plt.subplots(figsize=(16, 10), dpi=150)
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 10)
    ax.axis("off")

    # ----- Outer: Azure Cloud (lab VM host) -----
    azure_rect = FancyBboxPatch(
        (0.2, 0.2), 15.6, 9.6,
        boxstyle="round,pad=0.02,rounding_size=0.15",
        facecolor=C_AZURE, edgecolor=C_BORDER, linewidth=2,
    )
    ax.add_patch(azure_rect)
    ax.text(0.5, 9.55, "Microsoft Azure VM  (lab: test-2026@20.77.80.201)",
            fontsize=12, fontweight="bold", color=C_BORDER)

    # ----- Inner: Docker Compose -----
    docker_rect = FancyBboxPatch(
        (0.5, 0.6), 15.0, 8.6,
        boxstyle="round,pad=0.02,rounding_size=0.12",
        facecolor=C_DOCKER, edgecolor=C_ACCENT, linewidth=1.5, linestyle="--",
    )
    ax.add_patch(docker_rect)
    ax.text(0.75, 9.0, "Docker Compose  (postgres · mongodb · airflow · fastapi)",
            fontsize=10, fontweight="bold", color=C_ACCENT)

    # ====================================================================
    # External data sources column (far left)
    # ====================================================================
    ax.text(1.4, 8.5, "External Sources", fontsize=10,
            fontweight="bold", color="#555")

    box(ax, 0.8, 7.5, 1.7, 0.65, "EODHD API",
        fc=C_EXTERNAL, fontsize=9, subtitle="prices + news + sentiment")
    box(ax, 0.8, 6.6, 1.7, 0.65, "GDELT GKG 2.0",
        fc=C_EXTERNAL, fontsize=9, subtitle="open-data news")
    box(ax, 0.8, 5.7, 1.7, 0.65, "Wikipedia",
        fc=C_EXTERNAL, fontsize=9, subtitle="S&P 500 list")

    # ====================================================================
    # L0 — Maintenance
    # ====================================================================
    box(ax, 3.2, 5.7, 2.2, 0.65, "sp500_refresh_pipeline",
        fc=C_L0_MAINT, fontsize=9, subtitle="L0 maint. · weekly cron")

    # ====================================================================
    # L1 — Ingestion (three parallel DAGs)
    # ====================================================================
    ax.text(3.7, 8.5, "L1 — Ingestion (every 15 min)",
            fontsize=10, fontweight="bold", color="#1565C0")

    box(ax, 3.2, 7.5, 2.2, 0.65, "eodhd_price_pipeline",
        fc=C_L1_INGEST, fontsize=9, subtitle="OHLCV intraday")
    box(ax, 3.2, 6.6, 2.2, 0.65, "eodhd_news_pipeline",
        fc=C_L1_INGEST, fontsize=9, subtitle="ticker-tagged + sentiment")
    box(ax, 3.2, 4.8, 2.2, 0.65, "gdelt_news_pipeline",
        fc=C_L1_INGEST, fontsize=9, subtitle="GKG snapshot · 15-min")

    # External → L1 arrows
    arrow(ax, 2.5, 7.82, 3.2, 7.82)
    arrow(ax, 2.5, 7.82, 3.2, 6.92)  # eodhd also feeds news
    arrow(ax, 2.5, 6.92, 3.2, 5.12, style="->")  # gdelt feeds gdelt DAG
    arrow(ax, 2.5, 6.02, 3.2, 6.02)  # wiki feeds sp500_refresh

    # ====================================================================
    # Storage layer (centre-right)
    # ====================================================================
    ax.text(6.4, 8.5, "Storage", fontsize=10, fontweight="bold", color="#555")

    cylinder(ax, 6.85, 7.6, 1.5, 0.85,
             "PostgreSQL", fc=C_DB_PG, sublabel="price_data · companies")
    cylinder(ax, 6.85, 6.3, 1.5, 0.85,
             "MongoDB", fc=C_DB_MONGO, sublabel="news_articles")
    cylinder(ax, 6.85, 5.0, 1.5, 0.85,
             "MongoDB", fc=C_DB_MONGO, sublabel="fused_events")
    cylinder(ax, 6.85, 3.7, 1.5, 0.85,
             "MongoDB", fc=C_DB_MONGO, sublabel="sentiment_scores")

    # L0 → Postgres companies
    arrow(ax, 5.4, 6.02, 6.1, 7.6)

    # L1 → Storage arrows
    arrow(ax, 5.4, 7.82, 6.1, 7.7)   # price → postgres
    arrow(ax, 5.4, 6.92, 6.1, 6.4)   # eodhd news → mongo news
    arrow(ax, 5.4, 5.12, 6.1, 6.2)   # gdelt → mongo news

    # ====================================================================
    # L2 — Temporal Fusion
    # ====================================================================
    ax.text(9.2, 8.5, "L2 — Temporal Fusion (hourly)",
            fontsize=10, fontweight="bold", color="#2E7D32")

    box(ax, 8.5, 7.5, 2.8, 0.65, "Entity Mapper",
        fc=C_L2_FUSION, fontsize=9,
        subtitle="spaCy NER · GDELT pre-tagged")
    box(ax, 8.5, 6.6, 2.8, 0.65, "Sliding-Window Fusion",
        fc=C_L2_FUSION, fontsize=9,
        subtitle="Δ/δ per-interval · entity score ≥ τ")

    arrow(ax, 7.6, 7.6, 8.5, 7.82)     # postgres price → entity mapper
    arrow(ax, 7.6, 6.3, 8.5, 6.92)     # mongo news → entity mapper
    arrow(ax, 9.9, 7.5, 9.9, 7.25)     # entity → fusion
    arrow(ax, 9.9, 6.6, 7.6, 5.4)      # fusion → mongo fused_events

    # ====================================================================
    # L3 — Sentiment (right column)
    # ====================================================================
    ax.text(12.5, 8.5, "L3 — Sentiment + 4-Way Validation",
            fontsize=10, fontweight="bold", color="#6A1B9A")

    box(ax, 11.7, 7.5, 1.9, 0.65, "FinBERT",
        fc=C_L3_SENT, fontsize=9, subtitle="ProsusAI/finbert")
    box(ax, 13.8, 7.5, 1.9, 0.65, "RoBERTa",
        fc=C_L3_SENT, fontsize=9, subtitle="DistilRoBERTa-fin")
    box(ax, 11.7, 6.6, 4.0, 0.65, "4-way agreement",
        fc=C_L3_SENT, fontsize=9,
        subtitle="vs EODHD pre-score · vs GDELT tone")

    arrow(ax, 11.7, 6.92, 7.6, 5.0, style="->")   # fused → models
    arrow(ax, 12.65, 7.5, 12.65, 6.92)
    arrow(ax, 14.75, 7.5, 13.65, 6.92)
    arrow(ax, 13.5, 6.6, 7.6, 3.95)               # 4-way → sentiment_scores

    # ====================================================================
    # FastAPI / serving layer
    # ====================================================================
    box(ax, 8.7, 2.5, 5.0, 0.85, "FastAPI REST",
        fc=C_API, fontsize=10,
        subtitle="/v1/prices  /v1/news  /v1/fused  /v1/sentiment  /v1/stats")

    arrow(ax, 6.85, 3.3, 8.7, 2.92)   # mongo sentiment → fastapi
    arrow(ax, 6.85, 4.5, 8.7, 2.92)   # mongo fused → fastapi
    arrow(ax, 6.85, 6.0, 8.7, 2.92)   # mongo news → fastapi
    arrow(ax, 6.85, 7.2, 8.7, 2.92)   # postgres → fastapi

    # ====================================================================
    # Downstream consumers
    # ====================================================================
    box(ax, 14.0, 2.5, 1.5, 0.85, "AI / LLM\nconsumers",
        fc="#FFF8E1", fontsize=9, fontweight="bold",
        subtitle="FININ · MANA-Net · custom")
    arrow(ax, 13.7, 2.92, 14.0, 2.92)

    # ====================================================================
    # Orchestration label (left side, like reference)
    # ====================================================================
    ax.text(0.5, 4.5, "Airflow\n(LocalExecutor)\nscheduler\n+ webserver",
            fontsize=9, fontweight="bold",
            color=C_BORDER,
            rotation=90, va="center", ha="center",
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec=C_BORDER))

    # ====================================================================
    # CI/CD strip (bottom)
    # ====================================================================
    box(ax, 0.8, 1.0, 1.6, 0.65, "GitHub",
        fc="#EEEEEE", fontsize=9, subtitle="CI/CD · IaC")

    box(ax, 2.8, 1.0, 4.0, 0.65, "S&P 500 enforcement",
        fc="#F1F8E9", fontsize=9,
        subtitle="all DAGs ← companies.is_active = TRUE")

    box(ax, 7.0, 1.0, 4.0, 0.65, "Cross-source validation",
        fc="#F1F8E9", fontsize=9,
        subtitle="EODHD ↔ GDELT ↔ FinBERT ↔ RoBERTa")

    box(ax, 11.2, 1.0, 4.0, 0.65, "Sliding-window temporal fusion",
        fc="#F1F8E9", fontsize=9,
        subtitle="per-interval Δ/δ · ticker × timestamp_ms")

    # ====================================================================
    # Title
    # ====================================================================
    fig.text(0.5, 0.965,
             "Heterogeneous Realtime Financial Data Management and "
             "Integration for AI/LLM Models",
             ha="center", fontsize=14, fontweight="bold", color="#222")
    fig.text(0.5, 0.94,
             "Three-layer ETL · 5 active DAGs · Hybrid SQL/NoSQL · 4-way sentiment cross-validation",
             ha="center", fontsize=10, style="italic", color="#555")

    # Save
    plt.savefig(OUT_PATH, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"Wrote {OUT_PATH}  ({OUT_PATH.stat().st_size / 1024:.1f} KB)")


if __name__ == "__main__":
    main()
