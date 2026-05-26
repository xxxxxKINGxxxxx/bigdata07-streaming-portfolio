import streamlit as st
import snowflake.connector
import plotly.express as px
from dotenv import load_dotenv
import os
from typing import Any

load_dotenv(r"E:\School\Abdallah Assignment\assignment 2\.env")


@st.cache_resource
def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


@st.cache_data(ttl=300)
def query(_conn: Any, sql: str) -> list:
    cursor = _conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]
    cursor.close()
    # WHY explicit type casting here?
    # Snowflake connector returns numeric columns
    # as strings in some configurations. We cast
    # known numeric fields to float/int to ensure
    # Plotly can perform arithmetic on them.
    result = []
    for row in rows:
        d = dict(zip(cols, row))
        for k, v in d.items():
            if v is not None:
                try:
                    d[k] = float(v) if '.' in str(v) else int(v)
                except (ValueError, TypeError):
                    pass
        result.append(d)
    return result


def main() -> None:
    st.set_page_config(
        page_title="Global Superstore Analytics",
        page_icon="🛒",
        layout="wide"
    )

    st.title("🛒 Global Superstore — Sales Analytics Dashboard")
    st.caption("BigData 07 · Assignment 2 · Hayagriva Boodhoo · 2025_20379_6980")
    st.divider()

    conn = get_connection()

    # ── SIDEBAR FILTERS ──────────────────────────
    st.sidebar.header("🔍 Filters")
    selected_category = st.sidebar.selectbox(
        "Select Product Category",
        ["All Categories", "Technology", "Furniture", "Office Supplies"]
    )
    selected_year = st.sidebar.multiselect(
        "Select Year(s)",
        [2011, 2012, 2013, 2014],
        default=[2011, 2012, 2013, 2014]
    )

    if not selected_year:
        st.warning("Please select at least one year.")
        st.stop()

    year_filter = ", ".join(str(y) for y in selected_year)

    if selected_category == "All Categories":
        category_filter = "1=1"
    else:
        category_filter = f"CATEGORY = '{selected_category}'"

    # ── KPI ROW ──────────────────────────────────
    kpi_data = query(conn, f"""
        SELECT
            SUM(TOTAL_REVENUE) AS TOTAL_REV,
            SUM(TOTAL_ORDERS)  AS TOTAL_ORD,
            AVG(AVG_MARGIN)    AS AVG_MARGIN
        FROM YEARLY_REVENUE
        WHERE YEAR IN ({year_filter})
    """)

    cat_data = query(conn, f"""
        SELECT
            SUM(TOTAL_SALES)  AS CAT_SALES,
            SUM(TOTAL_PROFIT) AS CAT_PROFIT
        FROM CATEGORY_SALES
        WHERE {category_filter}
    """)

    total_rev  = float(kpi_data[0]["TOTAL_REV"]  or 0)
    total_ord  = int(kpi_data[0]["TOTAL_ORD"]    or 0)
    avg_margin = float(kpi_data[0]["AVG_MARGIN"] or 0)
    cat_sales  = float(cat_data[0]["CAT_SALES"]  or 0)
    cat_profit = float(cat_data[0]["CAT_PROFIT"] or 0)

    st.subheader("📊 Key Performance Indicators")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("💰 Total Revenue",     f"${total_rev:,.0f}")
    k2.metric("📦 Total Orders",      f"{total_ord:,}")
    k3.metric("📈 Avg Profit Margin", f"{avg_margin:.2f}%")
    k4.metric("🏷️ Category Sales",   f"${cat_sales:,.0f}")
    k5.metric("💵 Category Profit",   f"${cat_profit:,.0f}")

    st.divider()

    # ── CHART ROW 1 ──────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📈 Yearly Revenue Trend")
        yearly = query(conn, f"""
            SELECT YEAR, TOTAL_REVENUE, TOTAL_PROFIT
            FROM YEARLY_REVENUE
            WHERE YEAR IN ({year_filter})
            ORDER BY YEAR
        """)
        if yearly:
            fig = px.line(
                yearly,
                x="YEAR",
                y=["TOTAL_REVENUE", "TOTAL_PROFIT"],
                markers=True,
                labels={"value": "Amount ($)", "YEAR": "Year"},
                color_discrete_map={
                    "TOTAL_REVENUE": "#1f77b4",
                    "TOTAL_PROFIT":  "#2ca02c"
                }
            )
            fig.update_layout(legend_title="Metric", height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("🏷️ Sales & Profit by Category")
        cat_all = query(conn, """
            SELECT CATEGORY, TOTAL_SALES, TOTAL_PROFIT
            FROM CATEGORY_SALES
            ORDER BY TOTAL_SALES DESC
        """)
        if cat_all:
            fig2 = px.bar(
                cat_all,
                x="CATEGORY",
                y=["TOTAL_SALES", "TOTAL_PROFIT"],
                barmode="group",
                labels={"value": "Amount ($)", "CATEGORY": "Category"},
                color_discrete_map={
                    "TOTAL_SALES":  "#1f77b4",
                    "TOTAL_PROFIT": "#2ca02c"
                }
            )
            fig2.update_layout(legend_title="Metric", height=350)
            st.plotly_chart(fig2, use_container_width=True)

    # ── CHART ROW 2 ──────────────────────────────
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("🌍 Region: Revenue vs Profit Margin")
        region_data = query(conn, """
            SELECT
                REGION,
                CAST(TOTAL_REVENUE AS FLOAT)  AS TOTAL_REVENUE,
                CAST(AVG_MARGIN    AS FLOAT)  AS AVG_MARGIN,
                CAST(TOTAL_ORDERS  AS INTEGER) AS TOTAL_ORDERS
            FROM REGION_PERFORMANCE
            ORDER BY TOTAL_REVENUE DESC
        """)
        if region_data:
            fig3 = px.scatter(
                region_data,
                x="TOTAL_REVENUE",
                y="AVG_MARGIN",
                size="TOTAL_ORDERS",
                text="REGION",
                color="AVG_MARGIN",
                color_continuous_scale="RdYlGn",
                labels={
                    "TOTAL_REVENUE": "Total Revenue ($)",
                    "AVG_MARGIN":    "Avg Profit Margin (%)"
                }
            )
            fig3.update_traces(textposition="top center")
            fig3.update_layout(height=350)
            st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.subheader("🏆 Sub-Category Sales Ranking")
        sub_sql = "SELECT SUB_CATEGORY, CATEGORY, CAST(TOTAL_SALES AS FLOAT) AS TOTAL_SALES, CAST(TOTAL_PROFIT AS FLOAT) AS TOTAL_PROFIT FROM SUBCATEGORY_RANK"
        if selected_category != "All Categories":
            sub_sql += f" WHERE CATEGORY = '{selected_category}'"
        sub_sql += " ORDER BY TOTAL_SALES DESC LIMIT 10"

        sub_data = query(conn, sub_sql)
        if sub_data:
            fig4 = px.bar(
                sub_data,
                x="TOTAL_SALES",
                y="SUB_CATEGORY",
                orientation="h",
                color="TOTAL_PROFIT",
                color_continuous_scale="RdYlGn",
                labels={
                    "TOTAL_SALES":  "Total Sales ($)",
                    "SUB_CATEGORY": "Sub-Category",
                    "TOTAL_PROFIT": "Profit ($)"
                }
            )
            fig4.update_layout(height=350, yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig4, use_container_width=True)

    # ── DATA TABLE ───────────────────────────────
    st.divider()
    st.subheader("📋 Region Performance Data Table")
    table_data = query(conn, """
        SELECT REGION, TOTAL_REVENUE, AVG_ORDER_VALUE, AVG_MARGIN, TOTAL_ORDERS
        FROM REGION_PERFORMANCE
        ORDER BY TOTAL_REVENUE DESC
    """)
    if table_data:
        st.dataframe(table_data, use_container_width=True, height=300)

    st.caption("Data sourced from Snowflake · BIGDATA_07.ASSIGNMENT2 · Refreshes every 5 minutes")


if __name__ == "__main__":
    main()
