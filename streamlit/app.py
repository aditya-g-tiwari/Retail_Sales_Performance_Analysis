import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Retail Sales Performance", layout="wide")

DB = "RETAIL_SALES_DB"
SCHEMA = "ANALYTICS"


def get_session():
    return get_active_session()


def load_table(session, table_name):
    return session.table(f"{DB}.{SCHEMA}.{table_name}").to_pandas()


@st.cache_data(ttl=300)
def load_dashboard_data():
    session = get_session()
    return {
        "overall": load_table(session, "OVERALL_METRICS"),
        "products": load_table(session, "PRODUCT_SALES"),
        "regions": load_table(session, "REGIONAL_SALES"),
        "monthly": load_table(session, "MONTHLY_SALES"),
        "performance": load_table(session, "PRODUCT_PERFORMANCE"),
    }


st.title("Retail Sales Performance Dashboard")
st.caption("Snowflake + Snowpark Python + Streamlit")

try:
    data = load_dashboard_data()
except Exception as exc:
    st.error("Dashboard data could not be loaded.")
    st.exception(exc)
    st.stop()

overall = data["overall"]
products = data["products"]
regions = data["regions"]
monthly = data["monthly"]
performance = data["performance"]

if overall.empty:
    st.warning("No processed sales data is available. Run the Snowpark pipeline first.")
    st.stop()

kpi = overall.iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Revenue", f"${kpi['TOTAL_REVENUE']:,.2f}")
c2.metric("Units Sold", f"{kpi['TOTAL_UNITS_SOLD']:,.0f}")
c3.metric("Revenue / Unit", f"${kpi['REVENUE_PER_UNIT']:,.2f}")
c4.metric("Transactions", f"{kpi['TRANSACTION_COUNT']:,.0f}")

st.divider()

st.subheader("Product Sales Performance")
product_chart = products.set_index("PRODUCT")["TOTAL_REVENUE"].sort_values(ascending=False)
st.bar_chart(product_chart)

st.subheader("Monthly Revenue Trend")
monthly = monthly.copy()
monthly["PERIOD"] = monthly["YEAR"].astype(str) + "-" + monthly["MONTH"].astype(str).str.zfill(2)
monthly_chart = monthly.set_index("PERIOD")["TOTAL_REVENUE"]
st.line_chart(monthly_chart)

left, right = st.columns(2)

with left:
    st.subheader("Regional Revenue")
    region_chart = regions.set_index("REGION")["TOTAL_REVENUE"].sort_values(ascending=False)
    st.bar_chart(region_chart)

with right:
    st.subheader("Units Sold vs Revenue")
    comparison = performance.set_index("PRODUCT")[["TOTAL_UNITS_SOLD", "TOTAL_REVENUE"]]
    st.bar_chart(comparison)

st.divider()
st.subheader("Product Performance Detail")
st.dataframe(
    performance[
        [
            "PRODUCT",
            "TOTAL_UNITS_SOLD",
            "TOTAL_REVENUE",
            "AVERAGE_UNIT_PRICE",
            "REVENUE_PER_UNIT",
            "TRANSACTION_COUNT",
        ]
    ],
    use_container_width=True,
)

with st.expander("About the metrics"):
    st.write(
        "Revenue is calculated as units sold × unit price. "
        "Revenue / Unit follows the assignment's AOV-style formula. "
        "Traditional AOV is also calculated as revenue divided by transaction count."
    )
