from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from sla_monitor.config import get_database_path
from sla_monitor.database import create_database_engine


@st.cache_data(ttl=30)
def load_run_history(database_path: str) -> list[dict[str, Any]]:
    engine = create_database_engine(database_path)
    try:
        if not inspect(engine).has_table("runs"):
            return []
        with engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT run_id, started_at, as_of, status, expected_count,
                           loaded_count, rejected_count, failure_reason
                    FROM runs
                    ORDER BY started_at DESC
                    LIMIT 100
                    """
                )
            ).mappings()
            return [dict(row) for row in rows]
    finally:
        engine.dispose()


@st.cache_data(ttl=30)
def load_run_report(database_path: str, run_id: str) -> dict[str, Any]:
    engine = create_database_engine(database_path)
    try:
        with engine.connect() as connection:
            run_row = connection.execute(
                text("SELECT * FROM runs WHERE run_id = :run_id"),
                {"run_id": run_id},
            ).mappings().first()
            if run_row is None:
                raise ValueError(f"Run {run_id} was not found in the selected database.")

            parameters = {"run_id": run_id}
            result_counts = {}
            for field in ("arrival_status", "completion_status", "freshness_status"):
                rows = connection.execute(
                    text(
                        f"SELECT {field}, COUNT(*) AS count FROM dataset_results "
                        f"WHERE run_id = :run_id GROUP BY {field}"
                    ),
                    parameters,
                )
                result_counts[field] = {row[0]: row[1] for row in rows}

            alert_rows = connection.execute(
                text(
                    """
                    SELECT severity, alert_code, COUNT(*) AS count
                    FROM alerts WHERE run_id = :run_id
                    GROUP BY severity, alert_code
                    ORDER BY CASE severity WHEN 'critical' THEN 0
                             WHEN 'high' THEN 1 ELSE 2 END, alert_code
                    """
                ),
                parameters,
            ).mappings()
            alert_summary = [dict(row) for row in alert_rows]
            alert_count = sum(row["count"] for row in alert_summary)
            reject_count = connection.execute(
                text("SELECT COUNT(*) FROM rejects WHERE run_id = :run_id"),
                parameters,
            ).scalar_one()

            return {
                "run": dict(run_row),
                "result_counts": result_counts,
                "alert_summary": alert_summary,
                "alert_count": alert_count,
                "reject_count": reject_count,
            }
    finally:
        engine.dispose()


def _filtered_query(
    run_id: str,
    statuses: tuple[str, ...],
    search: str,
) -> tuple[str, dict[str, Any]]:
    conditions = ["run_id = :run_id"]
    parameters: dict[str, Any] = {"run_id": run_id}
    if statuses:
        placeholders = []
        for index, value in enumerate(statuses):
            parameter = f"status_{index}"
            placeholders.append(f":{parameter}")
            parameters[parameter] = value
        conditions.append(f"arrival_status IN ({', '.join(placeholders)})")
    else:
        conditions.append("1 = 0")
    if search:
        conditions.append("(LOWER(dataset_id) LIKE :search OR LOWER(dataset_name) LIKE :search)")
        parameters["search"] = f"%{search.casefold()}%"
    return " AND ".join(conditions), parameters


@st.cache_data(ttl=30)
def load_dataset_page(
    database_path: str,
    run_id: str,
    arrival_statuses: tuple[str, ...],
    search: str,
    limit: int,
    offset: int,
) -> tuple[int, list[dict[str, Any]]]:
    engine = create_database_engine(database_path)
    where, parameters = _filtered_query(run_id, arrival_statuses, search)
    try:
        with engine.connect() as connection:
            total = connection.execute(
                text(f"SELECT COUNT(*) FROM dataset_results WHERE {where}"),
                parameters,
            ).scalar_one()
            rows = connection.execute(
                text(
                    f"SELECT * FROM dataset_results WHERE {where} "
                    "ORDER BY dataset_id LIMIT :limit OFFSET :offset"
                ),
                {**parameters, "limit": limit, "offset": offset},
            ).mappings()
            return total, [dict(row) for row in rows]
    finally:
        engine.dispose()


@st.cache_data(ttl=30)
def load_alert_page(
    database_path: str,
    run_id: str,
    severities: tuple[str, ...],
    limit: int,
    offset: int,
) -> tuple[int, list[dict[str, Any]]]:
    engine = create_database_engine(database_path)
    parameters: dict[str, Any] = {"run_id": run_id}
    where = "run_id = :run_id"
    if severities:
        placeholders = []
        for index, severity in enumerate(severities):
            parameter = f"severity_{index}"
            placeholders.append(f":{parameter}")
            parameters[parameter] = severity
        where += f" AND severity IN ({', '.join(placeholders)})"
    else:
        where += " AND 1 = 0"
    try:
        with engine.connect() as connection:
            total = connection.execute(
                text(f"SELECT COUNT(*) FROM alerts WHERE {where}"), parameters
            ).scalar_one()
            rows = connection.execute(
                text(
                    f"SELECT * FROM alerts WHERE {where} "
                    "ORDER BY CASE severity WHEN 'critical' THEN 0 "
                    "WHEN 'high' THEN 1 ELSE 2 END, alert_id "
                    "LIMIT :limit OFFSET :offset"
                ),
                {**parameters, "limit": limit, "offset": offset},
            ).mappings()
            return total, [dict(row) for row in rows]
    finally:
        engine.dispose()


@st.cache_data(ttl=30)
def load_reject_page(
    database_path: str,
    run_id: str,
    limit: int,
    offset: int,
) -> tuple[int, list[dict[str, Any]]]:
    engine = create_database_engine(database_path)
    try:
        with engine.connect() as connection:
            total = connection.execute(
                text("SELECT COUNT(*) FROM rejects WHERE run_id = :run_id"),
                {"run_id": run_id},
            ).scalar_one()
            rows = connection.execute(
                text(
                    "SELECT * FROM rejects WHERE run_id = :run_id "
                    "ORDER BY reject_id LIMIT :limit OFFSET :offset"
                ),
                {"run_id": run_id, "limit": limit, "offset": offset},
            ).mappings()
            return total, [dict(row) for row in rows]
    finally:
        engine.dispose()


def _run_label(run: dict[str, Any]) -> str:
    return (
        f"{run['started_at']} · {run['status'].upper()} · "
        f"{run['run_id'][:8]}"
    )


def _percent(numerator: int, denominator: int) -> float:
    return round(100 * numerator / denominator, 1) if denominator else 0.0


st.set_page_config(
    page_title="Data Pipeline SLA Monitor",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(
    """
    <style>
    .block-container {padding-top: 2rem; padding-bottom: 3rem;}
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(28, 54, 92, .08), rgba(38, 166, 154, .08));
        border: 1px solid rgba(128, 128, 128, .18);
        padding: 1rem 1.1rem;
        border-radius: 14px;
    }
    .hero {
        padding: 1.35rem 1.6rem;
        border-radius: 18px;
        background: linear-gradient(115deg, #10243a 0%, #174e67 58%, #168a83 100%);
        color: white;
        margin-bottom: 1.2rem;
    }
    .hero h1 {color: white; margin: 0 0 .25rem 0; font-size: 2rem;}
    .hero p {color: rgba(255,255,255,.82); margin: 0;}
    </style>
    """,
    unsafe_allow_html=True,
)

database_path = get_database_path()
database_label = str(database_path)

st.sidebar.title("📡 SLA Monitor")
st.sidebar.caption("Delivery health · freshness · data quality")
st.sidebar.caption(f"SQLite database: `{database_label}`")
if st.sidebar.button("↻ Refresh dashboard", width="stretch"):
    load_run_history.clear()
    load_run_report.clear()
    load_dataset_page.clear()
    load_alert_page.clear()
    load_reject_page.clear()

try:
    run_history = load_run_history(str(database_path))
except (SQLAlchemyError, ValueError) as exc:
    st.error(f"Could not read run history from SQLite: {exc}")
    st.stop()

st.markdown(
    """
    <div class="hero">
      <h1>Data Pipeline SLA Monitor</h1>
      <p>One place to spot late deliveries, stale data, and records that need attention.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

if not run_history:
    st.info(
        "No pipeline runs found yet. Generate the sample data and run the pipeline "
        "from the project folder to populate this dashboard."
    )
    st.code(
        "python -m sla_monitor generate-sample --output-dir data/sample\n"
        "python -m sla_monitor run --datasets data/sample/datasets.csv "
        "--loads data/sample/load_events.csv "
        "--as-of 2025-01-01T05:00:00Z",
        language="powershell",
    )
    st.stop()

selected_run = st.sidebar.selectbox(
    "Run history",
    run_history,
    format_func=_run_label,
    help="Choose one of the most recent 100 runs.",
)

try:
    report = load_run_report(str(database_path), selected_run["run_id"])
except (SQLAlchemyError, ValueError) as exc:
    st.error(f"Could not load the selected run: {exc}")
    st.stop()

run = report["run"]
status_icon = {
    "succeeded": "🟢",
    "failed": "🔴",
    "running": "🟡",
}.get(run["status"], "⚪")
left, right = st.columns([4, 1])
left.subheader(f"{status_icon} Run {run['status'].title()}")
right.caption(f"Run ID: `{run['run_id']}`")
st.caption(
    f"Started {run['started_at']}  ·  Monitoring time {run['as_of']}  ·  "
    f"Failure reason: {run['failure_reason']}"
)

if run["status"] == "failed":
    st.error(f"Run failed: {run['failure_reason']}")

expected = int(run["expected_count"])
loaded = int(run["loaded_count"])
outputs = int(run["output_count"])
reject_count = int(run["rejected_count"])
alert_count = int(report["alert_count"])
load_rate = _percent(loaded, expected)

metric_columns = st.columns(5)
metric_values = [
    ("Expected datasets", expected, None),
    ("Accepted loads", loaded, f"{load_rate}% of expected"),
    ("Published results", outputs, None),
    ("Alerts raised", alert_count, None),
    ("Rejected records", reject_count, None),
]
for column, (label, value, delta) in zip(metric_columns, metric_values):
    column.metric(label, value, delta)

st.progress(min(load_rate / 100, 1.0), text=f"Load coverage · {load_rate}%")

tabs = st.tabs(["Overview", "Dataset health", "Alerts & rejects", "Run details"])

with tabs[0]:
    st.subheader("Delivery and freshness snapshot")
    result_counts = report["result_counts"]
    if not result_counts["arrival_status"]:
        st.info("This run has no dataset results.")
    else:
        arrival_counts = pd.Series(
            result_counts["arrival_status"], dtype="int64"
        ).sort_index()
        completion_counts = pd.Series(
            result_counts["completion_status"], dtype="int64"
        ).sort_index()
        freshness_counts = pd.Series(
            result_counts["freshness_status"], dtype="int64"
        ).sort_index()

        on_time = int(arrival_counts.get("on_time", 0))
        within_sla = int(completion_counts.get("within_sla", 0))
        fresh = int(freshness_counts.get("fresh", 0))
        status_columns = st.columns(3)
        status_columns[0].metric(
            "On-time arrivals", f"{on_time} / {expected}", f"{_percent(on_time, expected)}%"
        )
        completion_total = int(
            completion_counts.get("within_sla", 0)
            + completion_counts.get("breached", 0)
        )
        status_columns[1].metric(
            "Completion within SLA",
            f"{within_sla} / {completion_total}",
            f"{_percent(within_sla, completion_total)}%",
        )
        freshness_total = int(freshness_counts.get("fresh", 0) + freshness_counts.get("stale", 0))
        status_columns[2].metric(
            "Fresh datasets",
            f"{fresh} / {freshness_total}",
            f"{_percent(fresh, freshness_total)}%",
        )

        charts = st.columns(3)
        for column, title, series in (
            (charts[0], "Arrival status", arrival_counts),
            (charts[1], "Completion status", completion_counts),
            (charts[2], "Freshness status", freshness_counts),
        ):
            with column:
                st.markdown(f"**{title}**")
                if series.empty:
                    st.caption("No data")
                else:
                    chart_data = (
                        series.rename_axis("status")
                        .rename("datasets")
                        .reset_index()
                    )
                    st.bar_chart(
                        chart_data,
                        x="status",
                        y="datasets",
                        height=230,
                    )

    st.subheader("Attention required")
    alert_summary = pd.DataFrame(report["alert_summary"])
    if alert_summary.empty:
        st.success("No alerts for this run.")
    else:
        severity_summary = (
            alert_summary.groupby("severity", as_index=False)["count"]
            .sum()
        )
        st.dataframe(
            severity_summary.rename(
                columns={"severity": "Severity", "count": "Alerts"}
            ),
            width="stretch",
            hide_index=True,
        )
        st.dataframe(
            alert_summary.rename(
                columns={
                    "severity": "Severity",
                    "alert_code": "Alert",
                    "count": "Count",
                }
            ).head(8),
            width="stretch",
            hide_index=True,
        )

with tabs[1]:
    st.subheader("Dataset-level SLA results")
    arrival_statuses = tuple(sorted(report["result_counts"]["arrival_status"]))
    if not arrival_statuses:
        st.info("No dataset results found.")
    else:
        search = st.text_input("Find a dataset", placeholder="Search by ID or name")
        selected_statuses = st.multiselect(
            "Filter by arrival status",
            list(arrival_statuses),
            default=list(arrival_statuses),
        )
        page_size = st.selectbox("Rows per page", [50, 100, 250], index=1)
        total, page_rows = load_dataset_page(
            str(database_path),
            run["run_id"],
            tuple(selected_statuses),
            search,
            page_size,
            0,
        )
        total_pages = max(1, (total + page_size - 1) // page_size)
        page_number = st.number_input(
            "Page",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            key="dataset_page",
        )
        if page_number > 1:
            _, page_rows = load_dataset_page(
                str(database_path),
                run["run_id"],
                tuple(selected_statuses),
                search,
                page_size,
                (page_number - 1) * page_size,
            )
        filtered = pd.DataFrame(page_rows)
        visible_columns = [
            "dataset_id",
            "dataset_name",
            "arrival_status",
            "completion_status",
            "freshness_status",
            "freshness_minutes",
            "row_count",
            "expected_arrival_at",
            "completed_at",
        ]
        start = (page_number - 1) * page_size + 1 if total else 0
        end = min(start + page_size - 1, total)
        st.caption(f"Showing {start:,}–{end:,} of {total:,} matching datasets")
        if filtered.empty:
            st.info("No datasets match these filters.")
        else:
            st.dataframe(
                filtered[visible_columns],
                width="stretch",
                hide_index=True,
                column_config={
                    "dataset_id": st.column_config.TextColumn("Dataset ID"),
                    "dataset_name": st.column_config.TextColumn("Dataset"),
                    "arrival_status": st.column_config.TextColumn("Arrival"),
                    "completion_status": st.column_config.TextColumn("Completion SLA"),
                    "freshness_status": st.column_config.TextColumn("Freshness"),
                    "freshness_minutes": st.column_config.NumberColumn(
                        "Freshness (min)", format="%.2f"
                    ),
                    "row_count": st.column_config.NumberColumn(
                        "Source rows", format="%d"
                    ),
                    "expected_arrival_at": st.column_config.TextColumn(
                        "Expected arrival"
                    ),
                    "completed_at": st.column_config.TextColumn("Completed at"),
                },
            )
            st.download_button(
                "Download this page as CSV",
                filtered[visible_columns].to_csv(index=False).encode("utf-8"),
                file_name=f"dataset_results_{run['run_id']}_page_{page_number}.csv",
                mime="text/csv",
            )

with tabs[2]:
    alerts_column, rejects_column = st.columns(2)
    with alerts_column:
        st.subheader(f"Alerts · {alert_count:,}")
        if alert_count == 0:
            st.success("No alerts raised.")
        else:
            severity_options = sorted(
                alert_summary["severity"].dropna().unique().tolist()
            )
            severity_filter = tuple(st.multiselect(
                "Severity",
                severity_options,
                default=severity_options,
            ))
            alert_page_size = 100
            filtered_alert_count, alert_rows = load_alert_page(
                str(database_path),
                run["run_id"],
                severity_filter,
                alert_page_size,
                0,
            )
            alert_pages = max(1, (filtered_alert_count + alert_page_size - 1) // alert_page_size)
            alert_page = st.number_input(
                "Alert page",
                min_value=1,
                max_value=alert_pages,
                value=1,
                step=1,
                key="alert_page",
            )
            if alert_page > 1:
                _, alert_rows = load_alert_page(
                    str(database_path),
                    run["run_id"],
                    severity_filter,
                    alert_page_size,
                    (alert_page - 1) * alert_page_size,
                )
            filtered_alerts = pd.DataFrame(alert_rows)
            st.caption(
                f"Showing {len(alert_rows):,} of {filtered_alert_count:,} matching alerts"
            )
            if filtered_alerts.empty:
                st.info("No alerts match this severity filter.")
            else:
                st.dataframe(
                    filtered_alerts[
                        ["severity", "alert_code", "dataset_id", "message"]
                    ],
                    width="stretch",
                    hide_index=True,
                )
                st.download_button(
                    "Download this page as CSV",
                    filtered_alerts.to_csv(index=False).encode("utf-8"),
                    file_name=f"alerts_{run['run_id']}_page_{alert_page}.csv",
                    mime="text/csv",
                )

    with rejects_column:
        st.subheader(f"Quarantined records · {reject_count:,}")
        if reject_count == 0:
            st.success("No records were rejected.")
        else:
            reject_page_size = 100
            filtered_reject_count, reject_rows = load_reject_page(
                str(database_path), run["run_id"], reject_page_size, 0
            )
            reject_pages = max(
                1, (filtered_reject_count + reject_page_size - 1) // reject_page_size
            )
            reject_page = st.number_input(
                "Reject page",
                min_value=1,
                max_value=reject_pages,
                value=1,
                step=1,
                key="reject_page",
            )
            if reject_page > 1:
                _, reject_rows = load_reject_page(
                    str(database_path),
                    run["run_id"],
                    reject_page_size,
                    (reject_page - 1) * reject_page_size,
                )
            filtered_rejects = pd.DataFrame(reject_rows)
            st.caption(
                f"Showing {len(reject_rows):,} of {filtered_reject_count:,} rejected records"
            )
            if not filtered_rejects.empty:
                st.dataframe(
                    filtered_rejects[
                        [
                            "source_file",
                            "source_line",
                            "dataset_id",
                            "reason_code",
                            "reason",
                        ]
                    ],
                    width="stretch",
                    hide_index=True,
                )
                selected_reject = st.selectbox(
                    "Inspect raw rejected record",
                    range(len(filtered_rejects)),
                    format_func=lambda index: (
                        f"{filtered_rejects.iloc[index]['source_file']} · "
                        f"line {filtered_rejects.iloc[index]['source_line']}"
                    ),
                    key="selected_reject",
                )
                st.code(
                    filtered_rejects.iloc[selected_reject]["raw_record"],
                    language="json",
                )
                st.download_button(
                    "Download this page as CSV",
                    filtered_rejects.to_csv(index=False).encode("utf-8"),
                    file_name=f"rejects_{run['run_id']}_page_{reject_page}.csv",
                    mime="text/csv",
                )

with tabs[3]:
    st.subheader("Run metadata and reconciliation")
    detail_columns = st.columns(3)
    detail_columns[0].metric("Input rows", int(run["input_rows"]))
    detail_columns[1].metric("Output rows", outputs)
    detail_columns[2].metric("Rejected rows", reject_count)
    st.json(
        {
            "run_id": run["run_id"],
            "status": run["status"],
            "started_at": run["started_at"],
            "ended_at": run["ended_at"],
            "as_of": run["as_of"],
            "expected_count": expected,
            "loaded_count": loaded,
            "output_count": outputs,
            "rejected_count": reject_count,
            "failure_reason": run["failure_reason"],
        },
        expanded=True,
    )
