"""
MCP Server — Lakehouse Tools
Exposes Databricks lakehouse operations as MCP tools for AI assistants.
Tools: query, lineage, DQ results, freshness, PII check.
"""

import os
from datetime import datetime

from databricks import sql as databricks_sql
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
DATABRICKS_WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

server = Server("lakehouse-mcp")


def get_connection():
    return databricks_sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        access_token=DATABRICKS_TOKEN,
    )


@server.list_tools()
async def list_tools():
    return [
        Tool(
            name="query_lakehouse",
            description="Execute a read-only SQL query against the lakehouse (Unity Catalog). Supports Gold tables and virtual views.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL SELECT query to execute"}
                },
                "required": ["sql"],
            },
        ),
        Tool(
            name="get_lineage",
            description="Get upstream or downstream lineage for a table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Fully qualified table name (catalog.schema.table)"},
                    "direction": {"type": "string", "enum": ["upstream", "downstream"], "default": "upstream"},
                },
                "required": ["table_name"],
            },
        ),
        Tool(
            name="get_dq_results",
            description="Get latest data quality check results, optionally filtered by table or severity.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Filter by table name (optional)"},
                    "severity": {"type": "string", "enum": ["critical", "warning"], "description": "Filter by severity"},
                    "only_failures": {"type": "boolean", "default": True},
                },
            },
        ),
        Tool(
            name="get_freshness_status",
            description="Check data freshness status for all monitored tables.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="check_pii_exposure",
            description="Check which tables/columns contain PII and their masking status.",
            inputSchema={
                "type": "object",
                "properties": {
                    "catalog": {"type": "string", "default": "ecommerce_dev"},
                },
            },
        ),
        Tool(
            name="list_tables",
            description="List tables in a schema with their metadata tags.",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Schema name (bronze, silver, gold)"},
                },
                "required": ["schema"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        if name == "query_lakehouse":
            sql = arguments["sql"].strip()
            if not sql.upper().startswith("SELECT"):
                return [TextContent(type="text", text="Error: Only SELECT queries are allowed.")]
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchmany(100)
            result = [dict(zip(columns, row)) for row in rows]
            return [TextContent(type="text", text=format_results(result, columns))]

        elif name == "get_lineage":
            table = arguments["table_name"]
            direction = arguments.get("direction", "upstream")
            if direction == "upstream":
                cursor.execute(f"""
                    SELECT source_table_full_name, target_table_full_name, event_time
                    FROM system.access.table_lineage
                    WHERE target_table_full_name = '{table}'
                    ORDER BY event_time DESC LIMIT 20
                """)
            else:
                cursor.execute(f"""
                    SELECT source_table_full_name, target_table_full_name, event_time
                    FROM system.access.table_lineage
                    WHERE source_table_full_name = '{table}'
                    ORDER BY event_time DESC LIMIT 20
                """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [TextContent(type="text", text=format_results(
                [dict(zip(columns, r)) for r in rows], columns
            ))]

        elif name == "get_dq_results":
            where_clauses = ["1=1"]
            if arguments.get("table_name"):
                where_clauses.append(f"table_name = '{arguments['table_name']}'")
            if arguments.get("severity"):
                where_clauses.append(f"severity = '{arguments['severity']}'")
            if arguments.get("only_failures", True):
                where_clauses.append("passed = false")

            cursor.execute(f"""
                SELECT rule_id, rule_name, category, severity, table_name,
                       threshold, actual_value, passed, run_timestamp
                FROM ecommerce_dev.governance.dq_results
                WHERE {' AND '.join(where_clauses)}
                ORDER BY run_timestamp DESC LIMIT 20
            """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [TextContent(type="text", text=format_results(
                [dict(zip(columns, r)) for r in rows], columns
            ))]

        elif name == "get_freshness_status":
            cursor.execute("""
                SELECT table_name, last_ingestion, staleness_minutes, freshness_status, check_timestamp
                FROM ecommerce_dev.governance.freshness_metrics
                WHERE check_timestamp = (SELECT MAX(check_timestamp) FROM ecommerce_dev.governance.freshness_metrics)
            """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [TextContent(type="text", text=format_results(
                [dict(zip(columns, r)) for r in rows], columns
            ))]

        elif name == "check_pii_exposure":
            catalog = arguments.get("catalog", "ecommerce_dev")
            cursor.execute(f"""
                SELECT table_name, column_name, tag_name, tag_value
                FROM system.information_schema.column_tags
                WHERE catalog_name = '{catalog}' AND tag_name = 'classification' AND tag_value = 'pii'
            """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [TextContent(type="text", text=format_results(
                [dict(zip(columns, r)) for r in rows], columns
            ))]

        elif name == "list_tables":
            schema = arguments["schema"]
            cursor.execute(f"""
                SELECT table_name, comment, data_source_format,
                       ROUND(bytes/(1024*1024), 2) AS size_mb, rows
                FROM system.information_schema.tables
                WHERE table_catalog = 'ecommerce_dev' AND table_schema = '{schema}'
            """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [TextContent(type="text", text=format_results(
                [dict(zip(columns, r)) for r in rows], columns
            ))]

    finally:
        cursor.close()
        conn.close()


def format_results(rows: list[dict], columns: list[str]) -> str:
    if not rows:
        return "No results found."
    header = " | ".join(columns)
    separator = "-" * len(header)
    lines = [header, separator]
    for row in rows:
        lines.append(" | ".join(str(row.get(c, "")) for c in columns))
    return "\n".join(lines)


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
