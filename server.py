from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

# Import backend class
from database_analyser import DatabaseAnalyzer

app = FastAPI(title="Database Analyzer API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ConnectRequest(BaseModel):
    db_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    # For SQL Server convenience
    server: Optional[str] = None
    trusted_connection: Optional[bool] = None


class TableRef(BaseModel):
    schema: str
    table: str


class ColumnSearchRequest(BaseModel):
    column_name: str


class ViewRef(BaseModel):
    schema: str
    view: str


analyzer: Optional[DatabaseAnalyzer] = None


@app.post("/connect")
def connect(req: ConnectRequest) -> Dict[str, Any]:
    global analyzer
    analyzer = DatabaseAnalyzer()
    params: Dict[str, Any] = {}

    if req.db_type not in ["postgresql", "sqlserver", "mysql"]:
        raise HTTPException(status_code=400, detail="Unsupported db_type")

    # Map incoming fields to the analyzer expected params based on db_type
    if req.db_type == "postgresql":
        params = {
            "host": req.host or "localhost",
            "port": str(req.port or 5432),
            "database": req.database or "postgres",
            "username": req.username or "postgres",
            "password": req.password or "",
        }
    elif req.db_type == "mysql":
        params = {
            "host": req.host or "localhost",
            "port": int(req.port or 3306),
            "database": req.database or "mysql",
            "username": req.username or "root",
            "password": req.password or "",
        }
    elif req.db_type == "sqlserver":
        if req.trusted_connection:
            params = {
                "server": req.server or (req.host or "localhost"),
                "database": req.database or "master",
                "trusted_connection": True,
            }
        else:
            # Either server or host:port
            params = {
                "server": req.server or (req.host or "localhost"),
                "host": req.host,
                "port": str(req.port or 1433),
                "database": req.database or "master",
                "username": req.username or "sa",
                "password": req.password or "",
                "trusted_connection": False,
            }

    ok = analyzer.connect_database(req.db_type, params)
    if not ok:
        analyzer = None
        raise HTTPException(status_code=500, detail="Failed to connect to database")
    return {"status": "connected", "db_type": req.db_type}


def ensure_connected():
    if analyzer is None or analyzer.cursor is None:
        raise HTTPException(status_code=400, detail="Not connected. Call /connect first.")


@app.get("/overview")
def overview() -> Dict[str, Any]:
    ensure_connected()
    # Replicate what get_database_overview prints, but return JSON
    try:
        tables = analyzer.get_tables()
        views_count = analyzer._get_views_count()  # type: ignore[attr-defined]
        schemas_count = analyzer._get_schemas_count()  # type: ignore[attr-defined]
        db_size = analyzer._get_database_size()  # type: ignore[attr-defined]
        return {
            "db_type": analyzer.db_type,
            "tables_count": len(tables),
            "views_count": views_count,
            "schemas_count": schemas_count,
            "database_size": db_size,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/tables")
def list_tables() -> List[Dict[str, str]]:
    ensure_connected()
    try:
        tables = analyzer.get_tables()
        return [{"schema": s, "table": t} for s, t in tables]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/views")
def list_views() -> List[Dict[str, str]]:
    ensure_connected()
    try:
        views = analyzer.get_views()
        return [{"schema": s, "view": v} for s, v in views]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/table/details")
def table_details(ref: TableRef) -> Dict[str, Any]:
    ensure_connected()
    try:
        # Gather same metrics as get_table_details_and_quality without printing
        analyzer.cursor.execute(f"SELECT COUNT(*) FROM {ref.schema}.{ref.table}")
        row_count = analyzer.cursor.fetchone()[0]
        columns = analyzer._get_column_info(ref.schema, ref.table)  # type: ignore[attr-defined]
        table_size = analyzer._get_table_size(ref.schema, ref.table)  # type: ignore[attr-defined]
        return {
            "schema": ref.schema,
            "table": ref.table,
            "row_count": row_count,
            "columns": columns,
            "estimated_size": table_size,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/table/indexes")
def table_indexes(ref: TableRef) -> Dict[str, Any]:
    ensure_connected()
    try:
        # Provide a structured version by re-querying based on db_type
        indexes: List[Dict[str, Any]] = []
        db_type = analyzer.db_type
        if db_type == "postgresql":
            analyzer.cursor.execute(
                """
                SELECT i.relname AS index_name,
                       array_to_string(array_agg(a.attname), ', ') AS columns,
                       ix.indisunique AS is_unique,
                       ix.indisprimary AS is_primary,
                       am.amname AS index_type
                FROM pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_am am ON i.relam = am.oid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                WHERE t.relname = %s AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
                GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname
                ORDER BY i.relname
                """,
                (ref.table, ref.schema),
            )
            for row in analyzer.cursor.fetchall():
                indexes.append(
                    {
                        "index_name": row[0],
                        "columns": row[1],
                        "is_unique": bool(row[2]),
                        "is_primary": bool(row[3]),
                        "index_type": row[4],
                    }
                )
        elif db_type == "mysql":
            analyzer.cursor.execute(f"SHOW INDEXES FROM {ref.schema}.{ref.table}")
            for row in analyzer.cursor.fetchall():
                indexes.append(
                    {
                        "index_name": row[2],
                        "column": row[4],
                        "is_unique": row[1] == 0,
                        "index_type": row[10] if len(row) > 10 else "BTREE",
                    }
                )
        elif db_type == "sqlserver":
            analyzer.cursor.execute(
                """
                SELECT i.name AS index_name,
                       STUFF((SELECT ', ' + c.name
                              FROM sys.index_columns ic2
                              JOIN sys.columns c ON ic2.object_id = c.object_id AND ic2.column_id = c.column_id
                              WHERE ic2.object_id = ic.object_id AND ic2.index_id = ic.index_id
                              ORDER BY ic2.key_ordinal
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS columns,
                       i.is_unique,
                       i.is_primary_key,
                       i.type_desc AS index_type
                FROM sys.indexes i
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.objects o ON i.object_id = o.object_id
                WHERE o.schema_id = SCHEMA_ID(%s) AND o.name = %s AND i.is_hypothetical = 0
                GROUP BY i.name, i.is_unique, i.is_primary_key, i.type_desc
                ORDER BY i.name
                """,
                (ref.schema, ref.table),
            )
            for row in analyzer.cursor.fetchall():
                indexes.append(
                    {
                        "index_name": row[0],
                        "columns": row[1],
                        "is_unique": bool(row[2]),
                        "is_primary": bool(row[3]),
                        "index_type": row[4],
                    }
                )
        return {"schema": ref.schema, "table": ref.table, "indexes": indexes}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/table/duplicates")
def table_duplicates(ref: TableRef) -> Dict[str, Any]:
    ensure_connected()
    try:
        # Use existing helper methods to compute duplicates; expose counts only to keep it light
        # We will call the internal methods but not print/export
        columns_info = analyzer._get_column_info(ref.schema, ref.table)  # type: ignore[attr-defined]
        text_columns = [c["name"] for c in columns_info if "char" in c["type"].lower() or "text" in c["type"].lower()]
        exact = analyzer._find_exact_duplicates(ref.schema, ref.table, columns_info)  # type: ignore[attr-defined]
        fuzzy = analyzer._find_fuzzy_duplicates(ref.schema, ref.table, text_columns)  # type: ignore[attr-defined]
        return {
            "schema": ref.schema,
            "table": ref.table,
            "exact_duplicates_count": len(exact),
            "fuzzy_duplicates_count": len(fuzzy),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/column/search")
def column_search(req: ColumnSearchRequest) -> Dict[str, Any]:
    ensure_connected()
    try:
        matches = analyzer.find_tables_by_column(req.column_name)
        # method prints and exports; we re-run quickly to compute using _get_column_info across tables
        tables = analyzer.get_tables()
        result: List[Dict[str, str]] = []
        for schema, table in tables:
            try:
                columns = analyzer._get_column_info(schema, table)  # type: ignore[attr-defined]
                if any(req.column_name.lower() == c["name"].lower() for c in columns):
                    result.append({"schema": schema, "table": table})
            except Exception:
                continue
        return {"column": req.column_name, "tables": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/view/hierarchy")
def view_hierarchy(view: ViewRef) -> Dict[str, Any]:
    ensure_connected()
    try:
        output = analyzer.build_view_hierarchy(view.schema, view.view, output_path=f"{view.schema}_{view.view}", output_format="png")
        return {"schema": view.schema, "view": view.view, "image_path": output}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.on_event("shutdown")
def shutdown_event() -> None:
    global analyzer
    try:
        if analyzer is not None:
            analyzer.close_connection()
    finally:
        analyzer = None

