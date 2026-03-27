from deerflow.sql.lineage import extract_source_table_keys, extract_target_table_keys, parse_sql_statements, resolve_statement_kind, supports_lineage


def test_supports_lineage_purposes() -> None:
    assert supports_lineage("insert") is True
    assert supports_lineage("create") is True
    assert supports_lineage("merge") is True
    assert supports_lineage("query") is False
    assert supports_lineage("update") is False


def test_extract_target_table_keys_for_insert() -> None:
    statement = parse_sql_statements("INSERT INTO d.t_target SELECT id FROM d.t_source")[0]
    target_keys = extract_target_table_keys(statement, cte_norm=set())
    assert target_keys == {(None, "d", "t_target")}


def test_extract_source_table_keys_excludes_cte_and_target() -> None:
    statement = parse_sql_statements("INSERT INTO d.t_target WITH cte AS (SELECT id FROM d.t_source) SELECT id FROM cte")[0]
    target_keys = extract_target_table_keys(statement, cte_norm={"cte"})
    statement_meta = {
        "tables": [
            {"catalog": None, "db": "d", "name": "t_target"},
            {"catalog": None, "db": "d", "name": "t_source"},
            {"catalog": None, "db": None, "name": "cte"},
        ]
    }
    source_keys = extract_source_table_keys(statement_meta, cte_norm={"cte"}, target_keys=target_keys)
    assert source_keys == {(None, "d", "t_source")}


def test_resolve_statement_kind_for_with_insert() -> None:
    statement = parse_sql_statements("WITH base AS (SELECT 1) INSERT INTO d.t_target SELECT * FROM base")[0]
    assert resolve_statement_kind(statement) == "Insert"
