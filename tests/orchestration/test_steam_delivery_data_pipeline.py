from __future__ import annotations

import ast
from pathlib import Path

DAG_PATH = (
    Path(__file__).resolve().parents[2]
    / "orchestration"
    / "dags"
    / "steam_delivery_data_pipeline.py"
)


def _pipeline_function() -> ast.FunctionDef:
    module = ast.parse(DAG_PATH.read_text())
    return next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "steam_delivery_data_pipeline"
    )


def _task_function(name: str) -> ast.FunctionDef:
    return next(
        node
        for node in _pipeline_function().body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


def test_clickhouse_publication_task_has_clear_name_pool_and_bounded_execution() -> (
    None
):
    function = _task_function("publish_tested_dimensional_mart_to_clickhouse")
    decorator = next(
        item
        for item in function.decorator_list
        if isinstance(item, ast.Call)
        and isinstance(item.func, ast.Name)
        and item.func.id == "task"
    )
    keywords = {keyword.arg: keyword.value for keyword in decorator.keywords}

    assert ast.literal_eval(keywords["task_id"]) == (
        "publish_tested_dimensional_mart_to_clickhouse"
    )
    assert ast.literal_eval(keywords["pool"]) == "iceberg_writer"
    assert ast.literal_eval(keywords["pool_slots"]) == 1
    assert ast.literal_eval(keywords["retries"]) == 2
    assert ast.unparse(keywords["execution_timeout"]) == "timedelta(minutes=20)"
    assert ast.unparse(keywords["retry_delay"]) == "timedelta(minutes=1)"


def test_clickhouse_publication_receives_final_dbt_test_and_is_last_dependency() -> (
    None
):
    pipeline = _pipeline_function()
    assignment = next(
        node
        for node in pipeline.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "clickhouse_publication"
            for target in node.targets
        )
    )
    assert isinstance(assignment.value, ast.Call)
    assert isinstance(assignment.value.func, ast.Name)
    assert assignment.value.func.id == "publish_tested_dimensional_mart_to_clickhouse"
    assert [ast.unparse(argument) for argument in assignment.value.args] == [
        "plan",
        "coverage_result",
        "test_complete_mart",
    ]

    dependency_expression = next(
        node
        for node in pipeline.body
        if isinstance(node, ast.Expr)
        and "clickhouse_publication" in ast.unparse(node.value)
        and ">>" in ast.unparse(node.value)
    )
    dependency_text = ast.unparse(dependency_expression.value)
    assert dependency_text.endswith(">> clickhouse_publication")
    assert "test_complete_mart >> clickhouse_publication" in dependency_text
