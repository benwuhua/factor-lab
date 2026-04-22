from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from qlib_factor_lab.config import load_yaml
from qlib_factor_lab.factor_registry import FactorDef


@dataclass(frozen=True)
class ExpressionSpace:
    fields: frozenset[str]
    windows: frozenset[int]
    operators: frozenset[str]
    families: frozenset[str]
    max_expression_length: int
    max_operator_count: int
    max_window_count: int


@dataclass(frozen=True)
class ExpressionCandidate:
    name: str
    family: str
    expression: str
    direction: int
    description: str
    expected_behavior: str = ""

    def to_factor_def(self) -> FactorDef:
        return FactorDef(
            name=self.name,
            expression=self.expression,
            direction=self.direction,
            category=f"autoresearch_{self.family}",
            description=self.description,
        )


def load_expression_space(path: str | Path) -> ExpressionSpace:
    data = load_yaml(path)
    complexity = data.get("complexity", {})
    if not isinstance(complexity, dict):
        raise ValueError("complexity must be a mapping")
    return ExpressionSpace(
        fields=frozenset(_required_list(data, "fields")),
        windows=frozenset(int(value) for value in _required_list(data, "windows")),
        operators=frozenset(_required_list(data, "operators")),
        families=frozenset(_required_list(data, "families")),
        max_expression_length=int(complexity.get("max_expression_length", 500)),
        max_operator_count=int(complexity.get("max_operator_count", 20)),
        max_window_count=int(complexity.get("max_window_count", 6)),
    )


def load_expression_candidate(path: str | Path, space: ExpressionSpace) -> ExpressionCandidate:
    data = load_yaml(path)
    for field in ("name", "family", "expression", "direction", "description"):
        if field not in data:
            raise ValueError(f"missing required candidate field: {field}")
    candidate = ExpressionCandidate(
        name=str(data["name"]),
        family=str(data["family"]),
        expression=str(data["expression"]),
        direction=int(data["direction"]),
        description=str(data["description"]),
        expected_behavior=str(data.get("expected_behavior", "")),
    )
    validate_expression_candidate(candidate, space)
    return candidate


def validate_expression_candidate(candidate: ExpressionCandidate, space: ExpressionSpace) -> None:
    if candidate.family not in space.families:
        raise ValueError(f"disallowed family: {candidate.family}")
    if len(candidate.expression) > space.max_expression_length:
        raise ValueError("expression exceeds max_expression_length")
    for field in sorted(set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", candidate.expression))):
        if field not in space.fields:
            raise ValueError(f"disallowed field: {field}")
    operators = re.findall(r"\b([A-Z][A-Za-z0-9_]*)\s*\(", candidate.expression)
    if len(operators) > space.max_operator_count:
        raise ValueError("operator count exceeds max_operator_count")
    for operator in sorted(set(operators)):
        if operator not in space.operators:
            raise ValueError(f"disallowed operator: {operator}")
    windows = _extract_operator_windows(candidate.expression)
    if len(set(windows)) > space.max_window_count:
        raise ValueError("window count exceeds max_window_count")
    for window in windows:
        if window not in space.windows:
            raise ValueError(f"disallowed window: {window}")


def _required_list(data: dict[str, Any], field: str) -> list[Any]:
    value = data.get(field)
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} must be a non-empty list")
    return value


def _extract_operator_windows(expression: str) -> list[int]:
    windows: list[int] = []
    for match in re.finditer(r"\b[A-Z][A-Za-z0-9_]*\s*\(", expression):
        open_pos = expression.find("(", match.start())
        close_pos = _find_matching_paren(expression, open_pos)
        if close_pos is None:
            continue
        args = _split_top_level_args(expression[open_pos + 1 : close_pos])
        if len(args) >= 2 and re.fullmatch(r"\d+", args[-1].strip()):
            windows.append(int(args[-1].strip()))
    return windows


def _find_matching_paren(expression: str, open_pos: int) -> int | None:
    depth = 0
    for index in range(open_pos, len(expression)):
        char = expression[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
    return None


def _split_top_level_args(text: str) -> list[str]:
    args: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            args.append(text[start:index].strip())
            start = index + 1
    args.append(text[start:].strip())
    return args
