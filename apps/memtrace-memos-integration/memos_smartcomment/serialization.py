# 快照层: 将业务对象复制为可序列化的基础值, 在事件入队前按字段脱敏并限制大小.
# 普通消息正文只截断, 不扫描其中任意位置的凭据; 脱敏依据是结构化字段名.
from __future__ import annotations

import dataclasses
import json
import math

from collections.abc import Mapping
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any


_SECRET_PARTS = (
    "api_key",
    "apikey",
    "access_key",
    "password",
    "secret",
    "authorization",
    "cookie",
    "credential",
    "token",
)
_VECTOR_KEYS = {"embedding", "embeddings", "vector", "vectors"}
_ARGUMENT_KEYS = {"argument", "arguments"}


# 统一字段大小写和连接符后做子串匹配; 含 token 等敏感词的字段会整体脱敏.
def _is_secret_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return any(part in normalized for part in _SECRET_PARTS)


# 按字符数保留前缀并追加省略号; 限制作用于每个字符串, 不是整张图的总字节数.
def _truncate(value: str, max_value_chars: int) -> str:
    if len(value) <= max_value_chars:
        return value
    return value[:max_value_chars] + "…"


# 递归处理字典、集合、dataclass、Pydantic 模型和普通对象.
# max_items 限制每层容器的输出项数; _seen 检测当前递归路径上的循环引用.
def to_jsonable(
    value: Any,
    *,
    max_value_chars: int = 20_000,
    max_items: int = 200,
    _seen: set[int] | None = None,
) -> Any:
    """Create a bounded JSON-compatible snapshot without secrets or vectors."""

    if value is None or isinstance(value, bool | int):
        return value
    # JSON 不支持标准的 NaN/Infinity 数字表示, 因此把非有限浮点数转为字符串.
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, str):
        return _truncate(value, max_value_chars)
    if isinstance(value, bytes):
        return f"<bytes:{len(value)}>"
    if isinstance(value, date | datetime | Path | Enum):
        return str(getattr(value, "value", value))

    seen = _seen if _seen is not None else set()
    value_id = id(value)
    if value_id in seen:
        return "<cycle>"
    seen.add(value_id)

    try:
        # 先把模型展开为字段映射, 再统一递归处理, 让嵌套字段也经过脱敏.
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            value = {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
        elif hasattr(value, "model_dump"):
            try:
                value = value.model_dump(mode="json")
            except TypeError:
                value = value.model_dump()

        if isinstance(value, Mapping):
            result: dict[str, Any] = {}
            for index, (raw_key, item) in enumerate(value.items()):
                if index >= max_items:
                    result["<truncated>"] = f"{len(value) - max_items} more items"
                    break
                key = str(raw_key)
                if _is_secret_key(key):
                    result[key] = "<redacted>"
                # 向量只保留长度提示, 避免高维数组占用追踪队列和图文件.
                elif key.lower() in _VECTOR_KEYS:
                    length = len(item) if hasattr(item, "__len__") else "unknown"
                    result[key] = f"<omitted-vector:{length}>"
                # 工具调用的 arguments 可能是内嵌 JSON 字符串; 必须先解析和脱敏, 再截断.
                # 无法解析时省略整段参数, 避免保留无法检查敏感字段的原文.
                elif key.lower() in _ARGUMENT_KEYS and isinstance(item, str):
                    # OpenAI tool calls carry a JSON string, which must be decoded
                    # before key-based redaction. Incomplete arguments are not safe
                    # to retain because their secret fields cannot be inspected.
                    try:
                        arguments = json.loads(item)
                    except (ValueError, RecursionError):
                        arguments = None
                    if isinstance(arguments, dict | list):
                        sanitized = to_jsonable(
                            arguments,
                            max_value_chars=max_value_chars,
                            max_items=max_items,
                            _seen=seen,
                        )
                        result[key] = _truncate(
                            json.dumps(sanitized, ensure_ascii=False), max_value_chars
                        )
                    else:
                        result[key] = "<omitted-unparseable-arguments>"
                else:
                    result[key] = to_jsonable(
                        item,
                        max_value_chars=max_value_chars,
                        max_items=max_items,
                        _seen=seen,
                    )
            return result

        if isinstance(value, list | tuple | set | frozenset):
            items = list(value)
            result = [
                to_jsonable(
                    item,
                    max_value_chars=max_value_chars,
                    max_items=max_items,
                    _seen=seen,
                )
                for item in items[:max_items]
            ]
            if len(items) > max_items:
                result.append(f"<{len(items) - max_items} more items>")
            return result

        # 未知对象优先读取属性字典; 没有属性字典时使用截断后的 repr 作为兜底表示.
        if hasattr(value, "__dict__"):
            return to_jsonable(
                vars(value),
                max_value_chars=max_value_chars,
                max_items=max_items,
                _seen=seen,
            )
        return _truncate(repr(value), max_value_chars)
    finally:
        # 退出当前分支就释放标记, 因此兄弟分支共享同一个对象不会被误判为循环.
        seen.discard(value_id)
