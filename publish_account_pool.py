"""Persistent author account configuration for article publishing."""

from __future__ import annotations

import json
import os
import re
import threading
from pathlib import Path
from typing import Any, Iterable


DEFAULT_PUBLISH_ACCOUNTS = (
    {"user_id": 2318106, "user_name": "巴基诺夫斯基", "enabled": True},
    {"user_id": 13350610, "user_name": "羌笛芭乐", "enabled": True},
    {"user_id": 13550299, "user_name": "第一天来上班", "enabled": True},
    {"user_id": 3276, "user_name": "原始星球", "enabled": True},
    {"user_id": 13587436, "user_name": "戴明蜘蛛侠", "enabled": True},
    {"user_id": 19467373, "user_name": "外脚背奇迹", "enabled": True},
)
MAX_USER_ID = (1 << 63) - 1


class PublishAccountPoolError(ValueError):
    """Raised when the persistent account configuration is invalid."""


class PublishAccountPoolStorageError(PublishAccountPoolError):
    """Raised when the account configuration cannot be read or written."""


class PublishAccountPool:
    """Read and atomically maintain the local article author pool."""

    def __init__(
        self,
        path: Path,
        *,
        initial_accounts: Iterable[dict[str, Any]] = DEFAULT_PUBLISH_ACCOUNTS,
    ) -> None:
        self.path = path.expanduser().resolve()
        self._lock = threading.RLock()
        self._initial_accounts = [dict(account) for account in initial_accounts]
        self._initialization_error: PublishAccountPoolError | None = None
        self._initialized = False
        with self._lock:
            try:
                if not self.path.exists():
                    self._write(self._normalize_accounts(self._initial_accounts))
                else:
                    self._read()
                self._initialized = True
            except PublishAccountPoolError as exc:
                # Publishing may be unavailable, but a broken account file must
                # not prevent the Dashboard and GIF pipeline from starting.
                self._initialization_error = exc

    def list_accounts(self) -> list[dict[str, Any]]:
        with self._lock:
            try:
                if not self.path.exists():
                    if self._initialized:
                        raise PublishAccountPoolStorageError(
                            "发布账号池文件不存在，请在页面保存账号池后再发布"
                        )
                    self._write(self._normalize_accounts(self._initial_accounts))
                accounts = self._read()
            except PublishAccountPoolError as exc:
                self._initialization_error = exc
                raise
            self._initialization_error = None
            self._initialized = True
            return [dict(account) for account in accounts]

    def active_accounts(self) -> list[dict[str, Any]]:
        return [account for account in self.list_accounts() if account["enabled"]]

    def replace_accounts(
        self, accounts: Iterable[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        normalized = self._normalize_accounts(accounts)
        with self._lock:
            self._write(normalized)
            self._initialization_error = None
            self._initialized = True
        return [dict(account) for account in normalized]

    def status(self) -> dict[str, Any]:
        accounts = self.list_accounts()
        return {
            "enabled": True,
            "available": True,
            "path": str(self.path),
            "count": len(accounts),
            "active_count": sum(account["enabled"] for account in accounts),
            "accounts": accounts,
        }

    def _read(self) -> list[dict[str, Any]]:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise PublishAccountPoolStorageError(
                f"无法读取发布账号池：{exc}"
            ) from exc
        except json.JSONDecodeError as exc:
            raise PublishAccountPoolError(
                f"发布账号池不是有效 JSON：第 {exc.lineno} 行第 {exc.colno} 列"
            ) from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("accounts"), list):
            raise PublishAccountPoolError(
                "发布账号池必须包含 accounts 数组"
            )
        return self._normalize_accounts(payload["accounts"])

    def _write(self, accounts: list[dict[str, Any]]) -> None:
        temporary = self.path.with_name(
            f".{self.path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        body = json.dumps(
            {"version": 1, "accounts": accounts},
            ensure_ascii=False,
            indent=2,
        ) + "\n"
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary.write_text(body, encoding="utf-8")
            os.chmod(temporary, 0o644)
            os.replace(temporary, self.path)
        except OSError as exc:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise PublishAccountPoolStorageError(
                f"无法保存发布账号池：{exc}"
            ) from exc

    @staticmethod
    def _normalize_accounts(
        accounts: Iterable[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if isinstance(accounts, (str, bytes, dict)):
            raise PublishAccountPoolError("发布账号必须是数组")
        normalized: list[dict[str, Any]] = []
        seen_user_ids: set[int] = set()
        try:
            values = list(accounts)
        except TypeError as exc:
            raise PublishAccountPoolError("发布账号必须是数组") from exc
        for index, value in enumerate(values, start=1):
            if not isinstance(value, dict):
                raise PublishAccountPoolError(f"第 {index} 个发布账号格式不正确")
            user_id_text = str(value.get("user_id") or "").strip()
            if not re.fullmatch(r"[1-9]\d{0,19}", user_id_text):
                raise PublishAccountPoolError(
                    f"第 {index} 个发布账号的 user_id 必须是正整数"
                )
            user_id = int(user_id_text)
            if user_id > MAX_USER_ID:
                raise PublishAccountPoolError(
                    f"第 {index} 个发布账号的 user_id 超出支持范围"
                )
            if user_id in seen_user_ids:
                raise PublishAccountPoolError(f"user_id {user_id} 重复")
            user_name = str(value.get("user_name") or "").strip()
            if not user_name:
                raise PublishAccountPoolError(
                    f"第 {index} 个发布账号缺少 user_name"
                )
            if len(user_name) > 64:
                raise PublishAccountPoolError(
                    f"第 {index} 个发布账号的 user_name 不能超过 64 个字符"
                )
            enabled = value.get("enabled", True)
            if not isinstance(enabled, bool):
                raise PublishAccountPoolError(
                    f"第 {index} 个发布账号的 enabled 必须是 true 或 false"
                )
            seen_user_ids.add(user_id)
            normalized.append(
                {
                    "user_id": user_id,
                    "user_name": user_name,
                    "enabled": enabled,
                }
            )
        return normalized
