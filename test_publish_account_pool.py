import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import dashboard_server
from open_platform_client import OpenPlatformError
from article_publisher import (
    ArticlePublishError,
    ArticlePublisher,
    PublishedGifStore,
    build_article_fields,
)
from publish_account_pool import (
    DEFAULT_PUBLISH_ACCOUNTS,
    MAX_USER_ID,
    PublishAccountPool,
    PublishAccountPoolError,
    PublishAccountPoolStorageError,
)
from article_draft_queue import _public_task


def animated_gif_bytes() -> bytes:
    header = (
        b"GIF89a"
        b"\x01\x00\x01\x00"
        b"\x80\x00\x00"
        b"\x00\x00\x00\xff\xff\xff"
    )
    frame = (
        b"\x2c"
        b"\x00\x00\x00\x00\x01\x00\x01\x00\x00"
        b"\x02\x02\x44\x01\x00"
    )
    return header + frame + frame + b"\x3b"


class SequencePlatformClient:
    def __init__(self, outcomes=None):
        self.outcomes = list(outcomes or [])
        self.calls = []

    def status(self):
        return {"configured": True, "authorized": True}

    def create_article(self, fields):
        self.calls.append(dict(fields))
        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        return {
            "article_id": str(fields.get("archive_id") or "3801234"),
            "duplicate": False,
            "code": 0,
            "message": "ok",
        }


class PublishAccountPoolTests(unittest.TestCase):
    def test_missing_file_is_initialized_with_six_requested_accounts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "publish_accounts.json"
            pool = PublishAccountPool(path)

            accounts = pool.list_accounts()

            self.assertTrue(path.is_file())
            self.assertEqual(accounts, list(DEFAULT_PUBLISH_ACCOUNTS))
            self.assertEqual(len(pool.active_accounts()), 6)

    def test_replacement_persists_enabled_state_and_supports_empty_pool(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "publish_accounts.json"
            pool = PublishAccountPool(path)
            expected = [
                {"user_id": 1001, "user_name": "账号甲", "enabled": False},
                {"user_id": 1002, "user_name": "账号乙", "enabled": True},
            ]

            pool.replace_accounts(expected)
            reopened = PublishAccountPool(path)

            self.assertEqual(reopened.list_accounts(), expected)
            self.assertEqual(reopened.active_accounts(), [expected[1]])
            self.assertEqual(reopened.replace_accounts([]), [])
            self.assertEqual(reopened.active_accounts(), [])

    def test_replacement_rejects_duplicate_or_invalid_accounts(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = PublishAccountPool(Path(directory) / "publish_accounts.json")
            invalid_sets = (
                [
                    {"user_id": 1001, "user_name": "账号甲", "enabled": True},
                    {"user_id": "1001", "user_name": "账号乙", "enabled": True},
                ],
                [{"user_id": "abc", "user_name": "账号甲", "enabled": True}],
                [{"user_id": 1001, "user_name": "", "enabled": True}],
                [{"user_id": 1001, "user_name": "账号甲", "enabled": 1}],
                [
                    {
                        "user_id": MAX_USER_ID + 1,
                        "user_name": "账号甲",
                        "enabled": True,
                    }
                ],
            )

            for accounts in invalid_sets:
                with self.subTest(accounts=accounts):
                    with self.assertRaises(PublishAccountPoolError):
                        pool.replace_accounts(accounts)

    def test_broken_file_does_not_break_construction_and_can_be_repaired(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "publish_accounts.json"
            path.write_text("{broken", encoding="utf-8")

            pool = PublishAccountPool(path)

            with self.assertRaises(PublishAccountPoolError):
                pool.list_accounts()
            repaired = pool.replace_accounts(
                [{"user_id": 1001, "user_name": "账号甲", "enabled": True}]
            )
            self.assertEqual(pool.list_accounts(), repaired)

    def test_deleted_file_does_not_restore_default_authors(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "publish_accounts.json"
            pool = PublishAccountPool(path)
            pool.replace_accounts(
                [{"user_id": 1001, "user_name": "自定义账号", "enabled": True}]
            )
            path.unlink()

            with self.assertRaises(PublishAccountPoolStorageError):
                pool.list_accounts()

    def test_storage_error_has_distinct_error_type(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = PublishAccountPool(Path(directory))

            with self.assertRaises(PublishAccountPoolStorageError):
                pool.list_accounts()


class PublishAccountIntegrationTests(unittest.TestCase):
    def make_publisher(self, root, platform, accounts):
        pool = PublishAccountPool(
            root / "publish_accounts.json",
            initial_accounts=accounts,
        )
        publisher = ArticlePublisher(
            platform_client=platform,
            gif_store=PublishedGifStore(
                root / "published",
                "https://matchgif.aisportsapp.com",
            ),
            database_path=root / "publish.sqlite3",
            public_url_checker=lambda _url: None,
            account_pool=pool,
        )
        return publisher, pool

    def write_source(self, root, name="event.gif"):
        source = root / name
        source.write_bytes(animated_gif_bytes())
        return source

    def event(self, event_key="goal-19"):
        return {
            "event_key": event_key,
            "status": "encoded",
            "code": "G",
            "minute": "19",
            "person": "球员甲",
            "score": "1-0",
        }

    def test_article_fields_include_assigned_user_id_and_name(self):
        fields = build_article_fields(
            match_id="54478914",
            event=self.event(),
            gif_url="https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            cover_url="https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
            title="自动标题",
            publish_account={"user_id": 2318106, "user_name": "巴基诺夫斯基"},
        )

        self.assertEqual(fields["user_id"], 2318106)
        self.assertEqual(fields["user_name"], "巴基诺夫斯基")

    def test_ocr_public_status_exposes_assigned_account(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            account = {"user_id": 1001, "user_name": "账号甲", "enabled": True}
            platform = SequencePlatformClient()
            publisher, _pool = self.make_publisher(root, platform, [account])
            source = self.write_source(root)

            result = publisher.create_or_update_article(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
                delivery_mode="publish",
            )
            public_task = _public_task({
                "article_id": result["article_id"],
                "attempt_count": 1,
                "diagnostics_json": json.dumps(result["diagnostics"]),
            })

            self.assertEqual(
                result["diagnostics"]["request_summary"]["user_id"],
                account["user_id"],
            )
            self.assertEqual(public_task["publish_account"], {
                "user_id": account["user_id"],
                "user_name": account["user_name"],
            })

    def test_account_selection_stays_balanced_while_randomizing_ties(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = [
                {"user_id": 1001, "user_name": "账号甲", "enabled": True},
                {"user_id": 1002, "user_name": "账号乙", "enabled": True},
                {"user_id": 1003, "user_name": "账号丙", "enabled": True},
            ]
            platform = SequencePlatformClient()
            publisher, _pool = self.make_publisher(root, platform, accounts)
            source = self.write_source(root)

            for index in range(9):
                publisher.create_or_update_article(
                    match_id="54478914",
                    event=self.event(f"goal-{index}"),
                    match_detail={},
                    source_path=source,
                    delivery_mode="publish",
                )

            counts = {account["user_id"]: 0 for account in accounts}
            for fields in platform.calls:
                counts[fields["user_id"]] += 1
            self.assertEqual(set(counts.values()), {3})

    def test_default_and_ocr_publication_both_use_account_pool(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            account = {"user_id": 1001, "user_name": "账号甲", "enabled": True}
            platform = SequencePlatformClient()
            publisher, _pool = self.make_publisher(root, platform, [account])
            default_source = self.write_source(root, "default.gif")
            ocr_source = self.write_source(root, "ocr.gif")

            default_result = publisher.publish(
                match_id="54478914",
                event=self.event("goal-default"),
                match_detail={},
                source_path=default_source,
                artifact_kind="default",
            )
            ocr_result = publisher.create_or_update_article(
                match_id="54478914",
                event=self.event("goal-ocr"),
                match_detail={},
                source_path=ocr_source,
                delivery_mode="publish",
            )

            self.assertEqual(len(platform.calls), 2)
            for fields in platform.calls:
                self.assertEqual(fields["user_id"], account["user_id"])
                self.assertEqual(fields["user_name"], account["user_name"])
            self.assertEqual(default_result["publish_account"], {
                "user_id": account["user_id"],
                "user_name": account["user_name"],
            })
            self.assertEqual(ocr_result["publish_account"], {
                "user_id": account["user_id"],
                "user_name": account["user_name"],
            })

    def test_default_publish_retry_keeps_original_account_after_pool_changes(self):
        temporary_error = OpenPlatformError(
            "平台暂时繁忙",
            code=50001,
            status_code=503,
            retriable=True,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            platform = SequencePlatformClient([temporary_error])
            publisher, pool = self.make_publisher(
                root,
                platform,
                [{"user_id": 1001, "user_name": "账号甲", "enabled": True}],
            )
            source = self.write_source(root)

            with self.assertRaises(ArticlePublishError):
                publisher.publish(
                    match_id="54478914",
                    event=self.event(),
                    match_detail={},
                    source_path=source,
                )
            pool.replace_accounts(
                [{"user_id": 1002, "user_name": "账号乙", "enabled": True}]
            )
            completed = publisher.publish(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )

            self.assertEqual([call["user_id"] for call in platform.calls], [1001, 1001])
            self.assertEqual(completed["publish_account"]["user_id"], 1001)

    def test_ocr_draft_update_and_publish_keep_original_account(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            platform = SequencePlatformClient()
            publisher, pool = self.make_publisher(
                root,
                platform,
                [{"user_id": 1001, "user_name": "账号甲", "enabled": True}],
            )
            source = self.write_source(root)
            event = self.event()

            created = publisher.create_or_update_draft(
                match_id="54478914",
                event=event,
                match_detail={},
                source_path=source,
            )
            pool.replace_accounts(
                [{"user_id": 1002, "user_name": "账号乙", "enabled": True}]
            )
            publisher.create_or_update_draft(
                match_id="54478914",
                event=event,
                match_detail={},
                source_path=source,
                archive_id=created["article_id"],
            )
            publisher.publish_draft(
                match_id="54478914",
                event=event,
                match_detail={},
                source_path=source,
                archive_id=created["article_id"],
            )

            self.assertEqual([call["user_id"] for call in platform.calls], [1001, 1001, 1001])
            self.assertEqual([call["user_name"] for call in platform.calls], [
                "账号甲", "账号甲", "账号甲"
            ])

    def test_empty_pool_blocks_default_and_ocr_without_platform_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            platform = SequencePlatformClient()
            publisher, _pool = self.make_publisher(root, platform, [])
            source = self.write_source(root)

            for operation in (
                lambda: publisher.publish(
                    match_id="54478914",
                    event=self.event("goal-default"),
                    match_detail={},
                    source_path=source,
                ),
                lambda: publisher.create_or_update_article(
                    match_id="54478914",
                    event=self.event("goal-ocr"),
                    match_detail={},
                    source_path=source,
                    delivery_mode="publish",
                ),
            ):
                with self.subTest(operation=operation):
                    with self.assertRaises(ArticlePublishError) as caught:
                        operation()
                    self.assertEqual(caught.exception.code, "publish_account_pool_empty")
                    self.assertEqual(caught.exception.stage, "account_selection")

            self.assertEqual(platform.calls, [])

    def test_broken_pool_blocks_publish_without_stopping_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            account_path = root / "publish_accounts.json"
            account_path.write_text("{broken", encoding="utf-8")
            platform = SequencePlatformClient()
            publisher = ArticlePublisher(
                platform_client=platform,
                gif_store=PublishedGifStore(
                    root / "published",
                    "https://matchgif.aisportsapp.com",
                ),
                database_path=root / "publish.sqlite3",
                public_url_checker=lambda _url: None,
                account_pool=PublishAccountPool(account_path),
            )
            source = self.write_source(root)

            status = publisher.status()
            with self.assertRaises(ArticlePublishError) as caught:
                publisher.publish(
                    match_id="54478914",
                    event=self.event(),
                    match_detail={},
                    source_path=source,
                )

            self.assertFalse(status["account_pool"]["available"])
            self.assertEqual(
                caught.exception.code, "publish_account_pool_unavailable"
            )
            self.assertEqual(platform.calls, [])


class PublishAccountDashboardTests(unittest.TestCase):
    def test_dashboard_can_list_and_replace_accounts(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = PublishAccountPool(Path(directory) / "publish_accounts.json")
            client = dashboard_server.app.test_client()
            with patch.object(
                dashboard_server.article_publisher, "account_pool", pool
            ):
                listed = client.get("/api/article-publish/accounts")
                saved = client.put(
                    "/api/article-publish/accounts",
                    json={
                        "accounts": [
                            {
                                "user_id": 1001,
                                "user_name": "账号甲",
                                "enabled": True,
                            },
                            {
                                "user_id": 1002,
                                "user_name": "账号乙",
                                "enabled": False,
                            },
                        ]
                    },
                )

            self.assertEqual(listed.status_code, 200)
            self.assertEqual(len(listed.get_json()["accounts"]), 6)
            self.assertEqual(saved.status_code, 200)
            self.assertEqual(saved.get_json()["available_count"], 1)
            self.assertEqual(pool.list_accounts()[1]["enabled"], False)

    def test_dashboard_distinguishes_invalid_data_from_storage_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "publish_accounts.json"
            pool = PublishAccountPool(path)
            client = dashboard_server.app.test_client()
            with patch.object(
                dashboard_server.article_publisher, "account_pool", pool
            ):
                invalid = client.put(
                    "/api/article-publish/accounts",
                    json={
                        "accounts": [
                            {"user_id": 0, "user_name": "账号甲", "enabled": True}
                        ]
                    },
                )
            path.unlink()
            path.mkdir()
            with patch.object(
                dashboard_server.article_publisher, "account_pool", pool
            ):
                unavailable = client.put(
                    "/api/article-publish/accounts",
                    json={
                        "accounts": [
                            {
                                "user_id": 1001,
                                "user_name": "账号甲",
                                "enabled": True,
                            }
                        ]
                    },
                )

            self.assertEqual(invalid.status_code, 400)
            self.assertEqual(
                invalid.get_json()["code"], "publish_account_pool_invalid"
            )
            self.assertEqual(unavailable.status_code, 503)
            self.assertEqual(
                unavailable.get_json()["code"],
                "publish_account_pool_unavailable",
            )


if __name__ == "__main__":
    unittest.main()
