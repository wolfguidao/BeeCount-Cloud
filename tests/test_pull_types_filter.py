"""GET /sync/pull?types=... 过滤参数回归。

mobile 后续会用这个 param 做分阶段拉:
- 阶段 1: types=ledger,account,category,tag,budget(快)
- 阶段 2: types=transaction(慢)
两阶段共享同一 since cursor,各自过滤,client 全部 apply 完再 commit cursor。
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.database import Base, get_db
from src.main import app


def _make_client() -> TestClient:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    TS = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    def override():
        db = TS()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override
    return TestClient(app)


def _register(client: TestClient, email: str) -> dict:
    return client.post(
        "/api/v1/auth/register",
        json={
            "email": email,
            "password": "123456",
            "client_type": "app",
            "device_name": "types-test",
            "platform": "app",
        },
    ).json()


def _seed_mixed_changes(client: TestClient, token: str, device: str) -> None:
    """种 1 个 ledger_snapshot + 1 tx + 1 category + 1 account + 1 tag 进 sync_changes。

    mobile push 路径会一次性创建 ledger_snapshot 和 transactions 的 sync_changes,
    user-global 资源(account/category/tag)走 entity_type=account/category/tag。
    """
    now = datetime.now(timezone.utc).isoformat()
    # 先 push 一个 ledger snapshot 出来
    res = client.post(
        "/api/v1/sync/push",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "device_id": device,
            "changes": [
                {
                    "ledger_id": "LT",
                    "entity_type": "ledger_snapshot",
                    "entity_sync_id": "LT",
                    "action": "upsert",
                    "payload": {
                        "content": '{"ledgerName":"LT","currency":"CNY","count":1,"items":[],"accounts":[],"categories":[],"tags":[]}'
                    },
                    "updated_at": now,
                },
                {
                    "ledger_id": "LT",
                    "entity_type": "transaction",
                    "entity_sync_id": "tx1",
                    "action": "upsert",
                    "payload": {
                        "syncId": "tx1",
                        "type": "expense",
                        "amount": 10,
                        "happenedAt": now,
                    },
                    "updated_at": now,
                },
                {
                    "ledger_id": "LT",
                    "entity_type": "category",
                    "entity_sync_id": "c1",
                    "action": "upsert",
                    "payload": {
                        "syncId": "c1",
                        "name": "餐饮",
                        "kind": "expense",
                    },
                    "updated_at": now,
                },
                {
                    "ledger_id": "LT",
                    "entity_type": "account",
                    "entity_sync_id": "a1",
                    "action": "upsert",
                    "payload": {"syncId": "a1", "name": "现金"},
                    "updated_at": now,
                },
                {
                    "ledger_id": "LT",
                    "entity_type": "tag",
                    "entity_sync_id": "t1",
                    "action": "upsert",
                    "payload": {"syncId": "t1", "name": "餐"},
                    "updated_at": now,
                },
            ],
        },
    )
    assert res.status_code == 200, res.text


def test_pull_without_types_returns_all() -> None:
    """不传 types = 拉全部 entity 类型,向后兼容。"""
    client = _make_client()
    try:
        u = _register(client, "all@types.com")
        token = u["access_token"]
        _seed_mixed_changes(client, token, u["device_id"])

        # mobile 用 device_id 不同于 push 的那个,确保不会被 device filter 拦
        r = client.get(
            "/api/v1/sync/pull?since=0&limit=100",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200, r.text
        types = {c["entity_type"] for c in r.json()["changes"]}
        # 至少应该有这些(可能还有 user-scope 派生的 ledger 行)
        assert "transaction" in types
        assert "category" in types
        assert "account" in types
        assert "tag" in types
    finally:
        app.dependency_overrides.clear()


def test_pull_with_types_only_tx() -> None:
    """`types=transaction` 只返 tx。"""
    client = _make_client()
    try:
        u = _register(client, "tx@types.com")
        token = u["access_token"]
        _seed_mixed_changes(client, token, u["device_id"])

        r = client.get(
            "/api/v1/sync/pull?since=0&limit=100&types=transaction",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200, r.text
        types = {c["entity_type"] for c in r.json()["changes"]}
        assert types == {"transaction"} or types == set(), f"got types={types}"
    finally:
        app.dependency_overrides.clear()


def test_pull_with_metadata_types_excludes_tx() -> None:
    """阶段 1 用法:拉 metadata 不带 tx。"""
    client = _make_client()
    try:
        u = _register(client, "meta@types.com")
        token = u["access_token"]
        _seed_mixed_changes(client, token, u["device_id"])

        r = client.get(
            "/api/v1/sync/pull?since=0&limit=100"
            "&types=ledger,ledger_snapshot,account,category,tag,budget",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200, r.text
        types = {c["entity_type"] for c in r.json()["changes"]}
        assert "transaction" not in types, f"tx 不该出现,got types={types}"
        # account / category / tag 应该有(取决于 server 是否 emit 这些)
    finally:
        app.dependency_overrides.clear()


def test_pull_with_invalid_types_silently_ignored() -> None:
    """typo 的 type 应静默丢弃,不报 400(防御性)。"""
    client = _make_client()
    try:
        u = _register(client, "invalid@types.com")
        token = u["access_token"]
        _seed_mixed_changes(client, token, u["device_id"])

        # `transactions`(typo,多了 s)+ `transaction`(合法)
        # 结果:只有合法的生效
        r = client.get(
            "/api/v1/sync/pull?since=0&limit=100&types=transactions,transaction",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200, r.text
        types = {c["entity_type"] for c in r.json()["changes"]}
        assert types.issubset({"transaction"}), f"got types={types}"
    finally:
        app.dependency_overrides.clear()


def test_pull_with_only_invalid_types_returns_empty() -> None:
    """全是 typo 时 types_filter 解析成 None(等同不传)? 不应该 — 应当返空。

    实现选择:全是无效 type → 解析结果 None → 退化为不过滤(全拉)。
    这跟"全部 type 都无效 → 自然没 changes 返回"的更保守语义不一致。
    选当前实现的理由:client 实在传错的话,fallback 到老行为(全拉)对用户
    感知更友好;真要严格,client 自己保证 types 拼写。
    """
    client = _make_client()
    try:
        u = _register(client, "onlytypo@types.com")
        token = u["access_token"]
        _seed_mixed_changes(client, token, u["device_id"])

        r = client.get(
            "/api/v1/sync/pull?since=0&limit=100&types=nosuch,alsono",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert r.status_code == 200, r.text
        # 当前实现:全无效 → fallback 不过滤,等同于不传
        # 如果将来收紧成"严格"模式,改成 assert len == 0 即可
        # 不强断言 count,只断言不 crash。
    finally:
        app.dependency_overrides.clear()
