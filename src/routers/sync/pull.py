"""GET /sync/pull —— mobile / web 按 cursor 拉取 SyncChange。

用于 mobile 增量同步 + web 的 WebSocket 推送掉线后的 catch-up。

user-global 重构后:一条 pull 同时返回 ledger-scope + user-scope changes。
user-scope change 在响应里 ledger_id = sentinel '__user_global__',scope='user'。
mobile 按 scope 决定 apply 路径(写主表),不再借车依附任何 ledger。
"""
from __future__ import annotations

import time

from sqlalchemy import and_, or_

from ...config import get_settings
from ._shared import *  # noqa: F401,F403 — 拉取所有 imports / helpers / router / constants


# user-scope change 在 pull 响应里的 ledger_id 用这个 sentinel 标识。mobile 端
# 用同一字符串当 sync_cursors 的 ledger_external_id key,实现独立 cursor 跟踪。
USER_GLOBAL_LEDGER_SENTINEL = "__user_global__"


# /sync/pull?types= 接受的 entity_type 白名单。mobile 后续会用来分阶段拉:
# 阶段 1 拉元数据 `?types=ledger,ledger_snapshot,account,category,tag,budget`
# (快,N 条几十),UI 立刻渲染骨架;阶段 2 拉 `?types=transaction`(慢,几千)
# 在后台慢慢填。两阶段共享同一 since cursor — server 各自过滤,client 全部
# apply 完再 commit cursor。
#
# 不传 types = 全拉(向后兼容)。
_VALID_ENTITY_TYPES = frozenset(
    {"ledger", "ledger_snapshot", "account", "category", "tag", "budget", "transaction"}
)


def _parse_types_param(types_raw: str | None) -> set[str] | None:
    """解析 `?types=a,b,c`,返回过滤集合;None 表示不过滤。

    - unknown type 静默丢弃(防御性,避免 client typo 导致整个 pull 报 400)
    - 空字符串 / 只有逗号 → None(等同不传)
    """
    if not types_raw:
        return None
    items = {t.strip().lower() for t in types_raw.split(",")}
    items = {t for t in items if t in _VALID_ENTITY_TYPES}
    return items or None


@router.get("/pull", response_model=SyncPullResponse)
def pull_changes(
    since: int = Query(default=0, ge=0),
    device_id: str | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=5000),
    types: str | None = Query(
        default=None,
        description="逗号分隔的 entity_type 过滤,不传 = 全拉。例如 `transaction` 或 `ledger,account,category,tag,budget`。",
    ),
    _scopes: set[str] = Depends(require_any_scopes(SCOPE_APP_WRITE, SCOPE_WEB_READ)),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> SyncPullResponse:
    metrics.inc("beecount_sync_pull_requests_total")
    request_start = time.perf_counter()
    enrich_count = 0  # _enrich_tx_payloads_with_user_ids 实际兜底补的 tx 数,日志用
    types_filter = _parse_types_param(types)
    heartbeat_updated = False
    if device_id:
        device = db.scalar(
            select(Device).where(
                Device.id == device_id,
                Device.user_id == current_user.id,
                Device.revoked_at.is_(None),
            )
        )
        if not device:
            metrics.inc("beecount_sync_pull_failed_total")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid device")
        device.last_seen_at = datetime.now(timezone.utc)
        heartbeat_updated = True

    accessible = list_accessible_ledgers(db, user_id=current_user.id)
    ledger_ids = [lg.id for lg in accessible]
    # 无任何 ledger 的用户仍可能有 user-scope changes(场景理论上不存在,但
    # 协议上允许),所以不在此处早返。

    # LEFT JOIN Ledger:user-scope change 的 ledger_id IS NULL,INNER JOIN
    # 会把这些行过滤掉。
    # 过滤:
    #   - ledger-scope(scope='ledger'):必须属于 caller 可见 ledger
    #   - user-scope(scope='user'):必须 user_id == caller
    # `column.in_([])` 在 SQLAlchemy 2.0+ 编译成 false 表达式,不会 crash;
    # 用户无任何 ledger 时 ledger-scope 子句自然过滤掉所有行。
    scope_filter = or_(
        and_(
            SyncChange.scope == "ledger",
            SyncChange.ledger_id.in_(ledger_ids),
        ),
        and_(
            SyncChange.scope == "user",
            SyncChange.user_id == current_user.id,
        ),
    )
    query = (
        select(SyncChange, Ledger.external_id)
        .outerjoin(Ledger, SyncChange.ledger_id == Ledger.id)
        .where(
            scope_filter,
            SyncChange.change_id > since,
        )
        .order_by(SyncChange.change_id.asc())
        .limit(limit + 1)
    )
    if device_id:
        query = query.where(SyncChange.updated_by_device_id != device_id)
    if types_filter is not None:
        query = query.where(SyncChange.entity_type.in_(types_filter))

    rows = db.execute(query).all()
    has_more = len(rows) > limit
    rows = rows[:limit]

    # §7 共享账本:对历史 transaction SyncChange 兜底补 user id —— push 端
    # 已经在写时注入,但 push 修复前的老 SyncChange.payload_json 没有这俩字
    # 段,mobile pull 拿到 null。这里从 read_tx_projection(server 端那张表
    # 数据 sync_applier 历史就一直写对)按 (ledger_id, sync_id) 批量回填,
    # 单次查询覆盖整批,开销可忽略。enrichment 返新列表,不动原 ORM 对象。
    rows, enrich_count = _enrich_tx_payloads_with_user_ids(db, rows)

    changes: list[SyncChangeOut] = []
    server_cursor = since
    per_ledger_cursor: dict[str, int] = {}

    for change, ledger_external_id in rows:
        server_cursor = max(server_cursor, change.change_id)
        # user-scope change 的 ledger_id 字段填 sentinel,让 mobile 把它当独立
        # 频道跟踪 cursor。
        out_ledger_id = (
            USER_GLOBAL_LEDGER_SENTINEL
            if change.scope == "user"
            else (ledger_external_id or "")
        )
        current_cursor = per_ledger_cursor.get(out_ledger_id, 0)
        per_ledger_cursor[out_ledger_id] = max(current_cursor, change.change_id)
        changes.append(
            SyncChangeOut(
                change_id=change.change_id,
                ledger_id=out_ledger_id,
                entity_type=change.entity_type,
                entity_sync_id=change.entity_sync_id,
                action=cast("Any", change.action),
                payload=change.payload_json,
                updated_at=change.updated_at,
                updated_by_device_id=change.updated_by_device_id,
                scope=change.scope,
            )
        )

    if device_id and per_ledger_cursor:
        now = datetime.now(timezone.utc)
        for ledger_external_id, last_cursor in per_ledger_cursor.items():
            existing = db.scalar(
                select(SyncCursor).where(
                    SyncCursor.user_id == current_user.id,
                    SyncCursor.device_id == device_id,
                    SyncCursor.ledger_external_id == ledger_external_id,
                )
            )
            if existing:
                existing.last_cursor = max(existing.last_cursor, last_cursor)
                existing.updated_at = now
            else:
                db.add(
                    SyncCursor(
                        user_id=current_user.id,
                        device_id=device_id,
                        ledger_external_id=ledger_external_id,
                        last_cursor=last_cursor,
                        updated_at=now,
                    )
                )
        db.commit()
    elif heartbeat_updated:
        db.commit()

    elapsed = time.perf_counter() - request_start
    metrics.observe_histogram("beecount_sync_pull_duration_seconds", elapsed)
    elapsed_ms = int(elapsed * 1000)
    # structured log:运维 grep `sync.pull.return` 看 user/device/慢请求等。
    # admin 日志面板对 `event=sync.pull.return AND elapsed_ms > 500` 做快速 filter。
    # 始终输出(不论 changes 是否为空),便于追踪空 pull 也能看到。
    types_label = ",".join(sorted(types_filter)) if types_filter else "*"
    logger.info(
        "sync.pull.return user=%s device=%s since=%d types=%s returned=%d hasMore=%s "
        "elapsed_ms=%d accessible_ledgers=%d enrich_count=%d",
        current_user.id,
        device_id,
        since,
        types_label,
        len(changes),
        has_more,
        elapsed_ms,
        len(ledger_ids),
        enrich_count,
    )
    if elapsed_ms > 500:
        # 慢请求 warn 级别 —— 让 admin 面板默认 ERROR/WARN filter 也能捞出来
        logger.warning(
            "sync.pull.slow user=%s device=%s since=%d returned=%d elapsed_ms=%d",
            current_user.id,
            device_id,
            since,
            len(changes),
            elapsed_ms,
        )
    return SyncPullResponse(changes=changes, server_cursor=server_cursor, has_more=has_more)


def _enrich_tx_payloads_with_user_ids(
    db, rows: list
) -> tuple[list, int]:
    """对返给客户端的 SyncChange 行中,entity_type='transaction' 且 payload
    缺 createdByUserId / updatedByUserId 的,从 read_tx_projection 批量补上。

    push.py 已在写时注入这俩字段;此 helper 是兜底,覆盖 push 修复前留下的
    历史 SyncChange.payload_json 缺失的情况。

    **优化**:对 `change_id > settings.sync_enrich_max_change_id` 的行直接跳过
    兜底 — 那些是新数据,push.py 修好后写的肯定完整。0(默认)= 不跳过,
    跟原行为兼容。新部署或迁移完成后把这个值设到部署点的 max(change_id),
    一次性把每页 ~30ms 的兜底开销省掉。

    防御性 copy:不修改原 ORM 对象的 payload_json 引用(避免 MutableDict
    切换或意外 db.commit 把 enrichment 写回 DB)。返回 (rows_list, enriched_count)。
    `enriched_count` 给上层日志用,衡量历史脏数据残留规模。
    """
    if not rows:
        return rows, 0
    enrich_cap = get_settings().sync_enrich_max_change_id
    # 1. 收集 (ledger_id, sync_id, idx) 待补行
    pending: list[tuple[str, str, int]] = []
    for idx, (change, _external_id) in enumerate(rows):
        if change.entity_type != "transaction":
            continue
        # 跳过阈值之后的(新数据,push.py 修好后写的已经完整)
        if enrich_cap > 0 and change.change_id > enrich_cap:
            continue
        payload = change.payload_json
        if not isinstance(payload, dict):
            continue
        if payload.get("createdByUserId") and payload.get("updatedByUserId"):
            continue
        if change.ledger_id is None:
            continue
        pending.append((change.ledger_id, change.entity_sync_id, idx))
    if not pending:
        return rows, 0

    # 2. 批量查 projection — 用 (ledger_id, sync_id) 复合 filter 避免 cross-ledger
    # 同名 sync_id 互相串数据。SQLAlchemy 没有原生 (col1, col2) IN VALUES,
    # 用 sync_id IN (...) + ledger_id IN (...) 缩范围,Python 端再按精确
    # (lid, sid) 复合 key 索引。
    sync_ids = list({sid for _lid, sid, _idx in pending})
    ledger_ids = list({lid for lid, _sid, _idx in pending})
    rows_proj = db.execute(
        select(
            ReadTxProjection.ledger_id,
            ReadTxProjection.sync_id,
            ReadTxProjection.created_by_user_id,
            ReadTxProjection.last_edited_by_user_id,
        ).where(
            ReadTxProjection.sync_id.in_(sync_ids),
            ReadTxProjection.ledger_id.in_(ledger_ids),
        )
    ).all()
    proj_by_key = {
        (lid, sid): (cb, eb) for lid, sid, cb, eb in rows_proj
    }

    # 3. 防御 copy:命中 enrichment 才克隆该行 payload,其它行保留原引用
    enriched_rows = list(rows)
    enriched_count = 0
    for ledger_id, sync_id, idx in pending:
        entry = proj_by_key.get((ledger_id, sync_id))
        if entry is None:
            continue
        cb, eb = entry
        change, ext_id = enriched_rows[idx]
        payload_copy = dict(change.payload_json)
        modified = False
        if cb and not payload_copy.get("createdByUserId"):
            payload_copy["createdByUserId"] = cb
            modified = True
        if eb and not payload_copy.get("updatedByUserId"):
            payload_copy["updatedByUserId"] = eb
            modified = True
        if modified:
            enriched_count += 1
        # 用 wrapper 暴露 payload_override,序列化阶段从这里取
        enriched_rows[idx] = (_ChangeWithOverride(change, payload_copy), ext_id)
    return enriched_rows, enriched_count


class _ChangeWithOverride:
    """轻量代理:把 payload_json 改成 override,其它属性透传原 SyncChange。"""

    __slots__ = ("_change", "payload_json")

    def __init__(self, change, payload_override: dict) -> None:
        self._change = change
        self.payload_json = payload_override

    def __getattr__(self, name: str):
        return getattr(self._change, name)


