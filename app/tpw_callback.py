"""
第三方个人微信 JSON 回调：溯源、幂等、文本消息走 Coze + Gewe 发文字。
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

from fastapi import BackgroundTasks

from call_coze_api import async_call_coze_workflow, create_conversation_cozeAPI
from config import LOGGER
from gewe_client import post_text
from tpw_db import (
    get_or_create_device_account,
    get_or_create_end_user,
    get_or_create_tpw_conversation,
    renew_tpw_coze_conversation_id,
    save_callback_raw,
    try_insert_chat_message,
    update_chat_message_status,
    upsert_contact_profile_modcontacts,
)


def _nested_string(blob: Any, key: str) -> str:
    if not isinstance(blob, dict):
        return ""
    node = blob.get(key)
    if isinstance(node, dict):
        s = node.get("string")
        return str(s) if s is not None else ""
    if isinstance(node, str):
        return node
    return ""


def _tpw_sync_ingest(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    同步：写 raw、ModContacts、尝试排队文本消息。
    返回 {"action": "done"} | {"action": "duplicate"} | {"action": "queue", "task": {...}}
    """
    raw_id = save_callback_raw(data)
    type_name = (data.get("TypeName") or data.get("typeName") or "").strip()

    if type_name == "ModContacts":
        upsert_contact_profile_modcontacts(data.get("Data") or {})
        return {"action": "done"}

    if type_name != "AddMsg":
        return {"action": "done"}

    appid = data.get("Appid") or data.get("appid")
    owner_wxid = data.get("Wxid") or data.get("wxid")
    if not appid or not owner_wxid:
        LOGGER.warning("tpw AddMsg 缺少 Appid 或 Wxid")
        return {"action": "done"}
    appid = str(appid)
    owner_wxid = str(owner_wxid)

    d = data.get("Data") or {}
    from_wxid = _nested_string(d, "FromUserName")
    to_wxid = _nested_string(d, "ToUserName")
    msg_type = d.get("MsgType")
    new_msg_id = d.get("NewMsgId")

    if new_msg_id is None:
        LOGGER.debug("tpw AddMsg 无 NewMsgId，跳过幂等与消息表")
        return {"action": "done"}

    dedup_key = f"{appid}:{new_msg_id}"
    if from_wxid == owner_wxid:
        return {"action": "done"}

    if msg_type != 1:
        return {"action": "done"}

    content_node = d.get("Content") or {}
    text = ""
    if isinstance(content_node, dict):
        raw_s = content_node.get("string")
        text = str(raw_s).strip() if raw_s is not None else ""
    if not text:
        LOGGER.info("tpw 文本消息正文为空，跳过 Coze")
        return {"action": "done"}

    device_account_id, _ = get_or_create_device_account(appid, owner_wxid)
    end_user_id, internal_user_id, _ = get_or_create_end_user(from_wxid)

    coze_conversation_id, _ = get_or_create_tpw_conversation(
        end_user_id,
        device_account_id,
        internal_user_id,
        create_coze_conversation_fn=lambda uid, kf: create_conversation_cozeAPI(uid, kf),
    )

    msg_id_val = d.get("MsgId")
    msg_seq_val = d.get("MsgSeq")
    try:
        new_msg_int = int(new_msg_id)
    except (TypeError, ValueError):
        new_msg_int = None
    try:
        msg_id_int = int(msg_id_val) if msg_id_val is not None else None
    except (TypeError, ValueError):
        msg_id_int = None
    try:
        msg_seq_int = int(msg_seq_val) if msg_seq_val is not None else None
    except (TypeError, ValueError):
        msg_seq_int = None

    ct = d.get("CreateTime")
    try:
        client_ct = int(ct) if ct is not None else None
    except (TypeError, ValueError):
        client_ct = None

    inserted, chat_row_id = try_insert_chat_message(
        dedup_key=dedup_key,
        callback_raw_id=raw_id,
        conversation_id=coze_conversation_id,
        device_account_id=device_account_id,
        end_user_id=end_user_id,
        msg_id=msg_id_int,
        new_msg_id=new_msg_int,
        msg_seq=msg_seq_int,
        msg_type=int(msg_type),
        is_from_owner=False,
        from_wxid=from_wxid,
        to_wxid=to_wxid,
        content_raw=text,
        content_text=text,
        push_content=(str(d.get("PushContent")) if d.get("PushContent") is not None else None),
        msg_source=d.get("MsgSource") if isinstance(d.get("MsgSource"), str) else None,
        client_create_time=client_ct,
        ingest_status="queued",
    )
    if not inserted or not chat_row_id:
        return {"action": "duplicate"}

    return {
        "action": "queue",
        "task": {
            "message_id": chat_row_id,
            "appid": appid,
            "peer_wxid": from_wxid,
            "text": text,
            "internal_user_id": internal_user_id,
            "coze_conversation_id": coze_conversation_id,
            "end_user_id": end_user_id,
            "device_account_id": device_account_id,
        },
    }


async def _tpw_pipeline_async(task: Dict[str, Any]) -> None:
    msg_row_id = task["message_id"]

    def on_renew(new_cid: str) -> None:
        renew_tpw_coze_conversation_id(
            task["end_user_id"],
            task["device_account_id"],
            new_cid,
        )

    try:
        reply = await async_call_coze_workflow(
            task["internal_user_id"],
            task["coze_conversation_id"],
            task["text"],
            None,
            persist_legacy_message=False,
            on_coze_conversation_renewed=on_renew,
        )
    except Exception:
        LOGGER.exception("tpw Coze 调用异常 message_id=%s", msg_row_id)
        update_chat_message_status(msg_row_id, "failed")
        return

    if not reply or not str(reply).strip():
        reply = "抱歉，暂时无法回复。"

    try:
        r = await post_text(app_id=task["appid"], to_wxid=task["peer_wxid"], content=str(reply).strip())
        ret = r.get("ret")
        if ret != 200:
            LOGGER.warning("tpw Gewe 发送未成功 message_id=%s ret=%s body=%s", msg_row_id, ret, r)
            update_chat_message_status(msg_row_id, "failed")
        else:
            update_chat_message_status(msg_row_id, "coze_done")
    except Exception:
        LOGGER.exception("tpw Gewe 发送异常 message_id=%s", msg_row_id)
        update_chat_message_status(msg_row_id, "failed")


async def handle_personal_wechat_payload(data: Dict[str, Any], background_tasks: BackgroundTasks) -> None:
    outcome = await asyncio.to_thread(_tpw_sync_ingest, data)
    if outcome.get("action") == "queue" and outcome.get("task"):
        background_tasks.add_task(_tpw_pipeline_async, outcome["task"])
