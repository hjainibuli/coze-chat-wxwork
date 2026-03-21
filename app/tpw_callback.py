"""
第三方个人微信 JSON 回调：溯源、幂等；文本与媒体（图片/语音/视频/文件/emoji）异步走 Coze + Gewe。
媒体：调 Gewe 下载接口拿到临时 fileUrl，直接将该链接作为用户消息传给 Coze（不落盘）。
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

from fastapi import BackgroundTasks

from call_coze_api import async_call_coze_workflow, create_conversation_cozeAPI
from config import LOGGER
from gewe_client import (
    download_cdn_temp_url,
    download_emoji_temp_url,
    download_file_temp_url,
    download_image_temp_url_try_types,
    download_video_temp_url,
    download_voice_temp_url,
    post_text,
)
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
from tpw_xml import (
    content_xml_string,
    parse_appmsg_file_cdn,
    parse_appmsg_type,
    parse_emoji_md5,
    wrap_xml_if_needed,
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


def _int_field(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _tpw_sync_ingest(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    同步：写 raw、ModContacts、尝试排队可处理消息。
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

    if msg_type is None:
        return {"action": "done"}
    try:
        msg_type_int = int(msg_type)
    except (TypeError, ValueError):
        return {"action": "done"}

    xml_full = content_xml_string(d)
    push_content = str(d.get("PushContent")) if d.get("PushContent") is not None else None

    media_task: Optional[Dict[str, Any]] = None
    content_text_for_db: str
    content_raw_for_db: str = xml_full or ""

    if msg_type_int == 1:
        text = ""
        cnode = d.get("Content") or {}
        if isinstance(cnode, dict):
            raw_s = cnode.get("string")
            text = str(raw_s).strip() if raw_s is not None else ""
        if not text:
            LOGGER.info("tpw 文本消息正文为空，跳过 Coze")
            return {"action": "done"}
        content_text_for_db = text
        content_raw_for_db = text
        media_task = None
    elif msg_type_int == 3:
        if not xml_full:
            LOGGER.warning("tpw 图片消息缺少 Content.xml")
            return {"action": "done"}
        content_text_for_db = push_content or "[图片]"
        media_task = {
            "mode": "media",
            "media_kind": "image",
            "xml": wrap_xml_if_needed(xml_full),
        }
    elif msg_type_int == 34:
        if not xml_full:
            LOGGER.warning("tpw 语音消息缺少 Content.xml")
            return {"action": "done"}
        mid = _int_field(d.get("MsgId"))
        if mid is None:
            LOGGER.warning("tpw 语音消息缺少 MsgId")
            return {"action": "done"}
        content_text_for_db = push_content or "[语音]"
        media_task = {
            "mode": "media",
            "media_kind": "voice",
            "xml": wrap_xml_if_needed(xml_full),
            "msg_id": mid,
        }
    elif msg_type_int == 43:
        if not xml_full:
            LOGGER.warning("tpw 视频消息缺少 Content.xml")
            return {"action": "done"}
        content_text_for_db = push_content or "[视频]"
        media_task = {
            "mode": "media",
            "media_kind": "video",
            "xml": wrap_xml_if_needed(xml_full),
        }
    elif msg_type_int == 47:
        md5 = parse_emoji_md5(xml_full)
        if not md5:
            LOGGER.warning("tpw emoji 无法解析 md5")
            return {"action": "done"}
        content_text_for_db = push_content or "[动画表情]"
        media_task = {
            "mode": "media",
            "media_kind": "emoji",
            "emoji_md5": md5,
        }
    elif msg_type_int == 49:
        at = parse_appmsg_type(xml_full)
        if at == 74:
            LOGGER.info("tpw 文件上传中(type=74)，仅记录 raw，不排队")
            return {"action": "done"}
        if at == 6:
            fields = parse_appmsg_file_cdn(xml_full)
            if not fields:
                LOGGER.warning("tpw 文件消息(type=6) 解析 appattach 失败")
                return {"action": "done"}
            ext = (fields.get("fileext") or "bin").strip() or "bin"
            content_text_for_db = push_content or f"[文件].{ext}"
            media_task = {
                "mode": "media",
                "media_kind": "file_cdn",
                "aes_key": fields["aeskey"],
                "file_id": fields["cdnattachurl"],
                "total_size": fields["totallen"],
                "suffix": ext,
            }
        else:
            if not xml_full:
                return {"action": "done"}
            content_text_for_db = push_content or f"[应用消息 type={at}]"
            media_task = {
                "mode": "media",
                "media_kind": "file_xml",
                "xml": wrap_xml_if_needed(xml_full),
            }
    else:
        return {"action": "done"}

    device_account_id, _ = get_or_create_device_account(appid, owner_wxid)
    end_user_id, internal_user_id, _ = get_or_create_end_user(from_wxid)

    coze_conversation_id, _ = get_or_create_tpw_conversation(
        end_user_id,
        device_account_id,
        internal_user_id,
        create_coze_conversation_fn=lambda uid, kf: create_conversation_cozeAPI(uid, kf),
    )

    new_msg_int = _int_field(new_msg_id)
    msg_id_int = _int_field(d.get("MsgId"))
    msg_seq_int = _int_field(d.get("MsgSeq"))
    client_ct = _int_field(d.get("CreateTime"))

    inserted, chat_row_id = try_insert_chat_message(
        dedup_key=dedup_key,
        callback_raw_id=raw_id,
        conversation_id=coze_conversation_id,
        device_account_id=device_account_id,
        end_user_id=end_user_id,
        msg_id=msg_id_int,
        new_msg_id=new_msg_int,
        msg_seq=msg_seq_int,
        msg_type=msg_type_int,
        is_from_owner=False,
        from_wxid=from_wxid,
        to_wxid=to_wxid,
        content_raw=content_raw_for_db if content_raw_for_db else None,
        content_text=content_text_for_db,
        push_content=push_content,
        msg_source=d.get("MsgSource") if isinstance(d.get("MsgSource"), str) else None,
        client_create_time=client_ct,
        ingest_status="queued",
    )
    if not inserted or not chat_row_id:
        return {"action": "duplicate"}

    base_task = {
        "message_id": chat_row_id,
        "appid": appid,
        "peer_wxid": from_wxid,
        "internal_user_id": internal_user_id,
        "coze_conversation_id": coze_conversation_id,
        "end_user_id": end_user_id,
        "device_account_id": device_account_id,
    }

    if media_task:
        base_task.update(media_task)
        return {"action": "queue", "task": base_task}

    base_task["mode"] = "text"
    base_task["text"] = content_text_for_db
    return {"action": "queue", "task": base_task}


async def _resolve_gewe_temp_url(task: Dict[str, Any]) -> Optional[str]:
    app_id = task["appid"]
    kind = task["media_kind"]
    if kind == "image":
        return await download_image_temp_url_try_types(app_id=app_id, xml=task["xml"])
    if kind == "voice":
        return await download_voice_temp_url(
            app_id=app_id, xml=task["xml"], msg_id=int(task["msg_id"])
        )
    if kind == "video":
        return await download_video_temp_url(app_id=app_id, xml=task["xml"])
    if kind == "emoji":
        return await download_emoji_temp_url(app_id=app_id, emoji_md5=task["emoji_md5"])
    if kind == "file_cdn":
        return await download_cdn_temp_url(
            app_id=app_id,
            aes_key=task["aes_key"],
            file_id=task["file_id"],
            type_="5",
            total_size=task["total_size"],
            suffix=task["suffix"],
        )
    if kind == "file_xml":
        return await download_file_temp_url(app_id=app_id, xml=task["xml"])
    return None


async def _tpw_pipeline_async(task: Dict[str, Any]) -> None:
    msg_row_id = task["message_id"]

    def on_renew(new_cid: str) -> None:
        renew_tpw_coze_conversation_id(
            task["end_user_id"],
            task["device_account_id"],
            new_cid,
        )

    coze_input: str
    if task.get("mode") == "text":
        coze_input = task["text"]
    else:
        coze_input = await _resolve_gewe_temp_url(task)
        if not coze_input:
            LOGGER.error("tpw Gewe 未拿到临时下载地址 message_id=%s kind=%s", msg_row_id, task.get("media_kind"))
            update_chat_message_status(msg_row_id, "failed")
            return

    try:
        reply = await async_call_coze_workflow(
            task["internal_user_id"],
            task["coze_conversation_id"],
            coze_input,
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


def is_tpw_payload_whitelisted(data: Dict[str, Any], allowed: frozenset[str]) -> bool:
    """
    空 allowed：不限制。
    非 AddMsg（如 ModContacts）：始终放行，避免联系人同步被挡。
    AddMsg：仅当发送方为「他人」且不在白名单时拒绝；自己发给自己仍交给下游按原逻辑丢弃。
    """
    if not allowed:
        return True
    type_name = (data.get("TypeName") or data.get("typeName") or "").strip()
    if type_name != "AddMsg":
        return True
    owner_wxid = data.get("Wxid") or data.get("wxid")
    if not owner_wxid:
        return True
    owner_wxid = str(owner_wxid)
    d = data.get("Data") or {}
    from_wxid = _nested_string(d, "FromUserName").strip()
    if not from_wxid or from_wxid == owner_wxid:
        return True
    return from_wxid in allowed


async def handle_personal_wechat_payload(data: Dict[str, Any], background_tasks: BackgroundTasks) -> None:
    outcome = await asyncio.to_thread(_tpw_sync_ingest, data)
    if outcome.get("action") == "queue" and outcome.get("task"):
        background_tasks.add_task(_tpw_pipeline_async, outcome["task"])
