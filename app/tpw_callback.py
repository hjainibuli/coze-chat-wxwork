"""
第三方个人微信 JSON 回调：溯源、幂等；文本与媒体（图片/语音/视频/文件/emoji）异步走 Coze + Gewe。
媒体：调 Gewe 下载接口拿到临时 fileUrl，直接将该链接作为用户消息传给 Coze（不落盘）。
"""
from __future__ import annotations

import asyncio
import json
import re
import secrets
import time
from typing import Any, Dict, List, Optional, Tuple

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
    post_image,
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

# Coze 回复拆成多条 Gewe 文本时的分隔符（后续若改用自定义标记，只改此处）
COZE_REPLY_MULTIMESSAGE_SPLIT = "\n"

# 定时唤醒构造 AddMsg 时默认 MsgSource（与真实回调示例一致）
TPW_SCHEDULED_WAKE_DEFAULT_MSG_SOURCE = (
    "<msgsource>\n"
    "\t<bizflag>0</bizflag>\n"
    "\t<pua>1</pua>\n"
    "\t<eggIncluded>1</eggIncluded>\n"
    "\t<signature>N0_V1_TriE8fd8|v1_py5beFkX</signature>\n"
    "\t<tmp_node>\n"
    "\t\t<publisher-id></publisher-id>\n"
    "\t</tmp_node>\n"
    "</msgsource>\n"
)


def _tpw_rand_int_exclusive(upper: int) -> int:
    """[1, upper) 均匀整数，用于 MsgId / MsgSeq。"""
    return secrets.randbelow(upper - 1) + 1 if upper > 1 else 1


def _tpw_rand_new_msg_id() -> int:
    """与真实回调同量级的大整数，落在 JSON / MySQL BIGINT 安全范围。"""
    return secrets.randbelow(9_000_000_000_000_000_000) + 1_000_000_000_000_000_000


def build_tpw_scheduled_wake_addmsg_payload(
    *,
    appid: str,
    wxid: str,
    from_user_name: str,
    to_user_name: str,
    content: str = "这是一条自动唤醒跟进的消息",
    push_content: Optional[str] = None,
    push_display_name: str = "你说后来",
    msg_source: Optional[str] = None,
) -> Dict[str, Any]:
    """
    构造与 Gewe 回调一致的 AddMsg JSON，供定时任务触发，走与 /personal/wechat/callback 相同的 Coze + Gewe 流程。
    MsgId / NewMsgId / MsgSeq 每次随机；CreateTime 为当前 Unix 秒。
    """
    text = (content or "").strip() or "这是一条自动唤醒跟进的消息"
    if push_content is None:
        pc = f"{push_display_name.strip() or '用户'} : {text}"
    else:
        pc = push_content
    ms = msg_source if msg_source is not None else TPW_SCHEDULED_WAKE_DEFAULT_MSG_SOURCE
    now = int(time.time())
    return {
        "TypeName": "AddMsg",
        "Appid": str(appid).strip(),
        "Data": {
            "MsgId": _tpw_rand_int_exclusive(2_000_000_000),
            "FromUserName": {"string": str(from_user_name).strip()},
            "ToUserName": {"string": str(to_user_name).strip()},
            "MsgType": 1,
            "Content": {"string": text},
            "Status": 3,
            "ImgStatus": 1,
            "ImgBuf": {"iLen": 0},
            "CreateTime": now,
            "MsgSource": ms,
            "PushContent": pc,
            "NewMsgId": _tpw_rand_new_msg_id(),
            "MsgSeq": _tpw_rand_int_exclusive(2_000_000_000),
        },
        "Wxid": str(wxid).strip(),
    }


def _tpw_wechat_nick_from_push_content(push: Any) -> str:
    """Data.PushContent 按英文分号分隔，trim 后取第一段（见 docs/回调消息详解.md）。"""
    if push is None:
        return ""
    s = str(push).strip()
    if not s:
        return ""
    return s.split(";", 1)[0].strip()


def _tpw_coze_reply_chunks(reply: Any) -> list[str]:
    """将 Coze 回复按 COZE_REPLY_MULTIMESSAGE_SPLIT 拆成待发送片段；整体为空则返回 []."""
    if reply is None:
        return []
    s = str(reply).strip()
    if not s:
        return []
    return [p.strip() for p in s.split(COZE_REPLY_MULTIMESSAGE_SPLIT) if p.strip()]


_IMG_SRC_RE = re.compile(r"""<img[^>]+src\s*=\s*(["'])(.*?)\1""", re.IGNORECASE | re.DOTALL)


def _tpw_img_urls_from_html(html: str) -> List[str]:
    """从 HTML 片段中提取 <img src="..."> 的 URL（支持双引号或单引号）。"""
    if not html or not html.strip():
        return []
    out: List[str] = []
    for m in _IMG_SRC_RE.finditer(html):
        u = (m.group(2) or "").strip()
        if u:
            out.append(u)
    return out


def _normalize_coze_message_newlines(s: str) -> str:
    """
    Coze 有时把换行以「反斜杠 + 字母 n」两个字面字符放进 message_list（双重转义），
    json.loads 后仍不是真正的换行；_tpw_coze_reply_chunks 按换行切条前需先还原。
    """
    if not s:
        return s
    return s.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")


def _coze_extract_message_list_text(v: Any, *, max_depth: int = 3) -> str:
    """
    兼容 message_list 的多种形态：
    - 直接是文本
    - 是 JSON 字符串：{"message_list":"..."}（甚至多层嵌套）
    - 是 dict：{"message_list": "..."}
    """
    cur: Any = v
    for _ in range(max_depth + 1):
        if cur is None:
            return ""
        if isinstance(cur, dict):
            if "message_list" in cur:
                cur = cur.get("message_list")
                continue
            return json.dumps(cur, ensure_ascii=False)
        if isinstance(cur, str):
            s = cur.strip()
            if not s:
                return ""
            if s[0] in "{[":
                try:
                    parsed = json.loads(s)
                except json.JSONDecodeError:
                    return s
                cur = parsed
                continue
            return s
        return str(cur).strip()
    return str(cur).strip()


def _parse_coze_workflow_json_reply(raw: Any) -> Tuple[str, List[str]]:
    """
    Coze 返回 JSON 字符串或 dict。reply 为文本；fileInfos 支持两种形式::

        1) 原生数组::
            "fileInfos": [{"documentId":"...", "output":"<img src=\\"https://...\\"> ..."}]

        2) 数组再序列化成字符串（当前 Coze 常见）::
            "fileInfos": "[{\\"documentId\\":\\"...\\",\\"output\\":\\"<img src=\\\\\\"https://...\\\\\\"> ...\\"}]"

    每项 output 中用 <img src="..."> / src='...' 抽取图片 URL。
    无法解析为最外层 JSON 时，整段视为旧版纯文本 reply。
    """
    if raw is None:
        return "", []
    if isinstance(raw, dict):
        obj = raw
    else:
        s = str(raw).strip()
        if not s:
            return "", []
        try:
            obj = json.loads(s)
        except json.JSONDecodeError:
            return s, []
        if not isinstance(obj, dict):
            return str(raw).strip(), []

    reply = obj.get("message_list")
    reply_text = _coze_extract_message_list_text(reply)
    reply_text = _normalize_coze_message_newlines(reply_text)

    urls: List[str] = []
    fi = obj.get("fileInfos")
    items: Optional[List[Any]] = None
    if fi is None:
        pass
    elif isinstance(fi, list):
        items = fi
    elif isinstance(fi, str) and fi.strip():
        try:
            parsed = json.loads(fi)
            if isinstance(parsed, list):
                items = parsed
            else:
                LOGGER.warning("tpw Coze fileInfos 字符串 json.loads 后非数组，已忽略附件")
        except json.JSONDecodeError:
            LOGGER.warning("tpw Coze fileInfos 字符串无法解析为 JSON 数组，已忽略附件")
    else:
        LOGGER.warning(
            "tpw Coze fileInfos 类型不支持（应为数组或 JSON 数组字符串），type=%s，已忽略附件",
            type(fi).__name__,
        )

    if items:
        for it in items:
            if not isinstance(it, dict):
                continue
            out = it.get("output")
            if out is not None:
                urls.extend(_tpw_img_urls_from_html(str(out)))

    seen: set[str] = set()
    uniq: List[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return reply_text, uniq


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
        "wechat_id": from_wxid,
        "wechat_nick_name": _tpw_wechat_nick_from_push_content(d.get("PushContent")),
        "to_wxid": to_wxid
    }

    if media_task:
        base_task.update(media_task)
        return {"action": "queue", "task": base_task}

    base_task["mode"] = "text"
    base_task["text"] = content_text_for_db
    return {"action": "queue", "task": base_task}


async def _post_text_with_retry(
    *, app_id: str, to_wxid: str, content: str, message_id: Any
) -> Dict[str, Any]:
    """Gewe postText：网络异常或 ret!=200 时指数退避重试，返回最后一次响应。"""
    max_attempts = 3
    base_delay_s = 0.8
    last: Dict[str, Any] = {}
    for attempt in range(max_attempts):
        try:
            last = await post_text(app_id=app_id, to_wxid=to_wxid, content=content)
            if last.get("ret") == 200:
                return last
            LOGGER.warning(
                "tpw Gewe postText 未成功将重试 message_id=%s attempt=%s/%s ret=%s body=%s",
                message_id,
                attempt + 1,
                max_attempts,
                last.get("ret"),
                last,
            )
        except Exception as e:
            LOGGER.warning(
                "tpw Gewe postText 异常将重试 message_id=%s attempt=%s/%s: %s",
                message_id,
                attempt + 1,
                max_attempts,
                e,
            )
        if attempt + 1 < max_attempts:
            await asyncio.sleep(base_delay_s * (2**attempt))
    return last


async def _post_image_with_retry(
    *, app_id: str, to_wxid: str, img_url: str, message_id: Any
) -> Dict[str, Any]:
    """Gewe postImage：网络异常或 ret!=200 时指数退避重试。"""
    max_attempts = 3
    base_delay_s = 0.8
    last: Dict[str, Any] = {}
    for attempt in range(max_attempts):
        try:
            last = await post_image(app_id=app_id, to_wxid=to_wxid, img_url=img_url)
            if last.get("ret") == 200:
                return last
            LOGGER.warning(
                "tpw Gewe postImage 未成功将重试 message_id=%s attempt=%s/%s ret=%s body=%s",
                message_id,
                attempt + 1,
                max_attempts,
                last.get("ret"),
                last,
            )
        except Exception as e:
            LOGGER.warning(
                "tpw Gewe postImage 异常将重试 message_id=%s attempt=%s/%s: %s",
                message_id,
                attempt + 1,
                max_attempts,
                e,
            )
        if attempt + 1 < max_attempts:
            await asyncio.sleep(base_delay_s * (2**attempt))
    return last


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
            wechat_id=task.get("wechat_id"),
            wechat_nick_name=task.get("wechat_nick_name"),
            trigger_type=task.get("trigger_type", "user"),
            reception_wechat_id=task.get("to_wxid", ""),
            reception_app_id=task.get("appid", ""),
        )
    except Exception:
        LOGGER.exception("tpw Coze 调用异常 message_id=%s", msg_row_id)
        update_chat_message_status(msg_row_id, "failed")
        return

    reply_text, image_urls = _parse_coze_workflow_json_reply(reply)
    chunks = _tpw_coze_reply_chunks(reply_text)
    if not chunks and not image_urls:
        LOGGER.info("tpw Coze 回复无文本且无图片，跳过 Gewe 发送 message_id=%s", msg_row_id)
        update_chat_message_status(msg_row_id, "failed")
        return

    for idx, chunk in enumerate(chunks):
        r = await _post_text_with_retry(
            app_id=task["appid"],
            to_wxid=task["peer_wxid"],
            content=chunk,
            message_id=msg_row_id,
        )
        ret = r.get("ret")
        if ret != 200:
            LOGGER.warning(
                "tpw Gewe 文本发送未成功 message_id=%s part=%s/%s ret=%s body=%s",
                msg_row_id,
                idx + 1,
                len(chunks),
                ret,
                r,
            )
            update_chat_message_status(msg_row_id, "failed")
            return

    for idx, img_url in enumerate(image_urls):
        r = await _post_image_with_retry(
            app_id=task["appid"],
            to_wxid=task["peer_wxid"],
            img_url=img_url,
            message_id=msg_row_id,
        )
        ret = r.get("ret")
        if ret != 200:
            LOGGER.warning(
                "tpw Gewe 图片发送未成功 message_id=%s img=%s/%s ret=%s body=%s",
                msg_row_id,
                idx + 1,
                len(image_urls),
                ret,
                r,
            )
            update_chat_message_status(msg_row_id, "failed")
            return

    update_chat_message_status(msg_row_id, "coze_done")


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


async def handle_personal_wechat_payload(
    data: Dict[str, Any],
    background_tasks: BackgroundTasks,
    *,
    trigger_type: Optional[str] = None,
) -> None:
    outcome = await asyncio.to_thread(_tpw_sync_ingest, data)
    if outcome.get("action") == "queue" and outcome.get("task"):
        task = outcome["task"]
        if trigger_type is not None:
            task["trigger_type"] = trigger_type
        background_tasks.add_task(_tpw_pipeline_async, task)
