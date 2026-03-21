"""
Gewe API：发文字、下载图片/文件/语音/视频/emoji/CDN（见 docs/下载）。
"""
from typing import Any, Dict, Optional

import httpx

from config import GEWE_API_BASE, GEWE_TOKEN, LOGGER


async def _post_json(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    if not GEWE_TOKEN:
        LOGGER.error("GEWE_TOKEN 未配置")
        return {"ret": -1, "msg": "GEWE_TOKEN missing", "data": {}}
    url = f"{GEWE_API_BASE}{path}"
    headers = {"X-GEWE-TOKEN": GEWE_TOKEN, "Content-Type": "application/json"}
    timeout = httpx.Timeout(90.0, connect=15.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=body, headers=headers)
        try:
            return resp.json()
        except Exception:
            LOGGER.error("Gewe %s 非 JSON: %s", path, resp.text[:500])
            return {"ret": -1, "msg": resp.text[:200], "data": {}}


def _data_file_url(resp: Dict[str, Any]) -> Optional[str]:
    if resp.get("ret") != 200:
        LOGGER.warning("Gewe 下载接口失败 ret=%s msg=%s", resp.get("ret"), resp.get("msg"))
        return None
    data = resp.get("data") or {}
    u = data.get("fileUrl") or data.get("url")
    return str(u).strip() if u else None


async def download_image_temp_url(*, app_id: str, xml: str, image_type: int = 2) -> Optional[str]:
    """downloadImage：type 1高清 2常规 3缩略图"""
    resp = await _post_json(
        "/gewe/v2/api/message/downloadImage",
        {"appId": app_id, "xml": xml, "type": image_type},
    )
    return _data_file_url(resp)


async def download_image_temp_url_try_types(*, app_id: str, xml: str) -> Optional[str]:
    for t in (2, 1, 3):
        u = await download_image_temp_url(app_id=app_id, xml=xml, image_type=t)
        if u:
            return u
    return None


async def download_file_temp_url(*, app_id: str, xml: str) -> Optional[str]:
    """downloadFile：通用 XML（部分消息类型）"""
    resp = await _post_json("/gewe/v2/api/message/downloadFile", {"appId": app_id, "xml": xml})
    return _data_file_url(resp)


async def download_voice_temp_url(*, app_id: str, xml: str, msg_id: int) -> Optional[str]:
    resp = await _post_json(
        "/gewe/v2/api/message/downloadVoice",
        {"appId": app_id, "xml": xml, "msgId": msg_id},
    )
    return _data_file_url(resp)


async def download_video_temp_url(*, app_id: str, xml: str) -> Optional[str]:
    resp = await _post_json("/gewe/v2/api/message/downloadVideo", {"appId": app_id, "xml": xml})
    return _data_file_url(resp)


async def download_emoji_temp_url(*, app_id: str, emoji_md5: str) -> Optional[str]:
    resp = await _post_json(
        "/gewe/v2/api/message/downloadEmojiMd5",
        {"appId": app_id, "emojiMd5": emoji_md5},
    )
    return _data_file_url(resp)


async def download_cdn_temp_url(
    *,
    app_id: str,
    aes_key: str,
    file_id: str,
    type_: str,
    total_size: str,
    suffix: str,
) -> Optional[str]:
    """downloadCdn：type 5 为文件等，见 docs/下载/cdn下载.md"""
    resp = await _post_json(
        "/gewe/v2/api/message/downloadCdn",
        {
            "appId": app_id,
            "aesKey": aes_key,
            "fileId": file_id,
            "type": type_,
            "totalSize": str(total_size),
            "suffix": suffix,
        },
    )
    return _data_file_url(resp)


async def post_text(*, app_id: str, to_wxid: str, content: str, ats: Optional[str] = None) -> Dict[str, Any]:
    """
    返回第三方 JSON：含 ret / msg / data；失败时 ret != 200 或抛 httpx 异常。
    """
    if not GEWE_TOKEN:
        LOGGER.error("GEWE_TOKEN 未配置，无法发消息")
        return {"ret": -1, "msg": "GEWE_TOKEN missing", "data": {}}

    url = f"{GEWE_API_BASE}/gewe/v2/api/message/postText"
    body: Dict[str, Any] = {
        "appId": app_id,
        "toWxid": to_wxid,
        "content": content,
    }
    if ats:
        body["ats"] = ats

    headers = {"X-GEWE-TOKEN": GEWE_TOKEN, "Content-Type": "application/json"}
    timeout = httpx.Timeout(30.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=body, headers=headers)
        try:
            data = resp.json()
        except Exception:
            LOGGER.error("Gewe 响应非 JSON: %s", resp.text[:500])
            return {"ret": -1, "msg": resp.text[:200], "data": {}}
        if resp.status_code != 200:
            LOGGER.warning("Gewe HTTP %s: %s", resp.status_code, data)
        return data
