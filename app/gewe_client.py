"""
Gewe API：发文字、下载图片/文件/语音/视频/emoji/CDN（见 docs/下载）。
"""
import asyncio
import os
import shutil
import subprocess
import uuid
from typing import Any, Dict, Optional

import httpx
import pilk

from config import GEWE_API_BASE, GEWE_TOKEN, LOGGER, SERVER_BASE_URL, TEMP_VOICE_DIR


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


def _ffmpeg_wav_to_mp3(wav_path: str, mp3_path: str) -> bool:
    """WAV → MP3（容器内 Debian ffmpeg 无 SILK 解码器，SILK 须先经 pilk 转 WAV）。"""
    exe = shutil.which("ffmpeg")
    if not exe:
        LOGGER.error("未找到 ffmpeg，无法将 WAV 转为 MP3")
        return False
    r = subprocess.run(
        [exe, "-y", "-loglevel", "error", "-i", wav_path, "-acodec", "libmp3lame", "-q:a", "5", mp3_path],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        LOGGER.error("ffmpeg WAV→MP3 失败: %s", (r.stderr or "").strip())
        return False
    return True


def _wechat_silk_file_to_mp3(silk_path: str, wav_path: str, mp3_path: str) -> bool:
    """微信/腾讯 SILK（含 0x02 头）→ WAV（pilk）→ MP3（ffmpeg）。"""
    try:
        pilk.silk_to_wav(silk_path, wav_path)
    except Exception:
        LOGGER.exception("pilk 解码 SILK 失败（需微信 SILK，非标准 MP3 直链）")
        return False
    if not os.path.isfile(wav_path) or os.path.getsize(wav_path) == 0:
        LOGGER.error("pilk 未生成有效 WAV: %s", wav_path)
        return False
    return _ffmpeg_wav_to_mp3(wav_path, mp3_path)


async def _download_voice_to_local_mp3_url(*, file_url: str, msg_id: int) -> Optional[str]:
    os.makedirs(TEMP_VOICE_DIR, exist_ok=True)
    base = f"voice_{msg_id}_{uuid.uuid4().hex[:12]}"
    silk_path = os.path.join(TEMP_VOICE_DIR, f"{base}.silk")
    wav_path = os.path.join(TEMP_VOICE_DIR, f"{base}.wav")
    mp3_path = os.path.join(TEMP_VOICE_DIR, f"{base}.mp3")

    def _write_silk_and_convert(raw: bytes) -> bool:
        with open(silk_path, "wb") as f:
            f.write(raw)
        try:
            return _wechat_silk_file_to_mp3(silk_path, wav_path, mp3_path)
        finally:
            for p in (silk_path, wav_path):
                try:
                    if os.path.isfile(p):
                        os.remove(p)
                except OSError:
                    pass

    try:
        timeout = httpx.Timeout(90.0, connect=15.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(file_url)
            resp.raise_for_status()
            body = resp.content
        ok = await asyncio.to_thread(_write_silk_and_convert, body)
        if not ok:
            try:
                if os.path.isfile(mp3_path):
                    os.remove(mp3_path)
            except OSError:
                pass
            return None
        rel = f"{TEMP_VOICE_DIR}/{base}.mp3".replace("\\", "/")
        public = f"{SERVER_BASE_URL}/{rel}"
        LOGGER.info("语音已转存 MP3: %s", public)
        return public
    except Exception:
        LOGGER.exception("下载或转码语音失败 msg_id=%s url=%s", msg_id, file_url[:120] if file_url else "")
        for p in (silk_path, wav_path, mp3_path):
            try:
                if os.path.isfile(p):
                    os.remove(p)
            except OSError:
                pass
        return None


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
    file_url = _data_file_url(resp)
    if not file_url:
        return None
    return await _download_voice_to_local_mp3_url(file_url=file_url, msg_id=msg_id)


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
