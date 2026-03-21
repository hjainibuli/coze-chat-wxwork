"""
Gewe API：发送文字消息 POST /gewe/v2/api/message/postText
"""
from typing import Any, Dict, Optional

import httpx

from config import GEWE_API_BASE, GEWE_TOKEN, LOGGER


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
