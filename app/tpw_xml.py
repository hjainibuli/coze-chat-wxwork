"""
解析第三方个人微信 AddMsg 中 Content.string 的 XML 片段。
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional


def _strip_cdata(xml_str: str) -> str:
    return re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", xml_str, flags=re.DOTALL)


def content_xml_string(data: Dict[str, Any]) -> str:
    c = data.get("Content") or {}
    if isinstance(c, dict):
        s = c.get("string")
        return str(s).strip() if s is not None else ""
    return ""


def parse_appmsg_type(xml_str: str) -> Optional[int]:
    if not xml_str:
        return None
    s = _strip_cdata(xml_str)
    m = re.search(r"<type>(\d+)</type>", s)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def parse_appmsg_file_cdn(xml_str: str) -> Optional[Dict[str, str]]:
    """
    MsgType=49 且 appmsg.type=6（文件发送完成）时，提取 downloadCdn 所需字段。
    """
    if not xml_str or parse_appmsg_type(xml_str) != 6:
        return None
    s = _strip_cdata(xml_str)
    out: Dict[str, str] = {}
    for tag in ("cdnattachurl", "aeskey", "totallen", "fileext"):
        m = re.search(rf"<{tag}>([^<]*)</{tag}>", s, re.I)
        if m:
            out[tag] = m.group(1).strip()
    if not out.get("cdnattachurl") or not out.get("aeskey") or not out.get("totallen"):
        return None
    return out


def parse_emoji_md5(xml_str: str) -> Optional[str]:
    if not xml_str:
        return None
    m = re.search(r"<emoji[^>]*\bmd5\s*=\s*[\"']([^\"']+)[\"']", xml_str, re.I)
    if m:
        return m.group(1).strip()
    m = re.search(r'\bmd5\s*=\s*"([^"]+)"', xml_str, re.I)
    if m:
        return m.group(1).strip()
    return None


def wrap_xml_if_needed(fragment: str) -> str:
    """若回调只有 inner msg 片段，保证可被解析。"""
    s = (fragment or "").strip()
    if not s:
        return s
    if s.startswith("<?xml"):
        return s
    if s.startswith("<msg"):
        return s
    return f"<msg>{s}</msg>"
