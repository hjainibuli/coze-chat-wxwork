"""
Coze API 工具类
职责：仅封装与 Coze 平台的 HTTP 交互，不依赖数据库、Redis 或任何业务逻辑。
"""

import json
import timeit
import httpx
import requests
import logging

logger = logging.getLogger(__name__)

COZE_API_BASE = "https://api.coze.cn"


class CozeClient:
    """
    Coze API 客户端（同步 + 异步）

    Args:
        pat:         Personal Access Token（不含 "Bearer " 前缀）
        workflow_id: Workflow ID
        app_id:      App ID（可选）
    """

    def __init__(self, pat: str, workflow_id: str, app_id: str = ""):
        if not pat or not workflow_id:
            raise ValueError("pat 和 workflow_id 不能为空")
        self._headers = {
            "Authorization": f"Bearer {pat.strip()}",
            "Content-Type": "application/json",
        }
        self.workflow_id = workflow_id
        self.app_id = app_id

    # ------------------------------------------------------------------
    # 会话管理
    # ------------------------------------------------------------------

    def create_conversation(self, name: str = "default") -> str | None:
        """
        创建一个新的 Coze 会话，返回 conversation_id，失败返回 None。
        """
        payload = {"name": name}
        try:
            resp = requests.post(
                f"{COZE_API_BASE}/v1/conversation/create",
                headers=self._headers,
                json=payload,
                timeout=60,
            )
        except requests.RequestException as e:
            logger.error(f"创建会话网络异常: {e}")
            return None

        if resp.status_code != 200:
            logger.error(f"创建会话失败，状态码: {resp.status_code}，响应: {resp.text}")
            return None

        data = resp.json()
        conversation_id = data.get("data", {}).get("id")
        if not conversation_id:
            logger.error(f"创建会话响应格式异常: {data}")
            return None

        logger.info(f"会话创建成功: {conversation_id}")
        return conversation_id

    # ------------------------------------------------------------------
    # 构建消息体
    # ------------------------------------------------------------------

    def _build_request_body(
        self,
        conversation_id: str,
        user_id: str,
        questions: str | list,
    ) -> tuple[dict | None, str | None]:
        """
        根据 questions 类型构建请求体，返回 (json_data, user_latest_question)。
        questions 无效时返回 (None, None)。
        """
        payload = {
            "additional_messages": [],
            "parameters": {"user_id": user_id},
            "workflow_id": self.workflow_id,
            "conversation_id": conversation_id,
        }
        if self.app_id:
            payload["app_id"] = self.app_id

        user_latest_question: str | None = None

        if isinstance(questions, (str, int, float)):
            text = str(questions)
            payload["additional_messages"] = [
                {"content_type": "text", "role": "user", "content": text}
            ]
            user_latest_question = text

        elif isinstance(questions, list):
            if not questions:
                logger.error("问题列表为空")
                return None, None
            payload["additional_messages"] = [
                {"content_type": "text", "role": "user", "content": str(q)}
                for q in questions
            ]
            user_latest_question = str(questions[-1])

        else:
            logger.error(f"不支持的 questions 类型: {type(questions)}")
            return None, None

        return payload, user_latest_question

    # ------------------------------------------------------------------
    # 解析 SSE 流
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_sse_lines(lines) -> tuple[str, int | None, str | None]:
        """
        同步解析 SSE 行迭代器，返回 (assistant_reply, error_code, error_msg)。
        """
        reply = ""
        error_code = None
        error_msg = None
        for line in lines:
            if not line.startswith("data:"):
                continue
            data_str = line[5:].strip()
            try:
                data = json.loads(data_str)
            except json.JSONDecodeError:
                continue

            if data.get("role") == "assistant" and "content" in data:
                reply = data["content"].strip()
                break
            elif "code" in data and "msg" in data:
                error_code = data["code"]
                error_msg = data["msg"]
                break

        return reply, error_code, error_msg

    @staticmethod
    async def _async_parse_sse_lines(aiter_lines) -> tuple[str, int | None, str | None]:
        """
        异步解析 SSE 行迭代器，返回 (assistant_reply, error_code, error_msg)。
        """
        reply = ""
        error_code = None
        error_msg = None
        async for line in aiter_lines:
            if not line.startswith("data:"):
                continue
            data_str = line[5:].strip()
            try:
                data = json.loads(data_str)
            except json.JSONDecodeError:
                continue

            if data.get("role") == "assistant" and "content" in data:
                reply = data["content"].strip()
                break
            elif "code" in data and "msg" in data:
                error_code = data["code"]
                error_msg = data["msg"]
                break

        return reply, error_code, error_msg

    # ------------------------------------------------------------------
    # 同步调用
    # ------------------------------------------------------------------

    def call_workflow(
        self,
        conversation_id: str,
        user_id: str,
        questions: str | list,
    ) -> str:
        """
        同步调用 Coze Workflow，返回 AI 回复文本，失败返回空字符串。

        若会话失效（错误码 4002），自动新建会话并重试一次。
        """
        if not conversation_id:
            logger.error("conversation_id 为空")
            return ""

        payload, _ = self._build_request_body(conversation_id, user_id, questions)
        if payload is None:
            return ""

        try:
            start = timeit.default_timer()
            resp = requests.post(
                f"{COZE_API_BASE}/v1/workflows/chat",
                headers=self._headers,
                json=payload,
                timeout=60,
                stream=True,
            )
            elapsed = timeit.default_timer() - start
            logger.info(f"Coze API 响应耗时: {elapsed:.2f}s")
        except requests.RequestException as e:
            logger.error(f"网络异常: {e}")
            return ""

        if resp.status_code != 200:
            logger.error(f"请求失败: {resp.status_code}，响应: {resp.text}")
            return ""

        resp.encoding = "utf-8"
        reply, error_code, error_msg = self._parse_sse_lines(resp.iter_lines(decode_unicode=True))

        if reply:
            return reply

        # 会话失效，重建重试
        if error_code == 4002:
            logger.warning(f"会话 {conversation_id} 失效（4002），尝试新建会话重试...")
            new_cid = self.create_conversation(user_id)
            if not new_cid:
                logger.error("新建会话失败，无法重试")
                return ""

            payload["conversation_id"] = new_cid
            try:
                resp2 = requests.post(
                    f"{COZE_API_BASE}/v1/workflows/chat",
                    headers=self._headers,
                    json=payload,
                    timeout=60,
                    stream=True,
                )
            except requests.RequestException as e:
                logger.error(f"[重试] 网络异常: {e}")
                return ""

            if resp2.status_code != 200:
                logger.error(f"[重试] 请求失败: {resp2.status_code}")
                return ""

            resp2.encoding = "utf-8"
            reply, _, _ = self._parse_sse_lines(resp2.iter_lines(decode_unicode=True))
            return reply

        if error_msg:
            logger.error(f"Coze 错误 [{error_code}]: {error_msg}")

        return ""

    # ------------------------------------------------------------------
    # 异步调用
    # ------------------------------------------------------------------

    async def async_call_workflow(
        self,
        conversation_id: str,
        user_id: str,
        questions: str | list,
    ) -> str:
        """
        异步调用 Coze Workflow，返回 AI 回复文本，失败返回空字符串。

        若会话失效（错误码 4002），自动新建会话并重试一次。
        """
        if not conversation_id:
            logger.error("conversation_id 为空")
            return ""

        payload, _ = self._build_request_body(conversation_id, user_id, questions)
        if payload is None:
            return ""

        timeout = httpx.Timeout(60.0, connect=10.0)

        try:
            start = timeit.default_timer()
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream(
                    "POST",
                    f"{COZE_API_BASE}/v1/workflows/chat",
                    headers=self._headers,
                    json=payload,
                ) as resp:
                    if resp.status_code != 200:
                        body = await resp.aread()
                        logger.error(f"请求失败: {resp.status_code}，响应: {body.decode()}")
                        return ""

                    reply, error_code, error_msg = await self._async_parse_sse_lines(
                        resp.aiter_lines()
                    )

            elapsed = timeit.default_timer() - start
            logger.info(f"Coze API 响应耗时: {elapsed:.2f}s")

        except httpx.RequestError as e:
            logger.error(f"网络异常: {e}")
            return ""
        except Exception as e:
            logger.error(f"未知异常: {e}")
            return ""

        if reply:
            return reply

        # 会话失效，重建重试
        if error_code == 4002:
            logger.warning(f"会话 {conversation_id} 失效（4002），异步重建重试...")
            import asyncio
            new_cid = await asyncio.to_thread(self.create_conversation, user_id)
            if not new_cid:
                logger.error("新建会话失败，无法重试")
                return ""

            payload["conversation_id"] = new_cid
            try:
                start = timeit.default_timer()
                async with httpx.AsyncClient(timeout=timeout) as client:
                    async with client.stream(
                        "POST",
                        f"{COZE_API_BASE}/v1/workflows/chat",
                        headers=self._headers,
                        json=payload,
                    ) as resp:
                        if resp.status_code != 200:
                            body = await resp.aread()
                            logger.error(f"[重试] 请求失败: {resp.status_code}，响应: {body.decode()}")
                            return ""

                        reply, e_code, e_msg = await self._async_parse_sse_lines(
                            resp.aiter_lines()
                        )

                elapsed = timeit.default_timer() - start
                logger.info(f"[重试] Coze API 响应耗时: {elapsed:.2f}s")

                if e_msg:
                    logger.error(f"[重试] Coze 错误 [{e_code}]: {e_msg}")
                return reply

            except Exception as e:
                logger.error(f"[重试] 异常: {e}")
                return ""

        if error_msg:
            logger.error(f"Coze 错误 [{error_code}]: {error_msg}")

        return ""
