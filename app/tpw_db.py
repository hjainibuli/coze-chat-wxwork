"""
第三方个人微信 tpw_* 表：与 database_operation 共用 engine / Base。
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.mysql import BIGINT, JSON, MEDIUMTEXT, LONGBLOB
from sqlalchemy.exc import IntegrityError

from config import LOGGER, REDIS_CLIENT, generate_internal_uid
from database_operation import Base, SessionLocal, engine


class TpwDeviceAccount(Base):
    __tablename__ = "tpw_device_account"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    appid = Column(String(128), nullable=False)
    owner_wxid = Column(String(128), nullable=False)
    status = Column(SmallInteger, nullable=False, server_default="1")
    extra = Column(JSON, nullable=True)
    created_at = Column(DateTime, server_default=func.current_timestamp())
    updated_at = Column(
        DateTime, server_default=func.current_timestamp(), onupdate=func.current_timestamp()
    )

    __table_args__ = (UniqueConstraint("appid", "owner_wxid", name="uk_tpw_device_appid_owner"),)


class TpwEndUser(Base):
    __tablename__ = "tpw_end_user"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    wxid = Column(String(128), nullable=False, unique=True)
    internal_user_id = Column(String(128), nullable=False, unique=True)
    display_name = Column(String(255), nullable=True)
    first_seen_at = Column(DateTime, server_default=func.current_timestamp())
    last_seen_at = Column(
        DateTime, server_default=func.current_timestamp(), onupdate=func.current_timestamp()
    )
    extra = Column(JSON, nullable=True)


class TpwConversation(Base):
    __tablename__ = "tpw_conversation"

    conversation_id = Column(String(64), primary_key=True)
    end_user_id = Column(
        BigInteger, ForeignKey("tpw_end_user.id", ondelete="RESTRICT", onupdate="CASCADE"), nullable=False
    )
    device_account_id = Column(
        BigInteger,
        ForeignKey("tpw_device_account.id", ondelete="RESTRICT", onupdate="CASCADE"),
        nullable=False,
    )
    created_at = Column(DateTime, server_default=func.current_timestamp())
    updated_at = Column(
        DateTime, server_default=func.current_timestamp(), onupdate=func.current_timestamp()
    )
    extra = Column(JSON, nullable=True)

    __table_args__ = (
        UniqueConstraint("end_user_id", "device_account_id", name="uk_tpw_conv_user_device"),
    )


class TpwCallbackRaw(Base):
    __tablename__ = "tpw_callback_raw"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    received_at = Column(DateTime, server_default=func.current_timestamp())
    type_name = Column(String(64), nullable=False)
    appid = Column(String(128), nullable=True)
    owner_wxid = Column(String(128), nullable=True)
    payload = Column(JSON, nullable=False)
    http_request_id = Column(String(64), nullable=True)


class TpwChatMessage(Base):
    __tablename__ = "tpw_chat_message"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    dedup_key = Column(String(256), nullable=False, unique=True)
    callback_raw_id = Column(
        BigInteger, ForeignKey("tpw_callback_raw.id", ondelete="SET NULL", onupdate="CASCADE"), nullable=True
    )
    conversation_id = Column(
        String(64),
        ForeignKey("tpw_conversation.conversation_id", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False,
    )
    device_account_id = Column(
        BigInteger, ForeignKey("tpw_device_account.id", ondelete="RESTRICT", onupdate="CASCADE"), nullable=False
    )
    end_user_id = Column(
        BigInteger, ForeignKey("tpw_end_user.id", ondelete="RESTRICT", onupdate="CASCADE"), nullable=False
    )
    msg_id = Column(BigInteger, nullable=True)
    new_msg_id = Column(BIGINT(unsigned=True), nullable=True)
    msg_seq = Column(BigInteger, nullable=True)
    msg_type = Column(Integer, nullable=False)
    is_from_owner = Column(Boolean, nullable=False, server_default="0")
    from_wxid = Column(String(128), nullable=False)
    to_wxid = Column(String(128), nullable=False)
    content_raw = Column(MEDIUMTEXT, nullable=True)
    content_text = Column(MEDIUMTEXT, nullable=True)
    push_content = Column(String(512), nullable=True)
    msg_source = Column(MEDIUMTEXT, nullable=True)
    client_create_time = Column(BigInteger, nullable=True)
    ingest_status = Column(String(32), nullable=False, server_default="received")
    created_at = Column(DateTime, server_default=func.current_timestamp())


class TpwContactProfile(Base):
    __tablename__ = "tpw_contact_profile"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    wxid = Column(String(128), nullable=False, unique=True)
    end_user_id = Column(
        BigInteger, ForeignKey("tpw_end_user.id", ondelete="SET NULL", onupdate="CASCADE"), nullable=True
    )
    payload = Column(JSON, nullable=False)
    nickname = Column(String(255), nullable=True)
    avatar_url = Column(String(1024), nullable=True)
    updated_at = Column(DateTime, server_default=func.current_timestamp(), onupdate=func.current_timestamp())


class TpwMessageMedia(Base):
    __tablename__ = "tpw_message_media"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    chat_message_id = Column(
        BigInteger, ForeignKey("tpw_chat_message.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False
    )
    media_role = Column(String(32), nullable=False)
    content_type = Column(String(128), nullable=True)
    byte_length = Column(Integer, nullable=True)
    blob_data = Column(LONGBLOB, nullable=True)
    object_url = Column(String(1024), nullable=True)
    created_at = Column(DateTime, server_default=func.current_timestamp())


def init_tpw_tables() -> None:
    Base.metadata.create_all(engine)


def save_callback_raw(payload: Dict[str, Any], http_request_id: Optional[str] = None) -> int:
    type_name = str(payload.get("TypeName") or payload.get("typeName") or "unknown")
    appid = payload.get("Appid") or payload.get("appid")
    owner = payload.get("Wxid") or payload.get("wxid")
    if appid is not None:
        appid = str(appid)
    if owner is not None:
        owner = str(owner)
    session = SessionLocal()
    try:
        row = TpwCallbackRaw(
            type_name=type_name[:64],
            appid=appid,
            owner_wxid=owner,
            payload=payload,
            http_request_id=http_request_id,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        return int(row.id)
    finally:
        session.close()


def get_or_create_device_account(appid: str, owner_wxid: str) -> Tuple[int, bool]:
    session = SessionLocal()
    try:
        row = (
            session.query(TpwDeviceAccount)
            .filter(TpwDeviceAccount.appid == appid, TpwDeviceAccount.owner_wxid == owner_wxid)
            .one_or_none()
        )
        if row:
            return int(row.id), False
        row = TpwDeviceAccount(appid=appid, owner_wxid=owner_wxid)
        session.add(row)
        session.commit()
        session.refresh(row)
        return int(row.id), True
    finally:
        session.close()


def _cache_key_wxid(wxid: str) -> str:
    return f"map:tpw_wxid:{wxid}"


def get_or_create_end_user(wxid: str) -> Tuple[int, str, bool]:
    if not wxid:
        raise ValueError("wxid required")
    cache_key = _cache_key_wxid(wxid)
    try:
        cached = REDIS_CLIENT.get(cache_key)
        if cached:
            internal_id = cached.decode("utf-8")
            session = SessionLocal()
            try:
                row = session.query(TpwEndUser).filter(TpwEndUser.wxid == wxid).one_or_none()
                if row:
                    return int(row.id), row.internal_user_id, False
            finally:
                session.close()
    except Exception as e:
        LOGGER.warning("tpw Redis get failed: %s", e)

    session = SessionLocal()
    try:
        row = session.query(TpwEndUser).filter(TpwEndUser.wxid == wxid).one_or_none()
        if row:
            try:
                REDIS_CLIENT.set(cache_key, row.internal_user_id, ex=604800)
            except Exception as e:
                LOGGER.warning("tpw Redis set failed: %s", e)
            return int(row.id), row.internal_user_id, False

        internal_id = generate_internal_uid()
        row = TpwEndUser(wxid=wxid, internal_user_id=internal_id)
        session.add(row)
        created_new = False
        try:
            session.commit()
            session.refresh(row)
            created_new = True
        except IntegrityError:
            session.rollback()
            row = session.query(TpwEndUser).filter(TpwEndUser.wxid == wxid).one()
        try:
            REDIS_CLIENT.set(cache_key, row.internal_user_id, ex=604800)
        except Exception as e:
            LOGGER.warning("tpw Redis set failed: %s", e)
        return int(row.id), row.internal_user_id, created_new
    finally:
        session.close()


def get_or_create_tpw_conversation(
    end_user_id: int,
    device_account_id: int,
    internal_user_id: str,
    create_coze_conversation_fn: Callable[..., Optional[str]],
) -> Tuple[str, bool]:
    """
    create_coze_conversation_fn: (internal_user_id: str, open_kfid: None) -> Optional[str] Coze id
    """
    session = SessionLocal()
    try:
        row = (
            session.query(TpwConversation)
            .filter(
                TpwConversation.end_user_id == end_user_id,
                TpwConversation.device_account_id == device_account_id,
            )
            .one_or_none()
        )
        if row:
            return row.conversation_id, False
        coze_id = create_coze_conversation_fn(internal_user_id, None)
        if not coze_id:
            raise RuntimeError("Coze conversation create returned empty")
        row = TpwConversation(
            conversation_id=coze_id,
            end_user_id=end_user_id,
            device_account_id=device_account_id,
        )
        session.add(row)
        session.commit()
        return coze_id, True
    finally:
        session.close()


def renew_tpw_coze_conversation_id(end_user_id: int, device_account_id: int, new_conversation_id: str) -> None:
    session = SessionLocal()
    try:
        row = (
            session.query(TpwConversation)
            .filter(
                TpwConversation.end_user_id == end_user_id,
                TpwConversation.device_account_id == device_account_id,
            )
            .one_or_none()
        )
        if not row:
            LOGGER.error("tpw renew: no conversation for user=%s device=%s", end_user_id, device_account_id)
            return
        old_id = row.conversation_id
        if old_id == new_conversation_id:
            return
        row.conversation_id = new_conversation_id
        session.commit()
        LOGGER.info("tpw Coze conversation renewed %s -> %s", old_id, new_conversation_id)
    finally:
        session.close()


def try_insert_chat_message(
    *,
    dedup_key: str,
    callback_raw_id: Optional[int],
    conversation_id: str,
    device_account_id: int,
    end_user_id: int,
    msg_id: Optional[int],
    new_msg_id: Optional[int],
    msg_seq: Optional[int],
    msg_type: int,
    is_from_owner: bool,
    from_wxid: str,
    to_wxid: str,
    content_raw: Optional[str],
    content_text: Optional[str],
    push_content: Optional[str],
    msg_source: Optional[str],
    client_create_time: Optional[int],
    ingest_status: str = "queued",
) -> Tuple[bool, Optional[int]]:
    """
    Returns (inserted, message_id). False if duplicate dedup_key.
    """
    session = SessionLocal()
    try:
        row = TpwChatMessage(
            dedup_key=dedup_key,
            callback_raw_id=callback_raw_id,
            conversation_id=conversation_id,
            device_account_id=device_account_id,
            end_user_id=end_user_id,
            msg_id=msg_id,
            new_msg_id=new_msg_id,
            msg_seq=msg_seq,
            msg_type=msg_type,
            is_from_owner=is_from_owner,
            from_wxid=from_wxid,
            to_wxid=to_wxid,
            content_raw=content_raw,
            content_text=content_text,
            push_content=push_content,
            msg_source=msg_source,
            client_create_time=client_create_time,
            ingest_status=ingest_status,
        )
        session.add(row)
        try:
            session.commit()
            session.refresh(row)
            return True, int(row.id)
        except IntegrityError:
            session.rollback()
            return False, None
    finally:
        session.close()


def update_chat_message_status(message_id: int, status: str) -> None:
    session = SessionLocal()
    try:
        session.query(TpwChatMessage).filter(TpwChatMessage.id == message_id).update(
            {TpwChatMessage.ingest_status: status}
        )
        session.commit()
    finally:
        session.close()


def _modcontacts_field_str(data_obj: Dict[str, Any], key: str) -> Optional[str]:
    v = data_obj.get(key)
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        s = v.get("string")
        return str(s) if s is not None else None
    return None


def upsert_contact_profile_modcontacts(data_obj: Dict[str, Any]) -> None:
    """data_obj: ModContacts Data object (dict)."""
    wxid = _modcontacts_field_str(data_obj, "UserName")
    if not wxid:
        LOGGER.warning("ModContacts missing UserName")
        return
    nickname = _modcontacts_field_str(data_obj, "NickName")
    avatar = _modcontacts_field_str(data_obj, "BigHeadImgUrl") or _modcontacts_field_str(
        data_obj, "SmallHeadImgUrl"
    )

    session = SessionLocal()
    try:
        end_user = session.query(TpwEndUser).filter(TpwEndUser.wxid == wxid).one_or_none()
        end_user_id = int(end_user.id) if end_user else None
        row = session.query(TpwContactProfile).filter(TpwContactProfile.wxid == wxid).one_or_none()
        if row:
            row.payload = data_obj
            row.nickname = nickname
            row.avatar_url = avatar
            row.end_user_id = end_user_id
        else:
            row = TpwContactProfile(
                wxid=wxid,
                end_user_id=end_user_id,
                payload=data_obj,
                nickname=nickname,
                avatar_url=avatar,
            )
            session.add(row)
        session.commit()
    finally:
        session.close()


init_tpw_tables()
