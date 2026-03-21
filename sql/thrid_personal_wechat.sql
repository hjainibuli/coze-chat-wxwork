-- =============================================================================
-- tpw_* 第三方个人微信回调 - 映射 / 会话 / 消息 / 溯源 / 联系人 / 媒体
-- 字符集：utf8mb4（支持 emoji）
-- =============================================================================

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ---------------------------------------------------------------------------
-- 1. 设备 + 该设备上登录微信（路由上下文，类比 open_kfid）
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS `tpw_message_media`;
DROP TABLE IF EXISTS `tpw_chat_message`;
DROP TABLE IF EXISTS `tpw_contact_profile`;
DROP TABLE IF EXISTS `tpw_callback_raw`;
DROP TABLE IF EXISTS `tpw_conversation`;
DROP TABLE IF EXISTS `tpw_end_user`;
DROP TABLE IF EXISTS `tpw_device_account`;

CREATE TABLE `tpw_device_account` (
  `id`              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `appid`           VARCHAR(128)    NOT NULL COMMENT '第三方设备 Appid',
  `owner_wxid`      VARCHAR(128)    NOT NULL COMMENT '顶层 Wxid，该设备当前登录微信',
  `status`          TINYINT         NOT NULL DEFAULT 1 COMMENT '1=active 0=disabled',
  `extra`           JSON            NULL COMMENT '扩展：路由到哪个 Coze Bot 等',
  `created_at`      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at`      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_tpw_device_appid_owner` (`appid`, `owner_wxid`),
  KEY `idx_tpw_device_owner` (`owner_wxid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='第三方回调：设备与登录微信账号上下文';

-- ---------------------------------------------------------------------------
-- 2. 对端微信身份 -> 内部 user_id（供 Coze / 业务稳定引用）
-- ---------------------------------------------------------------------------
CREATE TABLE `tpw_end_user` (
  `id`                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `wxid`              VARCHAR(128)    NOT NULL COMMENT '微信侧用户 wxid',
  `internal_user_id`  VARCHAR(128)    NOT NULL COMMENT '系统内部用户 ID，传入 Coze user_id',
  `display_name`      VARCHAR(255)    NULL COMMENT '展示名（可选，可从 Push/联系人同步）',
  `first_seen_at`     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `last_seen_at`      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `extra`             JSON            NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_tpw_end_user_wxid` (`wxid`),
  UNIQUE KEY `uk_tpw_end_user_internal` (`internal_user_id`),
  KEY `idx_tpw_end_user_last_seen` (`last_seen_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='第三方回调：外部 wxid 与内部用户映射';

-- ---------------------------------------------------------------------------
-- 3. 会话：同一 end_user 在同一设备上下文中一条会话线
-- ---------------------------------------------------------------------------
CREATE TABLE `tpw_conversation` (
  `conversation_id`     VARCHAR(64)     NOT NULL COMMENT 'Coze 返回的 conversation_id',
  `end_user_id`         BIGINT UNSIGNED NOT NULL COMMENT '对端用户',
  `device_account_id`   BIGINT UNSIGNED NOT NULL COMMENT '设备+登录号上下文',
  `created_at`          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at`          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `extra`               JSON            NULL,
  PRIMARY KEY (`conversation_id`),
  UNIQUE KEY `uk_tpw_conv_user_device` (`end_user_id`, `device_account_id`),
  KEY `idx_tpw_conv_device` (`device_account_id`),
  CONSTRAINT `fk_tpw_conv_end_user`
    FOREIGN KEY (`end_user_id`) REFERENCES `tpw_end_user` (`id`)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT `fk_tpw_conv_device`
    FOREIGN KEY (`device_account_id`) REFERENCES `tpw_device_account` (`id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='第三方回调：用户 x 设备上下文 会话';

-- ---------------------------------------------------------------------------
-- 4. 原始回调包（溯源 / 补处理）
-- ---------------------------------------------------------------------------
CREATE TABLE `tpw_callback_raw` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `received_at`   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `type_name`     VARCHAR(64)     NOT NULL COMMENT '顶层 TypeName，如 AddMsg / ModContacts',
  `appid`         VARCHAR(128)    NULL,
  `owner_wxid`    VARCHAR(128)    NULL COMMENT '顶层 Wxid',
  `payload`       JSON            NOT NULL COMMENT '完整请求 JSON',
  `http_request_id` VARCHAR(64)   NULL COMMENT '可选：链路 ID',
  PRIMARY KEY (`id`),
  KEY `idx_tpw_raw_time` (`received_at`),
  KEY `idx_tpw_raw_type_time` (`type_name`, `received_at`),
  KEY `idx_tpw_raw_appid` (`appid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='第三方回调：原始 POST 包';

-- ---------------------------------------------------------------------------
-- 5. 规范化聊天消息（AddMsg 等）；排重：dedup_key = Appid + NewMsgId
-- ---------------------------------------------------------------------------
CREATE TABLE `tpw_chat_message` (
  `id`                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `dedup_key`           VARCHAR(256)    NOT NULL COMMENT '建议 appid:new_msg_id，文档幂等键',
  `callback_raw_id`     BIGINT UNSIGNED NULL COMMENT '来源原始回调',
  `conversation_id`     VARCHAR(64)     NOT NULL,
  `device_account_id`   BIGINT UNSIGNED NOT NULL COMMENT '冗余，便于按设备查',
  `end_user_id`         BIGINT UNSIGNED NOT NULL COMMENT '冗余：对端用户',
  `msg_id`              BIGINT          NULL COMMENT 'Data.MsgId',
  `new_msg_id`          BIGINT UNSIGNED NULL COMMENT 'Data.NewMsgId',
  `msg_seq`             BIGINT          NULL COMMENT 'Data.MsgSeq',
  `msg_type`            INT             NOT NULL COMMENT 'Data.MsgType',
  `is_from_owner`       TINYINT(1)      NOT NULL DEFAULT 0 COMMENT '1=FromUserName==顶层Wxid 自己发',
  `from_wxid`           VARCHAR(128)    NOT NULL,
  `to_wxid`             VARCHAR(128)    NOT NULL,
  `content_raw`         MEDIUMTEXT      NULL COMMENT 'Content.string 原文(XML/文本)',
  `content_text`        MEDIUMTEXT      NULL COMMENT '文本消息可直接解析出的正文',
  `push_content`        VARCHAR(512)    NULL,
  `msg_source`          MEDIUMTEXT      NULL,
  `client_create_time`  BIGINT UNSIGNED NULL COMMENT 'Data.CreateTime UNIX 秒',
  `ingest_status`       VARCHAR(32)     NOT NULL DEFAULT 'received' COMMENT 'received/queued/coze_done/failed 等',
  `created_at`          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_tpw_msg_dedup` (`dedup_key`),
  KEY `idx_tpw_msg_conv_time` (`conversation_id`, `client_create_time`),
  KEY `idx_tpw_msg_device_time` (`device_account_id`, `created_at`),
  KEY `idx_tpw_msg_new_msg_id` (`new_msg_id`),
  CONSTRAINT `fk_tpw_msg_conv`
    FOREIGN KEY (`conversation_id`) REFERENCES `tpw_conversation` (`conversation_id`)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_tpw_msg_device`
    FOREIGN KEY (`device_account_id`) REFERENCES `tpw_device_account` (`id`)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT `fk_tpw_msg_end_user`
    FOREIGN KEY (`end_user_id`) REFERENCES `tpw_end_user` (`id`)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT `fk_tpw_msg_raw`
    FOREIGN KEY (`callback_raw_id`) REFERENCES `tpw_callback_raw` (`id`)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='第三方回调：规范化消息（幂等 dedup_key）';

-- ---------------------------------------------------------------------------
-- 6. ModContacts 等联系人资料（大 JSON + 常用列）
-- ---------------------------------------------------------------------------
CREATE TABLE `tpw_contact_profile` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `wxid`          VARCHAR(128)    NOT NULL COMMENT '联系人 wxid',
  `end_user_id`   BIGINT UNSIGNED NULL COMMENT '若已映射则关联 tpw_end_user',
  `payload`       JSON            NOT NULL COMMENT 'ModContacts.Data 等整包',
  `nickname`      VARCHAR(255)    NULL,
  `avatar_url`    VARCHAR(1024)   NULL,
  `updated_at`    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_tpw_profile_wxid` (`wxid`),
  KEY `idx_tpw_profile_end_user` (`end_user_id`),
  CONSTRAINT `fk_tpw_profile_end_user`
    FOREIGN KEY (`end_user_id`) REFERENCES `tpw_end_user` (`id`)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='第三方回调：联系人资料快照（ModContacts）';

-- ---------------------------------------------------------------------------
-- 7. 大二进制：ImgBuf / 语音等，避免撑爆 tpw_chat_message
-- ---------------------------------------------------------------------------
CREATE TABLE `tpw_message_media` (
  `id`               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `chat_message_id`  BIGINT UNSIGNED NOT NULL,
  `media_role`       VARCHAR(32)     NOT NULL COMMENT 'thumb / voice / other',
  `content_type`     VARCHAR(128)    NULL COMMENT '可选 MIME',
  `byte_length`      INT UNSIGNED    NULL COMMENT '原始长度，便于校验',
  `blob_data`        LONGBLOB        NULL COMMENT '二进制；大可改对象存储只填 object_url',
  `object_url`       VARCHAR(1024)   NULL COMMENT '对象存储地址（可选）',
  `created_at`       DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  KEY `idx_tpw_media_msg` (`chat_message_id`),
  CONSTRAINT `fk_tpw_media_msg`
    FOREIGN KEY (`chat_message_id`) REFERENCES `tpw_chat_message` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='第三方回调：消息附属大媒体';

SET FOREIGN_KEY_CHECKS = 1;