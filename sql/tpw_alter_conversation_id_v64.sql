-- 已有库升级：将 Coze conversation_id 扩为 VARCHAR(64)
ALTER TABLE `tpw_chat_message` DROP FOREIGN KEY `fk_tpw_msg_conv`;
ALTER TABLE `tpw_conversation` MODIFY COLUMN `conversation_id` VARCHAR(64) NOT NULL COMMENT 'Coze 返回的 conversation_id';
ALTER TABLE `tpw_chat_message` MODIFY COLUMN `conversation_id` VARCHAR(64) NOT NULL;
ALTER TABLE `tpw_chat_message` ADD CONSTRAINT `fk_tpw_msg_conv`
  FOREIGN KEY (`conversation_id`) REFERENCES `tpw_conversation` (`conversation_id`)
  ON DELETE CASCADE ON UPDATE CASCADE;
