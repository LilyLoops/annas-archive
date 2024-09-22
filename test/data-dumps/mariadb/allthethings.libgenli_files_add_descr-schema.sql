/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `libgenli_files_add_descr` (
  `f_add_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `f_id` int(10) unsigned NOT NULL DEFAULT 0,
  `key` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'Ссылка на описание elem_descr ',
  `value` mediumtext DEFAULT NULL,
  `value_add1` mediumtext DEFAULT NULL,
  `value_add2` mediumtext DEFAULT NULL,
  `value_add3` mediumtext DEFAULT NULL,
  `value_hash` bigint(20) unsigned NOT NULL,
  `date_start` date DEFAULT NULL,
  `date_end` date DEFAULT NULL,
  `issue_start` varchar(45) NOT NULL DEFAULT '' COMMENT 'Начальное издание, при наличие issue_able в elem_descr',
  `issue_end` varchar(45) NOT NULL DEFAULT '' COMMENT 'Конечное издание, при наличие issue_able в elem_descr',
  `time_added` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `time_last_modified` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `commentary` varchar(250) NOT NULL DEFAULT '',
  `uid` int(10) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (`f_add_id`) USING BTREE,
  UNIQUE KEY `VAL_UNIQ` (`value_hash`,`f_id`,`key`) USING BTREE,
  KEY `F_ID` (`f_id`)
) ENGINE=MyISAM AUTO_INCREMENT=80111852 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Дополнительные элементы описания к сериям';
