/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `libgenli_publishers` (
  `p_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `title` varchar(500) NOT NULL DEFAULT '' COMMENT 'Название',
  `org_type` varchar(100) DEFAULT '' COMMENT 'Вид организации',
  `add_info` varchar(45) DEFAULT '',
  `time_added` datetime NOT NULL,
  `time_last_modified` datetime NOT NULL,
  `date_start` date DEFAULT NULL,
  `date_end` date DEFAULT NULL,
  `uid` int(10) unsigned NOT NULL DEFAULT 0,
  `visible` varchar(3) DEFAULT '',
  `editable` tinyint(1) DEFAULT 1,
  `commentary` varchar(45) DEFAULT '',
  PRIMARY KEY (`p_id`)
) ENGINE=MyISAM AUTO_INCREMENT=43874 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Издательства';
