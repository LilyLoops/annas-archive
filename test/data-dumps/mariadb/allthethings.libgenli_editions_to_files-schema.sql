/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `libgenli_editions_to_files` (
  `etf_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `f_id` int(10) unsigned NOT NULL,
  `e_id` int(10) unsigned NOT NULL,
  `time_added` datetime NOT NULL,
  `time_last_modified` datetime NOT NULL,
  `uid` int(10) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (`etf_id`) USING BTREE,
  UNIQUE KEY `IDS` (`f_id`,`e_id`),
  KEY `EID` (`e_id`)
) ENGINE=MyISAM AUTO_INCREMENT=97670702 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
