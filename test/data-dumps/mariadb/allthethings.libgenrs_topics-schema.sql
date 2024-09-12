/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `libgenrs_topics` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `topic_descr` varchar(500) NOT NULL DEFAULT '',
  `lang` varchar(2) NOT NULL DEFAULT '',
  `kolxoz_code` varchar(10) NOT NULL DEFAULT '',
  `topic_id` int(10) unsigned DEFAULT NULL,
  `topic_id_hl` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `LANG` (`lang`) USING BTREE,
  KEY `topic_id+topic_id_hl` (`topic_id`,`topic_id_hl`),
  KEY `topic_id` (`topic_id`),
  KEY `topic_id_hl` (`topic_id_hl`),
  KEY `topic_id+lang` (`topic_id`,`lang`),
  KEY `topic_id_hl+lang` (`topic_id_hl`,`lang`)
) ENGINE=MyISAM AUTO_INCREMENT=647 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
