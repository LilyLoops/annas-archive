/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `annas_archive_meta__aacid__nexusstc_records` (
  `aacid` varchar(250) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  `primary_id` varchar(250) DEFAULT NULL,
  `md5` char(32) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,
  `byte_offset` bigint(20) NOT NULL,
  `byte_length` bigint(20) NOT NULL,
  PRIMARY KEY (`aacid`),
  KEY `primary_id` (`primary_id`),
  KEY `md5` (`md5`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
