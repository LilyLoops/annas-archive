/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `aa_ia_2023_06_metadata` (
  `ia_id` varchar(200) NOT NULL,
  `has_thumb` tinyint(1) NOT NULL,
  `libgen_md5` char(32) DEFAULT NULL,
  `json` longtext DEFAULT NULL CHECK (json_valid(`json`)),
  PRIMARY KEY (`ia_id`),
  KEY `libgen_md5` (`libgen_md5`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
