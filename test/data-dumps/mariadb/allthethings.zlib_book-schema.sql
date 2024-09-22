/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `zlib_book` (
  `zlibrary_id` int(11) NOT NULL,
  `date_added` text NOT NULL,
  `date_modified` text NOT NULL,
  `extension` text NOT NULL,
  `filesize` bigint(20) DEFAULT NULL,
  `filesize_reported` bigint(20) NOT NULL,
  `md5` char(32) DEFAULT NULL,
  `md5_reported` char(32) NOT NULL,
  `title` text NOT NULL,
  `author` text NOT NULL,
  `publisher` text NOT NULL,
  `language` text NOT NULL,
  `series` text NOT NULL,
  `volume` text NOT NULL,
  `edition` text NOT NULL,
  `year` text NOT NULL,
  `pages` text NOT NULL,
  `description` text NOT NULL,
  `cover_url` text NOT NULL,
  `in_libgen` tinyint(1) NOT NULL DEFAULT 0,
  `pilimi_torrent` varchar(50) DEFAULT NULL,
  `unavailable` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`zlibrary_id`),
  KEY `md5` (`md5`),
  KEY `md5_reported` (`md5_reported`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
