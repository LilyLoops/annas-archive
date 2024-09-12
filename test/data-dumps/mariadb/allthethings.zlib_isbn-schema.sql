/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `zlib_isbn` (
  `zlibrary_id` int(11) NOT NULL,
  `isbn` varchar(13) NOT NULL,
  PRIMARY KEY (`zlibrary_id`,`isbn`),
  UNIQUE KEY `isbn_id` (`isbn`,`zlibrary_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
