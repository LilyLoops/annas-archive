/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `libgenrs_description` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `md5` varchar(32) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `descr` mediumtext NOT NULL,
  `toc` mediumtext NOT NULL,
  `TimeLastModified` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `md5_unique` (`md5`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=2748279 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
