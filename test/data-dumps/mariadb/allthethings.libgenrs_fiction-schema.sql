/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `libgenrs_fiction` (
  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `MD5` char(32) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,
  `Title` varchar(2000) NOT NULL DEFAULT '',
  `Author` varchar(300) NOT NULL DEFAULT '',
  `Series` varchar(300) NOT NULL DEFAULT '',
  `Edition` varchar(50) NOT NULL DEFAULT '',
  `Language` varchar(45) NOT NULL DEFAULT '',
  `Year` varchar(10) NOT NULL DEFAULT '',
  `Publisher` varchar(100) NOT NULL DEFAULT '',
  `Pages` varchar(10) NOT NULL DEFAULT '',
  `Identifier` varchar(400) NOT NULL DEFAULT '',
  `GooglebookID` varchar(45) NOT NULL DEFAULT '',
  `ASIN` varchar(10) NOT NULL DEFAULT '',
  `Coverurl` varchar(200) NOT NULL DEFAULT '',
  `Extension` varchar(10) NOT NULL,
  `Filesize` int(10) unsigned NOT NULL,
  `Library` varchar(50) NOT NULL DEFAULT '',
  `Issue` varchar(100) NOT NULL DEFAULT '',
  `Locator` varchar(512) NOT NULL DEFAULT '',
  `Commentary` varchar(500) DEFAULT NULL,
  `Generic` char(32) NOT NULL DEFAULT '',
  `Visible` char(3) NOT NULL DEFAULT '',
  `TimeAdded` timestamp NOT NULL DEFAULT current_timestamp(),
  `TimeLastModified` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`ID`),
  UNIQUE KEY `MD5UNIQUE` (`MD5`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=2488238 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
