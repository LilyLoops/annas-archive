/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `libgenrs_hashes` (
  `md5` char(32) NOT NULL,
  `crc32` char(8) NOT NULL DEFAULT '',
  `edonkey` char(32) NOT NULL DEFAULT '',
  `aich` char(32) NOT NULL DEFAULT '',
  `sha1` char(40) NOT NULL DEFAULT '',
  `tth` char(39) NOT NULL DEFAULT '',
  `torrent` text DEFAULT NULL,
  `btih` char(40) NOT NULL DEFAULT '',
  `sha256` char(64) NOT NULL DEFAULT '',
  `ipfs_cid` char(62) NOT NULL DEFAULT '',
  PRIMARY KEY (`md5`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
