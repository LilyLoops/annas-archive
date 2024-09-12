/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `aarecords_codes` (
  `code` varbinary(680) NOT NULL,
  `aarecord_id` varbinary(300) NOT NULL,
  `aarecord_id_prefix` varbinary(20) NOT NULL,
  `row_number_order_by_code` bigint(20) NOT NULL,
  `dense_rank_order_by_code` bigint(20) NOT NULL,
  `row_number_partition_by_aarecord_id_prefix_order_by_code` bigint(20) NOT NULL,
  `dense_rank_partition_by_aarecord_id_prefix_order_by_code` bigint(20) NOT NULL,
  PRIMARY KEY (`code`,`aarecord_id`),
  KEY `aarecord_id_prefix` (`aarecord_id_prefix`,`code`,`aarecord_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
