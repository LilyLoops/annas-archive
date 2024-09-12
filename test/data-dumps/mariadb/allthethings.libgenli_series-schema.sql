/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'*/;
/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `libgenli_series` (
  `s_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `libgen_topic` enum('s','a','l','f','r','m','c') NOT NULL COMMENT 'Раздел LG',
  `title` varchar(500) NOT NULL COMMENT 'Заголовок серии',
  `add_info` varchar(100) DEFAULT '',
  `type` varchar(3) NOT NULL DEFAULT '' COMMENT 'Тип серии - mag - журнал com - комикс и т.п.',
  `volume` varchar(20) NOT NULL DEFAULT '' COMMENT 'Том',
  `volume_type` varchar(50) NOT NULL DEFAULT '' COMMENT 'Тип серии - HS, INT, Annual, OS и т. п.',
  `volume_name` varchar(200) NOT NULL DEFAULT '' COMMENT 'Название тома',
  `publisher` varchar(1000) NOT NULL DEFAULT '' COMMENT 'Издательство',
  `commentary` varchar(250) NOT NULL DEFAULT '' COMMENT 'Комментарий',
  `date_start` date DEFAULT '0000-00-00' COMMENT 'Дата начала издания',
  `date_end` date DEFAULT '9999-00-00' COMMENT 'Дата окончания издания',
  `time_last_modified` datetime NOT NULL COMMENT 'Дата изменения',
  `time_added` datetime NOT NULL COMMENT 'Дата добавления',
  `visible` varchar(3) NOT NULL DEFAULT '' COMMENT 'Видимая, если пусто - видимая, cpr - копирайт, del - удаленная, dbl -дубль',
  `editable` int(11) DEFAULT 1 COMMENT 'Запрет на редактирование пользователям',
  `uid` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'ID пользователя',
  PRIMARY KEY (`s_id`)
) ENGINE=MyISAM AUTO_INCREMENT=332428 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Серии';
