CREATE TABLE `stock_article` (
  `url` varchar(25) NOT NULL,
  `title` varchar(80) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `date` varchar(10) DEFAULT NULL,
  `push` varchar(5) DEFAULT NULL,
  `arrow` varchar(5) DEFAULT NULL,
  `boo` varchar(5) DEFAULT NULL,
  `last_push_time` varchar(15) DEFAULT NULL,
  `update_time` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`url`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
