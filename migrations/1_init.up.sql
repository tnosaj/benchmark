CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` varchar(36) NOT NULL,
  PRIMARY KEY (`id`),
  index (`user_id`)
) ENGINE=InnoDB;