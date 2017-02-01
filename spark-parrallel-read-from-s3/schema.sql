CREATE TABLE `TheTable` (
  `timestamp` int(11) NOT NULL,
  `year` varchar(4) DEFAULT NULL,
  `month` varchar(2) DEFAULT NULL,
  `day` varchar(2) DEFAULT NULL,
  `hour` varchar(2) DEFAULT NULL,
  `minute` varchar(2) DEFAULT NULL,
  `second` varchar(2) DEFAULT NULL,
  `timezone` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
