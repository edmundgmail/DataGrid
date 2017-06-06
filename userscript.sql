-- MySQL dump 10.13  Distrib 5.1.73, for redhat-linux-gnu (x86_64)
--
-- Host: localhost    Database: metadata_ddp
-- ------------------------------------------------------
-- Server version	5.1.73

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `userscript`
--

DROP TABLE IF EXISTS `userscript`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `userscript` (
  `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `cdate` datetime NOT NULL,
  `owner` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `version` int(11) NOT NULL,
  `content` longtext CHARACTER SET utf8 NOT NULL,
  `filename` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `serial` smallint(6) NOT NULL,
  PRIMARY KEY (`owner`,`name`,`version`,`filename`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `userscript`
--

LOCK TABLES `userscript` WRITE;
/*!40000 ALTER TABLE `userscript` DISABLE KEYS */;
INSERT INTO `userscript` VALUES ('b','2017-06-05 10:13:37','a',1,'content','d',0),('b','2017-06-05 10:16:42','a',2,'content','d',0),('report1','2017-06-05 06:44:08','guoe2',1,'case class report1(a:String,b:String)','report.scala',1),('report2','2017-06-05 06:44:17','guoe2',1,'case class report2(a:String,b:String)','report.scala',1),('report3','2017-06-05 06:44:24','guoe2',1,'case class report3(a:String,b:String)','report.scala',1),('report1','2017-06-05 06:44:36','guoe3',1,'case class report1(a:String,b:String)','report.scala',1),('report1','2017-06-05 06:44:42','guoe4',1,'case class report1(a:String,b:String)','report.scala',1),('report2','2017-06-05 06:44:51','guoe4',1,'case class report2(a:String,b:String)','report.scala',1);
/*!40000 ALTER TABLE `userscript` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2017-06-05 21:14:11
