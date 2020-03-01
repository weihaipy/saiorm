-- MySQL dump 10.13  Distrib 5.7.26, for Win64 (x86_64)
--
-- Host: localhost    Database: saiorm_test
-- ------------------------------------------------------
-- Server version	5.7.26

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
-- Table structure for table `prefix_blog`
--

DROP TABLE IF EXISTS `prefix_blog`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `prefix_blog` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(10) unsigned NOT NULL,
  `content` text,
  `title` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `prefix_blog`
--

LOCK TABLES `prefix_blog` WRITE;
/*!40000 ALTER TABLE `prefix_blog` DISABLE KEYS */;
INSERT INTO `prefix_blog` VALUES (1,1,'content aaa','aaa'),(2,2,'content bbb','bbb'),(3,3,'content ccc','ccc'),(4,4,'content ddd','ddd');
/*!40000 ALTER TABLE `prefix_blog` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `prefix_login_log`
--

DROP TABLE IF EXISTS `prefix_login_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `prefix_login_log` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(10) unsigned NOT NULL DEFAULT '0',
  `login_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `prefix_login_log`
--

LOCK TABLES `prefix_login_log` WRITE;
/*!40000 ALTER TABLE `prefix_login_log` DISABLE KEYS */;
INSERT INTO `prefix_login_log` VALUES (1,1,'2020-01-01 00:00:00'),(2,1,'2020-01-01 10:00:00'),(3,2,'2020-01-01 11:00:00'),(4,3,'2010-01-02 00:00:00'),(5,2,'2020-01-02 07:00:00'),(6,4,'2020-01-02 07:00:00');
/*!40000 ALTER TABLE `prefix_login_log` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `prefix_user`
--

DROP TABLE IF EXISTS `prefix_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `prefix_user` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT NULL,
  `phone` varchar(100) DEFAULT NULL,
  `login_times` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `prefix_user`
--

LOCK TABLES `prefix_user` WRITE;
/*!40000 ALTER TABLE `prefix_user` DISABLE KEYS */;
INSERT INTO `prefix_user` VALUES (1,'zhangsan','13112345678',111),(2,'lisi','13212345678',222),(3,'wangwu','13312345678',333),(4,'maliu','13412345678',444);
/*!40000 ALTER TABLE `prefix_user` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-03-01  9:09:49
