CREATE DATABASE  IF NOT EXISTS `dbframework` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */;
USE `dbframework`;
-- MySQL dump 10.13  Distrib 8.0.13, for Win64 (x86_64)
--
-- Host: tst-linux    Database: dbframework
-- ------------------------------------------------------
-- Server version	8.0.12

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
 SET NAMES utf8 ;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `tbl_apps`
--

DROP TABLE IF EXISTS `tbl_apps`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tbl_apps` (
  `app_id` int(11) NOT NULL AUTO_INCREMENT,
  `app_key` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `app_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `app_description` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `app_logo` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `license_key` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `web_link` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `app_path` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `extra` longtext,
  PRIMARY KEY (`app_id`)
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tbl_apps`
--

LOCK TABLES `tbl_apps` WRITE;
/*!40000 ALTER TABLE `tbl_apps` DISABLE KEYS */;
INSERT INTO `tbl_apps` VALUES (1,'UM','Users Management','The Users Management System  enables you to create and manage users and workgroups. You can also define permissions that limit user access to applications functionalities.','fwk_components/styles/images/um.png',NULL,'FWK_URL_WEB_SERVER_PATH','/um',NULL),(2,'AM','Application Management','The Applications Management','fwk_components/styles/images/am.png',NULL,'FWK_URL_WEB_SERVER_PATH','/am',NULL);
/*!40000 ALTER TABLE `tbl_apps` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tbl_audit`
--

DROP TABLE IF EXISTS `tbl_audit`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tbl_audit` (
  `audit_log_id` bigint(20) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `log_date` datetime(6) DEFAULT NULL,
  `machine` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `param_json` longtext,
  PRIMARY KEY (`audit_log_id`),
  KEY `FK_tbl_audit_tbl_users` (`user_id`),
  CONSTRAINT `FK_tbl_audit_tbl_users` FOREIGN KEY (`user_id`) REFERENCES `tbl_users` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tbl_audit`
--

LOCK TABLES `tbl_audit` WRITE;
/*!40000 ALTER TABLE `tbl_audit` DISABLE KEYS */;
/*!40000 ALTER TABLE `tbl_audit` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tbl_imported_files`
--

DROP TABLE IF EXISTS `tbl_imported_files`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tbl_imported_files` (
  `file_id` int(11) NOT NULL AUTO_INCREMENT,
  `file_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `file_type` int(11) DEFAULT NULL,
  `file_path` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `user_id` int(11) NOT NULL,
  `file_date` datetime(6) NOT NULL,
  `result` longtext,
  PRIMARY KEY (`file_id`)
) ENGINE=InnoDB AUTO_INCREMENT=118 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tbl_imported_files`
--

LOCK TABLES `tbl_imported_files` WRITE;
/*!40000 ALTER TABLE `tbl_imported_files` DISABLE KEYS */;
/*!40000 ALTER TABLE `tbl_imported_files` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tbl_languages`
--

DROP TABLE IF EXISTS `tbl_languages`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tbl_languages` (
  `language_id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
  `language` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `language_code` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`language_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tbl_languages`
--

LOCK TABLES `tbl_languages` WRITE;
/*!40000 ALTER TABLE `tbl_languages` DISABLE KEYS */;
INSERT INTO `tbl_languages` VALUES (1,'English','en'),(2,'French','fr'),(3,'Arabic','ar'),(4,'Portuguese','pt'),(5,'Russian','ru');
/*!40000 ALTER TABLE `tbl_languages` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tbl_password_history`
--

DROP TABLE IF EXISTS `tbl_password_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tbl_password_history` (
  `history_id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `password` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `history_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`history_id`),
  KEY `FK_tbl_password_history_tbl_users` (`user_id`),
  CONSTRAINT `FK_tbl_password_history_tbl_users` FOREIGN KEY (`user_id`) REFERENCES `tbl_users` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tbl_password_history`
--

LOCK TABLES `tbl_password_history` WRITE;
/*!40000 ALTER TABLE `tbl_password_history` DISABLE KEYS */;
/*!40000 ALTER TABLE `tbl_password_history` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tbl_permissions`
--

DROP TABLE IF EXISTS `tbl_permissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tbl_permissions` (
  `permission_id` int(11) NOT NULL AUTO_INCREMENT,
  `permission_key` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `permission_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `permission_description` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `app_id` int(11) DEFAULT NULL,
  `folder` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`permission_id`),
  UNIQUE KEY `IX_tbl_CCMS_permissions` (`permission_key`),
  KEY `FK_tbl_permissions_tbl_apps` (`app_id`),
  CONSTRAINT `FK_tbl_permissions_tbl_apps` FOREIGN KEY (`app_id`) REFERENCES `tbl_apps` (`app_id`)
) ENGINE=InnoDB AUTO_INCREMENT=196 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tbl_permissions`
--

LOCK TABLES `tbl_permissions` WRITE;
/*!40000 ALTER TABLE `tbl_permissions` DISABLE KEYS */;
INSERT INTO `tbl_permissions` VALUES (1,'UM-ACCESS','Users Management Access ','Permission to access Users Management Module',1,NULL),(2,'UM-USERS-VIEW','View users list','Permission to view users list',1,'Users'),(3,'UM-USERS-EDIT','Add/Edit user','Permission to add/ edit a user',1,'Users'),(4,'UM-USERS-DELETE','Delete user','Permission to delete a user',1,'Users'),(5,'UM-WORKGROUPS-VIEW','View workgroups hierarchy','Permission to view workgroups hierarchy',1,'Workgroups'),(6,'UM-WORKGROUPS-EDIT','Add/Edit workgroup','Permission to add/ edit a workgroup',1,'Workgroups'),(7,'UM-WORKGROUPS-DELETE','Delete workgroup','Permission to delete a workgroup',1,'Workgroups'),(8,'UM-TOOLS','Tools','Permission to create/delete users from a file',1,'Users'),(9,'AM-ACCESS','Applications Management Access','Permission to access Applications Management',2,NULL);
/*!40000 ALTER TABLE `tbl_permissions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tbl_settings`
--

DROP TABLE IF EXISTS `tbl_settings`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tbl_settings` (
  `key` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `value` varchar(4000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tbl_settings`
--

LOCK TABLES `tbl_settings` WRITE;
/*!40000 ALTER TABLE `tbl_settings` DISABLE KEYS */;
/*!40000 ALTER TABLE `tbl_settings` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tbl_user_workgroups`
--

DROP TABLE IF EXISTS `tbl_user_workgroups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tbl_user_workgroups` (
  `user_id` int(11) NOT NULL,
  `workgroup_id` int(11) NOT NULL,
  PRIMARY KEY (`user_id`,`workgroup_id`),
  KEY `FK_tbl_user_workgroups_tbl_workgroups` (`workgroup_id`),
  CONSTRAINT `FK_tbl_user_workgroups_tbl_users` FOREIGN KEY (`user_id`) REFERENCES `tbl_users` (`user_id`),
  CONSTRAINT `FK_tbl_user_workgroups_tbl_workgroups` FOREIGN KEY (`workgroup_id`) REFERENCES `tbl_workgroups` (`workgroup_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tbl_user_workgroups`
--

LOCK TABLES `tbl_user_workgroups` WRITE;
/*!40000 ALTER TABLE `tbl_user_workgroups` DISABLE KEYS */;
INSERT INTO `tbl_user_workgroups` VALUES (1,1),(2,2);
/*!40000 ALTER TABLE `tbl_user_workgroups` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tbl_users`
--

DROP TABLE IF EXISTS `tbl_users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tbl_users` (
  `user_id` int(11) NOT NULL AUTO_INCREMENT,
  `user_key` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `login_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `password` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `password_expiry` datetime(6) DEFAULT NULL,
  `title` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `firstname` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `lastname` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `department` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `company` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_title` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `tel_office` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `tel_mobile` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `tel_out_of_hours` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `email_addr` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `reqistration_token` varchar(250) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `language_id` tinyint(3) unsigned DEFAULT NULL,
  `created_date` datetime DEFAULT CURRENT_TIMESTAMP,
  `created_by` int(11) DEFAULT NULL,
  `modified` datetime(6) DEFAULT NULL,
  `modified_by` int(11) DEFAULT NULL,
  `last_login` datetime(6) DEFAULT NULL,
  `retries` smallint(6) DEFAULT NULL,
  `enabled` tinyint(1) DEFAULT NULL,
  `reset_pass` tinyint(1) DEFAULT NULL,
  `expire_pass` tinyint(1) DEFAULT NULL,
  `visible` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1157 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tbl_users`
--

LOCK TABLES `tbl_users` WRITE;
/*!40000 ALTER TABLE `tbl_users` DISABLE KEYS */;
INSERT INTO `tbl_users` VALUES (1,'USR-1','administrator','E8-F9-7F-BA-91-04-D1-EA-50-47-94-8E-6D-FB-67-FA-CD-9F-5B-73','2099-11-05 00:00:00.000000','Mr','admin','administrator','admin','Evolving',NULL,NULL,NULL,NULL,'octavian.ratis@evolving.com','null',1,'2009-12-31 22:00:00',1,'2018-11-28 00:00:00.000000',1,'2018-11-28 12:34:47.000000',0,1,1,0,1),(2,'USR-2','3rdparty','9A-B6-1D-75-48-49-EE-03-3F-AF-1D-F2-2A-7D-4A-E7-87-61-D4-A7','9999-01-01 00:00:00.000000','Mister','3rdparty','3rdparty','3rdparty','3rdparty','3rdparty','','','','3rdparty@3rdparty.com',NULL,1,'2018-11-27 00:00:00',1,'2018-11-27 00:00:00.000000',1,'2018-11-27 14:23:16.000000',0,1,0,0,1);
/*!40000 ALTER TABLE `tbl_users` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tbl_users_password_recovery_tokens`
--

DROP TABLE IF EXISTS `tbl_users_password_recovery_tokens`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tbl_users_password_recovery_tokens` (
  `token_id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `token` varchar(250) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `token_request_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `token_expiry_date` datetime(6) NOT NULL,
  PRIMARY KEY (`token_id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tbl_users_password_recovery_tokens`
--

LOCK TABLES `tbl_users_password_recovery_tokens` WRITE;
/*!40000 ALTER TABLE `tbl_users_password_recovery_tokens` DISABLE KEYS */;
/*!40000 ALTER TABLE `tbl_users_password_recovery_tokens` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tbl_workgroups`
--

DROP TABLE IF EXISTS `tbl_workgroups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tbl_workgroups` (
  `workgroup_id` int(11) NOT NULL AUTO_INCREMENT,
  `workgroup_key` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `parent_workgroup_id` int(11) DEFAULT NULL,
  `workgroup_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `workgroup_description` longtext,
  `created_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by` int(11) NOT NULL,
  `enabled` tinyint(1) NOT NULL,
  PRIMARY KEY (`workgroup_id`),
  UNIQUE KEY `ix_tbl_workgroups_wokgroup_name` (`workgroup_name`),
  UNIQUE KEY `ix_tbl_workgroups_workgroup_key` (`workgroup_key`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tbl_workgroups`
--

LOCK TABLES `tbl_workgroups` WRITE;
/*!40000 ALTER TABLE `tbl_workgroups` DISABLE KEYS */;
INSERT INTO `tbl_workgroups` VALUES (1,'WKG-ADMIN',0,'Adminstrator workgroup ','All rights enabled...','2016-12-31 22:00:00',1,1),(2,'WKG-3RDPARTY',1,'3rdParty','3rdParty workgroup for API access','2018-11-26 10:12:53',1,1);
/*!40000 ALTER TABLE `tbl_workgroups` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tbl_workgroups_permissions`
--

DROP TABLE IF EXISTS `tbl_workgroups_permissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tbl_workgroups_permissions` (
  `workgroup_id` int(11) NOT NULL,
  `permission_id` int(11) NOT NULL,
  PRIMARY KEY (`workgroup_id`,`permission_id`),
  KEY `FK_tbl_workgroups_permissions_tbl_permissions` (`permission_id`),
  CONSTRAINT `FK_tbl_workgroups_permissions_tbl_permissions` FOREIGN KEY (`permission_id`) REFERENCES `tbl_permissions` (`permission_id`),
  CONSTRAINT `FK_tbl_workgroups_permissions_tbl_workgroups` FOREIGN KEY (`workgroup_id`) REFERENCES `tbl_workgroups` (`workgroup_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tbl_workgroups_permissions`
--

LOCK TABLES `tbl_workgroups_permissions` WRITE;
/*!40000 ALTER TABLE `tbl_workgroups_permissions` DISABLE KEYS */;
INSERT INTO `tbl_workgroups_permissions` VALUES (1,1),(2,1),(1,2),(2,2),(1,3),(1,4),(1,5),(2,5),(1,6),(1,7),(1,8),(1,9);
/*!40000 ALTER TABLE `tbl_workgroups_permissions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary view structure for view `vw_apps_permissions`
--

DROP TABLE IF EXISTS `vw_apps_permissions`;
/*!50001 DROP VIEW IF EXISTS `vw_apps_permissions`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_apps_permissions` AS SELECT 
 1 AS `app_id`,
 1 AS `app_key`,
 1 AS `app_name`,
 1 AS `permission_id`,
 1 AS `permission_key`,
 1 AS `permission_name`,
 1 AS `permission_description`,
 1 AS `folder`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary view structure for view `vw_users`
--

DROP TABLE IF EXISTS `vw_users`;
/*!50001 DROP VIEW IF EXISTS `vw_users`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_users` AS SELECT 
 1 AS `user_id`,
 1 AS `user_key`,
 1 AS `login_name`,
 1 AS `password`,
 1 AS `password_expiry`,
 1 AS `title`,
 1 AS `firstname`,
 1 AS `lastname`,
 1 AS `department`,
 1 AS `company`,
 1 AS `job_title`,
 1 AS `tel_office`,
 1 AS `tel_mobile`,
 1 AS `tel_out_of_hours`,
 1 AS `email_addr`,
 1 AS `reqistration_token`,
 1 AS `language_id`,
 1 AS `created_date`,
 1 AS `created_by`,
 1 AS `modified`,
 1 AS `modified_by`,
 1 AS `last_login`,
 1 AS `retries`,
 1 AS `enabled`,
 1 AS `reset_pass`,
 1 AS `expire_pass`,
 1 AS `visible`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary view structure for view `vw_workgroup_join_all_permissions`
--

DROP TABLE IF EXISTS `vw_workgroup_join_all_permissions`;
/*!50001 DROP VIEW IF EXISTS `vw_workgroup_join_all_permissions`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_workgroup_join_all_permissions` AS SELECT 
 1 AS `app_id`,
 1 AS `app_key`,
 1 AS `app_name`,
 1 AS `permission_id`,
 1 AS `permission_key`,
 1 AS `permission_name`,
 1 AS `permission_description`,
 1 AS `folder`,
 1 AS `workgroup_id`,
 1 AS `parent_workgroup_id`,
 1 AS `workgroup_name`,
 1 AS `checked`*/;
SET character_set_client = @saved_cs_client;

--
-- Dumping events for database 'dbframework'
--

--
-- Dumping routines for database 'dbframework'
--
/*!50003 DROP FUNCTION IF EXISTS `SET_COUNT` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` FUNCTION `SET_COUNT`($strlist BLOB) RETURNS smallint(5) unsigned
    NO SQL
    DETERMINISTIC
RETURN 1+CHAR_LENGTH($strlist)-CHAR_LENGTH(REPLACE($strlist,',','')) ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `SET_EXTRACT` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` FUNCTION `SET_EXTRACT`($i SMALLINT UNSIGNED, $strlist BLOB) RETURNS varbinary(255)
    NO SQL
    DETERMINISTIC
RETURN NULLIF(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(0b0, ',', $strlist, ",", 0b0), ",", $i+1.5*(SIGN($i+0.5)+1)-1), ',', -SIGN($i+0.5)),0b0) ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `split_string_into_rows` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` FUNCTION `split_string_into_rows`($split_string_into_rows BLOB) RETURNS blob
    NO SQL
    DETERMINISTIC
RETURN IF($split_string_into_rows IS NULL, IFNULL(@split_string_into_rows,""), "1"|@split_string_into_rows:=$split_string_into_rows) ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_add_app` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_add_app`(in app_key nchar(10),in app_name nvarchar(100),in app_description nvarchar(500),in app_logo nvarchar(500),in license_key nvarchar(500),in web_link nvarchar(500),in app_path nvarchar(500),in extra nvarchar(4000))
BEGIN
	insert into tbl_apps
	(`app_key`,`app_name`,`app_description`,`app_logo`,`license_key`,`web_link`,`app_path`,`extra`)
	values
	(app_key,app_name,app_description,app_logo,license_key,web_link,app_path,extra);
    
	select LAST_INSERT_ID() as app_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_add_app_permission` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_add_app_permission`(in permission_key nvarchar(50),in permission_name nvarchar(50),in permission_description nvarchar(500),in app_id int(11),in folder nvarchar(50))
BEGIN
 insert into tbl_permissions
    (`permission_key`,
	`permission_name`,
	`permission_description`,
	`app_id`,
	`folder`
    )
    values
    (permission_key,
	permission_name,
	permission_description,
	app_id,
	folder
    );

    select LAST_INSERT_ID() as permission_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_add_file` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_add_file`(in file_name nvarchar(255),in file_type int,in file_path nvarchar(500),in user_id int(11),in file_date datetime(6),in result LONGTEXT)
begin
	  insert into tbl_imported_files
	  (`file_name`,`file_type`,`file_path`,`user_id`,`file_date`,`result`)
	  values
	  (file_name,file_type,file_path,user_id,file_date,result);
	  select LAST_INSERT_ID() as file_id;
end ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_add_password_history` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_add_password_history`(in user_id int(11),in password nvarchar(59),in history_date timestamp)
BEGIN
	insert into tbl_password_history
	(`user_id`,`password`,`history_date`)
	values
	(user_id,password,history_date);
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_add_permission` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_add_permission`(in permission_key nvarchar(50),in permission_name nvarchar(50),in permission_description nvarchar(500))
BEGIN
	insert into tbl_permissions
	(`permission_key`,`permission_name`,`permission_description`)
	select permission_key,permission_key,permission_description
	from tbl_permissions a
	where not exists
	  (
		 select tbl_permissions.`permission_id`
		 from tbl_permissions
		 where tbl_permissions.`permission_key` = permission_key
	  );
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_add_user` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_add_user`(in login_name nvarchar(100),
in password nvarchar(100),
in title nvarchar(20),
in firstname nvarchar(50),
in lastname nvarchar(50),
in department nvarchar(30),
in company nvarchar(30),
in job_title nvarchar(20),
in tel_office nvarchar(20),
in tel_mobile nvarchar(20),
in tel_out_of_hours nvarchar(20),
in email_addr nvarchar(100),
in reqistration_token nvarchar(250),
in language_id tinyint(3),
in created_by int(11),
in enabled tinyint(1),
in workgroup_id int(11),
in password_expiry datetime,
in reset_pass tinyint(1),
in expire_pass tinyint(1)
)
BEGIN
DECLARE userid int;
DECLARE createddate datetime;
declare wid int;
Select IFNULL(workgroup_id,0) into wid;

SELECT CURDATE() into createddate;

IF(SELECT count(*) FROM tbl_users WHERE tbl_users.`login_name` = login_name and tbl_users.`visible`=1) < 1 THEN

	SET password_expiry = IFNULL(password_expiry, '9999-01-01');

	INSERT INTO `tbl_users`    
	(`user_key`,
    `login_name`,
	`password`,
	`password_expiry`,
	`title`,
	`firstname`,
	`lastname`,
	`department`,
	`company`,
	`job_title`,
	`tel_office`,
	`tel_mobile`,
	`tel_out_of_hours`,
	`email_addr`,
	`reqistration_token`,
	`language_id`,
	`created_date`,
	`created_by`,
	`modified`,
	`modified_by`,
	`last_login`,
	`retries`,
	`enabled`,
	`reset_pass`,
	`expire_pass`,
	`visible`)
	values
    (login_name,    
	login_name,
	password,
	password_expiry,
	title,
	firstname,
	lastname,
	department,
	company,
	job_title,
	tel_office,
	tel_mobile,
	tel_out_of_hours,
	email_addr,
	reqistration_token,
	language_id,
	createddate,
	created_by,
	createddate,
	created_by,
	null,
	0,
	enabled,
	reset_pass,
	expire_pass,
	1
    );
	select LAST_INSERT_ID() into userid;
	update tbl_users
		set
		    tbl_users.`user_key` = CONCAT('USR-', userid)
	   where tbl_users.`user_id` = userid;
       
     IF wid>0 then 
			 insert into tbl_user_workgroups
			 (`user_id`,
			  `workgroup_id`
			 )
			 values
			 (userid,
			  wid
			 );
	 END IF;
	select userid as user_id;
END IF;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_add_users_password_recovery_tokens` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_add_users_password_recovery_tokens`(in user_id int(11),in token nvarchar(250),in token_request_date timestamp,in token_expiry_date  datetime(6))
BEGIN
	insert into tbl_users_password_recovery_tokens
	(`user_id`,`token`,`token_request_date`,`token_expiry_date`)
	values
	(user_id,token,token_request_date,token_expiry_date);
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_add_user_workgroup` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_add_user_workgroup`(in user_id int(11),in workgroup_id int(11))
BEGIN
IF(SELECT count(*) FROM tbl_user_workgroups WHERE tbl_user_workgroups.`user_id` = user_id and tbl_user_workgroups.`workgroup_id` = workgroup_id) < 1 THEN
	insert into tbl_user_workgroups
	(`user_id`,`workgroup_id`)
	values
	(user_id,workgroup_id);
END IF;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_add_workgroup` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_add_workgroup`(in workgroup_name nvarchar(100),
in workgroup_key         nvarchar(50),
in parent_workgroup_id   int(11),
in created_by            int(11),
in enabled               tinyint(1),
in workgroup_description LONGTEXT,
in permission_list       LONGTEXT
)
BEGIN
declare workgroupid int;
DECLARE createddate datetime;
declare word nvarchar(20);
declare wordsremain LONGTEXT;

SELECT CURDATE() into createddate;
 
IF(SELECT count(*) FROM tbl_workgroups WHERE tbl_workgroups.`workgroup_name` = workgroup_name ) < 1 THEN
	    insert into tbl_workgroups
			 (parent_workgroup_id,
			  workgroup_key,
			  workgroup_name,
			  created_date,
			  created_by,
			  enabled,
			  workgroup_description
			 )
		VALUES(parent_workgroup_id,
						workgroup_key,
						workgroup_name,
						createddate,
						created_by,
						enabled,
						workgroup_description);

		Select LAST_INSERT_ID() into workgroupid ;
 
		CREATE TEMPORARY TABLE tablepermissions (workgroupid int, permissionid int ) ENGINE=MEMORY; 

		SET wordsremain =  permission_list;
				 
		WHILE LENGTH(wordsremain)>0
		DO
			SET word = SUBSTRING_INDEX(wordsremain, ",", 1);
			SET wordsremain =  SUBSTRING(wordsremain, LENGTH(word)+2);
			insert into tablepermissions 
			(workgroupid,permissionid)
			value (workgroupid,CAST(word AS UNSIGNED));
		END WHILE;

		select * from tablepermissions;
        
        insert into tbl_workgroups_permissions
        (workgroup_id,permission_id)
		select workgroupid as workgroup_id,permissionid as permission_id
		from tablepermissions;

		drop table tablepermissions;
END IF;

select workgroupid as workgroup_id;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_add_workgroups_permission` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_add_workgroups_permission`(in workgroup_id  int(11),in permission_id int(11))
BEGIN
IF(select count(*) from tbl_workgroups_permissions where tbl_workgroups_permissions.workgroup_id = workgroup_id and tbl_workgroups_permissions.permission_id = permission_id) < 1 THEN
	insert into tbl_workgroups_permissions
	(`workgroup_id`,`permission_id`)
	values
	(workgroup_id,permission_id);
END IF;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_delete_app` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_delete_app`(in app_id int(11),in force_delete tinyint(1))
BEGIN

SET force_delete = IFNULL(force_delete, 0);

IF (force_delete = 0 ) THEN
	delete from tbl_apps where tbl_apps.`app_id` = app_id ;
ELSE 
    delete from tbl_workgroups_permissions where tbl_workgroups_permissions.permission_id in (select tbl_permissions.`permission_id` from tbl_permissions where tbl_permissions.`app_id` = app_id);

    delete from tbl_permissions where tbl_permissions.`app_id` = app_id;

    delete from tbl_apps where tbl_apps.`app_id` = app_id;
END IF;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_delete_app_permission` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_delete_app_permission`(in permission_id int(11),in force_delete tinyint(1))
BEGIN

SET force_delete = IFNULL(force_delete, 0);

IF (force_delete = 0 ) THEN
	delete from tbl_permissions where tbl_permissions.permission_id = permission_id;
ELSE
	delete from tbl_workgroups_permissions where tbl_workgroups_permissions.permission_id = permission_id;
    delete from tbl_permissions where tbl_permissions.permission_id = permission_id;
END IF;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_delete_user` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_delete_user`(in user_id int(11))
BEGIN
	declare uid int;
    Select user_id into uid;
    
	update tbl_users
	set tbl_users.`visible` = 0,
	tbl_users.`login_name` = CONCAT('delete=',tbl_users.`user_id`,'=',SUBSTRING(tbl_users.`login_name`, 1, 82))
	where tbl_users.`user_id` = uid;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_delete_user_workgroup` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_delete_user_workgroup`(in user_id int(11),in workgroup_id int(11))
BEGIN

SET workgroup_id = IFNULL(workgroup_id, 0);

IF (workgroup_id > 0 ) THEN
	delete from tbl_user_workgroups where tbl_user_workgroups.`user_id` = user_id and tbl_user_workgroups.`workgroup_id` = workgroup_id;
ELSE
	delete from tbl_user_workgroups where tbl_user_workgroups.`user_id` = user_id;
END IF;
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_delete_workgroup` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_delete_workgroup`(in workgroup_id int(11))
BEGIN
	delete from tbl_workgroups where tbl_workgroups.`workgroup_id` = workgroup_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_delete_workgroups_permission` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_delete_workgroups_permission`(in workgroup_id int(11),in permission_id int(11))
BEGIN
	delete from tbl_workgroups_permissions
    where tbl_workgroups_permissions.`workgroup_id` = workgroup_id and tbl_workgroups_permissions.`permission_id` = permission_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_expire_recovery_token` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_expire_recovery_token`(in token_id int(11))
BEGIN	
	delete from tbl_users_password_recovery_tokens where tbl_users_password_recovery_tokens.`token_id` = token_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_app` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_app`(in app_id int(11))
BEGIN
	select `app_id`,
		  `app_key`,
		  `app_name`,
		  `app_description`,
		  `app_logo`,
		  `license_key`,
		  `web_link`,
		  `app_path`,
		  `extra`
	from tbl_apps
	where tbl_apps.`app_id` = app_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_apps` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_apps`()
BEGIN
	select app_id,
		  app_key,
		  app_name,
		  app_description,
		  app_logo,
		  license_key,
		  web_link,
		  app_path,
		  extra
	from tbl_apps;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_apps_by_user_id` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_apps_by_user_id`(in user_id int(11))
begin
select a.app_id,
	  app_key,
	  a.app_name,
	  a.app_description,
	  a.app_logo,
	  license_key,
	  web_link,
	  app_path,
	  extra
from tbl_apps a 
	join tbl_permissions b on a.`app_id` = b.`app_id`
	join tbl_workgroups_permissions c  on b.`permission_id` = c.`permission_id`
	join tbl_user_workgroups d on c.`workgroup_id` = d.`workgroup_id`
where d.`user_id` = user_id
group by a.`app_id`,
	    a.`app_key`,
	    a.`app_name`,
	    a.`app_description`,
	    a.`app_logo`,
		a.`license_key`,
	    a.`web_link`,
	    a.`app_path`,
	    a.`extra`;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_apps_by_workgroup_id` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_apps_by_workgroup_id`(in workgroup_id int(11))
BEGIN

declare xlevel_depth int;
declare xlevel_depth2 int;
declare xworkgroup_id int;
select workgroup_id into xworkgroup_id;

CREATE TEMPORARY TABLE tableresult
    (
	    app_id                 int(11),
	    app_key                nvarchar(10),
	    app_name               nvarchar(100),
	    permission_id          int(11),
	    permission_key         nvarchar(50),
	    permission_name        nvarchar(50),
	    permission_description nvarchar(500),
	    folder                 nvarchar(50),
	    workgroup_id           int(11),
	    parent_workgroup_id    int(11),
	    workgroup_name         nvarchar(100),
	    checked                tinyint(1),
	    checked_parent         tinyint(1)
)ENGINE=MEMORY;
  
CREATE TEMPORARY TABLE tabletree
(
	workgroup_id        int,
	parent_workgroup_id int,
	level_depth         int
) ENGINE=MEMORY;

CREATE TEMPORARY TABLE tablewpp
(
	permission_id int,
	checked tinyint(1),
	checked_parent tinyint(1)
) ENGINE=MEMORY;

insert into tabletree
(workgroup_id,parent_workgroup_id,level_depth)
with RECURSIVE items( workgroup_id,parent_workgroup_id,level_depth)
as (
	select v.`workgroup_id`,
		  v.`parent_workgroup_id`,
		  1 as level_depth
	from tbl_workgroups v
	where v.`workgroup_id` = xworkgroup_id
	union all
	select a.`workgroup_id`,
		  a.`parent_workgroup_id`,
		  1 + IFNULL(b.`level_depth`,1) as level_depth
	from tbl_workgroups a
	inner join `items` b on a.`workgroup_id` = b.`parent_workgroup_id`
)
select workgroup_id,parent_workgroup_id,IFNULL(level_depth,1) as level_depth
from items;

select MAX(tabletree.`level_depth`) INTO xlevel_depth from tabletree;
select xlevel_depth into xlevel_depth2;

insert into tablewpp
(permission_id,checked,checked_parent)
select a.`permission_id`,
		    a.`checked`,
		    1 as checked_parent
	  from vw_workgroup_join_all_permissions a
		  join tabletree b on a.`workgroup_id` = b.`workgroup_id`
	  where b.`level_depth` = xlevel_depth;
set xlevel_depth = xlevel_depth - 1;
WHILE xlevel_depth >= 1 
DO
	   update tablewpp t
		   join vw_workgroup_join_all_permissions a on t.`permission_id` = a.`permission_id`
		   join tabletree b on a.`workgroup_id` = b.`parent_workgroup_id`
		set
		    t.`checked_parent` = case when t.`checked_parent`= 0 then 0
								else
									case when b.`parent_workgroup_id` is not null
									   then a.`checked`	
								     else
                                     0
                                     end
							     end
	    where b.`level_depth` = xlevel_depth; 

	   set xlevel_depth = xlevel_depth - 1;
END WHILE;

insert into tableresult 
(app_id ,app_key ,app_name,permission_id,permission_key,permission_name,
permission_description ,folder,workgroup_id,parent_workgroup_id,workgroup_name , checked ,checked_parent)
select a.`app_id`,
    a.`app_key`,
    a.`app_name`,
    a.`permission_id`,
    a.`permission_key`,
    a.`permission_name`,
    a.`permission_description`,
    a.`folder`,
    a.`workgroup_id`,
    a.`parent_workgroup_id`,
    a.`workgroup_name`,
    a.`checked`,
	  case
		 when t.`checked` = t.`checked_parent`
			 and b.`parent_workgroup_id` is not null
			then t.`checked`
		 when b.`parent_workgroup_id` is  not null
			then t.`checked_parent`
		 else 0
	  end as checked_parent
from vw_workgroup_join_all_permissions a
	join tablewpp t on t.`permission_id` = a.`permission_id`
	join tabletree b on a.`workgroup_id` = b.`workgroup_id`
where b.`level_depth` = xlevel_depth2;

delete from tableresult where `checked` = 0 or `checked_parent` = 0;

select a.`app_id`,
		 a.`app_key`,
		 a.`app_name`,
		 a.`app_description`,
		 a.`app_logo`,
		 `license_key`,
		 `web_link`,
		 `app_path`,
		 `extra`
    from tbl_apps a 
	    join tbl_permissions b  on a.`app_id` = b.`app_id`
	    join tbl_workgroups_permissions c on b.`permission_id` = c.`permission_id`
	    join tableresult e on b.`permission_id` = e.`permission_id`
    where c.`workgroup_id` = xworkgroup_id 
    and e.`checked_parent` = 1 and e.`checked` = 1
    group by a.`app_id`,
		   a.`app_key`,
		   a.`app_name`,
		   a.`app_description`,
		   a.`app_logo`,
		   `license_key`,
		   `web_link`,
		   `app_path`,
		   `extra`;
drop table tabletree;
drop table tablewpp;
drop table tableresult;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_app_for_export` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_app_for_export`(in app_id int(11))
BEGIN
    select a.app_key,
		 a.app_name,
		 a.app_description,
		 a.app_logo,
		 a.license_key,
		 a.web_link,
		 a.app_path,
		 a.extra,
		 b.permission_key,
		 b.permission_name,
		 b.permission_description,
		 b.folder
    from tbl_apps a
	join tbl_permissions b on a.`app_id` = b.`app_id`
    where a.`app_id` = app_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_app_permissions` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_app_permissions`(in app_id int(11))
BEGIN
 select `permission_id`,
		 `permission_key`,
		 `permission_name`,
		 `permission_description`,
		 `folder`,
		 a.`app_id`,
		 b.`app_key`,
		 b.`app_name`
    from tbl_permissions a 
	    join tbl_apps b on a.`app_id` = b.`app_id`
    where a.`app_id` = app_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_files` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_files`()
BEGIN
   select file_id,
		file_name,
		file_type,
		file_path,
		user_id,
		file_date
   from tbl_imported_files;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_file_by_id` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_file_by_id`(in file_id int(11))
BEGIN
	select `file_id`,
			`file_name`,
			`file_type`,
			`file_path`,
			`user_id`,
			`file_date`,
			`result`
	   from tbl_imported_files
	where tbl_imported_files.`file_id` = file_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_language` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_language`(in language_id tinyint(3))
BEGIN
	select `language_id`,
		  `language`,
		  `language_code`
	from tbl_languages
	where tbl_languages.`language_id` = language_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_languages` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_languages`()
BEGIN
	select language_id,
		  `language`,
		  language_code
	from tbl_languages;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_password_history` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_password_history`(in user_id int,in topx int)
BEGIN
    PREPARE STMT FROM 
	"select history_id,user_id,password,history_date from tbl_password_history where user_id = ? order by history_id desc LIMIT ?"; 
	SET @userid = user_id; 
	SET @topno = topx; 
	EXECUTE STMT USING @userid, @topno;
	DEALLOCATE PREPARE STMT;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_permission` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_permission`(in permission_id int)
BEGIN
	select permission_id,permission_key,permission_name,permission_description,folder
	from tbl_permissions
	where tbl_permissions.`permission_id` = permission_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_permissions` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_permissions`()
BEGIN
	select `permission_id`,
		  permission_key,
		  permission_name,
		  permission_description,
		  folder,
		  a.app_id,
		  b.app_key,
		  b.app_name
	from tbl_permissions a
		join tbl_apps b on a.`app_id` = b.`app_id`;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_setting` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_setting`(in _key nvarchar(50))
BEGIN
	select `value`,
		  `key`
	from tbl_settings
	where tbl_settings.`key` = _key;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_settings` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_settings`()
BEGIN
	select `key`,
		  `value`
	from tbl_settings;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_user` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_user`(in user_id int(11))
BEGIN

declare uid int;
Select user_id into uid;

	select a.`user_id`,
	  user_key,
	  `login_name`,
	  `password`,
	  password_expiry,
	  title,
	  firstname,
	  lastname,
	  department,
	  company,
	  job_title,
	  tel_office,
	  tel_mobile,
	  tel_out_of_hours,
	  email_addr,
	  language_id,
	  created_date,
	  created_by,
	  modified,
	  modified_by,
	  last_login,
	  retries,
	  enabled,
	  reset_pass,
	  expire_pass,
	  `visible`,
	  reqistration_token,
	  b.workgroup_id
	from tbl_users a
	left join tbl_user_workgroups b on a.`user_id` = b.`user_id`
	where a.`user_id` = uid
	and a.`visible` = 1;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_users` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_users`(in user_id int)
BEGIN

DECLARE  workgroup_id int;
SELECT a.`workgroup_id` into workgroup_id
from tbl_user_workgroups a 
	join tbl_workgroups b on a.`workgroup_id` = b.`workgroup_id`
	join tbl_users c on c.`user_id` = a.`user_id`
where a.`user_id` = user_id
	 and c.`visible` = 1;

with RECURSIVE childworkgroups(workgroup_id,parent_workgroup_id,workgroup_name)
	as (
	select w.`workgroup_id`,
		  parent_workgroup_id,
		  workgroup_name
	from tbl_workgroups w
	where w.`parent_workgroup_id` = workgroup_id
	union all
	select a.`workgroup_id`,
		  a.parent_workgroup_id,
		  a.workgroup_name
	from tbl_workgroups a
		inner join childworkgroups b on a.`parent_workgroup_id` = b.`workgroup_id`)
	select a.`user_id`,
		  a.firstname,
		  a.lastname,
		  a.login_name,
		  a.enabled,
		  a.email_addr,
		  c.workgroup_id,
		  c.workgroup_name
	from tbl_users a
		join tbl_user_workgroups b on a.`user_id` = b.`user_id`
		join childworkgroups c on b.`workgroup_id` = c.`workgroup_id`
	where a.`visible` = 1
	union
	select a.`user_id`,
		  a.firstname,
		  a.lastname,
		  a.login_name,
		  a.enabled,
		  a.email_addr,
		  -1 as workgroup_id,
		  'No Workgroup' as workgroup_name
	from tbl_users a
		left join tbl_user_workgroups b on a.`user_id` = b.`user_id`
	where a.`visible` = 1
		 and b.`workgroup_id` is null
	union
	select a.`user_id`,
		  a.firstname,
		  a.lastname,
		  a.login_name,
		  a.enabled,
		  a.email_addr,
		  c.`workgroup_id`,
		  c.workgroup_name
	from tbl_users a
		join tbl_user_workgroups b on a.`user_id` = b.`user_id`
		join tbl_workgroups c on b.`workgroup_id` = c.`workgroup_id`
	where a.visible = 1
		 and c.`workgroup_id` = workgroup_id
	order by 5 desc;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_users_count` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_users_count`(in user_id int(11))
BEGIN

declare xworkgroup_id int;
declare xlevel_depth int;
declare xcount_all_users int;
declare uid int(11);
declare  workgroup_id int;

SELECT user_id into uid;

SELECT a.`workgroup_id` into workgroup_id
FROM tbl_user_workgroups a 
	join tbl_workgroups b on a.`workgroup_id` = b.`workgroup_id`
	join tbl_users c on c.`user_id` = a.`user_id`
where a.`user_id` = uid
	 and c.`visible` = 1;

CREATE TEMPORARY TABLE tablewppu
(
	user_id int
) ENGINE=MEMORY;

insert into tablewppu
(user_id)	
with RECURSIVE childworkgroups(workgroup_id,parent_workgroup_id,workgroup_name)
	as (
	SELECT w.`workgroup_id`,
		  parent_workgroup_id,
		  workgroup_name
	FROM tbl_workgroups w
	where w.`parent_workgroup_id` = workgroup_id
	union all
	select a.`workgroup_id`,
		  a.parent_workgroup_id,
		  a.workgroup_name
	FROM tbl_workgroups a
		inner join childworkgroups b on a.`parent_workgroup_id` = b.`workgroup_id`)
	SELECT a.`user_id` 
	FROM tbl_users a
		join tbl_user_workgroups b on a.`user_id` = b.`user_id`
		join childworkgroups c on b.`workgroup_id` = c.`workgroup_id`
	where a.`visible` = 1
	union
	SELECT a.`user_id`
	FROM tbl_users a
		left join tbl_user_workgroups b on a.`user_id` = b.`user_id`
	where a.`visible` = 1
		 and b.`workgroup_id` is null
	union
	SELECT a.`user_id`
	FROM tbl_users a
		join tbl_user_workgroups b on a.`user_id` = b.`user_id`
		join tbl_workgroups c on b.`workgroup_id` = c.`workgroup_id`
	where a.visible = 1
		 and c.`workgroup_id` = workgroup_id;

SELECT COUNT(DISTINCT tablewppu.user_id) into xcount_all_users  from tablewppu;
SELECT xcount_all_users as count_hierarchy_users;
drop table tablewppu;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_user_by_email` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_user_by_email`(in email_addr nvarchar(100))
BEGIN
	select `user_id`,
	  user_key,
	  login_name,
	  `password`,
	  password_expiry,
	  title,
	  firstname,
	  lastname,
	  department,
	  company,
	  job_title,
	  tel_office,
	  tel_mobile,
	  tel_out_of_hours,
	  `email_addr`,
	  language_id,
	  created_date,
	  created_by,
	  modified,
	  modified_by,
	  last_login,
	  retries,
	  enabled,
	  reset_pass,
	  expire_pass,
	  `visible`
	from tbl_users
	where tbl_users.`email_addr` = email_addr
			and `visible` = 1;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_user_by_login_name` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_user_by_login_name`(in login_name nvarchar(100))
BEGIN
Declare lname nvarchar(100);
Select login_name into lname;

	select a.`user_id`,
	  `user_key`,
	  a.`login_name`,
	  `password`,
	  password_expiry,
	  title,
	  firstname,
	  lastname,
	  department,
	  company,
	  job_title,
	  tel_office,
	  tel_mobile,
	  tel_out_of_hours,
	  email_addr,
	  language_id,
	  created_date,
	  created_by,
	  modified,
	  modified_by,
	  last_login,
	  retries,
	  enabled,
	  reset_pass,
	  expire_pass,
	  `visible`,
	  b.`workgroup_id`
	from tbl_users a
	left join tbl_user_workgroups b on a.`user_id` = b.`user_id`
	where a.`login_name` = lname 
	 and `visible` = 1;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_user_by_recovery_token` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_user_by_recovery_token`(in token nvarchar(250))
BEGIN
	select token_id,
	  a.user_id,
	  token,
	  token_request_date,
	  token_expiry_date,
	  b.login_name,
	  b.`password`,
	  b.firstname,
	  b.lastname,
	  b.enabled,
	  b.reset_pass,
	  b.`visible`
	from tbl_users_password_recovery_tokens a
	join tbl_users b on a.`user_id` = b.`user_id`
	where a.`token` = token and b.`visible` = 1;
    
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_user_by_reqistration_token` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_user_by_reqistration_token`(reqistration_token nvarchar(250))
BEGIN
	select a.`user_id`,
	  user_key,
	  login_name,
	  `password`,
	  password_expiry,
	  title,
	  firstname,
	  lastname,
	  department,
	  company,
	  job_title,
	  tel_office,
	  tel_mobile,
	  tel_out_of_hours,
	  email_addr,
	  `reqistration_token`,
	  language_id,
	  created_date,
	  created_by,
	  modified,
	  modified_by,
	  last_login,
	  retries,
	  enabled,
	  reset_pass,
	  expire_pass,
	  `visible`,
	  b.`workgroup_id`
	from tbl_users a
	left join tbl_user_workgroups b on a.`user_id` = b.`user_id`
	where a.`reqistration_token` = reqistration_token
	 and a.`visible` = 1;
     
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_user_permissions` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_user_permissions`(in user_id int)
BEGIN

declare xworkgroup_id int;
declare xlevel_depth int;
declare xlevel_depth2 int;
declare uid int;
Select user_id into uid;

select a.`workgroup_id` INTO xworkgroup_id from tbl_user_workgroups a where a.`user_id` = uid;
set @workgroup_id:=xworkgroup_id;

CREATE TEMPORARY TABLE  tableresult
(
	app_id                 int null,
	app_key                nchar(10) null,
	app_name               nvarchar(100) null,
	permission_id          int null,
	permission_key         nvarchar(50) null,
	permission_name        nvarchar(50) null,
	permission_description nvarchar(500) null,
	folder                 nvarchar(50) null,
	workgroup_id           int null,
	parent_workgroup_id    int null,
	workgroup_name         nvarchar(100) not null,
	checked                tinyint(1) null,
	checked_parent         tinyint(1) null
) ENGINE=MEMORY;

CREATE TEMPORARY TABLE tabletree 
(
	workgroup_id        int,
	parent_workgroup_id int,
	level_depth         int
) ENGINE=MEMORY;

insert into tabletree
(workgroup_id,parent_workgroup_id,level_depth)
with RECURSIVE items( workgroup_id,parent_workgroup_id,level_depth)
as (
	select v.workgroup_id,
		  v.parent_workgroup_id,
		  1 as level_depth
	from tbl_workgroups v
	where v.`workgroup_id` = xworkgroup_id
	union all
	select a.`workgroup_id`,
		  a.`parent_workgroup_id`,
		  1 + IFNULL(b.`level_depth`,1) as level_depth
	from `tbl_workgroups` a
	inner join `items` b on a.`workgroup_id` = b.`parent_workgroup_id`
)
select items.`workgroup_id`,items.`parent_workgroup_id`,IFNULL(level_depth,1) as level_depth
from items;

CREATE TEMPORARY TABLE tablewpp
(
	permission_id  int null,
	checked        tinyint(1) null,
	checked_parent tinyint(1) null
) ENGINE=MEMORY;

select MAX(level_depth) INTO xlevel_depth from tabletree;
select xlevel_depth into xlevel_depth2;

insert into tablewpp
	  select a.`permission_id`,
		    a.`checked`,
		    1 as checked_parent
	  from vw_workgroup_join_all_permissions a
		  join tabletree b on a.`workgroup_id` = b.`workgroup_id`
	  where b.`level_depth` = xlevel_depth;
      
set xlevel_depth = xlevel_depth - 1;
WHILE xlevel_depth >= 1
DO
	   update tablewpp t
		   join vw_workgroup_join_all_permissions a on t.permission_id = a.permission_id
		   join tabletree b on a.workgroup_id = b.parent_workgroup_id
		set
		    t.`checked_parent` = case when t.`checked_parent`= 0 then 0
								else
									case when b.`parent_workgroup_id` is not null
									   then a.`checked`
								     else 
                                     0
                                     end
							     end
	    where b.level_depth = xlevel_depth; 

	   set xlevel_depth = xlevel_depth - 1;
END WHILE;

insert into tableresult
(app_id,
    app_key,
    app_name,
    permission_id,
    permission_key,
    permission_name,
    permission_description,
    folder,
    workgroup_id,
    parent_workgroup_id,
    workgroup_name,
    checked,
    checked_parent)
select a.app_id,
    a.app_key,
    a.app_name,
    a.permission_id,
    a.permission_key,
    a.permission_name,
    a.permission_description,
    a.folder,
    a.workgroup_id,
    a.parent_workgroup_id,
    a.workgroup_name,
    a.checked,
	  case
		 when t.`checked` = t.`checked_parent`
			 and b.`parent_workgroup_id` is not null
			then t.`checked`
		 when b.`parent_workgroup_id` is null
			then t.`checked_parent`
		 else 0
	  end as checked_parent
from vw_workgroup_join_all_permissions a
	join tablewpp t on t.`permission_id` = a.`permission_id`
	join tabletree b on a.`workgroup_id` = b.`workgroup_id`
where b.`level_depth` = xlevel_depth2;

delete from tableresult
where tableresult.`checked` = 0 or tableresult.`checked_parent` = 0;

select distinct
	  a.permission_id,
	  a.permission_key,
	  a.permission_name,
	  a.permission_description
from tbl_permissions a
	join tbl_workgroups_permissions b on a.`permission_id` = b.`permission_id`
	join tbl_user_workgroups c on b.`workgroup_id` = c.`workgroup_id`
	join tbl_workgroups d on b.`workgroup_id` = d.`workgroup_id`
	join tableresult e on a.`permission_id` = e.`permission_id`
where c.`user_id` = uid
	 and d.`enabled` = 1;

drop table tableresult;
drop table tabletree;
drop table tablewpp;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_user_workgroup` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_user_workgroup`(in user_id int)
BEGIN
	select a.`user_id`,
		  a.`workgroup_id`
	from tbl_user_workgroups a
		join tbl_users b on a.`user_id` = b.`user_id`
	where a.`user_id` = user_id
		 and b.`visible` = 1;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_user_workgroups` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_user_workgroups`()
BEGIN
	select a.`user_id`,
		  workgroup_id
	from tbl_user_workgroups
		join tbl_users b on a.`user_id` = b.`user_id`
	where b.`visible` = 1;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_user_workgroup_hierarchy` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_user_workgroup_hierarchy`(in user_id int(11))
BEGIN

declare wid int;
declare uid int;
select user_id into uid;

SELECT a.`workgroup_id` into wid
from tbl_user_workgroups a 
join tbl_workgroups b  on a.`workgroup_id` = b.`workgroup_id`
join tbl_users c  on c.`user_id`= a.`user_id`
where a.`user_id` = uid  and c.`visible` = 1;

select a.`workgroup_id`,
		 a.workgroup_name,
		 a.workgroup_key
    from tbl_workgroups a
    where a.`workgroup_id` = workgroup_id;

with RECURSIVE items( workgroup_id,parent_workgroup_id,workgroup_key,workgroup_name)
	    as (
	    select w.`workgroup_id`,
			 w.parent_workgroup_id,
			 w.workgroup_key,
			 w.workgroup_name
	    from tbl_workgroups w
	    where w.`workgroup_id` = wid
	    union all
	    select a.`workgroup_id`,
			 a.parent_workgroup_id,
			 a.workgroup_key,
			 a.workgroup_name
	    from tbl_workgroups a
		    inner join items b on a.`workgroup_id` = b.`parent_workgroup_id`)
	    select ip.`workgroup_id`,
			 ip.workgroup_name,
			 ip.workgroup_key
	    from items ip
	    where ip.`workgroup_id` <> wid;

with RECURSIVE items( workgroup_id,parent_workgroup_id,workgroup_key,workgroup_name)
	    as (
	    select w.`workgroup_id`,
			 w.parent_workgroup_id,
			 w.workgroup_key,
			 w.workgroup_name
	    from tbl_workgroups w
	    where w.`parent_workgroup_id` = wid
	    union all
	    select a.`workgroup_id`,
			 a.parent_workgroup_id,
			 a.workgroup_key,
			 a.workgroup_name
	    from tbl_workgroups a
		    inner join items b on a.`parent_workgroup_id` = b.`workgroup_id`)
	    select ic.`workgroup_id`,
			 ic.workgroup_name,
			 ic.workgroup_key
	    from items ic;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_workgroup` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_workgroup`(in workgroup_id int)
BEGIN
	select
	a.`workgroup_id`,
	a.workgroup_name,
	a.workgroup_key,
	c.`workgroup_id` as parent_workgroup_id,
	c.workgroup_name as parent_workgroup_name,
	a.created_date,
	a.created_by,
	a.enabled,
	a.workgroup_description,
	b.firstname,
	b.lastname,
	b.email_addr,
	b.login_name
	from tbl_workgroups a
	join tbl_users b on a.`created_by` = b.`user_id`
	left join tbl_workgroups c on a.`parent_workgroup_id` = c.`workgroup_id`
	where a.`workgroup_id` = workgroup_id;
    
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_workgroups` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_workgroups`(in user_id int(11))
BEGIN

declare xworkgroup_id int;
declare xlevel_depth int;
declare xcount_all_wokgroups int;
declare uid int;
select user_id into uid;
select a.`workgroup_id` INTO xworkgroup_id from tbl_user_workgroups a where a.`user_id` = uid;

with RECURSIVE items( workgroup_id,parent_workgroup_id,level_depth)
	as (
	select `workgroup_id`,
		  `parent_workgroup_id`,
		  1 as level_depth
	from tbl_workgroups
	where tbl_workgroups.`workgroup_id` = xworkgroup_id
	union all
	select a.`workgroup_id`,
		  a.`parent_workgroup_id`,
		  b.level_depth + 1 as level_depth
	from tbl_workgroups a
		inner join items b on a.`parent_workgroup_id` = b.`workgroup_id`),
	usercount (workgroup_id,cnt)
	as (select `workgroup_id`,
			 COUNT(*) as cnt
	    from tbl_user_workgroups a
		    join tbl_users b on a.`user_id` = b.`user_id`
	    where b.`visible`= 1
	    group by `workgroup_id`)
	select b.workgroup_id,
		  IFNULL(b.`parent_workgroup_id`, 0) as parent_workgroup_id,
		  b.workgroup_name,
		  b.workgroup_key,
		  b.workgroup_description,
		  b.created_date,
		  b.created_by,
		  b.enabled,
		  a.level_depth,
		  IFNULL(c.cnt, 0) as number_of_users_in_this_workgroup
	from items a
		join tbl_workgroups b on a.`workgroup_id` = b.`workgroup_id`
		left join usercount c on a.`workgroup_id` = c.`workgroup_id`
	order by a.`level_depth` asc,
		    2 asc,
		    1 asc;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_workgroups_count` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_workgroups_count`(in user_id int(11))
BEGIN

declare xworkgroup_id int;
declare xlevel_depth int;
declare xcount_all_wokgroups int;
declare uid int;

select user_id into uid;

select a.`workgroup_id` INTO xworkgroup_id from tbl_user_workgroups a where a.`user_id` = uid;

with RECURSIVE items( workgroup_id,parent_workgroup_id,level_depth)
	as (
	select `workgroup_id`,
		  `parent_workgroup_id`,
		  1 as level_depth
	from tbl_workgroups
	where tbl_workgroups.`workgroup_id` = xworkgroup_id
	union all
	select a.`workgroup_id`,
		  a.`parent_workgroup_id`,
		  b.level_depth + 1 as level_depth
	from tbl_workgroups a
		inner join items b on a.`parent_workgroup_id` = b.`workgroup_id`),
	usercount (workgroup_id,cnt)
	as (select `workgroup_id`,
			 COUNT(*) as cnt
	    from tbl_user_workgroups a
		    join tbl_users b on a.`user_id` = b.`user_id`
	    where b.`visible`= 1
	    group by `workgroup_id`)
	select COUNT(DISTINCT b.workgroup_id) into xcount_all_wokgroups
	from items a
		join tbl_workgroups b on a.`workgroup_id` = b.`workgroup_id`
		left join usercount c on a.`workgroup_id` = c.`workgroup_id`;
        
select xcount_all_wokgroups as count_hierarchy_workgroups;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_workgroups_enabled` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_workgroups_enabled`()
BEGIN
	select a.workgroup_id,
		  a.workgroup_name,
		  a.created_date,
		  a.created_by,
		  a.enabled,
		  a.workgroup_description
	from tbl_workgroups a
	where a.`enabled` = 1;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_workgroup_by_key` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_workgroup_by_key`(in workgroup_key nvarchar(40))
BEGIN
	select
	a.workgroup_id,
	a.workgroup_name,
	a.workgroup_key,
	a.created_date,
	a.created_by,
	a.enabled,
	a.workgroup_description
	from tbl_workgroups a
	where a.`workgroup_key` = workgroup_key;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_workgroup_by_name` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_workgroup_by_name`(in workgroup_name nvarchar(40))
BEGIN
	select
	a.`workgroup_id`,
	a.`workgroup_name`,
	a.`workgroup_key`,
	a.`created_date`,
	a.`created_by`,
	a.`enabled`,
	a.`workgroup_description`
	from tbl_workgroups a 
	where a.`workgroup_name` = workgroup_name;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_workgroup_hierarchy` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_workgroup_hierarchy`(in workgroup_id int(11))
BEGIN

declare wid int;
select workgroup_id into wid;

select a.`workgroup_id`,
		 a.workgroup_name,
		 a.workgroup_key
    from tbl_workgroups a
    where a.`workgroup_id` = wid;

with RECURSIVE items( workgroup_id,parent_workgroup_id,workgroup_key,workgroup_name)
	    as (
	    select w.workgroup_id,
			 w.parent_workgroup_id,
			 w.workgroup_key,
			 w.workgroup_name
	    from tbl_workgroups w
	    where w.`workgroup_id` = wid
	    union all
	    select a.`workgroup_id`,
			 a.parent_workgroup_id,
			 a.workgroup_key,
			 a.workgroup_name
	    from tbl_workgroups a
		    inner join items b on a.`workgroup_id` = b.`parent_workgroup_id`)
	    select ip.`workgroup_id`,
			 ip.workgroup_name,
			 ip.workgroup_key
	    from items ip
	    where ip.`workgroup_id` <> wid;

with RECURSIVE items( workgroup_id,parent_workgroup_id,workgroup_key,workgroup_name)
	    as (
	    select w.`workgroup_id`,
			 w.`parent_workgroup_id`,
			 w.`workgroup_key`,
			 w.`workgroup_name`
	    from tbl_workgroups w
	    where w.`parent_workgroup_id` = wid
	    union all
	    select a.`workgroup_id`,
			 a.`parent_workgroup_id`,
			 a.`workgroup_key`,
			 a.`workgroup_name`
	    from tbl_workgroups a
		    inner join items b on a.`parent_workgroup_id` = b.`workgroup_id`)
	    select ic.`workgroup_id`,
			 ic.`workgroup_name`,
			 ic.`workgroup_key`
	    from items ic;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_workgroup_permissions` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_workgroup_permissions`(in workgroup_id int)
BEGIN

declare xlevel_depth int;
declare xlevel_depth2 int;
declare xworkgroup_id int;
select workgroup_id into xworkgroup_id;

CREATE TEMPORARY TABLE tabletree 
(
	workgroup_id        int,
	parent_workgroup_id int,
	level_depth         int
) ENGINE=MEMORY;

CREATE TEMPORARY TABLE tablewpp 
(
	permission_id int,
	checked tinyint(1),
	checked_parent tinyint(1)
) ENGINE=MEMORY;

insert into tabletree
(workgroup_id,parent_workgroup_id,level_depth)
with RECURSIVE items( workgroup_id,parent_workgroup_id,level_depth)
as (
	select v.`workgroup_id`,
		  v.`parent_workgroup_id`,
		  1 as level_depth
	from tbl_workgroups v
	where v.`workgroup_id` = xworkgroup_id
	union all
	select a.`workgroup_id`,
		  a.`parent_workgroup_id`,
		  1 + IFNULL(b.`level_depth`,1) as level_depth
	from tbl_workgroups a
	inner join `items` b on a.`workgroup_id` = b.`parent_workgroup_id`
)
select workgroup_id,parent_workgroup_id,IFNULL(level_depth,1) as level_depth
from items;

select MAX(`level_depth`) INTO xlevel_depth from tabletree;
select xlevel_depth into xlevel_depth2;

insert into tablewpp
(permission_id,checked,checked_parent) 
select a.`permission_id`,
		    a.`checked`,
		    1 as checked_parent
	  from vw_workgroup_join_all_permissions a
		  join tabletree b on a.`workgroup_id` = b.`workgroup_id`
	  where b.`level_depth` = xlevel_depth;
set xlevel_depth = xlevel_depth - 1;
WHILE xlevel_depth >= 1 
DO
	   update tablewpp t
		   join vw_workgroup_join_all_permissions a on t.`permission_id` = a.`permission_id`
		   join tabletree b on a.`workgroup_id` = b.`parent_workgroup_id`
		set
		    t.`checked_parent` = case when t.`checked_parent`= 0 then 0
								else
									case when b.`parent_workgroup_id` is not null
									   then a.`checked`
								     else 
                                     0
                                     end
							     end
	    where b.`level_depth` = xlevel_depth;

	   set xlevel_depth = xlevel_depth - 1;
END WHILE;

select a.`app_id`,
    a.`app_key`,
    a.`app_name`,
    a.`permission_id`,
    a.`permission_key`,
    a.`permission_name`,
    a.`permission_description`,
    a.`folder`,
    a.`workgroup_id`,
    a.`parent_workgroup_id`,
    a.`workgroup_name`,
    a.`checked`,
	  case
		 when t.`checked` = t.`checked_parent`
			 and b.`parent_workgroup_id` is not null
			then t.`checked`
		 when b.`parent_workgroup_id` is  not null
			then t.`checked_parent`
		 else 0
	  end as checked_parent
from vw_workgroup_join_all_permissions a
	join tablewpp t on t.`permission_id` = a.`permission_id`
	join tabletree b on a.`workgroup_id` = b.`workgroup_id`
where b.`level_depth` = xlevel_depth2;

drop table tabletree;
drop table tablewpp;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_get_workgroup_users` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_get_workgroup_users`(in workgroup_id int)
BEGIN

declare wid int;
select workgroup_id into wid;
 
	select
	a.`user_id`,
	a.`workgroup_id`
	from tbl_user_workgroups a
	join tbl_users b on a.`user_id` = b.`user_id`
	where a.`workgroup_id` = wid AND b.visible = 1;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_update_app` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_update_app`(in app_id int,in app_key nchar(10),in app_name nvarchar(100),in app_description nvarchar(500),in app_logo nvarchar(500),in license_key nvarchar(500),in web_link nvarchar(500),in app_path nvarchar(500),in extra longtext)
BEGIN
    update tbl_apps
	 set
		tbl_apps.`app_key` = app_key,
		tbl_apps.`app_name` = app_name,
		tbl_apps.`app_description` = app_description,
		tbl_apps.`app_logo` = app_logo,
		tbl_apps.`license_key` = license_key,
		tbl_apps.`web_link` = web_link,
		tbl_apps.`app_path` = app_path,
		tbl_apps.`extra` = extra
    where tbl_apps.`app_id` = app_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_update_app_permission` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_update_app_permission`(in permission_id int,in permission_key nvarchar(50),in permission_name nvarchar(50),in permission_description nvarchar(500),in folder nvarchar(50))
BEGIN
    update tbl_permissions
	 set
		tbl_permissions.`permission_key` = permission_key,
		tbl_permissions.`permission_name` = permission_name,
		tbl_permissions.`permission_description` = permission_description,
		tbl_permissions.`folder` = folder
    where tbl_permissions.`permission_id` = permission_id;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_update_setting` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_update_setting`(in _key nvarchar(50),in _value nvarchar(4000))
BEGIN
IF exists(select * from settings.tbl_settings where `key` = _key) THEN
	update tbl_settings
	set `value` = _value
	where `key` = _key;
ELSE       
	insert into tbl_settings
	(`key`,`value`)
	values
	(_key,_value);
END IF;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_update_user` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_update_user`(
in user_id int(11),
in user_key nvarchar(50),
in login_name nvarchar(100),
in password nvarchar(100),
in password_expiry datetime ,
in title nvarchar(20) ,
in firstname nvarchar(50) ,
in lastname nvarchar(50) ,
in department nvarchar(30) ,
in company nvarchar(30) ,
in job_title nvarchar(30) ,
in tel_office nvarchar(20) ,
in tel_mobile nvarchar(20) ,
in tel_out_of_hours nvarchar(20) ,
in email_addr nvarchar(100),
in language_id tinyint(3),
in register tinyint(1),
in modified_by int(11),
in last_login datetime ,
in retries smallint,
in enabled tinyint(1),
in reset_pass tinyint(1),
in expire_pass tinyint(1),
in visible tinyint(1),
in workgroup_id int(11))
BEGIN

Declare uid int;
Declare wid int;
Declare cid int;
DECLARE createddate datetime;

SELECT CURDATE() into createddate;
SELECT user_id into uid;
SELECT workgroup_id into wid;

IF(SELECT count(*) FROM tbl_users WHERE tbl_users.`login_name` = login_name and tbl_users.`visible`=1 and tbl_users.`user_id` <> uid) < 1 THEN
	update tbl_users
	set
	 tbl_users.`user_key` = IFNULL(user_key,tbl_users.`user_key`),
	 tbl_users.`login_name` = IFNULL(login_name,tbl_users.`login_name`),
	 tbl_users.`password` = IFNULL(password,tbl_users.`password`),
	 tbl_users.`password_expiry` = IFNULL(password_expiry,tbl_users.`password_expiry`),
	 tbl_users.`title` = IFNULL(title,tbl_users.`title`),
	 tbl_users.`firstname` = IFNULL(firstname,tbl_users.`firstname`),
	 tbl_users.`lastname` = IFNULL(lastname,tbl_users.`lastname`),
	 tbl_users.`department` = IFNULL(department,tbl_users.`department`),
	 tbl_users.`company` = IFNULL(company,tbl_users.`company`),
	 tbl_users.`job_title` = IFNULL(job_title,tbl_users.`job_title`),
	 tbl_users.`tel_office` = IFNULL(tel_office,tbl_users.`tel_office`),
	 tbl_users.`tel_mobile` = IFNULL(tel_mobile,tbl_users.`tel_mobile`),
	 tbl_users.`tel_out_of_hours` = IFNULL(tel_out_of_hours,tbl_users.`tel_out_of_hours`),
	 tbl_users.`email_addr` = IFNULL(email_addr,tbl_users.`email_addr`),
	 tbl_users.`reqistration_token` = case
						   when register = 1
							  then null
						   else reqistration_token
					    end,
	 tbl_users.`language_id` = IFNULL(language_id,tbl_users.`language_id`),
	 tbl_users.`modified` = createddate,
	 tbl_users.`modified_by` = IFNULL(modified_by,tbl_users.`modified_by`),
	 tbl_users.`last_login` = IFNULL(last_login,tbl_users.`last_login`),
	 tbl_users.`retries` = IFNULL(retries,tbl_users.`retries`),
	 tbl_users.`enabled` = IFNULL(enabled,tbl_users.`enabled`),
	 tbl_users.`reset_pass` = IFNULL(reset_pass,tbl_users.`reset_pass`),
	 tbl_users.`expire_pass` = IFNULL(expire_pass,tbl_users.`expire_pass`),
	 tbl_users.`visible` = IFNULL(visible,tbl_users.`visible`)
	 where tbl_users.`user_id` = uid
	 and tbl_users.`visible` = 1;
     
    IF IFNULL(visible, 1) = 0 THEN
	   update tbl_users
		set
		    tbl_users.`visible` = 0,
		    tbl_users.`enabled` = 0,
		    tbl_users.`login_name` = CONCAT('delete=',tbl_users.`user_id`,'_',SUBSTRING(tbl_users.`login_name`, 1, 92))
	   where tbl_users.`user_id` = uid;
	END IF;

    delete from tbl_user_workgroups where tbl_user_workgroups.`user_id` = uid;
    
	IF IFNULL(wid , 0) > 0 THEN
		insert into tbl_user_workgroups
	    (`user_id`,`workgroup_id`)
		values
		(uid,wid);
	END IF;
END IF;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_update_user_workgroup` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_update_user_workgroup`(in user_id int,in workgroup_id int)
BEGIN
 IF EXISTS(select * from tbl_user_workgroups where tbl_user_workgroups.`user_id` = user_id) THEN
	   update tbl_user_workgroups
		set tbl_user_workgroups.`workgroup_id` = workgroup_id
	   where tbl_user_workgroups.`user_id` = user_id;
ELSE
	   insert into tbl_user_workgroups
	   (workgroup_id,user_id)
	   values
	   (workgroup_id,user_id);
END IF;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stp_update_workgroup` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_update_workgroup`(in workgroup_id int,
in workgroup_name 		 nvarchar(100),
in workgroup_key         nvarchar(50),
in enabled               tinyint(1),
in workgroup_description LONGTEXT,
in permission_list       LONGTEXT,
in parent_workgroup_id   int(11)
)
BEGIN
declare word nvarchar(20);
declare wordsremain LONGTEXT;
declare wid int;
select workgroup_id into wid;

IF(SELECT count(*) FROM tbl_workgroups WHERE tbl_workgroups.`workgroup_name` = workgroup_name and tbl_workgroups.`workgroup_id` <> workgroup_id ) < 1 THEN
	
		update tbl_workgroups
		  set
			 tbl_workgroups.`workgroup_name` = IFNULL(workgroup_name, tbl_workgroups.`workgroup_name`),
			 tbl_workgroups.`workgroup_key` = IFNULL(workgroup_key, tbl_workgroups.`workgroup_key`),
			 tbl_workgroups.`enabled` = IFNULL(enabled, tbl_workgroups.`enabled`),
			 tbl_workgroups.`workgroup_description` = IFNULL(workgroup_description, tbl_workgroups.`workgroup_description`),
			 tbl_workgroups.`parent_workgroup_id` = IFNULL(parent_workgroup_id, tbl_workgroups.`parent_workgroup_id`)
		where tbl_workgroups.`workgroup_id` = workgroup_id;
			
		delete from tbl_workgroups_permissions where tbl_workgroups_permissions.`workgroup_id` = wid;

		CREATE TEMPORARY TABLE tablepermissions (workgroupid int, permissionid int ) ENGINE=MEMORY;

		SET wordsremain =  permission_list;
				 
		WHILE LENGTH(wordsremain)>0
		DO
			SET word = SUBSTRING_INDEX(wordsremain, ",", 1);
			SET wordsremain =  SUBSTRING(wordsremain, LENGTH(word)+2);
			insert into tablepermissions 
			(workgroupid,permissionid)
			value (wid,CAST(word AS UNSIGNED));
		END WHILE;

        insert into tbl_workgroups_permissions
        (workgroup_id,permission_id)
		select workgroupid as workgroup_id,permissionid as permission_id
		from tablepermissions;

		drop table tablepermissions;
END IF;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Final view structure for view `vw_apps_permissions`
--

/*!50001 DROP VIEW IF EXISTS `vw_apps_permissions`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `vw_apps_permissions` AS select `a`.`app_id` AS `app_id`,`a`.`app_key` AS `app_key`,`a`.`app_name` AS `app_name`,`b`.`permission_id` AS `permission_id`,`b`.`permission_key` AS `permission_key`,`b`.`permission_name` AS `permission_name`,`b`.`permission_description` AS `permission_description`,`b`.`folder` AS `folder` from (`tbl_apps` `a` join `tbl_permissions` `b` on((`a`.`app_id` = `b`.`app_id`))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vw_users`
--

/*!50001 DROP VIEW IF EXISTS `vw_users`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `vw_users` AS select `tbl_users`.`user_id` AS `user_id`,`tbl_users`.`user_key` AS `user_key`,`tbl_users`.`login_name` AS `login_name`,`tbl_users`.`password` AS `password`,`tbl_users`.`password_expiry` AS `password_expiry`,`tbl_users`.`title` AS `title`,`tbl_users`.`firstname` AS `firstname`,`tbl_users`.`lastname` AS `lastname`,`tbl_users`.`department` AS `department`,`tbl_users`.`company` AS `company`,`tbl_users`.`job_title` AS `job_title`,`tbl_users`.`tel_office` AS `tel_office`,`tbl_users`.`tel_mobile` AS `tel_mobile`,`tbl_users`.`tel_out_of_hours` AS `tel_out_of_hours`,`tbl_users`.`email_addr` AS `email_addr`,`tbl_users`.`reqistration_token` AS `reqistration_token`,`tbl_users`.`language_id` AS `language_id`,`tbl_users`.`created_date` AS `created_date`,`tbl_users`.`created_by` AS `created_by`,`tbl_users`.`modified` AS `modified`,`tbl_users`.`modified_by` AS `modified_by`,`tbl_users`.`last_login` AS `last_login`,`tbl_users`.`retries` AS `retries`,`tbl_users`.`enabled` AS `enabled`,`tbl_users`.`reset_pass` AS `reset_pass`,`tbl_users`.`expire_pass` AS `expire_pass`,`tbl_users`.`visible` AS `visible` from `tbl_users` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vw_workgroup_join_all_permissions`
--

/*!50001 DROP VIEW IF EXISTS `vw_workgroup_join_all_permissions`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `vw_workgroup_join_all_permissions` AS select `a`.`app_id` AS `app_id`,`a`.`app_key` AS `app_key`,`a`.`app_name` AS `app_name`,`a`.`permission_id` AS `permission_id`,`a`.`permission_key` AS `permission_key`,`a`.`permission_name` AS `permission_name`,`a`.`permission_description` AS `permission_description`,`a`.`folder` AS `folder`,`w`.`workgroup_id` AS `workgroup_id`,`w`.`parent_workgroup_id` AS `parent_workgroup_id`,`w`.`workgroup_name` AS `workgroup_name`,(case when (ifnull(`wa`.`permission_id`,0) = 0) then 0 else 1 end) AS `checked` from ((`vw_apps_permissions` `a` join `tbl_workgroups` `w` on((1 = 1))) left join `tbl_workgroups_permissions` `wa` on(((`w`.`workgroup_id` = `wa`.`workgroup_id`) and (`a`.`permission_id` = `wa`.`permission_id`)))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-11-28 14:13:33
