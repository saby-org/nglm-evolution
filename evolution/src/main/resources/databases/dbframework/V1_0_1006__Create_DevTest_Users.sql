USE `dbframework`;
SET NAMES utf8 ;

DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_proc_create_dev_test_users`()
BEGIN

  IF ('true' = 'DEV_OR_TEST' ) then
      
	ALTER TABLE `tbl_users` DISABLE KEYS;
    INSERT INTO `tbl_users` VALUES  (3,'USR-3','rob','61-21-54-93-B3-15-7E-A7-59-81-9F-35-5A-2F-24-F7-ED-F6-67-77','9999-01-01 00:00:00.000000','Mister','Rob','Francombe','sales','Evolving',NULL,NULL,NULL,NULL,'rob.francombe@evolving.com',NULL,1,'2018-11-20 00:00:00',1,'2018-11-22 00:00:00.000000',1,'2018-11-22 14:21:32.000000',0,1,1,0,1);
    INSERT INTO `tbl_users` VALUES  (4,'USR-4','oana','EE-A7-38-66-D8-9B-7B-6A-70-93-62-AE-75-3C-C2-8A-23-3B-A7-EE','9999-01-01 00:00:00.000000','Miss','Oana','Oros','dev','Evolving',NULL,NULL,NULL,NULL,'oana.oros@evolving.com',NULL,1,'2018-11-21 00:00:00',1,'2018-11-27 00:00:00.000000',1,'2018-11-27 14:37:10.000000',0,1,1,0,1);
    INSERT INTO `tbl_users` VALUES  (5,'USR-5','Ana','5D-75-87-83-B9-52-B1-CD-3F-60-32-4A-B0-2D-E2-DB-48-2C-D4-A8','9999-01-01 00:00:00.000000','Mister','Corovei','Ana','dev','Evolving',NULL,NULL,NULL,NULL,'Ana.Bota@evolving.com',NULL,1,'2018-11-22 00:00:00',1,'2018-11-28 00:00:00.000000',1,'2018-11-28 08:59:44.000000',0,1,1,0,1);
    INSERT INTO `tbl_users` VALUES  (6,'USR-6','cd','EE-A7-38-66-D8-9B-7B-6A-70-93-62-AE-75-3C-C2-8A-23-3B-A7-EE','2019-02-20 07:40:14.000000','Miss','Szidi','Huber','dev','Evolving',NULL,NULL,NULL,NULL,'szidi.huber@evolving.com',NULL,2,'2018-11-22 00:00:00',1,'2018-11-28 00:00:00.000000',1,'2018-11-28 11:00:13.000000',0,1,1,1,1);
    INSERT INTO `tbl_users` VALUES  (7,'USR-7','r_evo','22-77-C2-80-35-27-51-49-D0-1A-8D-E5-30-CC-13-B7-4F-59-ED-FB','2019-02-20 07:41:30.000000','Mister','Rob','Evo','test','Evolving',NULL,NULL,NULL,NULL,'reka.barra@evolving.com',NULL,1,'2018-11-22 00:00:00',1,'2018-11-28 00:00:00.000000',1,'2018-11-28 11:43:58.000000',0,1,1,1,1);
    INSERT INTO `tbl_users` VALUES  (8,'USR-8','annamaria.iuhos','5D-75-87-83-B9-52-B1-CD-3F-60-32-4A-B0-2D-E2-DB-48-2C-D4-A8','9999-01-01 00:00:00.000000','Miss','Annamaria','Iuhos','dev','Evolving',NULL,NULL,NULL,NULL,'annamaria.iuhos@evolving.com',NULL,1,'2018-11-22 00:00:00',1,'2018-11-28 00:00:00.000000',1,'2018-11-28 10:45:36.000000',0,1,1,0,1);
    INSERT INTO `tbl_users` VALUES  (9,'USR-9','sorin.anghelut','09-D7-84-4D-2E-8A-1A-BF-A7-2D-E4-65-38-15-3B-D6-79-BF-B8-24','9999-01-01 00:00:00.000000','Mister','anghelut','sorin','dev','Evolving',NULL,NULL,NULL,NULL,'sorin.anghelut@evolving.com',NULL,1,'2018-11-22 00:00:00',1,'2018-11-28 00:00:00.000000',1,'2018-11-28 09:51:37.000000',0,1,1,0,1);
    INSERT INTO `tbl_users` VALUES  (10,'USR-10','fouad.benkhalfallah','FA-FA-F3-64-B9-95-CB-6E-CD-B8-46-A9-99-EE-19-4E-A6-72-71-C2','9999-01-01 00:00:00.000000','Mister','Fouad','B','sales','Evolving',NULL,NULL,NULL,NULL,'fouad.benkhalfallah@evolving.com',NULL,1,'2018-11-22 00:00:00',1,'2018-11-26 00:00:00.000000',1,'2018-11-26 12:40:01.000000',0,1,1,0,1);
    INSERT INTO `tbl_users` VALUES  (11,'USR-11','paula.vaum','5D-75-87-83-B9-52-B1-CD-3F-60-32-4A-B0-2D-E2-DB-48-2C-D4-A8','2019-02-25 12:00:39.000000','Miss','Paula','Vaum','dev','Evolving',NULL,NULL,NULL,NULL,'paula.vaum@evolving.com',NULL,1,'2018-11-27 00:00:00',1,'2018-11-28 00:00:00.000000',1,'2018-11-28 10:04:14.000000',0,1,1,1,1);
    INSERT INTO `tbl_users` VALUES  (12,'USR-12','Irina','78-DA-EC-D2-43-0C-C6-FC-33-B1-FB-BC-C6-FE-05-87-5B-AD-02-7A','9999-01-01 00:00:00.000000','Miss','Bagut','Irina','dev','Evolving',NULL,NULL,NULL,NULL,'bagutirina@yahoo.com','81-12-EA-F1-18-A9-8C-BB-1A-CA-24-FD-37-9E-38-B4-C9-7A-40-DE',1,'2018-11-05 00:00:00',1,'2018-11-26 00:00:00.000000',1,'2018-11-26 15:40:27.000000',0,1,1,1,1);
    INSERT INTO `tbl_users` VALUES  (13,'USR-13','Christophe.Masson','16-5F-4E-0F-80-F3-80-FA-1A-B0-E2-7C-15-18-0C-F4-C8-73-94-A3','9999-01-01 00:00:00.000000','Mister','Christophe','Masson','dev','Evolving',NULL,NULL,NULL,NULL,'Christophe.Masson@evolving.com','81-12-EA-F1-18-A9-8C-BB-1A-CA-24-FD-37-9E-38-B4-C9-7A-40-DE',1,'2018-11-05 00:00:00',1,'2018-11-26 00:00:00.000000',1,'2018-11-26 15:40:27.000000',0,1,1,1,1);    
    INSERT INTO `tbl_users` VALUES  (14,'USR-14','bogdan.turlea','CB-A4-E5-45-B7-EC-91-81-29-72-51-54-B2-9F-05-5E-4C-D5-AE-A8','9999-01-01 00:00:00.000000','Mister','Bogdan','Turlea','dev','Evolving',NULL,NULL,NULL,NULL,'bogdan.turlea@evolving.com','81-12-EA-F1-18-A9-8C-BB-1A-CA-24-FD-37-9E-38-B4-C9-7A-40-DE',1,'2018-11-05 00:00:00',1,'2018-11-26 00:00:00.000000',1,'2018-11-26 15:40:27.000000',0,1,1,1,1);    
	ALTER TABLE `tbl_users` ENABLE KEYS ;


    ALTER TABLE `tbl_user_workgroups` DISABLE KEYS ;
    INSERT INTO `tbl_user_workgroups` VALUES (3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(10,1),(11,1),(12,1);
    ALTER TABLE `tbl_user_workgroups` ENABLE KEYS;

  END IF;
END ;;
DELIMITER ;

-- Execute the procedure
call `dbframework`.`stp_proc_create_dev_test_users`();

-- Drop the procedure
DROP PROCEDURE `dbframework`.`stp_proc_create_dev_test_users`;

