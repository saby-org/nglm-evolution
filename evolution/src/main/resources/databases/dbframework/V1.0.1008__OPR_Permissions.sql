USE `dbframework`;
SET NAMES utf8 ;

DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_proc_install_OPR`()
BEGIN

  IF ('true' = 'OPR_INSTALL' ) then

INSERT INTO `dbframework`.`tbl_apps`(`app_key`,`app_name`,`app_description`,`app_logo`,`web_link`,`app_path`)  
SELECT * FROM (SELECT 'OPR' as `app_key`,'Operations' as `app_name`,'Operations Reports' as `app_description`,'fwk_components/styles/images/am.png','OPR_URL_WEB_SERVER_PATH' as `web_link`,'/opr' as `app_path`) AS tmp WHERE NOT EXISTS (SELECT `app_key` FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'OPR') LIMIT 1;
SELECT `app_id` INTO @app_id FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'OPR';
SELECT `workgroup_id` INTO @wid from `dbframework`.`tbl_workgroups` WHERE `workgroup_key`='WKG-ADMIN';
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'OPR-ACCESS' as  `permission_key` ,'Applications Access' as `permission_name`,'Permission to access Applications' as `permission_description` ,@app_id as `app_id`,null as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'OPR-ACCESS') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='OPR-ACCESS';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;

  END IF;
END ;;
DELIMITER ;

-- Execute the procedure
call `dbframework`.`stp_proc_install_OPR`();

-- Drop the procedure
DROP PROCEDURE `dbframework`.`stp_proc_install_OPR`;