USE `dbframework`;
SET NAMES utf8 ;

DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_proc_install_IAR`()
BEGIN

  IF ('true' = 'IAR_INSTALL' ) then
 
	INSERT INTO `dbframework`.`tbl_apps`(`app_key`,`app_name`,`app_description`,`app_logo`,`web_link`,`app_path`)  
	SELECT * FROM (SELECT 'IAR' as `app_key`,'InsightsAnalytics' as `app_name`,'Insights Analytics Reports' as `app_description`,'fwk_components/styles/images/am.png','IAR_URL_WEB_SERVER_PATH' as `web_link`,'/iar' as `app_path`) AS tmp WHERE NOT EXISTS (SELECT `app_key` FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'IAR') LIMIT 1;
	SELECT `app_id` INTO @app_id FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'IAR';
	SELECT `workgroup_id` INTO @wid from `dbframework`.`tbl_workgroups` WHERE `workgroup_key`='WKG-ADMIN';
	INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'IAR-ACCESS' as  `permission_key` ,'Applications Access' as `permission_name`,'Permission to access Applications' as `permission_description` ,@app_id as `app_id`,null as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'IAR-ACCESS') LIMIT 1;
	SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='IAR-ACCESS';
	INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1; 
  
  END IF;
END ;;
DELIMITER ;

-- Execute the procedure
call `dbframework`.`stp_proc_install_IAR`();

-- Drop the procedure
DROP PROCEDURE `dbframework`.`stp_proc_install_IAR`;