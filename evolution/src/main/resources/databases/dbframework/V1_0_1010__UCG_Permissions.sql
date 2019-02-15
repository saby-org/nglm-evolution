USE `dbframework`;
SET NAMES utf8 ;

DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_proc_install_UCG`()
BEGIN

 IF ('true' = 'UCG_INSTALL' ) then

INSERT INTO `dbframework`.`tbl_apps`(`app_key`,`app_name`,`app_description`,`app_logo`,`web_link`,`app_path`)  
SELECT * FROM (SELECT 'UCG' as `app_key`,'Subscriber Base Management' as `app_name`,'Universal Control Group' as `app_description`,'fwk_components/styles/images/am.png','UCG_URL_WEB_SERVER_PATH' as `web_link`,'/' as `app_path`) AS tmp WHERE NOT EXISTS (SELECT `app_key` FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'UCG') LIMIT 1;
SELECT `app_id` INTO @app_id FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'UCG';
SELECT `workgroup_id` INTO @wid from `dbframework`.`tbl_workgroups` WHERE `workgroup_key`='WKG-ADMIN';
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'UCG-ACCESS' as  `permission_key` ,'Access' as `permission_name`,'Permission to access UCG module' as `permission_description` ,@app_id as `app_id`,null as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'UCG-ACCESS') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='UCG-ACCESS';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'UCG-RULES-ADD-EDIT' as  `permission_key` ,'Add/edit a rule' as `permission_name`,'Permission to add/edit a rule' as `permission_description` ,@app_id as `app_id`,'Rules' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'UCG-RULES-ADD-EDIT') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='UCG-RULES-ADD-EDIT';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'UCG-RULES-View' as  `permission_key` ,'Rules View' as `permission_name`,'Permission to view rules module' as `permission_description` ,@app_id as `app_id`,'Rules' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'UCG-RULES-View') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='UCG-RULES-View';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'UCG-RULES-DELETE' as  `permission_key` ,'Dele a rule' as `permission_name`,'Permission to delete a rule' as `permission_description` ,@app_id as `app_id`,'Rules' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'UCG-RULES-DELETE') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='UCG-RULES-DELETE';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'UCG-CALCULATION' as  `permission_key` ,'Ucg Calculation' as `permission_name`,'Permission to access UCG Calculation' as `permission_description` ,@app_id as `app_id`,null as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'UCG-CALCULATION') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='UCG-CALCULATION';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;

 END IF;
END ;;
DELIMITER ;

-- Execute the procedure
call `dbframework`.`stp_proc_install_UCG`();

-- Drop the procedure
DROP PROCEDURE `dbframework`.`stp_proc_install_UCG`;