USE `dbframework`;
SET NAMES utf8 ;

DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_proc_install_STG`()
BEGIN

 IF ('true' = 'STG_INSTALL' ) then

INSERT INTO `dbframework`.`tbl_apps`(`app_key`,`app_name`,`app_description`,`app_logo`,`web_link`,`app_path`)  
SELECT * FROM (SELECT 'STG' as `app_key`,'Settings' as `app_name`,'Settings &  Configuration' as `app_description`,'stg_components/styles/images/stg.png','STG_URL_WEB_SERVER_PATH' as `web_link`,'/svg' as `app_path`) AS tmp WHERE NOT EXISTS (SELECT `app_key` FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'STG') LIMIT 1;
SELECT `app_id` INTO @app_id FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'STG';
SELECT `workgroup_id` INTO @wid from `dbframework`.`tbl_workgroups` WHERE `workgroup_key`='WKG-ADMIN';
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'STG-CHARACTERISTICS' as  `permission_key` ,'Characteristics' as `permission_name`,'Access characteristics' as `permission_description` ,@app_id as `app_id`,'Characteristics' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'STG-CHARACTERISTICS') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='STG-CHARACTERISTICS';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'STG-DIALOGUE-TEMPLATES' as  `permission_key` ,'Dialogue Templates' as `permission_name`,'Access dialogue templates' as `permission_description` ,@app_id as `app_id`,'Dialogue Templates' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'STG-DIALOGUE-TEMPLATES') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='STG-DIALOGUE-TEMPLATES';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'STG-ACCESS' as  `permission_key` ,'Access' as `permission_name`,'Access to settings application' as `permission_description` ,@app_id as `app_id`,null as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'STG-ACCESS') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='STG-ACCESS';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'STG-CHARACTERISTIC-ADD-EDIT' as  `permission_key` ,'Add/Edit' as `permission_name`,'Add/Edit characteristic' as `permission_description` ,@app_id as `app_id`,'Characteristics' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'STG-CHARACTERISTIC-ADD-EDIT') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='STG-CHARACTERISTIC-ADD-EDIT';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'STG-CHARACTERISTIC-DELETE' as  `permission_key` ,'Delete' as `permission_name`,'Delete characteristic' as `permission_description` ,@app_id as `app_id`,'Characteristics' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'STG-CHARACTERISTIC-DELETE') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='STG-CHARACTERISTIC-DELETE';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'STG-DIALOGUE-TEMPLATES-ADD-EDIT' as  `permission_key` ,'Add/Edit' as `permission_name`,'Add/Edit dialogue template' as `permission_description` ,@app_id as `app_id`,'Dialogue Templates' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'STG-DIALOGUE-TEMPLATES-ADD-EDIT') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='STG-DIALOGUE-TEMPLATES-ADD-EDIT';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'STG-DIALOGUE-TEMPLATES-DELETE' as  `permission_key` ,'Delete' as `permission_name`,'Delete dialogue templete' as `permission_description` ,@app_id as `app_id`,'Dialogue Templates' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'STG-DIALOGUE-TEMPLATES-DELETE') LIMIT 1;
SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='STG-DIALOGUE-TEMPLATES-DELETE';
INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;

 END IF;
END ;;
DELIMITER ;

-- Execute the procedure
call `dbframework`.`stp_proc_install_STG`();

-- Drop the procedure
DROP PROCEDURE `dbframework`.`stp_proc_install_STG`;