USE `dbframework`;
SET NAMES utf8 ;

DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_proc_install_ITM`()
BEGIN

  IF ('true' = 'ITM_INSTALL' ) then
      
    INSERT INTO `dbframework`.`tbl_apps`(`app_key`,`app_name`,`app_description`,`app_logo`,`web_link`,`app_path`)  
    SELECT * FROM (SELECT 'ITM' as `app_key`,'Trigger Engine' as `app_name`,'Trigger Engine' as `app_description`,'itm_components/styles/images/itm.png','ITM_URL_WEB_SERVER_PATH' as `web_link`,'/itm' as `app_path`) AS tmp WHERE NOT EXISTS (SELECT `app_key` FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'ITM') LIMIT 1;
    SELECT `app_id` INTO @app_id FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'ITM'; 
    SELECT `workgroup_id` INTO @wid from `dbframework`.`tbl_workgroups` WHERE `workgroup_key`='WKG-ADMIN';
    INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'ITM-ACCESS' as  `permission_key` ,'Access' as `permission_name`,'Permission to access ITM module' as `permission_description` ,@app_id as `app_id`,null as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'ITM-ACCESS') LIMIT 1;
    SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='ITM-ACCESS';
    INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
    INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'ITM-TRIGGERS-ADD-EDIT' as  `permission_key` ,'Add/edit a trigger' as `permission_name`,'Permission to add/edit a trigger' as `permission_description` ,@app_id as `app_id`,'Triggers' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'ITM-TRIGGERS-ADD-EDIT') LIMIT 1;
    SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='ITM-TRIGGERS-ADD-EDIT';
    INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
    INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'ITM-TRIGGERS-DELETE' as  `permission_key` ,'Delete a trigger' as `permission_name`,'Permission to delete a trigger' as `permission_description` ,@app_id as `app_id`,'Triggers' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'ITM-TRIGGERS-DELETE') LIMIT 1;
    SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='ITM-TRIGGERS-DELETE';
    INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
    INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'ITM-FILES' as  `permission_key` ,'Files Management' as `permission_name`,'Permission to access files management' as `permission_description` ,@app_id as `app_id`,'Files' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'ITM-FILES') LIMIT 1;
    SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='ITM-FILES';
    INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
    INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'ITM-FILE-UPLOAD' as  `permission_key` ,'Upload File' as `permission_name`,'Permission to upload a file' as `permission_description` ,@app_id as `app_id`,'Files' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'ITM-FILE-UPLOAD') LIMIT 1;
    SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='ITM-FILE-UPLOAD';
    INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
    INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'ITM-FILE-DELETE' as  `permission_key` ,'Delete File' as `permission_name`,'Permission to delete an uploaded file' as `permission_description` ,@app_id as `app_id`,'Files' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'ITM-FILE-DELETE') LIMIT 1;
    SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='ITM-FILE-DELETE';
    INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;


  END IF;
END ;;
DELIMITER ;

-- Execute the procedure
call `dbframework`.`stp_proc_install_ITM`();

-- Drop the procedure
DROP PROCEDURE `dbframework`.`stp_proc_install_ITM`;