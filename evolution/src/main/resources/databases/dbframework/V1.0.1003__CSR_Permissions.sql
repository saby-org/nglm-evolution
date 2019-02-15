USE `dbframework`;
SET NAMES utf8 ;

DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `stp_proc_install_CSR`()
BEGIN

  IF ('true' = 'CSR_INSTALL' ) then
      

    INSERT INTO `dbframework`.`tbl_apps`(`app_key`,`app_name`,`app_description`,`app_logo`,`web_link`,`app_path`)  
	SELECT * FROM (SELECT 'CSR' as `app_key`,'CSR' as `app_name`,'Customer Care' as `app_description`,'csr_components/styles/images/csr.png','CSR_URL_WEB_SERVER_PATH' as `web_link`,'/csr' as `app_path`) AS tmp WHERE NOT EXISTS (SELECT `app_key` FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'CSR') LIMIT 1;
	SELECT `app_id` INTO @app_id FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'CSR';
	SELECT `workgroup_id` INTO @wid from `dbframework`.`tbl_workgroups` WHERE `workgroup_key`='WKG-ADMIN';
	INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'CSR-ACCESS' as  `permission_key` ,'CSR Access' as `permission_name`,'Permission to access CSR Module' as `permission_description` ,@app_id as `app_id`,null as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'CSR-ACCESS') LIMIT 1;
	SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='CSR-ACCESS';
	INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
	INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'CSR-CUSTOMER' as  `permission_key` ,'Access' as `permission_name`,'Permission to access Customer Module' as `permission_description` ,@app_id as `app_id`,'Customer' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'CSR-CUSTOMER') LIMIT 1;
	SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='CSR-CUSTOMER';
	INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
	INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'CSR-CUSTOMER-OVERVIEW' as  `permission_key` ,'Overview' as `permission_name`,'Permission to access Customer Overview' as `permission_description` ,@app_id as `app_id`,'Customer' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'CSR-CUSTOMER-OVERVIEW') LIMIT 1;
	SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='CSR-CUSTOMER-OVERVIEW';
	INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
	INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'CSR-CUSTOMER-ACTIVITYLOG' as  `permission_key` ,'Activity Log' as `permission_name`,'Permission to access Customer Activity Log' as `permission_description` ,@app_id as `app_id`,'Customer' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'CSR-CUSTOMER-ACTIVITYLOG') LIMIT 1;
	SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='CSR-CUSTOMER-ACTIVITYLOG';
	INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
	INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'CSR-CUSTOMER-ELIGIBLECAMPAIGNS' as  `permission_key` ,'Eligible Campaigns' as `permission_name`,'Permission to access Customer Eligible Campaigns' as `permission_description` ,@app_id as `app_id`,'Customer' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'CSR-CUSTOMER-ELIGIBLECAMPAIGNS') LIMIT 1;
	SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='CSR-CUSTOMER-ELIGIBLECAMPAIGNS';
	INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
	INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'CSR-CUSTOMER-LATESTOFFERS' as  `permission_key` ,'Latest Offers' as `permission_name`,'Permission to access Customer Latest Offers' as `permission_description` ,@app_id as `app_id`,'Customer' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'CSR-CUSTOMER-LATESTOFFERS') LIMIT 1;
	SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='CSR-CUSTOMER-LATESTOFFERS';
	INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
	INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'CSR-CUSTOMER-LOYALITYPROGRAMS' as  `permission_key` ,'Loyality Programs' as `permission_name`,'Permission to access Customer Loyality Programs' as `permission_description` ,@app_id as `app_id`,'Customer' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'CSR-CUSTOMER-LOYALITYPROGRAMS') LIMIT 1;
	SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='CSR-CUSTOMER-LOYALITYPROGRAMS';
	INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;
	INSERT INTO `dbframework`.`tbl_permissions`(`permission_key`,`permission_name`,`permission_description`,`app_id`,`folder`) SELECT * FROM (SELECT 'CSR-CUSTOMER-NBOA' as  `permission_key` ,'NBO/A' as `permission_name`,'Permission to access Customer NBO/A' as `permission_description` ,@app_id as `app_id`,'Customer' as `folder`) AS tmp WHERE NOT EXISTS (SELECT `permission_key` FROM `dbframework`.`tbl_permissions` WHERE `permission_key` = 'CSR-CUSTOMER-NBOA') LIMIT 1;
	SELECT permission_id INTO @perid from  `dbframework`.`tbl_permissions` WHERE `permission_key`='CSR-CUSTOMER-NBOA';
	INSERT INTO `dbframework`.`tbl_workgroups_permissions`(`workgroup_id`,`permission_id`) SELECT * FROM (SELECT @wid as `workgroup_id` ,@perid as `permission_id`) AS tmp WHERE NOT EXISTS ( SELECT `workgroup_id` FROM `dbframework`.`tbl_workgroups_permissions` WHERE `workgroup_id` = @wid AND `permission_id`=@perid) LIMIT 1;


  END IF;
END ;;
DELIMITER ;

-- Execute the procedure
call `dbframework`.`stp_proc_install_CSR`();

-- Drop the procedure
DROP PROCEDURE `dbframework`.`stp_proc_install_CSR`;