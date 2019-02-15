
-- JOIN UM and AM into a single APP, AM permissions will be moved under UM
SELECT `app_id` INTO @appam_id FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'AM'; 
SELECT `app_id` INTO @appum_id FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'UM'; 
UPDATE `dbframework`.`tbl_permissions` SET `app_id` =  @appum_id WHERE `app_id` =  @appam_id;
DELETE FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'AM'; 

-- UPDATE APPS NAMES
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='fwk_components/styles/images/um.png',`app_name`='Application & User Management',`app_description`= 'The User Management System  enables you to create and manage users and workgroups. You can also add or remove permissions that limit user access to applications functionalities.' where `app_key`='UM';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='itm_components/styles/images/itm.png',`app_name`='Trigger Engine',`app_description`= 'Trigger Engine' where `app_key`='ITM';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='jmr_components/styles/images/jmr.png',`app_name`='Journey Manager',`app_description`= 'Journey Manager' where `app_key`='JMR';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='opc_components/styles/images/opc.png',`app_name`='Real Time Offer Manager',`app_description`= 'Real Time Offer Manager' where `app_key`='OPC';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='fwk_components/styles/images/am.png',`app_name`='Subscriber Base Management',`app_description`= 'Universal Control Group' where `app_key`='UCG';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='csr_components/styles/images/csr.png',`app_name`='Customer Care',`app_description`= 'Customer Care' where `app_key`='CSR';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='fwk_components/styles/images/am.png',`app_name`='Insights and Analytics',`app_description`= 'Insights Analytics Reports' where `app_key`='IAR';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='fwk_components/styles/images/am.png',`app_name`='Operations',`app_description`= 'Operations Reports' where `app_key`='OPR';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='stg_components/styles/images/stg.png',`app_name`='Settings',`app_description`= 'Settings &  Configuration' where `app_key`='STG';