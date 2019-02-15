
-- JOIN UM and AM into a single APP, AM permissions will be moved under UM
SELECT `app_id` INTO @appam_id FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'AM'; 
SELECT `app_id` INTO @appum_id FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'UM'; 
UPDATE `dbframework`.`tbl_permissions` SET `app_id` =  @appum_id WHERE `app_id` =  @appam_id;
DELETE FROM `dbframework`.`tbl_apps` WHERE `app_key` = 'AM'; 

-- UPDATE APPS NAMES
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='fwk_components/styles/images/um.png',`app_name`='Application & User Management',`app_description`= 'Application & User Management, allows administrator to configure Applications available on the system, as well as authorised Users and Workgroups with permissions.' where `app_key`='UM';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='itm_components/styles/images/itm.png',`app_name`='Trigger Engine',`app_description`= 'Trigger Engine' where `app_key`='ITM';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='jmr_components/styles/images/jmr.png',`app_name`='Journey Manager',`app_description`= 'Journey Manager, allows to configure and manage Customer Journeys and Campaigns with corresponding objectives and limits.' where `app_key`='JMR';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='opc_components/styles/images/opc.png',`app_name`='Real Time Offer Manager',`app_description`= 'Offer Manager, allows to configure and manage Offers, Products, Suppliers and Partners to be used for both inbound and outbound.' where `app_key`='OPC';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='fwk_components/styles/images/am.png',`app_name`='Subscriber Base Management',`app_description`= 'Universal Control Group' where `app_key`='UCG';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='csr_components/styles/images/csr.png',`app_name`='Customer Care',`app_description`= 'Customer Care, allows agents to visualise Customer’s details, profile and activity history, as well as to propose Offers & Campaigns in a personalised manner.' where `app_key`='CSR';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='fwk_components/styles/images/am.png',`app_name`='Insights and Analytics',`app_description`= 'Insights & Analytics, allows to visualise the Marketing Dashboard presenting KPIs, Graphs and Charts reporting the performance of the solution, as well as to dig down the data for detailed analysis.' where `app_key`='IAR';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='fwk_components/styles/images/am.png',`app_name`='Operations',`app_description`= 'Monitoring, allows to visualise the System Monitoring Dashboard presenting Graphs, Statistics and Alarms used to monitor the level of resource utilisation to supervise the good health of the system and to detect possible incidents.' where `app_key`='OPR';
UPDATE `dbframework`.`tbl_apps` SET `app_logo`='stg_components/styles/images/stg.png',`app_name`='Settings',`app_description`= 'Settings, allows to configure the solution Global Parameters and Shared Variables such as for Reports, Characteristics, Templates…' where `app_key`='STG';