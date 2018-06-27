IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'<_IOM_DB_DATABASE_>')
  BEGIN
    CREATE DATABASE [<_IOM_DB_DATABASE_>]
  END;
go

use [<_IOM_DB_DATABASE_>]
go

/****************************************
*
*  PRESENTED OFFERS
*
****************************************/

/****************
*  drop objects
****************/

drop procedure if exists [dbo].[stp_save_presented_offers]
go

drop table if exists [dbo].[tbl_presented_offers]
go

/*************************************
*  create tbl_presented_offers table
*************************************/

CREATE TABLE [dbo].[tbl_presented_offers]
  (
    [presented_offer_id] [bigint] IDENTITY(1,1) NOT NULL,
    [subscriber_id] [bigint] NOT NULL,
    [offer_id] [bigint] NOT NULL,
    [channel_id] [int] NOT NULL,
    [user_id] [bigint] NULL,
    [presented_datetime] [datetimeoffset] NOT NULL,
    [position] [int] NULL,
    [date_key] [date] NULL,
    CONSTRAINT [PK_tbl_presented_offers] PRIMARY KEY CLUSTERED 
      (
        [presented_offer_id] ASC
      )
    WITH 
      (
        PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
      ) 
    ON [PRIMARY]
  ) 
GO

/***********************************************************
*  create stp_save_presented_offers stored procedure
***********************************************************/

create procedure [dbo].[stp_save_presented_offers]
  @p_subscriber_id bigint,
  @p_offer_id bigint,
  @p_channel_id int,
  @p_user_id bigint,
  @p_presented_datetime datetimeoffset,
  @p_position int
as
begin
  set nocount on
  insert into tbl_presented_offers(subscriber_id, offer_id, channel_id, user_id, presented_datetime, position, date_key) 
    values (@p_subscriber_id, @p_offer_id, @p_channel_id, @p_user_id, @p_presented_datetime, @p_position, cast(@p_presented_datetime as Date))
end
go

/****************************************
*
*  PRESENTED OFFER DETAILS
*
****************************************/

/****************
*  drop objects
****************/

drop procedure if exists [dbo].[stp_save_presented_offer_details]
go

drop table if exists [dbo].[tbl_presented_offer_details]
go

/*************************************
*  create tbl_presented_offer_details table
*************************************/

CREATE TABLE [dbo].[tbl_presented_offer_details]
  (
    [presented_offer_details_id] [bigint] IDENTITY(1,1) NOT NULL,
    [subscriber_id] [bigint] NOT NULL,
    [offer_id] [bigint] NOT NULL,
    [channel_id] [int] NOT NULL,
    [user_id] [bigint] NULL,
    [presented_details_datetime] [datetimeoffset] NOT NULL,
    [position] [int] NULL,
    [date_key] [date] NULL,
    CONSTRAINT [PK_tbl_presented_offer_details] PRIMARY KEY CLUSTERED 
      (
        [presented_offer_details_id] ASC
      )
    WITH 
      (
        PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
      ) 
    ON [PRIMARY]
  ) 
GO

/***********************************************************
*  create stp_save_presented_offers stored procedure
***********************************************************/

create procedure [dbo].[stp_save_presented_offer_details]
    @p_subscriber_id bigint,
    @p_channel_id int,
    @p_offer_id bigint,
    @p_user_id bigint,
    @p_presented_details_datetime datetimeoffset,
    @p_position int
as
begin
  set nocount on
  insert into tbl_presented_offer_details(subscriber_id, offer_id, channel_id, user_id, presented_details_datetime, position, date_key)
  values (@p_subscriber_id, @p_channel_id, @p_offer_id, @p_user_id, @p_presented_details_datetime, @p_position, cast(@p_presented_details_datetime as Date))
end
go

/****************************************
*
*  ACCEPTED OFFERS
*
****************************************/

/****************
*  drop objects
****************/

drop procedure if exists [dbo].[stp_save_accepted_offers]
go

drop table if exists [dbo].[tbl_accepted_offers]
go

/*************************************
*  create tbl_accepted_offers table
*************************************/

CREATE TABLE [dbo].[tbl_accepted_offers]
  (
    [accepted_offer_id] [bigint] IDENTITY(1,1) NOT NULL,
    [transaction_id] [bigint] NOT NULL,
    [subscriber_id] [bigint] NOT NULL,
    [offer_id] [bigint] NOT NULL,
    [channel_id] [int] NOT NULL,
    [user_id] [bigint] NULL,
    [accepted_datetime] [datetimeoffset] NOT NULL,
    [fulfilled_datetime] [datetimeoffset] NULL,
    [position] [int] NULL,
    [date_key] [date] NULL,
    CONSTRAINT [PK_tbl_accepted_offers] PRIMARY KEY CLUSTERED 
      (
	[accepted_offer_id] ASC
      )
    WITH 
      (
        PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
      ) 
    ON [PRIMARY]
  ) 
GO

/***********************************************************
*  create stp_save_presented_offers stored procedure
***********************************************************/

create procedure [dbo].[stp_save_accepted_offers]
    @p_transaction_id bigint,
    @p_subscriber_id bigint,
    @p_channel_id int,
    @p_offer_id bigint,
    @p_user_id bigint,
    @p_accepted_datetime datetimeoffset,
    @p_fulfilled_datetime datetimeoffset,
    @p_position int
as
begin
  set nocount on
  insert into tbl_accepted_offers(transaction_id, subscriber_id, offer_id, channel_id, user_id, accepted_datetime, fulfilled_datetime, position, date_key)
  values (@p_transaction_id, @p_subscriber_id, @p_offer_id, @p_channel_id, @p_user_id, @p_accepted_datetime, @p_fulfilled_datetime, @p_position, cast(@p_accepted_datetime as Date))
end
go

/****************************************
*
*  INTERACTED OFFERS
*
****************************************/

/****************
*  drop objects
****************/

drop procedure if exists [dbo].[stp_save_interactions]
go

drop table if exists [dbo].[tbl_interactions]
go

/*************************************
*  create tbl_interacted_record table
*************************************/

CREATE TABLE [dbo].[tbl_interactions]
  (
    [interaction_id] [bigint] IDENTITY(1,1) NOT NULL,
    [subscriber_id] [bigint] NOT NULL,
    [channel_id] [int] NOT NULL,
    [user_id] [bigint] NULL,
    [interaction_datetime] [datetimeoffset] NOT NULL,
    [is_available] [bit] NOT NULL,
    [date_key] [date] NULL,
    CONSTRAINT [PK_tbl_interactions] PRIMARY KEY CLUSTERED 
      (
	[interaction_id] ASC
      )
    WITH 
      (
        PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
      ) 
    ON [PRIMARY]
  ) 
GO

/***********************************************************
*  create stp_save_interactions stored procedure
***********************************************************/

create procedure [dbo].[stp_save_interactions]
    @p_subscriber_id bigint,
    @p_channel_id int,
    @p_user_id bigint,
    @p_interaction_datetime datetimeoffset,
    @p_is_available bit
as
begin
  set nocount on
  insert into tbl_interactions(subscriber_id, channel_id, user_id, interaction_datetime, is_available, date_key)
  values (@p_subscriber_id, @p_channel_id, @p_user_id, @p_interaction_datetime, @p_is_available, cast(@p_interaction_datetime as Date))
end
go
