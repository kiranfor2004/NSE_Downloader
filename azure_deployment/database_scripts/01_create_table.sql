-- Azure SQL Database Table Creation Script
-- step03_compare_monthvspreviousmonth table structure

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Drop table if exists (for fresh deployment)
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[step03_compare_monthvspreviousmonth]') AND type in (N'U'))
DROP TABLE [dbo].[step03_compare_monthvspreviousmonth]
GO

-- Create the main table
CREATE TABLE [dbo].[step03_compare_monthvspreviousmonth](
    [id] [bigint] IDENTITY(1,1) NOT NULL,
    
    -- Current month data (February/March data)
    [current_trade_date] [date] NOT NULL,
    [symbol] [nvarchar](50) NOT NULL,
    [series] [nvarchar](10) NULL,
    [current_prev_close] [decimal](18, 4) NULL,
    [current_open_price] [decimal](18, 4) NULL,
    [current_high_price] [decimal](18, 4) NULL,
    [current_low_price] [decimal](18, 4) NULL,
    [current_last_price] [decimal](18, 4) NULL,
    [current_close_price] [decimal](18, 4) NULL,
    [current_avg_price] [decimal](18, 4) NULL,
    [current_ttl_trd_qnty] [bigint] NULL,
    [current_turnover_lacs] [decimal](18, 4) NULL,
    [current_no_of_trades] [bigint] NULL,
    [current_deliv_qty] [bigint] NULL,
    [current_deliv_per] [decimal](8, 4) NULL,
    [current_source_file] [nvarchar](255) NULL,
    
    -- Previous month baseline data (January data)
    [previous_baseline_date] [date] NULL,
    [previous_prev_close] [decimal](18, 4) NULL,
    [previous_open_price] [decimal](18, 4) NULL,
    [previous_high_price] [decimal](18, 4) NULL,
    [previous_low_price] [decimal](18, 4) NULL,
    [previous_last_price] [decimal](18, 4) NULL,
    [previous_close_price] [decimal](18, 4) NULL,
    [previous_avg_price] [decimal](18, 4) NULL,
    [previous_ttl_trd_qnty] [bigint] NULL,
    [previous_turnover_lacs] [decimal](18, 4) NULL,
    [previous_no_of_trades] [bigint] NULL,
    [previous_deliv_qty] [bigint] NULL,
    [previous_deliv_per] [decimal](8, 4) NULL,
    [previous_source_file] [nvarchar](255) NULL,
    
    -- Analysis calculations
    [delivery_increase_abs] [bigint] NULL,
    [delivery_increase_pct] [decimal](10, 2) NULL,
    [comparison_type] [nvarchar](50) NULL,
    [created_at] [datetime2](7) NULL DEFAULT (getdate()),
    
    -- Enhanced columns for dashboard
    [index_name] [nvarchar](100) NULL,
    [category] [nvarchar](100) NULL,
    [sector] [nvarchar](100) NULL,
    
 CONSTRAINT [PK_step03_compare_monthvspreviousmonth] PRIMARY KEY CLUSTERED ([id] ASC)
) ON [PRIMARY]
GO

-- Create indexes for performance
CREATE NONCLUSTERED INDEX [IX_step03_symbol] ON [dbo].[step03_compare_monthvspreviousmonth]
(
    [symbol] ASC
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [IX_step03_current_date] ON [dbo].[step03_compare_monthvspreviousmonth]
(
    [current_trade_date] ASC
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [IX_step03_delivery_pct] ON [dbo].[step03_compare_monthvspreviousmonth]
(
    [delivery_increase_pct] DESC
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [IX_step03_symbol_date] ON [dbo].[step03_compare_monthvspreviousmonth]
(
    [symbol] ASC,
    [current_trade_date] ASC
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [IX_step03_category] ON [dbo].[step03_compare_monthvspreviousmonth]
(
    [category] ASC
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [IX_step03_sector] ON [dbo].[step03_compare_monthvspreviousmonth]
(
    [sector] ASC
) ON [PRIMARY]
GO

PRINT 'Table step03_compare_monthvspreviousmonth created successfully with all indexes!'
GO