-- Step 05: Delivery-based F&O Strike Price Analysis Table
-- This table stores the analysis of F&O strike prices based on highest delivery day closing prices

USE master;
GO

-- Drop table if exists
IF OBJECT_ID('step05_delivery_fo_analysis', 'U') IS NOT NULL
    DROP TABLE step05_delivery_fo_analysis;
GO

-- Create step05_delivery_fo_analysis table
CREATE TABLE step05_delivery_fo_analysis (
    id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Source delivery data from step02_monthly_analysis
    source_symbol NVARCHAR(50) NOT NULL,
    analysis_month NVARCHAR(20) NOT NULL,
    highest_delivery_date NVARCHAR(20) NOT NULL,
    highest_delivery_qty BIGINT NOT NULL,
    closing_price_on_delivery_date DECIMAL(18,8) NOT NULL,
    
    -- F&O matching data from step04_fo_udiff_daily
    fo_trade_date NVARCHAR(20) NOT NULL,
    fo_symbol NVARCHAR(50) NOT NULL,
    strike_price DECIMAL(18,8) NOT NULL,
    option_type NVARCHAR(10) NOT NULL, -- CE or PE
    fo_close_price DECIMAL(18,8) NOT NULL,
    
    -- Analysis metadata
    strike_position NVARCHAR(20) NOT NULL, -- 'down_3', 'down_2', 'down_1', 'exact', 'up_1', 'up_2', 'up_3'
    price_difference DECIMAL(18,8) NOT NULL, -- strike_price - closing_price_on_delivery_date
    percentage_difference DECIMAL(10,4) NOT NULL, -- ((strike_price - closing_price) / closing_price) * 100
    
    -- Processing metadata
    created_at DATETIME2 DEFAULT GETDATE(),
    analysis_file NVARCHAR(255) DEFAULT 'step05_delivery_fo_analysis.py'
);
GO

-- Create indexes for better performance
CREATE INDEX IX_step05_source_symbol_month ON step05_delivery_fo_analysis(source_symbol, analysis_month);
CREATE INDEX IX_step05_option_type ON step05_delivery_fo_analysis(option_type);
CREATE INDEX IX_step05_strike_position ON step05_delivery_fo_analysis(strike_position);
CREATE INDEX IX_step05_fo_trade_date ON step05_delivery_fo_analysis(fo_trade_date);
GO

-- Add constraints
ALTER TABLE step05_delivery_fo_analysis 
ADD CONSTRAINT CK_option_type CHECK (option_type IN ('CE', 'PE'));

ALTER TABLE step05_delivery_fo_analysis 
ADD CONSTRAINT CK_strike_position CHECK (strike_position IN ('down_3', 'down_2', 'down_1', 'exact', 'up_1', 'up_2', 'up_3'));
GO

PRINT 'Step05 Delivery F&O Analysis table created successfully!';
PRINT 'Expected records per symbol: 14 (7 CE + 7 PE for each highest delivery day)';
GO

-- Sample query to verify table structure
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'step05_delivery_fo_analysis'
ORDER BY ORDINAL_POSITION;
GO