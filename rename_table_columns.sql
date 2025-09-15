-- SQL Script to rename columns in step03_compare_monthvspreviousmonth table
-- Change feb_ prefix to current_ and jan_ prefix to previous_

USE master;
GO

PRINT 'Starting column renaming for step03_compare_monthvspreviousmonth table...'
PRINT '================================================================='

-- Rename feb_ columns to current_
PRINT 'Renaming feb_ columns to current_...'

EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_trade_date', 'current_trade_date', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_prev_close', 'current_prev_close', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_open_price', 'current_open_price', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_high_price', 'current_high_price', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_low_price', 'current_low_price', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_last_price', 'current_last_price', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_close_price', 'current_close_price', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_avg_price', 'current_avg_price', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_ttl_trd_qnty', 'current_ttl_trd_qnty', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_turnover_lacs', 'current_turnover_lacs', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_no_of_trades', 'current_no_of_trades', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_deliv_qty', 'current_deliv_qty', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_deliv_per', 'current_deliv_per', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.feb_source_file', 'current_source_file', 'COLUMN';

PRINT 'Completed feb_ to current_ column renaming.'

-- Rename jan_ columns to previous_
PRINT 'Renaming jan_ columns to previous_...'

EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_baseline_date', 'previous_baseline_date', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_prev_close', 'previous_prev_close', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_open_price', 'previous_open_price', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_high_price', 'previous_high_price', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_low_price', 'previous_low_price', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_last_price', 'previous_last_price', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_close_price', 'previous_close_price', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_avg_price', 'previous_avg_price', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_ttl_trd_qnty', 'previous_ttl_trd_qnty', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_turnover_lacs', 'previous_turnover_lacs', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_no_of_trades', 'previous_no_of_trades', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_deliv_qty', 'previous_deliv_qty', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_deliv_per', 'previous_deliv_per', 'COLUMN';
EXEC sp_rename 'step03_compare_monthvspreviousmonth.jan_source_file', 'previous_source_file', 'COLUMN';

PRINT 'Completed jan_ to previous_ column renaming.'

PRINT '================================================================='
PRINT 'Column renaming completed successfully!'
PRINT 'feb_ columns are now current_ columns'
PRINT 'jan_ columns are now previous_ columns'

-- Verify the changes
PRINT 'Verifying column names...'
SELECT COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
ORDER BY ORDINAL_POSITION;

PRINT 'Script execution completed.'