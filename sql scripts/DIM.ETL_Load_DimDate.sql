USE [LailoDWH]
GO
/****** Object:  StoredProcedure [DIM].[ETL_Load_DimDate]    Script Date: 12/3/2025 11:48:22 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [DIM].[ETL_Load_DimDate]
AS
BEGIN
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS LailoDWH.DIM.Date
    SELECT
        [Year Month]   AS 'Year Month',
        [Year Quarter] AS 'Year Quarter',
        [Year]         AS Year,
        [Quarter Date] AS 'Quarter Date',
        [Month Date]   AS 'Month Date',
        [Quarter]      AS Quarter,
        [Month]        AS Month,
        [Year Date]    AS YearDate
    INTO LailoDWH.DIM.Date
    FROM LailoStaging.DIM.Date;
END
