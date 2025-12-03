USE [LailoDWH]
GO
/****** Object:  StoredProcedure [DIM].[ETL_Load_DimCompany]    Script Date: 12/3/2025 11:47:53 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [DIM].[ETL_Load_DimCompany]
AS
BEGIN
    SET NOCOUNT ON;

    DROP TABLE IF EXISTS LailoDWH.DIM.Company;

    SELECT
        [Company]  AS 'Công ty',
        [LogoUrl] AS LogoUrl
    INTO LailoDWH.DIM.Company
    FROM LailoStaging.DIM.Company;
END
