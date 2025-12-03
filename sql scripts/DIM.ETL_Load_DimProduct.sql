USE [LailoDWH]
GO
/****** Object:  StoredProcedure [DIM].[ETL_Load_DimProduct]    Script Date: 12/3/2025 11:48:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [DIM].[ETL_Load_DimProduct]
AS
BEGIN
    SET NOCOUNT ON;

    DROP TABLE IF EXISTS LailoDWH.DIM.Product;

    SELECT
        [Mã hàng hóa] AS 'Mã hàng hóa',
        [Tên hàng hóa] AS 'Tên hàng hóa',
        [Nhóm HHDV]   AS 'Nhóm HHDV',
        [ĐVT]         AS 'ĐVT'
    INTO LailoDWH.DIM.Product
    FROM LailoStaging.DIM.Product;
END
