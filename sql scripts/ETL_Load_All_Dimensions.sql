USE [LailoDWH]
GO

/****** Object:  StoredProcedure [DIM].[ETL_Load_All_Dimensions] ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Description: Load toàn bộ các bảng Dimension (Company, Date, Product) từ Staging sang DWH
-- =============================================
CREATE OR ALTER PROCEDURE [DIM].[ETL_Load_All_Dimensions]
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Bắt đầu Transaction để đảm bảo tính toàn vẹn dữ liệu
        BEGIN TRANSACTION;

        -- =============================================
        -- 1. LOAD DIM COMPANY
        -- =============================================
        DROP TABLE IF EXISTS LailoDWH.DIM.Company;

        SELECT 
            [Company] AS 'Công ty',
            [LogoUrl] AS LogoUrl
        INTO LailoDWH.DIM.Company
        FROM LailoStaging.DIM.Company;

        -- =============================================
        -- 2. LOAD DIM DATE
        -- =============================================
        DROP TABLE IF EXISTS LailoDWH.DIM.Date;

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

        -- =============================================
        -- 3. LOAD DIM PRODUCT
        -- =============================================
        DROP TABLE IF EXISTS LailoDWH.DIM.Product;

        SELECT 
            [Mã hàng hóa]  AS 'Mã hàng hóa',
            [Tên hàng hóa] AS 'Tên hàng hóa',
            [Nhóm HHDV]    AS 'Nhóm HHDV',
            [ĐVT]          AS 'ĐVT'
        INTO LailoDWH.DIM.Product
        FROM LailoStaging.DIM.Product;

        -- Nếu chạy đến đây mà không lỗi thì lưu thay đổi
        COMMIT TRANSACTION;
        
        PRINT 'Load Dimensions successfully.';
    END TRY
    BEGIN CATCH
        -- Nếu có lỗi xảy ra ở bất kỳ bước nào, hoàn tác lại toàn bộ
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Hiển thị thông báo lỗi chi tiết
        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END
GO
