USE [LailoDWH]
GO

/****** Object:  StoredProcedure [FACT].[ETL_Load_FactLailo] ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Description: Load dữ liệu vào bảng Fact Lailo từ Staging với cơ chế an toàn (Transaction)
-- =============================================
CREATE OR ALTER PROCEDURE [FACT].[ETL_Load_FactLailo]
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Bắt đầu Transaction
        BEGIN TRANSACTION;

        -- 1. Xóa bảng Fact cũ nếu tồn tại
        DROP TABLE IF EXISTS LailoDWH.FACT.Lailo;

        -- 2. Tạo bảng mới và đổ dữ liệu từ Staging sang
        SELECT 
            [Mã hàng hóa],
            [Tên hàng hóa],
            [Nhóm HHDV],
            [ĐVT],
            [Số lượng],
            [Doanh thu],
            [Các khoản giảm trừ],
            [Doanh thu thuần],
            [Giá vốn],
            [Lợi nhuận],
            [Year Month],
            [Công ty]
        INTO LailoDWH.FACT.Lailo
        FROM LailoStaging.FACT.Lailo;

        -- Nếu chạy êm xuôi thì Commit
        COMMIT TRANSACTION;
        
        PRINT 'Load Fact Lailo successfully.';
    END TRY
    BEGIN CATCH
        -- Nếu có lỗi, hoàn tác lại (không xóa bảng cũ)
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Báo lỗi chi tiết ra màn hình
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END
GO
