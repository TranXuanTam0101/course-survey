-- ==================================================
-- XÓA TOÀN BỘ DỮ LIỆU THEO ĐÚNG THỨ TỰ KHÓA NGOẠI
-- ==================================================

-- ========== 1. XÓA BẢNG FACT (CÓ KHÓA NGOẠI TRỎ ĐẾN CÁC DIM) ==========
DELETE FROM FACT_TRA_LOI_KHAO_SAT;
PRINT '✅ Xóa FACT_TRA_LOI_KHAO_SAT: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' dòng';

-- ========== 2. XÓA BẢNG DIM_LOP_HOC_PHAN (CÓ FK ĐẾN DIM_HOC_PHAN, DIM_GIANG_VIEN) ==========
DELETE FROM DIM_LOP_HOC_PHAN;
PRINT '✅ Xóa DIM_LOP_HOC_PHAN: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' dòng';

-- ========== 3. XÓA BẢNG DIM_SINH_VIEN (CÓ FK ĐẾN DIM_LOP_SINH_VIEN) ==========
DELETE FROM DIM_SINH_VIEN;
PRINT '✅ Xóa DIM_SINH_VIEN: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' dòng';

-- ========== 4. XÓA BẢNG DIM_LOP_SINH_VIEN (CÓ FK ĐẾN DIM_CHUYEN_NGANH) ==========
DELETE FROM DIM_LOP_SINH_VIEN;
PRINT '✅ Xóa DIM_LOP_SINH_VIEN: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' dòng';

-- ========== 5. XÓA BẢNG DIM_CHUYEN_NGANH (CÓ FK ĐẾN DIM_KHOA, DIM_CTDT) ==========
DELETE FROM DIM_CHUYEN_NGANH;
PRINT '✅ Xóa DIM_CHUYEN_NGANH: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' dòng';

-- ========== 6. XÓA BẢNG DIM_HOC_PHAN (CÓ FK ĐẾN DIM_KHOA) ==========
DELETE FROM DIM_HOC_PHAN;
PRINT '✅ Xóa DIM_HOC_PHAN: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' dòng';

-- ========== 7. XÓA BẢNG DIM_GIANG_VIEN ==========
DELETE FROM DIM_GIANG_VIEN;
PRINT '✅ Xóa DIM_GIANG_VIEN: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' dòng';

-- ========== 8. XÓA BẢNG DIM_KHOA ==========
DELETE FROM DIM_KHOA;
PRINT '✅ Xóa DIM_KHOA: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' dòng';

-- ========== 9. XÓA BẢNG DIM_CTDT ==========
DELETE FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY';
PRINT '✅ Xóa DIM_CHUONG_TRINH_DAO_TAO: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' dòng';

-- ========== 10. XÓA BẢNG DIM_HOC_KY ==========
DELETE FROM DIM_HOC_KY;
PRINT '✅ Xóa DIM_HOC_KY: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' dòng';
