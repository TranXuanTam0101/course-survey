-- =====================================================
-- SURVEY DATABASE SCHEMA
-- Database: course-survey-db
-- Server: course-survey.database.windows.net
-- =====================================================

-- =====================================================
-- 1. DIM_HOC_KY
-- =====================================================
CREATE TABLE DIM_HOC_KY (
    MaHocKy VARCHAR(10) PRIMARY KEY,
    NamHoc VARCHAR(20) NOT NULL,
    HocKy INT NOT NULL
);

-- =====================================================
-- 2. DIM_KHOA
-- =====================================================
CREATE TABLE DIM_KHOA (
    MaKhoa VARCHAR(10) PRIMARY KEY,
    TenKhoa NVARCHAR(100) NOT NULL
);

-- =====================================================
-- 3. DIM_CHUONG_TRINH_DAO_TAO
-- =====================================================
CREATE TABLE DIM_CHUONG_TRINH_DAO_TAO (
    MaCTDT VARCHAR(20) PRIMARY KEY,
    TenCTDT NVARCHAR(100) NOT NULL
);

-- =====================================================
-- 4. DIM_CAU_HOI
-- =====================================================
CREATE TABLE DIM_CAU_HOI (
    MaCauHoi INT PRIMARY KEY,
    ThuTuCauHoi INT,
    Phan VARCHAR(20),
    NoiDung NVARCHAR(MAX),
    LoaiTraLoi VARCHAR(20)
);

-- =====================================================
-- 5. DIM_CHUYEN_NGANH
-- =====================================================
CREATE TABLE DIM_CHUYEN_NGANH (
    MaChuyenNganh VARCHAR(20) PRIMARY KEY,
    TenChuyenNganh NVARCHAR(100) NOT NULL,
    MaKhoa VARCHAR(10) NOT NULL,
    MaCTDT VARCHAR(20) NOT NULL,
    CONSTRAINT FK_ChuyenNganh_Khoa FOREIGN KEY (MaKhoa) 
        REFERENCES DIM_KHOA(MaKhoa),
    CONSTRAINT FK_ChuyenNganh_CTDT FOREIGN KEY (MaCTDT) 
        REFERENCES DIM_CHUONG_TRINH_DAO_TAO(MaCTDT)
);

-- =====================================================
-- 6. DIM_HOC_PHAN
-- =====================================================
CREATE TABLE DIM_HOC_PHAN (
    MaHP VARCHAR(20) PRIMARY KEY,
    TenHP NVARCHAR(100) NOT NULL,
    MaKhoa VARCHAR(10) NOT NULL,
    CONSTRAINT FK_HocPhan_Khoa FOREIGN KEY (MaKhoa) 
        REFERENCES DIM_KHOA(MaKhoa)
);

-- =====================================================
-- 7. DIM_GIANG_VIEN
-- =====================================================
CREATE TABLE DIM_GIANG_VIEN (
    MaGV VARCHAR(20) PRIMARY KEY,
    HoDemGV NVARCHAR(50),
    TenGV NVARCHAR(50) NOT NULL
);

-- =====================================================
-- 8. DIM_LOP_SINH_VIEN
-- =====================================================
CREATE TABLE DIM_LOP_SINH_VIEN (
    MaLop VARCHAR(20) PRIMARY KEY,
    Lop NVARCHAR(20) NOT NULL,
    MaChuyenNganh VARCHAR(20) NOT NULL,
    CONSTRAINT FK_LopSinhVien_ChuyenNganh FOREIGN KEY (MaChuyenNganh) 
        REFERENCES DIM_CHUYEN_NGANH(MaChuyenNganh)
);

-- =====================================================
-- 9. DIM_SINH_VIEN
-- =====================================================
CREATE TABLE DIM_SINH_VIEN (
    MaSV VARCHAR(20) PRIMARY KEY,
    HoDem NVARCHAR(50),
    Ten NVARCHAR(50) NOT NULL,
    NgaySinh DATE,
    MaLop VARCHAR(20) NOT NULL,
    CONSTRAINT FK_SinhVien_Lop FOREIGN KEY (MaLop) 
        REFERENCES DIM_LOP_SINH_VIEN(MaLop)
);

-- =====================================================
-- 10. DIM_LOP_HOC_PHAN
-- =====================================================
CREATE TABLE DIM_LOP_HOC_PHAN (
    MaLopHP VARCHAR(50) PRIMARY KEY,
    LopHP NVARCHAR(100) NOT NULL,
    MaHP VARCHAR(20) NOT NULL,
    MaGV VARCHAR(20) NOT NULL,
    MaHocKy VARCHAR(10) NOT NULL,
    CONSTRAINT FK_LopHocPhan_HocPhan FOREIGN KEY (MaHP) 
        REFERENCES DIM_HOC_PHAN(MaHP),
    CONSTRAINT FK_LopHocPhan_GiangVien FOREIGN KEY (MaGV) 
        REFERENCES DIM_GIANG_VIEN(MaGV),
    CONSTRAINT FK_LopHocPhan_HocKy FOREIGN KEY (MaHocKy) 
        REFERENCES DIM_HOC_KY(MaHocKy)
);

-- =====================================================
-- 11. FACT_TRA_LOI_KHAO_SAT
-- =====================================================
CREATE TABLE FACT_TRA_LOI_KHAO_SAT (
    SubmissionID VARCHAR(100) NOT NULL,
    MaCauHoi INT NOT NULL,
    MaSV VARCHAR(20) NOT NULL,
    MaLopHP VARCHAR(50) NOT NULL,
    TraLoiSo FLOAT NULL,
    TraLoiText NVARCHAR(MAX) NULL,
    CONSTRAINT PK_FACT_TRA_LOI_KHAO_SAT PRIMARY KEY (SubmissionID, MaCauHoi),
    CONSTRAINT FK_Fact_CauHoi FOREIGN KEY (MaCauHoi) 
        REFERENCES DIM_CAU_HOI(MaCauHoi),
    CONSTRAINT FK_Fact_SinhVien FOREIGN KEY (MaSV) 
        REFERENCES DIM_SINH_VIEN(MaSV),
    CONSTRAINT FK_Fact_LopHocPhan FOREIGN KEY (MaLopHP) 
        REFERENCES DIM_LOP_HOC_PHAN(MaLopHP)
);

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================
CREATE INDEX IX_FACT_MaSV ON FACT_TRA_LOI_KHAO_SAT(MaSV);
CREATE INDEX IX_FACT_MaLopHP ON FACT_TRA_LOI_KHAO_SAT(MaLopHP);
CREATE INDEX IX_FACT_MaCauHoi ON FACT_TRA_LOI_KHAO_SAT(MaCauHoi);

-- =====================================================
-- INSERT DIM_CAU_HOI (16 câu)
-- =====================================================
INSERT INTO DIM_CAU_HOI (MaCauHoi, ThuTuCauHoi, Phan, NoiDung, LoaiTraLoi) VALUES
(1, 1, N'Trắc nghiệm', N'Câu hỏi 1', N'Thang5'),
(2, 2, N'Trắc nghiệm', N'Câu hỏi 2', N'Thang5'),
(3, 3, N'Trắc nghiệm', N'Câu hỏi 3', N'Thang5'),
(4, 4, N'Trắc nghiệm', N'Câu hỏi 4', N'Thang5'),
(5, 5, N'Trắc nghiệm', N'Câu hỏi 5', N'Thang5'),
(6, 6, N'Trắc nghiệm', N'Câu hỏi 6', N'Thang5'),
(7, 7, N'Trắc nghiệm', N'Câu hỏi 7', N'Thang5'),
(8, 8, N'Trắc nghiệm', N'Câu hỏi 8', N'Thang5'),
(9, 9, N'Trắc nghiệm', N'Câu hỏi 9', N'Thang5'),
(10, 10, N'Trắc nghiệm', N'Câu hỏi 10', N'Thang5'),
(11, 11, N'Trắc nghiệm', N'Câu hỏi 11', N'Thang5'),
(12, 12, N'Trắc nghiệm', N'Câu hỏi 12', N'Thang5'),
(13, 13, N'Tự luận', N'Câu hỏi 13', N'VanBan'),
(14, 14, N'Tự luận', N'Câu hỏi 14', N'VanBan'),
(15, 15, N'Tự luận', N'Câu hỏi 15', N'VanBan'),
(16, 16, N'Tự luận', N'Câu hỏi 16', N'VanBan');

-- =====================================================
-- INSERT DIM_CTDT
-- =====================================================
INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) 
VALUES ('CTDT_CHINHQUY', N'Chính quy');

PRINT '✅ All tables created successfully!';
