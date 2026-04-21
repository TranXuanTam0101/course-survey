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
-- Tạo bảng FACT mới với cấu trúc 1 dòng/submission
CREATE TABLE FACT_TRA_LOI_KHAO_SAT (
    SubmissionID varchar(100) PRIMARY KEY,
    MaSV varchar(20) FOREIGN KEY REFERENCES DIM_SINH_VIEN(MaSV),
    MaLopHP varchar(50) FOREIGN KEY REFERENCES DIM_LOP_HOC_PHAN(MaLopHP),
    
    -- 12 câu trắc nghiệm (1-12)
    C1 int, C2 int, C3 int, C4 int, C5 int, C6 int,
    C7 int, C8 int, C9 int, C10 int, C11 int, C12 int,
    
    -- 4 câu tự luận (13-16)
    C13 nvarchar(4000), C14 nvarchar(4000), C15 nvarchar(4000), C16 nvarchar(4000)
);

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================
CREATE INDEX IX_FACT_MaSV ON FACT_TRA_LOI_KHAO_SAT(MaSV);
CREATE INDEX IX_FACT_MaLopHP ON FACT_TRA_LOI_KHAO_SAT(MaLopHP);

-- =====================================================
-- INSERT DIM_CAU_HOI (16 câu)
-- =====================================================
INSERT INTO DIM_CAU_HOI (MaCauHoi, ThuTuCauHoi, Phan, NoiDung, LoaiTraLoi) VALUES
(1, 1, 'I', N'Giảng viên giới thiệu rõ ràng, đầy đủ về đề cương chi tiết học phần, gồm: chuẩn đầu ra, nội dung, phương pháp dạy - học, phương pháp kiểm tra - đánh giá, tài liệu học tập của học phần', 'so'),
(2, 2, 'I', N'Nội dung của học phần phù hợp với năng lực của người học', 'so'),
(3, 3, 'I', N'Phương pháp dạy - học phù hợp với chuẩn đầu ra và nội dung của học phần', 'so'),
(4, 4, 'I', N'Giảng viên thực hiện đầy đủ kế hoạch dạy - học đã công bố và tuân thủ các quy định trong giảng dạy', 'so'),
(5, 5, 'I', N'Giảng viên có cập nhật kiến thức mới và thực tế trong bài giảng', 'so'),
(6, 6, 'I', N'Hoạt động dạy - học khơi gợi đam mê khám phá và giúp phát triển khả năng tự học', 'so'),
(7, 7, 'I', N'Giảng viên khuyến khích người học chủ động tham gia thảo luận, giải quyết vấn đề trong giờ học', 'so'),
(8, 8, 'I', N'Giảng viên tận tụy, sẵn sàng giúp đỡ, giải đáp thỏa đáng các thắc mắc của người học', 'so'),
(9, 9, 'I', N'Giảng viên sử dụng hiệu quả Elearning và các phương tiện công nghệ trong tổ chức dạy học', 'so'),
(10, 10, 'I', N'Phương pháp kiểm tra, đánh giá phù hợp với chuẩn đầu ra và nội dung của học phần', 'so'),
(11, 11, 'I', N'Việc đánh giá được thực hiện công bằng, khách quan và đảm bảo độ tin cậy', 'so'),
(12, 12, 'I', N'Anh/Chị hài lòng về chất lượng và hiệu quả giảng dạy của giảng viên đối với sự tiến bộ trong học tập của bản thân', 'so'),
(13, 13, 'II', N'Về chuẩn đầu ra và nội dung của học phần', 'text'),
(14, 14, 'II', N'Về hoạt động dạy - học', 'text'),
(15, 15, 'II', N'Về công tác kiểm tra – đánh giá', 'text'),
(16, 16, 'II', N'Các góp ý khác', 'text');

-- =====================================================
-- INSERT DIM_CTDT
-- =====================================================
INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) 
VALUES ('CTDT_CHINHQUY', N'Chính quy');

