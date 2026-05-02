-- ==================== SURVEY DATABASE SCHEMA ====================
-- Database: course-survey-db
-- Description: Hệ thống khảo sát đánh giá giảng viên và học phần

-- ==================== DIMENSION TABLES ====================

-- 1. DIM_KHOA: Danh sách các Khoa/Trường
CREATE TABLE DIM_KHOA (
    MaKhoa NVARCHAR(20) PRIMARY KEY,
    TenKhoa NVARCHAR(200) NOT NULL
);

-- 2. DIM_NGANH: Danh sách các Ngành đào tạo
CREATE TABLE DIM_NGANH (
    MaNganh NVARCHAR(20) PRIMARY KEY,
    TenNganh NVARCHAR(200) NOT NULL,
    MaKhoa NVARCHAR(20) NOT NULL,
    CONSTRAINT FK_DIM_NGANH_KHOA FOREIGN KEY (MaKhoa) REFERENCES DIM_KHOA(MaKhoa)
);

-- 3. DIM_CHUYEN_NGANH: Danh sách các Chuyên ngành
CREATE TABLE DIM_CHUYEN_NGANH (
    MaChuyenNganh NVARCHAR(20) PRIMARY KEY,
    TenChuyenNganh NVARCHAR(200) NOT NULL,
    MaNganh NVARCHAR(20) NOT NULL,
    CONSTRAINT FK_DIM_CHUYEN_NGANH_NGANH FOREIGN KEY (MaNganh) REFERENCES DIM_NGANH(MaNganh)
);

-- 4. DIM_LOP_SINH_VIEN: Danh sách các Lớp
CREATE TABLE DIM_LOP_SINH_VIEN (
    MaLop NVARCHAR(20) PRIMARY KEY,
    Lop NVARCHAR(50) NOT NULL,
    MaChuyenNganh NVARCHAR(20) NOT NULL,
    CONSTRAINT FK_DIM_LOP_SV_CHUYEN_NGANH FOREIGN KEY (MaChuyenNganh) REFERENCES DIM_CHUYEN_NGANH(MaChuyenNganh)
);

-- 5. DIM_SINH_VIEN: Danh sách Sinh viên
CREATE TABLE DIM_SINH_VIEN (
    MaSV NVARCHAR(20) PRIMARY KEY,
    HoDem NVARCHAR(100),
    Ten NVARCHAR(50) NOT NULL,
    NgaySinh DATE,
    MaLop NVARCHAR(20) NOT NULL,
    CONSTRAINT FK_DIM_SINH_VIEN_LOP FOREIGN KEY (MaLop) REFERENCES DIM_LOP_SINH_VIEN(MaLop)
);

-- 6. DIM_GIANG_VIEN: Danh sách Giảng viên
CREATE TABLE DIM_GIANG_VIEN (
    MaGV NVARCHAR(20) PRIMARY KEY,
    HoDemGV NVARCHAR(100),
    TenGV NVARCHAR(50) NOT NULL
);

-- 7. DIM_HOC_PHAN: Danh sách Học phần
CREATE TABLE DIM_HOC_PHAN (
    MaHP NVARCHAR(20) PRIMARY KEY,
    TenHP NVARCHAR(200) NOT NULL,
    MaKhoa NVARCHAR(20) NOT NULL,
    CONSTRAINT FK_DIM_HOC_PHAN_KHOA FOREIGN KEY (MaKhoa) REFERENCES DIM_KHOA(MaKhoa)
);

-- 8. DIM_HOC_KY: Danh sách Học kỳ
CREATE TABLE DIM_HOC_KY (
    MaHocKy NVARCHAR(20) PRIMARY KEY,
    NamHoc NVARCHAR(20) NOT NULL,
    HocKy INT NOT NULL CHECK (HocKy IN (1, 2, 3))
);

-- 9. DIM_LOP_HOC_PHAN: Danh sách Lớp học phần
CREATE TABLE DIM_LOP_HOC_PHAN (
    MaLopHP NVARCHAR(50) PRIMARY KEY,
    LopHP NVARCHAR(100),
    MaHP NVARCHAR(20) NOT NULL,
    MaGV NVARCHAR(20) NOT NULL,
    MaHocKy NVARCHAR(20) NOT NULL,
    CONSTRAINT FK_DIM_LOP_HP_HOC_PHAN FOREIGN KEY (MaHP) REFERENCES DIM_HOC_PHAN(MaHP),
    CONSTRAINT FK_DIM_LOP_HP_GIANG_VIEN FOREIGN KEY (MaGV) REFERENCES DIM_GIANG_VIEN(MaGV),
    CONSTRAINT FK_DIM_LOP_HP_HOC_KY FOREIGN KEY (MaHocKy) REFERENCES DIM_HOC_KY(MaHocKy)
);

-- 10. DIM_CAU_HOI: Danh sách câu hỏi khảo sát
CREATE TABLE DIM_CAU_HOI (
    MaCauHoi INT PRIMARY KEY,
    ThuTuCauHoi INT NOT NULL,
    NoiDungCauHoi NVARCHAR(MAX) NOT NULL,
    NhomTieuChi NVARCHAR(255)
);

-- ==================== FACT TABLES ====================

-- 11. FACT_GOP_Y_TU_LUAN: Góp ý tự luận từ sinh viên
CREATE TABLE FACT_GOP_Y_TU_LUAN (
    SubmissionID NVARCHAR(150) PRIMARY KEY,
    MaSV NVARCHAR(20) NOT NULL,
    MaLopHP NVARCHAR(50) NOT NULL,
    NoiDungGopY NVARCHAR(MAX),
    Sentiment NVARCHAR(20),
    Is_Valid BIT DEFAULT 1,
    Tag_HocPhan BIT DEFAULT 0,
    Tag_DayHoc BIT DEFAULT 0,
    Tag_KiemTra BIT DEFAULT 0,
    Tag_Khac BIT DEFAULT 0,
    CONSTRAINT FK_FACT_GOP_Y_SINH_VIEN FOREIGN KEY (MaSV) REFERENCES DIM_SINH_VIEN(MaSV),
    CONSTRAINT FK_FACT_GOP_Y_LOP_HP FOREIGN KEY (MaLopHP) REFERENCES DIM_LOP_HOC_PHAN(MaLopHP)
);

-- 12. FACT_KET_QUA_DANH_GIA: Kết quả đánh giá chi tiết
CREATE TABLE FACT_KET_QUA_DANH_GIA (
    ID_KetQua INT IDENTITY(1,1) PRIMARY KEY,
    SubmissionID NVARCHAR(150) NOT NULL,
    MaCauHoi INT NOT NULL,
    Diem INT NOT NULL CHECK (Diem BETWEEN 1 AND 5),
    CONSTRAINT FK_FACT_KET_QUA_CAU_HOI FOREIGN KEY (MaCauHoi) REFERENCES DIM_CAU_HOI(MaCauHoi),
    CONSTRAINT UQ_FACT_KET_QUA UNIQUE (SubmissionID, MaCauHoi)
);

-- ==================== INDEXES ====================

-- DIM indexes
CREATE INDEX IX_DIM_NGANH_MaKhoa ON DIM_NGANH(MaKhoa);
CREATE INDEX IX_DIM_CHUYEN_NGANH_MaNganh ON DIM_CHUYEN_NGANH(MaNganh);
CREATE INDEX IX_DIM_LOP_SV_MaChuyenNganh ON DIM_LOP_SINH_VIEN(MaChuyenNganh);
CREATE INDEX IX_DIM_SINH_VIEN_MaLop ON DIM_SINH_VIEN(MaLop);
CREATE INDEX IX_DIM_HOC_PHAN_MaKhoa ON DIM_HOC_PHAN(MaKhoa);
CREATE INDEX IX_DIM_LOP_HP_MaHP ON DIM_LOP_HOC_PHAN(MaHP);
CREATE INDEX IX_DIM_LOP_HP_MaGV ON DIM_LOP_HOC_PHAN(MaGV);
CREATE INDEX IX_DIM_LOP_HP_MaHocKy ON DIM_LOP_HOC_PHAN(MaHocKy);

-- FACT indexes
CREATE INDEX IX_FACT_GOP_Y_MaSV ON FACT_GOP_Y_TU_LUAN(MaSV);
CREATE INDEX IX_FACT_GOP_Y_MaLopHP ON FACT_GOP_Y_TU_LUAN(MaLopHP);
CREATE INDEX IX_FACT_GOP_Y_Sentiment ON FACT_GOP_Y_TU_LUAN(Sentiment);
CREATE INDEX IX_FACT_GOP_Y_Is_Valid ON FACT_GOP_Y_TU_LUAN(Is_Valid);
CREATE INDEX IX_FACT_GOP_Y_Tags ON FACT_GOP_Y_TU_LUAN(Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac);

CREATE INDEX IX_FACT_KET_QUA_SubmissionID ON FACT_KET_QUA_DANH_GIA(SubmissionID);
CREATE INDEX IX_FACT_KET_QUA_MaCauHoi ON FACT_KET_QUA_DANH_GIA(MaCauHoi);

-- ==================== DIM_CAU_HOI DATA ====================

INSERT INTO DIM_CAU_HOI (MaCauHoi, ThuTuCauHoi, NoiDungCauHoi, NhomTieuChi) VALUES
(1, 1, N'Giảng viên giới thiệu rõ ràng, đầy đủ về đề cương chi tiết học phần (chuẩn đầu ra, nội dung, phương pháp dạy - học, phương pháp kiểm tra - đánh giá, tài liệu học tập)', N'Tổ chức học phần'),
(2, 2, N'Nội dung của học phần phù hợp với năng lực của người học', N'Nội dung đào tạo'),
(3, 3, N'Phương pháp dạy - học phù hợp với chuẩn đầu ra và nội dung của học phần', N'Phương pháp giảng dạy'),
(4, 4, N'Giảng viên thực hiện đầy đủ kế hoạch dạy - học đã công bố và tuân thủ các quy định trong giảng dạy', N'Trách nhiệm giảng dạy'),
(5, 5, N'Giảng viên có cập nhật kiến thức mới và thực tế trong bài giảng', N'Nội dung đào tạo'),
(6, 6, N'Hoạt động dạy - học khơi gợi đam mê khám phá và giúp phát triển khả năng tự học', N'Phương pháp giảng dạy'),
(7, 7, N'Giảng viên khuyến khích người học chủ động tham gia thảo luận, giải quyết vấn đề trong giờ học', N'Phương pháp giảng dạy'),
(8, 8, N'Giảng viên tận tụy, sẵn sàng giúp đỡ, giải đáp thỏa đáng các thắc mắc của người học', N'Thái độ & Hỗ trợ'),
(9, 9, N'Giảng viên sử dụng hiệu quả Elearning và các phương tiện công nghệ trong tổ chức dạy học', N'Phương pháp giảng dạy'),
(10, 10, N'Phương pháp kiểm tra, đánh giá phù hợp với chuẩn đầu ra và nội dung của học phần', N'Kiểm tra - Đánh giá'),
(11, 11, N'Việc đánh giá được thực hiện công bằng, khách quan và đảm bảo độ tin cậy', N'Kiểm tra - Đánh giá'),
(12, 12, N'Anh/Chị hài lòng về chất lượng và hiệu quả giảng dạy của giảng viên đối với sự tiến bộ trong học tập của bản thân', N'Đánh giá tổng thể');
