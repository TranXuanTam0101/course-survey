-- ======================================================
-- TẠO CƠ SỞ DỮ LIỆU KHẢO SÁT HỌC PHẦN
-- 12 BẢNG DIMENSION + 3 BẢNG FACT
-- ======================================================

-- ======================================================
-- NHÓM 1: CÁC BẢNG DIMENSION (DANH MỤC) - 12 BẢNG
-- ======================================================

-- 1. DIM_KHOA: Quản lý cấp cao nhất
CREATE TABLE DIM_KHOA (
    MaKhoa NVARCHAR(50) PRIMARY KEY,
    TenKhoa NVARCHAR(255)
);

-- 2. DIM_NGANH: Các ngành thuộc khoa
CREATE TABLE DIM_NGANH (
    MaNganh NVARCHAR(50) PRIMARY KEY,
    TenNganh NVARCHAR(255),
    MaKhoa NVARCHAR(50) REFERENCES DIM_KHOA(MaKhoa)
);

-- 3. DIM_CHUONG_TRINH_DAO_TAO: Hệ đào tạo
CREATE TABLE DIM_CHUONG_TRINH_DAO_TAO (
    MaCTDT NVARCHAR(50) PRIMARY KEY,
    TenCTDT NVARCHAR(255)
);

-- 4. DIM_CHUYEN_NGANH: Cấp nhỏ nhất của ngành
CREATE TABLE DIM_CHUYEN_NGANH (
    MaChuyenNganh NVARCHAR(50) PRIMARY KEY,
    TenChuyenNganh NVARCHAR(255),
    MaNganh NVARCHAR(50) REFERENCES DIM_NGANH(MaNganh),
    MaCTDT NVARCHAR(50) REFERENCES DIM_CHUONG_TRINH_DAO_TAO(MaCTDT)
);

-- 5. DIM_LOP_SINH_VIEN: Lớp hành chính của sinh viên
CREATE TABLE DIM_LOP_SINH_VIEN (
    MaLop NVARCHAR(50) PRIMARY KEY,
    Lop NVARCHAR(100),
    MaChuyenNganh NVARCHAR(50) REFERENCES DIM_CHUYEN_NGANH(MaChuyenNganh)
);

-- 6. DIM_SINH_VIEN: Thông tin định danh người làm khảo sát
CREATE TABLE DIM_SINH_VIEN (
    MaSV NVARCHAR(50) PRIMARY KEY,
    HoDem NVARCHAR(100),
    Ten NVARCHAR(50),
    NgaySinh DATE,
    MaLop NVARCHAR(50) REFERENCES DIM_LOP_SINH_VIEN(MaLop)
);

-- 7. DIM_GIANG_VIEN: Thông tin người dạy
CREATE TABLE DIM_GIANG_VIEN (
    MaGV NVARCHAR(50) PRIMARY KEY,
    HoDemGV NVARCHAR(100),
    TenGV NVARCHAR(50)
);

-- 8. DIM_HOC_PHAN: Danh mục môn học
CREATE TABLE DIM_HOC_PHAN (
    MaHP NVARCHAR(50) PRIMARY KEY,
    TenHP NVARCHAR(255),
    MaKhoa NVARCHAR(50) REFERENCES DIM_KHOA(MaKhoa)
);

-- 9. DIM_HOC_KY: Trục thời gian
CREATE TABLE DIM_HOC_KY (
    MaHocKy NVARCHAR(50) PRIMARY KEY,
    NamHoc NVARCHAR(20),
    HocKy INT
);

-- 10. DIM_LOP_HOC_PHAN: Thực thể nối SV và GV
CREATE TABLE DIM_LOP_HOC_PHAN (
    MaLopHP NVARCHAR(50) PRIMARY KEY,
    LopHP NVARCHAR(100),
    MaHP NVARCHAR(50) REFERENCES DIM_HOC_PHAN(MaHP),
    MaGV NVARCHAR(50) REFERENCES DIM_GIANG_VIEN(MaGV),
    MaHocKy NVARCHAR(50) REFERENCES DIM_HOC_KY(MaHocKy)
);

-- 11. DIM_CAU_HOI: Danh sách 12 câu hỏi trắc nghiệm
CREATE TABLE DIM_CAU_HOI (
    MaCauHoi INT PRIMARY KEY,
    ThuTuCauHoi INT,
    NoiDungCauHoi NVARCHAR(MAX),
    NhomTieuChi NVARCHAR(255)  -- Phân nhóm: Tổ chức học phần, Nội dung đào tạo, Phương pháp giảng dạy, Trách nhiệm giảng dạy, Thái độ & Hỗ trợ, Kiểm tra - Đánh giá, Đánh giá tổng thể
);

-- Insert 12 câu hỏi trắc nghiệm
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

-- 12. DIM_TAG: Danh mục các nhãn NLP cho câu hỏi tự luận (13-16)
CREATE TABLE DIM_TAG (
    MaTag NVARCHAR(50) PRIMARY KEY,
    TenTag NVARCHAR(255),
    MoTaTag NVARCHAR(MAX)
);

-- Insert 4 tag tương ứng 4 câu tự luận
INSERT INTO DIM_TAG (MaTag, TenTag, MoTaTag) VALUES 
('TAG_HP', N'Nội dung & Chuẩn đầu ra', N'Góp ý về chuẩn đầu ra, khối lượng kiến thức và giáo trình học phần - Tương ứng câu 13'),
('TAG_DH', N'Hoạt động dạy - học', N'Góp ý về phương pháp truyền đạt, sự nhiệt tình và tương tác của giảng viên - Tương ứng câu 14'),
('TAG_KT', N'Kiểm tra - Đánh giá', N'Góp ý về hình thức thi, độ khó của đề và tính minh bạch trong chấm điểm - Tương ứng câu 15'),
('TAG_K', N'Góp ý khác', N'Các kiến nghị khác ngoài chuyên môn giảng dạy - Tương ứng câu 16');


-- ======================================================
-- NHÓM 2: CÁC BẢNG FACT (DỮ LIỆU GIAO DỊCH) - 3 BẢNG
-- ======================================================

-- FACT 1: Góp ý tự luận (Bảng trung tâm)
-- Lưu kết quả NLP từ 4 câu hỏi tự luận (13,14,15,16)
CREATE TABLE FACT_GOP_Y_TU_LUAN (
    SubmissionID NVARCHAR(100) PRIMARY KEY,
    MaSV NVARCHAR(50) REFERENCES DIM_SINH_VIEN(MaSV),
    MaLopHP NVARCHAR(50) REFERENCES DIM_LOP_HOC_PHAN(MaLopHP),
    NoiDungGopY NVARCHAR(MAX),   -- Toàn bộ nội dung gộp từ câu 13,14,15,16
    Sentiment NVARCHAR(50),       -- 'positive', 'negative', 'neutral'
    Is_Valid BIT                   -- 1: Hợp lệ, 0: Dữ liệu rác/Spam
);

-- FACT 2: Kết quả đánh giá trắc nghiệm (Dạng dọc)
-- Mỗi dòng là 1 câu trả lời (Câu 1-12), 1 phiếu có 12 dòng
CREATE TABLE FACT_KET_QUA_DANH_GIA (
    ID_KetQua INT IDENTITY(1,1) PRIMARY KEY,
    SubmissionID NVARCHAR(100) REFERENCES FACT_GOP_Y_TU_LUAN(SubmissionID),
    MaCauHoi INT REFERENCES DIM_CAU_HOI(MaCauHoi),  -- 1-12
    Diem INT  -- Giá trị từ 1 đến 5
);

-- FACT 3: Bảng cầu nối Tags (Multi-label Mapping)
-- Cho phép 1 góp ý có nhiều tag (vừa khen vừa chê)
CREATE TABLE FACT_TAG_MAPPING (
    ID_Mapping INT IDENTITY(1,1) PRIMARY KEY,
    SubmissionID NVARCHAR(100) REFERENCES FACT_GOP_Y_TU_LUAN(SubmissionID),
    MaTag NVARCHAR(50) REFERENCES DIM_TAG(MaTag)
);
