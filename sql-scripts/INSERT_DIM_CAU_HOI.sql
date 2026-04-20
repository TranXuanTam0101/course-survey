-- Xóa dữ liệu cũ
DELETE FROM DIM_CAU_HOI;
GO

-- Chèn dữ liệu với N
INSERT INTO DIM_CAU_HOI (MaCauHoi, ThuTuCauHoi, Phan, NoiDung, LoaiTraLoi)
VALUES
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
GO

-- Kiểm tra lại
SELECT MaCauHoi, NoiDung FROM DIM_CAU_HOI WHERE MaCauHoi = 1;
GO
