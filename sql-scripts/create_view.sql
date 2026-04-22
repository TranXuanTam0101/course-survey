CREATE VIEW VW_TONG_HOP AS
SELECT 
    -- Thông tin từ DIM_CAU_HOI
    ch.MaCauHoi,
    ch.ThuTuCauHoi,
    ch.Phan AS PhanCauHoi,
    ch.NoiDung AS NoiDungCauHoi,
    ch.LoaiTraLoi,
    
    -- TÁCH THÀNH 2 CỘT: SỐ (Điểm) và TEXT (Feedback)
    CASE 
        WHEN ch.MaCauHoi BETWEEN 1 AND 12 THEN
            CASE ch.MaCauHoi
                WHEN 1 THEN fts.C1 WHEN 2 THEN fts.C2 WHEN 3 THEN fts.C3 WHEN 4 THEN fts.C4
                WHEN 5 THEN fts.C5 WHEN 6 THEN fts.C6 WHEN 7 THEN fts.C7 WHEN 8 THEN fts.C8
                WHEN 9 THEN fts.C9 WHEN 10 THEN fts.C10 WHEN 11 THEN fts.C11 WHEN 12 THEN fts.C12
            END
        ELSE NULL
    END AS DiemSo,
    
    CASE 
        WHEN ch.MaCauHoi BETWEEN 13 AND 16 THEN
            CASE ch.MaCauHoi
                WHEN 13 THEN fts.C13 WHEN 14 THEN fts.C14 WHEN 15 THEN fts.C15 WHEN 16 THEN fts.C16
            END
        ELSE NULL
    END AS Feedback,
    
    -- Thông tin từ FACT_TRA_LOI_KHAO_SAT
    fts.SubmissionID,
    
    -- Thông tin từ DIM_SINH_VIEN
    sv.MaSV,
    sv.HoDem AS HoDemSV,
    sv.Ten AS TenSV,
    sv.NgaySinh,
    
    -- Thông tin từ DIM_LOP_SINH_VIEN
    lop.MaLop,
    lop.Lop AS TenLop,
    
    -- Thông tin từ DIM_CHUYEN_NGANH (của sinh viên)
    cn.MaChuyenNganh,
    cn.TenChuyenNganh,
    
    -- Thông tin từ DIM_KHOA (Khoa quản lý sinh viên)
    k_sv.MaKhoa AS MaKhoa_SV,
    k_sv.TenKhoa AS TenKhoa_SV,
    
    -- Thông tin từ DIM_CHUONG_TRINH_DAO_TAO
    ct.MaCTDT,
    ct.TenCTDT,
    
    -- Thông tin từ DIM_LOP_HOC_PHAN
    lhp.MaLopHP,
    lhp.LopHP AS TenLopHP,
    
    -- Thông tin từ DIM_HOC_PHAN
    hp.MaHP,
    hp.TenHP,
    
    -- Thông tin từ DIM_KHOA (Khoa quản lý học phần) ⭐ THÊM MỚI
    k_hp.MaKhoa AS MaKhoa_HP,
    k_hp.TenKhoa AS TenKhoa_HP,
    
    -- Thông tin từ DIM_GIANG_VIEN
    gv.MaGV,
    gv.HoDemGV,
    gv.TenGV,
    
    -- Thông tin từ DIM_HOC_KY
    hk.MaHocKy,
    hk.NamHoc,
    hk.HocKy
    
FROM DIM_CAU_HOI ch
CROSS JOIN FACT_TRA_LOI_KHAO_SAT fts
LEFT JOIN DIM_SINH_VIEN sv ON fts.MaSV = sv.MaSV
LEFT JOIN DIM_LOP_SINH_VIEN lop ON sv.MaLop = lop.MaLop
LEFT JOIN DIM_CHUYEN_NGANH cn ON lop.MaChuyenNganh = cn.MaChuyenNganh
LEFT JOIN DIM_KHOA k_sv ON cn.MaKhoa = k_sv.MaKhoa  -- Khoa quản lý sinh viên
LEFT JOIN DIM_CHUONG_TRINH_DAO_TAO ct ON cn.MaCTDT = ct.MaCTDT
LEFT JOIN DIM_LOP_HOC_PHAN lhp ON fts.MaLopHP = lhp.MaLopHP
LEFT JOIN DIM_HOC_PHAN hp ON lhp.MaHP = hp.MaHP
LEFT JOIN DIM_KHOA k_hp ON hp.MaKhoa = k_hp.MaKhoa  -- ⭐ Khoa quản lý học phần
LEFT JOIN DIM_GIANG_VIEN gv ON lhp.MaGV = gv.MaGV
LEFT JOIN DIM_HOC_KY hk ON lhp.MaHocKy = hk.MaHocKy;
