-- 1. Olay Öncesi Durum (10 Sipariş, $5,405.50 Tutar)
SELECT '1. OLAY ONCESI DURUM' AS adim, count(*) AS siparis_sayisi, round(sum(amount), 2) AS toplam_tutar FROM iceberg.bronze.orders;

-- 2. Hatalı Silme Simülasyonu (Electronics kategorisindeki 4 sipariş siliniyor)
DELETE FROM iceberg.bronze.orders WHERE category = 'Electronics';

-- 3. Hata Sonrası Hasarlı Durum (Yalnızca 6 sipariş ve $825.50 kaldı!)
SELECT '2. HATA SONRASI HASARLI DURUM' AS adim, count(*) AS siparis_sayisi, round(sum(amount), 2) AS toplam_tutar FROM iceberg.bronze.orders;

-- 4. Iceberg Zaman Yolculuğu (Time Travel - Olay öncesi Snapshot 3972403013475769839 üzerinden sorgulama)
SELECT '3. TIME TRAVEL ILE GECMIS SNAPSHOT' AS adim, count(*) AS siparis_sayisi, round(sum(amount), 2) AS toplam_tutar 
FROM iceberg.bronze.orders FOR VERSION AS OF 3972403013475769839;

-- 5. Silinen Verilerin Snapshot Üzerinden Geri Yüklenmesi (Disaster Recovery)
INSERT INTO iceberg.bronze.orders
SELECT * FROM iceberg.bronze.orders FOR VERSION AS OF 3972403013475769839
WHERE category = 'Electronics';

-- 6. Nihai Kurtarılmış Durum (Tablo yeniden eksiksiz 10 siparişe ve $5,405.50 tutara kavuştu!)
SELECT '4. GERI YUKLEME SONRASI FINAL DURUM' AS adim, count(*) AS siparis_sayisi, round(sum(amount), 2) AS toplam_tutar FROM iceberg.bronze.orders;

