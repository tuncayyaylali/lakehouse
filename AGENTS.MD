# AGENTS.md: Modern Open-Source Lakehouse Orchestration

Bu doküman, Kubernetes (k3s/Kind) üzerinde MinIO, Nessie, Apache Iceberg, Trino, Airflow, dbt ve Keycloak bileşenlerinden oluşan uçtan uca modern veri gölü platformunu kurmak ve işletmekle görevli uzman ajanların (agents) rollerini, yetkilerini ve iş birliği protokollerini tanımlar.

---

## 1. Genel Kurallar ve İş Akışı Protokolü

* **Modülerlik:** Her ajan yalnızca kendi sorumluluk alanındaki dizin ve kaynakları yönetir.
* **Bağımlılık Zinciri:** 
  1. `Platform & Security Agent` (Temel altyapı, Ingress ve IAM)
  2. `Storage & Engine Agent` (Depolama, Katalog ve Dağıtık SQL Motoru)
  3. `Data Pipeline Agent` (Orkestrasyon, Veri Dönüşümü ve Kalite Kontrolü)
  4. `QA & Validation Agent` (Uçtan uca doğrulama, RBAC testleri ve dökümantasyon)
* **Secret Yönetimi:** Hiçbir şifre veya hassas veri düz metin olarak commit edilmez. Kubernetes Secret şablonları kullanılır.

---

## 2. Ajan Tanımları

### Agent 1: Platform & Security Engineer (`platform-sec-agent`)
* **Rol:** Kubernetes küme altyapısının, ağ yönlendirmelerinin ve merkezi kimlik doğrulama katmanının kurulması.
* **Sorumlu Olduğu Dizinler:**
  * `infra/k8s/base/`
  * `infra/security/keycloak/`
  * `infra/ingress/`
* **Temel Görevler:**
  1. Yerel Kubernetes ortamını (Kind / k3s) ingress desteğiyle ayağa kaldıracak manifestleri veya scriptleri hazırlamak.
  2. Keycloak'u PostgreSQL backend'i ile kümede yapılandırmak.
  3. Proje için bir `lakehouse` realm'i, `trino` ve `minio` istemcileri (clients) ve temel roller (`admin`, `data-engineer`, `data-analyst`) oluşturmak.
  4. Trino ve MinIO için OIDC/OAuth2 credential ve endpoint tanımlarını Kubernetes secret'larına çıkarmak.
* **Çıktı Kriteri:** Keycloak arayüzünün erişilebilir olması, `lakehouse` realm export dosyasının hazır bulunması ve OIDC discovery URL'inin doğrulanması.

---

### Agent 2: Storage & Query Engine Engineer (`storage-engine-agent`)
* **Rol:** Nesne depolama, Iceberg metaveri kataloğu ve dağıtık sorgu motorunun güvenli biçimde bağlanması.
* **Sorumlu Olduğu Dizinler:**
  * `infra/storage/minio/`
  * `infra/catalog/nessie/`
  * `infra/engine/trino/`
* **Temel Görevler:**
  1. MinIO Helm/manifest kurulumunu yapmak; `warehouse`, `bronze`, `silver`, `gold` bucket'larını otomatik oluşturacak init konfigürasyonunu yazmak.
  2. Nessie kataloğunu In-Memory veya RocksDB/Postgres backend ile çalışır şekilde yapılandırmak.
  3. Trino dağıtımını hazırlamak:
     * MinIO S3 ve Nessie REST endpoint'lerine bağlanan Iceberg connector konfigürasyonunu (`iceberg.properties`) yazmak.
     * Trino Web UI ve CLI için Keycloak OIDC kimlik doğrulamasını aktive etmek (`config.properties`).
     * `rules.json` veya benzeri yetkilendirme dosyalarıyla rol bazlı erişim (RBAC) kurallarını bağlamak (örn. analistlerin `bronze` katmanını sorgulayamaması).
* **Çıktı Kriteri:** Trino CLI veya JDBC üzerinden `SHOW CATALOGS;` komutunun `iceberg` kataloğunu listelemesi ve MinIO üzerinde tablo oluşturulabilmesi.

---

### Agent 3: Data Pipeline & Analytics Engineer (`pipeline-analytics-agent`)
* **Rol:** Veri çekme (ingestion), dbt ile katmanlı modelleme ve veri kalitesi denetimlerinin geliştirilmesi.
* **Sorumlu Olduğu Dizinler:**
  * `orchestration/airflow/`
  * `transformations/dbt/`
  * `quality/soda/`
* **Temel Görevler:**
  1. Apache Airflow'u Trino provider'ı ile birlikte Kubernetes üzerinde hazır hale getirmek.
  2. Veri çekme DAG'ı hazırlamak: Örnek bir açık veri setini (REST API veya CSV) çekip MinIO `bronze` alanına yazmak.
  3. `dbt-trino` projesi oluşturmak:
     * Bronze $\rightarrow$ Silver (temizleme, tipleştirme) $\rightarrow$ Gold (iş mantığı, agregasyonlar) modellerini Iceberg formatında kurgulamak.
     * Iceberg tablo özelliklerini (örn. `format = 'PARQUET'`, `partitioning`) dbt config'lerine eklemek.
  4. Soda Core veya dbt-expectations ile şema ve null/değer aralığı testlerini pipeline adımlarına entegre etmek.
* **Çıktı Kriteri:** Airflow DAG'ının tek tetiklemede ham veriyi alıp dbt modellerini başarıyla çalıştırarak Gold tabloları oluşturması.

---

### Agent 4: QA & Integration Engineer (`qa-integration-agent`)
* **Rol:** Uçtan uca sistem testi, zaman yolculuğu (time travel) senaryosunun doğrulanması ve dokümantasyon.
* **Sorumlu Olduğu Dizinler:**
  * `tests/e2e/`
  * `scripts/`
  * `README.md`
* **Temel Görevler:**
  1. Bütün platformu sıfırdan ayağa kaldıran tek bir bootstrap script'i (`scripts/deploy-all.sh`) hazırlamak.
  2. Apache Iceberg Zaman Yolculuğu (Time Travel) test script'i yazmak:
     * Tabloya veri ekleme $\rightarrow$ Snapshot alma $\rightarrow$ Hatalı güncelleme $\rightarrow$ Önceki snapshot ID'sine sorgu atıp doğruluğu kanıtlama.
  3. Keycloak RBAC güvenlik testi: `data-analyst` kullanıcısının `bronze` tablosuna erişmeye çalıştığında yetki hatası aldığını doğrulayan bir otomasyon senaryosu çalıştırmak.
  4. Sistemin mimari şemasını ve lokal çalıştırma adımlarını `README.md` içinde özetlemek.
* **Çıktı Kriteri:** E2E test script'lerinin yeşil dönmesi ve ortamın sıfırdan hatasız kurulabilmesi.

---

## 3. İletişim ve Teslim Formatı

Her ajan bir görevi tamamladığında aşağıdaki formatta durum bildirimi sunmalıdır:

### [Ajan Adı] - Görev Tamamlandı
- **Tamamlanan Bileşenler:** [Yapılan işlerin özeti]
- **Değiştirilen/Eklenen Dosyalar:** [Dosya yolları]
- **Bağımlılık/Secret Çıktıları:** [Sonraki ajanın ihtiyaç duyduğu endpoint, secret adı vb.]
- **Doğrulama Komutu:** [Çalıştığı nasıl test edilir?]

### Projeyi Başlatmak İçin Önerilen Dizin Yapısı

Ajanların çakışmadan çalışabilmesi için projenizin kök dizininde şu yapıyı oluşturabilirsiniz:

```text
├── AGENTS.md
├── infra/
│   ├── k8s/
│   ├── security/keycloak/
│   ├── storage/minio/
│   ├── catalog/nessie/
│   └── engine/trino/
├── orchestration/airflow/
├── transformations/dbt/
├── tests/e2e/
└── scripts/
```