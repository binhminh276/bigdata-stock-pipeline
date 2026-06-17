# BIGDATA-STOCK-PIPELINE

Hệ thống xử lý dữ liệu lớn (Big Data) phân tích thị trường chứng khoán Việt Nam. Dự án tích hợp hệ sinh thái Hadoop (HDFS, MapReduce, Hive, Sqoop, Spark), Apache Airflow cùng các dịch vụ đám mây AWS (S3, RDS). Kiến trúc bao gồm một lớp middleware sử dụng Apache Drill để truy vấn dữ liệu phân tán và được trực quan hóa thông qua giao diện Streamlit.

## 1. Thông tin đề tài
- STT: 02
- Tên đề tài: XÂY DỰNG DATA PIPELINE PHÂN TÍCH DỮ LIỆU CỦA NGÂN HÀNG TRÊN THỊ TRƯỜNG CHỨNG KHOÁN VIỆT NAM SỬ DỤNG HADOOP ECOSYSTEM VÀ AWS CLOUD
- Lớp học phần: BDES333877_04
- Năm học: HKII/2025-2026

## 2. Thông tin nhóm
1. Nguyễn Thanh Bình Minh (23110266)
2. Nguyễn Phúc An (23110175)
3. Trần Quang Duy (23110195)
4. Nguyễn Quân Bảo (23110181)
5. Nguyễn Hữu Thưởng (23110338)

## 3. Công nghệ sử dụng
- Ngôn ngữ lập trình: Python, SQL (HiveQL, MySQL)
- Hệ sinh thái Hadoop: HDFS, MapReduce, Apache Hive, Apache Spark, Apache Sqoop, Apache Kafka
- Nền tảng Cloud (AWS): Amazon S3, Amazon EC2, Amazon RDS, AWS Step Functions
- Công cụ điều phối (Orchestration): Apache Airflow, Apache Oozie
- Truy vấn và Trực quan hóa: Apache Drill, Streamlit
- Môi trường: Ubuntu Server và AWS Cloud
## 4. Kiến trúc hệ thống
Dự án được thiết kế và triển khai thành kiến trúc ELT trên 2 môi trường: môi trường máy chủ cục bộ (Local) và nền tảng đám mây (AWS Cloud). Cả hai đều hoạt động dựa trên luồng xử lý (Data Pipeline) chuẩn:
- Lớp Thu thập: Cào dữ liệu chứng khoán ngân hàng từ các nguồn tài chính bằng Python.
- Lớp Lưu trữ thô: Dữ liệu được đẩy vào HDFS (đối với Local) hoặc Amazon S3 (đối với Cloud).
- Lớp Tiền xử lý và Tính toán: Sử dụng Apache Hive để làm sạch dữ liệu thô và Apache Spark để tính toán phân tán các chỉ số kỹ thuật.
- Lớp Truyền tải và Lưu trữ đích: Đẩy dữ liệu đã xử lý qua Apache Kafka / Apache Sqoop và lưu trữ tập trung tại MySQL / Amazon RDS.
- Lớp Điều phối: Tự động hóa toàn bộ tiến trình bằng Airflow (Local) và AWS Step Functions (Cloud).
- Lớp Giao diện: Apache Drill kết nối đa nguồn dữ liệu, cung cấp endpoint cho Streamlit vẽ biểu đồ trực quan.

## 5. Các chức năng chính
- Tự động hóa luồng thu thập dữ liệu thô liên tục định kỳ 5 phút/lần.
- Xử lý phân tán theo lô (batch processing) 2 lần/ngày để cập nhật các chỉ số kỹ thuật chứng khoán.
- Giao diện người dùng (GUI) trực quan hóa dữ liệu theo thời gian thực, hỗ trợ các thao tác tương tác dữ liệu (xem, thêm, sửa, xóa) an toàn.
- Kiến trúc AWS Cloud giúp giải quyết bài toán giới hạn phần cứng, dễ dàng mở rộng tài nguyên (RAM, CPU) và chạy ngầm 24/7 không cần treo máy cá nhân.

## 6. Cấu trúc thư mục

BIGDATA-STOCK-PIPELINE/
├── cloud_aws_env/
│   ├── aws_ingestion/
│   ├── frontend/
│   ├── hadoop_scripts/
│   ├── middleware_drill/
├── local_env/
│   ├── data_ingestion/
│   ├── database/
│   ├── frontend/
│   ├── hadoop_scripts/
│   ├── middleware_drill/
│   └── airflow_workflow/
├── shared_resources/
│   ├── data_csv/
│   └── libs/
├── .gitignore
└── README.md

## 7. Mục đích của từng thư mục

### Môi trường Cloud AWS (cloud_aws_env)
Thư mục chứa các mã nguồn và cấu hình chuyên biệt để chạy trên nền tảng điện toán đám mây AWS.
- aws_ingestion: Chứa script thu thập dữ liệu và đẩy trực tiếp lên Data Lake AWS S3.
- frontend: Mã nguồn giao diện Streamlit kết nối với hệ thống Drill/RDS trên Cloud.
- hadoop_scripts: Chứa các script tiền xử lý dữ liệu lớn tương thích với môi trường Cloud.
- middleware_drill: Cấu hình kết nối cho Apache Drill tương tác với HDFS, AWS S3 và AWS RDS.

### Môi trường Local (local_env)
Thư mục chứa mã nguồn chạy cục bộ trên máy cá nhân, dùng để kiểm thử độc lập trước khi đưa lên Cloud.
- data_ingestion: Script Python cào dữ liệu thô (Raw data) về máy tính cá nhân.
- database: Chứa file SQL khởi tạo các bảng dữ liệu gốc trong MySQL Local.
- frontend: Mã nguồn giao diện Streamlit kết nối với dữ liệu cục bộ.
- hadoop_scripts: Chứa các file HiveQL, Mapper, Reducer để chạy tính toán trên Hadoop giả lập (Pseudo-Distributed) ở máy tính cá nhân.
- middleware_drill: Cấu hình kết nối Drill với hệ thống HDFS và MySQL nội bộ.
- airflow_workflow: File cấu hình tuần tự hóa quá trình chạy trên máy Local.

### Tài nguyên dùng chung (shared_resources)
Thư mục lưu trữ những dữ liệu tĩnh và thư viện bắt buộc mà cả môi trường Local và Cloud đều sử dụng.
- data_csv: Lưu trữ dataset raw và cleaned gốc.
- libs: Chứa các thư viện bổ sung cần thiết như driver kết nối MySQL (mysql-connector-java.jar).

## 8. Hướng dẫn cài đặt và sử dụng

### Yêu cầu hệ thống
- Môi trường máy tính cài đặt sẵn Python 3.x, Java 8/11.
- Hệ sinh thái cục bộ: Hadoop 3.x, Spark 3.x, Hive, Drill, MySQL Server.
- Tài khoản AWS IAM có đủ quyền truy cập S3, EC2, RDS (nếu chạy luồng Cloud).
