# BIGDATA-STOCK-PIPELINE
Hệ thống xử lý dữ liệu lớn (Big Data) phân tích thị trường chứng khoán Việt Nam. Dự án tích hợp hệ sinh thái Hadoop (HDFS, MapReduce, Hive, Sqoop, Airflow) cùng các dịch vụ đám mây AWS (S3, RDS). Kiến trúc bao gồm một lớp middleware sử dụng Apache Drill để truy vấn dữ liệu phân tán và được trực quan hóa thông qua giao diện Streamlit.

## Danh sách thành viên nhóm
- Nguyễn Phúc An
- Nguyễn Quân Bảo
- Trần Quang Duy
- Nguyễn Thanh Bình Minh
- Nguyễn Hữu Thưởng

## Cấu trúc thư mục

BIGDATA-STOCK-PIPELINE/
├── cloud_aws_env/
│   ├── aws_ingestion/
│   ├── frontend/
│   ├── hadoop_scripts/
│   ├── middleware_drill/
│   └── oozie_aws/
├── local_env/
│   ├── data_ingestion/
│   ├── database/
│   ├── frontend/
│   ├── hadoop_scripts/
│   ├── middleware_drill/
│   └── oozie_workflow/
├── shared_resources/
│   ├── data_csv/
│   └── libs/
├── .gitignore
└── README.md

## Mục đích của từng thư mục

### 1. Môi trường Cloud AWS (cloud_aws_env)
Thư mục chứa các mã nguồn và cấu hình chuyên biệt để chạy trên nền tảng điện toán đám mây AWS.
- aws_ingestion: Chứa script thu thập dữ liệu và đẩy trực tiếp lên Data Lake AWS S3.
- frontend: Mã nguồn giao diện Streamlit kết nối với hệ thống Drill/RDS trên Cloud.
- hadoop_scripts: Chứa các script tiền xử lý dữ liệu lớn tương thích với môi trường Cloud.
- middleware_drill: Cấu hình kết nối cho Apache Drill tương tác với HDFS, AWS S3 và AWS RDS.
- oozie_aws: Chứa file cấu hình workflow để tự động hóa toàn bộ luồng xử lý dữ liệu trên Cloud.

### 2. Môi trường Local (local_env)
Thư mục chứa mã nguồn chạy cục bộ trên máy cá nhân, dùng để kiểm thử độc lập trước khi đưa lên Cloud.
- data_ingestion: Script Python cào dữ liệu thô (Raw data) về máy tính cá nhân.
- database: Chứa file SQL khởi tạo các bảng dữ liệu gốc trong MySQL Local.
- frontend: Mã nguồn giao diện Streamlit kết nối với dữ liệu cục bộ.
- hadoop_scripts: Chứa các file HiveQL, Mapper, Reducer để chạy tính toán trên Hadoop giả lập (Pseudo-Distributed) ở máy tính cá nhân.
- middleware_drill: Cấu hình kết nối Drill với hệ thống HDFS và MySQL nội bộ.
- oozie_workflow: File cấu hình tuần tự hóa quá trình chạy trên máy Local.

### 3. Tài nguyên dùng chung (shared_resources)
Thư mục lưu trữ những dữ liệu tĩnh và thư viện bắt buộc mà cả môi trường Local và Cloud đều sử dụng.
- data_csv: Lưu trữ các file dữ liệu giả lập (dummy data) hoặc dữ liệu tĩnh có cấu trúc chuẩn để test các module code.
- libs: Chứa các thư viện bổ sung cần thiết như driver kết nối MySQL (mysql-connector-java.jar).
