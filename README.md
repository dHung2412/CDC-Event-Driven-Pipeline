# ETL Data Pipeline Project

Dự án ETL (Extract, Transform, Load) để xử lý dữ liệu GitHub Events với Apache Spark, Kafka và nhiều database backends (MySQL, MongoDB, Redis).

## 📋 Mục lục

- [Tổng quan](#tổng-quan)
- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Công nghệ sử dụng](#công-nghệ-sử-dụng)
- [Cài đặt](#cài-đặt)
- [Cấu hình](#cấu-hình)
- [Cấu trúc thư mục](#cấu-trúc-thư-mục)
- [Sử dụng](#sử-dụng)
- [Tính năng chính](#tính-năng-chính)

## 🎯 Tổng quan

Dự án này xây dựng một pipeline ETL hoàn chỉnh để:
- Đọc và xử lý dữ liệu GitHub Events từ file JSON
- Sử dụng Apache Spark để transform và load dữ liệu
- Hỗ trợ ghi dữ liệu vào nhiều database (MySQL, MongoDB, Redis)
- Streaming data real-time với Kafka
- Theo dõi thay đổi dữ liệu thông qua MySQL triggers
- Validate tính toàn vẹn dữ liệu sau khi ghi

## 🏗️ Kiến trúc hệ thống

```
GitHub JSON Data
      ↓
Apache Spark (ETL Processing)
      ↓
├─→ MySQL (Relational DB)
├─→ MongoDB (Document DB)
└─→ Redis (Cache/Key-Value)
      ↓
MySQL Triggers → Kafka → Consumer
```

### Workflow:
1. **Extract**: Đọc dữ liệu GitHub Events từ JSON files
2. **Transform**: Xử lý và chuẩn hóa dữ liệu với Spark
3. **Load**: Ghi dữ liệu vào MySQL, MongoDB, Redis
4. **CDC (Change Data Capture)**: MySQL triggers capture changes
5. **Streaming**: Kafka producer/consumer xử lý real-time data

## 🛠️ Công nghệ sử dụng

- **Apache Spark**: Xử lý dữ liệu phân tán
- **Apache Kafka**: Message streaming platform
- **MySQL**: Relational database với triggers
- **MongoDB**: NoSQL document database
- **Redis**: In-memory cache/key-value store
- **Python 3.x**: Ngôn ngữ chính
- **PySpark**: Spark Python API

## 📦 Cài đặt

### Yêu cầu hệ thống

- Python 3.8+
- Java 8 hoặc 11 (cho Spark)
- MySQL Server
- MongoDB Server
- Redis Server
- Apache Kafka

### Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### Requirements.txt bao gồm:
```
mysql-connector-python
pyspark
kafka-python
python-dotenv
pymongo
redis
findspark
avro-python3
fastparquet
```

## ⚙️ Cấu hình

### 1. Tạo file `.env`

Tạo file `.env` trong thư mục gốc với các thông tin sau:

```env
# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=github_data
MONGO_PACKAGE_PATH=/path/to/mongo-spark-connector.jar

# MySQL Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=github_data
MYSQL_JAR_PATH=/path/to/mysql-connector.jar

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_USER=default
REDIS_PASSWORD=your_password
REDIS_DB=0
REDIS_JAR_PATH=/path/to/redis-connector.jar
```

### 2. Setup Database Schema

#### MySQL:
```bash
# Chạy schema.sql
mysql -u root -p < sql/schema.sql

# Chạy triggers
mysql -u root -p < sql/trigger.sql
```

#### MongoDB:
Schema sẽ được tạo tự động khi chạy `main.py`

## 📁 Cấu trúc thư mục

```
project/
├── config/
│   ├── database_config.py      # Cấu hình databases
│   └── spark_config.py         # Cấu hình Spark
├── database/
│   ├── mongodb_connect.py      # MongoDB connector
│   ├── mysql_connect.py        # MySQL connector
│   └── redis_connect.py        # Redis connector
├── sql/
│   ├── schema.sql              # Database schema
│   └── trigger.sql             # MySQL triggers
├── src/
│   ├── schema_manager.py       # Quản lý schema
│   ├── main.py                 # Main entry point
│   ├── ETL/
│   │   ├── trigger-kafka.py    # Kafka producer
│   │   └── consumer-spark.py   # Kafka consumer
│   └── spark/
│       ├── mainSpark.py        # Spark main job
│       └── spark_write_data.py # Spark write operations
├── data/
│   └── 2015-03-01-17.json     # Sample data
├── requirements.txt
└── .env
```

## 🚀 Sử dụng

### 1. Khởi tạo Database và Schema

```bash
python src/main.py
```

Chức năng:
- Tạo collections/tables trong MongoDB và MySQL
- Insert dữ liệu mẫu
- Validate schema

### 2. Chạy Spark ETL Job

```bash
python src/spark/mainSpark.py
```

Job này sẽ:
- Đọc JSON data với schema định nghĩa
- Transform dữ liệu (extract actor, repo info)
- Ghi vào MySQL và MongoDB
- Validate dữ liệu sau khi ghi

### 3. Kafka Streaming

#### Chạy Producer (Trigger-based CDC):
```bash
python src/ETL/trigger-kafka.py
```

Producer này:
- Monitor MySQL trigger log tables
- Capture INSERT/UPDATE/DELETE events
- Send changes to Kafka topic

#### Chạy Consumer:
```bash
python src/ETL/consumer-spark.py
```

Consumer này:
- Subscribe Kafka topic
- Process real-time messages
- Display change events

## ✨ Tính năng chính

### 1. Multi-Database Support
- Ghi dữ liệu đồng thời vào MySQL, MongoDB, Redis
- Quản lý kết nối với context managers
- Error handling và validation

### 2. Change Data Capture (CDC)
- MySQL triggers capture DML operations (INSERT, UPDATE, DELETE)
- Log changes vào `user_log_before` và `user_log_after` tables
- Timestamp tracking cho incremental processing

### 3. Kafka Streaming
- Producer theo dõi trigger logs
- Incremental load based on timestamp
- Real-time data streaming

### 4. Spark ETL Processing
- Schema enforcement
- Data transformation
- Batch processing
- Write validation

### 5. Data Validation
- Verify record count
- Compare data integrity
- Auto-retry missing records
- Cleanup temporary columns

### 6. Configuration Management
- Environment-based configuration
- Dataclass validation
- Centralized config management

## 📊 Data Flow

### ETL Flow:
```
JSON Files → Spark Read (with schema)
          ↓
    Transform (select, alias columns)
          ↓
    Write to Databases
          ↓
    Validation (count + data comparison)
          ↓
    Cleanup (drop temp columns)
```

### CDC Flow:
```
MySQL DML Operations
          ↓
Triggers Fire (BEFORE/AFTER)
          ↓
Log to user_log_after table
          ↓
Producer polls new records
          ↓
Send to Kafka topic
          ↓
Consumer processes events
```

## 🔍 Validation Logic

Spark validation process:
1. Thêm temporary column (`spark_temp`) để đánh dấu
2. Write data với temporary marker
3. Read back data với filter
4. So sánh count và data integrity
5. Insert missing records (nếu có)
6. Drop temporary column sau khi validate

## 🤝 Contributing

Mọi đóng góp đều được chào đón! Vui lòng:
1. Fork repository
2. Tạo feature branch
3. Commit changes
4. Push và tạo Pull Request

## 📝 License

Project này được phát triển cho mục đích học tập và nghiên cứu.

## 📧 Contact

Nếu có câu hỏi hoặc đề xuất, vui lòng tạo issue trên GitHub.

---

**Happy Coding!** 🎉