          ┌────────────────────────────────────────────────────────┐
          │                        Data Source                     │
          │                     (Postgres DB)                      │
          └────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                          Debezium (CDC Connector)
                                      │
                                      ▼
                             Kafka (Message Bus)
                                      │
                                      ▼
                      Spark Structured Streaming (Ingest)
                                      │
                                      ▼
                  ┌───────────────────────────────────────┐
                  │        Bronze Layer (Raw, Delta)      │
                  │      stored in MinIO + Delta Lake     │
                  └───────────────────────────────────────┘
                                 │
             ┌───────────────────┴───────────────────┐
             │                                       │
     Real-time ETL (Streaming)              Batch ETL (Cuối ngày)
             │                                       │
             ▼                                       ▼
   Silver_RT Layer (Clean, near real-time)   Silver_H Layer (Historical clean)
             │                                       │
             ▼                                       ▼
   Gold_RT Layer (Aggregated real-time)      Gold_H Layer (Historical aggregate)
             │                                       │
             └───────────────────┬───────────────────┘
                                 ▼
                    BI Layer (Dashboard)
                 (PowerBI, Superset, Grafana, v.v.)




1. Config deltalake + MinIO  (Việt)   
2. Streaming (Phát)  
3. Batch ETL (Phúc hoặc Đăng)
4. BI (Streaming - Grafana, Batch ETL - PowerBI, supperset ) (Phượng)
  