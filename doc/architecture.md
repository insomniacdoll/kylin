# Apache Kylin 5.0.4 架构文档

## 1. 总体架构图

```mermaid
graph TB
    subgraph Client["客户端层 (Client Layer)"]
        WebUI["Web UI<br/>(Kystudio)"]
        JDBC["JDBC Driver"]
        BITools["BI Tools<br/>(Tableau等)"]
        RESTAPI["REST API"]
    end

    subgraph Controller["控制器层 (Controller Layer)"]
        NQueryCtrl["NQueryCtrl<br/>(查询控制器)"]
        JobCtrl["JobCtrl<br/>(作业控制器)"]
        NModelCtrl["NModelCtrl<br/>(模型控制器)"]
        SegmentCtrl["SegmentCtrl<br/>(分段控制器)"]
        ProjectCtrl["ProjectCtrl<br/>(项目控制器)"]
        TableCtrl["TableCtrl<br/>(表控制器)"]
    end

    subgraph Service["服务层 (Service Layer)"]
        QueryService["QueryService<br/>(查询服务)"]
        JobService["JobService<br/>(作业服务)"]
        ModelService["ModelService<br/>(模型服务)"]
        AsyncQueryService["AsyncQueryService<br/>(异步查询)"]
        SnapshotService["SnapshotService<br/>(快照服务)"]
        IndexPlanService["IndexPlanService<br/>(索引计划服务)"]
    end

    subgraph Metadata["元数据层 (Metadata Layer)"]
        NProjectManager["NProjectManager<br/>(项目管理器)"]
        NModelManager["NModelManager<br/>(模型管理器)"]
        IndexPlanMgr["IndexPlanMgr<br/>(索引计划管理器)"]
        DataflowManager["DataflowManager<br/>(数据流管理器)"]
        SegmentManager["SegmentManager<br/>(分段管理器)"]
        TableMetadataMgr["TableMetadataMgr<br/>(表元数据管理器)"]
    end

    subgraph Engine["引擎层 (Engine Layer)"]
        QueryRouting["QueryRouting<br/>(查询路由)"]
        QueryExec["QueryExec<br/>(查询执行器)"]
        CalcitePlanner["CalcitePlanner<br/>(Calcite规划器)"]
        QueryOptimizer["QueryOptimizer<br/>(查询优化器)"]
        NSparkCubingEng["NSparkCubingEng<br/>(Spark构建引擎)"]
        StorageEngine["StorageEngine<br/>(存储引擎)"]
    end

    subgraph Storage["存储层 (Storage Layer)"]
        ParquetStorage["ParquetStorage<br/>(Parquet存储)"]
        DeltaLake["DeltaLake<br/>(Delta湖)"]
        InternalTable["InternalTable<br/>(内部表)"]
        HDFS["HDFS<br/>(分布式文件系统)"]
        ResourceStore["ResourceStore<br/>(元数据存储)"]
    end

    subgraph Infrastructure["基础设施层 (Infrastructure)"]
        ApacheSpark["Apache Spark<br/>3.3.0"]
        Hadoop["Hadoop/HDFS<br/>2.10.1"]
        Zookeeper["Zookeeper<br/>(集群协调)"]
        Kafka["Kafka<br/>2.8.2<br/>(流式)"]
        HiveMetastore["Hive Metastore<br/>2.3.10"]
        Redis["Redis<br/>(缓存)"]
    end

    WebUI --> Controller
    JDBC --> Controller
    BITools --> Controller
    RESTAPI --> Controller

    Controller --> Service
    Service --> Metadata
    Metadata --> Engine
    Engine --> Storage
    Storage --> Infrastructure

    style Client fill:#e1f5ff
    style Controller fill:#fff4e1
    style Service fill:#e8f5e9
    style Metadata fill:#f3e5f5
    style Engine fill:#fce4ec
    style Storage fill:#fff3e0
    style Infrastructure fill:#e0f7fa
```

## 2. 模块依赖关系图

```mermaid
graph TD
    core_common["core-common<br/>(基础工具库)"]
    core_metadata["core-metadata<br/>(元数据管理)"]
    common_service["common-service<br/>(公共服务)"]
    core_storage["core-storage<br/>(存储抽象)"]
    query_service["query-service<br/>(查询服务)"]
    query["query<br/>(查询引擎)"]
    query_common["query-common<br/>(查询公共库)"]
    core_job["core-job<br/>(作业框架)"]
    core_metrics["core-metrics<br/>(指标收集)"]
    modeling_svc["modeling-service<br/>(建模服务)"]
    datasource_svc["datasource-service<br/>(数据源服务)"]
    streaming_service["streaming-service<br/>(流式服务)"]
    source_hive["source-hive<br/>(Hive数据源)"]
    streaming_sdk["streaming-sdk<br/>(流式SDK)"]
    data_loading_svc["data-loading-service<br/>(数据加载服务)"]
    job_service["job-service<br/>(作业服务)"]
    rec_service["rec-service<br/>(推荐服务)"]
    engine_spark["engine-spark<br/>(Spark引擎)"]
    spark_common["spark-common<br/>(Spark公共库)"]
    query_srv["query-server<br/>(查询服务器)"]
    common_srv["common-server<br/>(通用服务器)"]
    metadata_srv["metadata-server<br/>(元数据服务器)"]
    data_loading_srv["data-loading-server<br/>(数据加载服务器)"]
    query_booter["query-booter<br/>(查询启动器)"]
    common_booter["common-booter<br/>(通用启动器)"]
    data_loading_booter["data-loading-booter<br/>(数据加载启动器)"]

    core_common --> core_metadata
    core_metadata --> common_service
    core_metadata --> core_storage
    common_service --> query_service
    common_service --> modeling_svc
    common_service --> datasource_svc
    common_service --> streaming_service

    query_service --> query
    query_service --> query_common
    query --> query_common

    core_metadata --> core_job
    core_metadata --> core_metrics

    modeling_svc --> source_hive
    modeling_svc --> data_loading_svc
    modeling_svc --> rec_service

    streaming_service --> streaming_sdk

    data_loading_svc --> job_service
    data_loading_svc --> engine_spark
    job_service --> modeling_svc

    engine_spark --> spark_common

    query_service --> query_srv
    query_service --> common_srv
    common_service --> common_srv
    modeling_svc --> metadata_srv
    data_loading_svc --> data_loading_srv

    query_srv --> query_booter
    common_srv --> common_booter
    data_loading_srv --> data_loading_booter

    style core_common fill:#ffcdd2
    style core_metadata fill:#f8bbd0
    style common_service fill:#e1bee7
    style core_storage fill:#d1c4e9
    style query_service fill:#c5cae9
    style query fill:#bbdefb
    style query_common fill:#b3e5fc
    style core_job fill:#b2ebf2
    style core_metrics fill:#b2dfdb
    style modeling_svc fill:#c8e6c9
    style datasource_svc fill:#dcedc8
    style streaming_service fill:#f0f4c3
    style data_loading_svc fill:#fff9c4
    style job_service fill:#ffecb3
    style rec_service fill:#ffe0b2
    style engine_spark fill:#ffccbc
    style spark_common fill:#ffab91
    style query_srv fill:#ffcc80
    style common_srv fill:#ffb74d
    style metadata_srv fill:#ffa726
    style data_loading_srv fill:#ff9800
    style query_booter fill:#fb8c00
    style common_booter fill:#f57c00
    style data_loading_booter fill:#ef6c00
```

## 3. 核心模块说明

### 3.1 基础模块 (Foundation Modules)

| 模块 | 路径 | 功能说明 |
|------|------|----------|
| **core-common** | `src/core-common` | 基础工具库，包含配置管理、加密工具、日志基础设施等 |
| **core-metadata** | `src/core-metadata` | 元数据管理层，负责项目、模型、索引、分段等元数据的持久化管理 |
| **core-storage** | `src/core-storage` | 存储抽象层，定义存储接口，支持Parquet、Delta Lake等多种存储格式 |
| **core-job** | `src/core-job` | 作业执行框架，提供作业调度、生命周期管理等功能 |
| **core-metrics** | `src/core-metrics` | 指标收集和上报模块 |

### 3.2 服务模块 (Service Modules)

| 模块 | 路径 | 功能说明 |
|------|------|----------|
| **common-service** | `src/common-service` | 公共服务基础设施，包含安全认证、LDAP、会话管理等 |
| **query-service** | `src/query-service` | 查询业务逻辑层，处理SQL查询、异步查询、查询历史等 |
| **modeling-service** | `src/modeling-service` | 建模服务，负责数据模型的管理和语义分析 |
| **data-loading-service** | `src/data-loading-service` | 数据加载服务，管理作业执行和监控 |
| **job-service** | `src/job-service` | 作业编排服务，负责作业调度和执行协调 |
| **datasource-service** | `src/datasource-service` | 数据源管理服务 |
| **streaming-service** | `src/streaming-service` | 流式数据处理服务 |
| **rec-service** | `src/rec-service` | 推荐引擎服务，自动推荐模型和索引 |

### 3.3 引擎模块 (Engine Modules)

| 模块 | 路径 | 功能说明 |
|------|------|----------|
| **query** | `src/query` | 核心查询引擎，包含查询路由、执行器、Calcite优化器等 |
| **query-common** | `src/query-common` | 查询引擎公共库 |
| **engine-spark** | `src/spark-project/engine-spark` | Spark构建和查询引擎，负责Segment构建、合并等作业 |
| **spark-common** | `src/spark-project/spark-common` | Spark公共库 |
| **sparder** | `src/spark-project/sparder` | Spark执行框架 |

### 3.4 服务器模块 (Server Modules)

| 模块 | 路径 | 功能说明 |
|------|------|----------|
| **common-server** | `src/common-server` | 单体服务器，包含所有服务 |
| **query-server** | `src/query-server` | 查询专用服务器 |
| **metadata-server** | `src/metadata-server` | 元数据管理专用服务器 |
| **data-loading-server** | `src/data-loading-server` | 数据加载专用服务器 |
| **ops-server** | `src/ops-server` | 运维服务器 |
| **rec-server** | `src/rec-server` | 推荐引擎服务器 |

### 3.5 启动器模块 (Booter Modules)

| 模块 | 路径 | 功能说明 |
|------|------|----------|
| **common-booter** | `src/common-booter` | 单体模式启动器 |
| **query-booter** | `src/query-booter` | 查询服务启动器 |
| **data-loading-booter** | `src/data-loading-booter` | 数据加载服务启动器 |
| **ops-booter** | `src/ops-booter` | 运维服务启动器 |
| **rec-booter** | `src/rec-booter` | 推荐服务启动器 |

## 4. 技术栈

### 核心技术
- **Java**: 1.8
- **Scala**: 2.12.13
- **Spring Boot**: 2.7.18
- **Spring Security**: 认证授权
- **Calcite**: 1.30.0-kylin-5.x-r3 (SQL解析和优化)
- **Apache Spark**: 3.3.0-kylin-5.2.2 (计算引擎)
- **Hadoop**: 2.10.1 (分布式存储)
- **Hive**: 2.3.10 (元数据和目录)
- **MyBatis**: 3.5.6 (数据库访问)

### 存储
- **Parquet**: 列式存储格式
- **Delta Lake**: ACID事务存储
- **HDFS**: 分布式文件系统
- **Gluten**: 原生计算引擎 (ClickHouse后端)

### 缓存
- **Redis**: 分布式缓存
- **Memcached**: 可选缓存
- **Ehcache**: 本地缓存

### 流式
- **Kafka**: 2.8.2 (流式数据源)

### 安全
- **Spring Security**: 安全框架
- **LDAP**: 企业认证
- **Kerberos**: Hadoop安全认证

## 5. 部署架构

```mermaid
graph TB
    subgraph Standalone["独立部署模式"]
        Booter1["common-booter<br/>(单体启动器)"]
        Server1["common-server<br/>(单体服务器)"]
        AllServices["所有服务模块<br/>(查询/建模/数据加载等)"]
    end

    subgraph Distributed["分布式部署模式"]
        QueryBooter["query-booter"]
        DataLoadingBooter["data-loading-booter"]
        OpsBooter["ops-booter"]
        RecBooter["rec-booter"]

        QueryServer["query-server"]
        DataLoadingServer["data-loading-server"]
        MetadataServer["metadata-server"]
        OpsServer["ops-server"]
        RecServer["rec-server"]

        QueryService["query-service"]
        DataLoadingService["data-loading-service"]
        ModelingService["modeling-service"]
        RecService["rec-service"]
    end

    subgraph External["外部服务"]
        HDFS["HDFS<br/>存储"]
        Spark["Spark<br/>计算"]
        Kafka["Kafka<br/>流式"]
        Zookeeper["Zookeeper<br/>协调"]
        Redis["Redis<br/>缓存"]
    end

    Booter1 --> Server1
    Server1 --> AllServices
    AllServices --> External

    QueryBooter --> QueryServer
    DataLoadingBooter --> DataLoadingServer
    OpsBooter --> OpsServer
    RecBooter --> RecServer

    QueryServer --> QueryService
    DataLoadingServer --> DataLoadingService
    MetadataServer --> ModelingService
    RecServer --> RecService

    QueryService --> External
    DataLoadingService --> External
    ModelingService --> External
    RecService --> External

    style Standalone fill:#e3f2fd
    style Distributed fill:#f3e5f5
    style External fill:#fff3e0
```