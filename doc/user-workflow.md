# Apache Kylin 用户执行流程文档

## 1. 用户查询流程

### 1.1 查询流程总览图

```mermaid
flowchart TD
    Start([客户端请求]) --> Controller[REST 控制器层<br/>NQueryController.query]
    Controller --> Service[查询服务层<br/>QueryService.query]
    Service --> Routing[查询路由引擎<br/>QueryRoutingEngine]
    Routing --> Exec[查询执行器<br/>QueryExec.execute]

    Exec --> Parser[步骤1: SQL解析<br/>Calcite Parser]
    Parser --> Validator[步骤2: 语义验证<br/>Calcite Validator]
    Validator --> Optimizer[步骤3: 查询优化<br/>QueryOptimizer]
    Optimizer --> Realization[步骤4: 实现路由<br/>Realization Router]
    Realization --> Plan[步骤5: 执行计划<br/>CalcitePlanExec/SparderPlanExec]

    Plan --> Storage[存储层<br/>Parquet/Delta Storage]
    Storage --> Result[结果处理<br/>序列化/格式转换/记录历史]
    Result --> End([返回客户端])

    style Start fill:#e1f5ff
    style End fill:#e1f5ff
    style Controller fill:#fff4e1
    style Service fill:#e8f5e9
    style Routing fill:#f3e5f5
    style Exec fill:#fce4ec
    style Parser fill:#fff3e0
    style Validator fill:#fff3e0
    style Optimizer fill:#fff3e0
    style Realization fill:#fff3e0
    style Plan fill:#fff3e0
    style Storage fill:#e0f7fa
    style Result fill:#e0f7fa
```

### 1.2 查询优化规则流程

```mermaid
flowchart LR
    SQL[原始SQL] --> Parser[1. 语法解析<br/>Parser<br/>SqlNode AST]
    Parser --> Validator[2. 语义验证<br/>Validator<br/>表/列/类型检查]
    Validator --> Logical[3. 逻辑优化<br/>Logical Optimizer]

    Logical --> |应用优化规则| Rules[优化规则:<br/>• OlapFilterJoinRule<br/>• OlapProjectJoinTransposeRule<br/>• SumConstantConvertRule<br/>• FilterAggregateTransposeRule<br/>• JoinAssociateRule<br/>• JoinCommuteRule<br/>• ProjectMergeRule<br/>• FilterMergeRule]

    Rules --> Converter[4. 实现转换<br/>RelToOlapRelConverter<br/>RelNode → OlapRelNode]
    Converter --> Matcher[5. 索引匹配<br/>IndexMatcher<br/>查找最佳索引/Layout]
    Matcher --> Plan[优化后的执行计划]

    style SQL fill:#e1f5ff
    style Plan fill:#e1f5ff
    style Parser fill:#fff4e1
    style Validator fill:#fff4e1
    style Logical fill:#e8f5e9
    style Rules fill:#e8f5e9
    style Converter fill:#f3e5f5
    style Matcher fill:#f3e5f5
```

## 2. 数据构建流程

### 2.1 数据构建流程总览图

```mermaid
flowchart TD
    Start([用户触发构建]) --> Controller[REST 控制器层<br/>JobController.build]
    Controller --> Service[作业服务层<br/>JobService.submitBuildJob]
    Service --> Manager[作业管理器<br/>ExecutableManager.addJob]
    Manager --> Engine[Spark 构建引擎<br/>NSparkCubingEngine]
    Engine --> Job[SegmentBuildJob.execute]

    Job --> Stage1[阶段1: WAIT_FOR_RESOURCE<br/>检查Spark集群资源]
    Stage1 --> Stage2[阶段2: REFRESH_SNAPSHOT<br/>SnapshotService.refreshSnapshot<br/>构建表快照]
    Stage2 --> Stage3[阶段3: BUILD_FLAT_TABLE_STATS<br/>计算统计信息]
    Stage3 --> Stage4[阶段4: BUILD_GLOBAL_DICT<br/>构建全局字典]
    Stage4 --> Stage5[阶段5: BUILD_LAYER<br/>分层构建Cube]
    Stage5 --> Stage6[阶段6: MATERIALIZE_FLAT_TABLE<br/>物化扁平表]
    Stage6 --> Stage7[阶段7: MATERIALIZE_FACT_VIEW<br/>物化事实表视图]
    Stage7 --> Stage8[阶段8: LAYOUT_BUILD<br/>生成Parquet/Delta文件<br/>写入HDFS]

    Stage8 --> Metadata[更新元数据<br/>Segment状态→READY<br/>更新IndexPlan/Dataflow]
    Metadata --> Notify([发送通知<br/>构建完成])

    style Start fill:#e1f5ff
    style Notify fill:#e1f5ff
    style Controller fill:#fff4e1
    style Service fill:#e8f5e9
    style Manager fill:#f3e5f5
    style Engine fill:#fce4ec
    style Job fill:#fce4ec
    style Stage1 fill:#fff3e0
    style Stage2 fill:#fff3e0
    style Stage3 fill:#fff3e0
    style Stage4 fill:#fff3e0
    style Stage5 fill:#fff3e0
    style Stage6 fill:#fff3e0
    style Stage7 fill:#fff3e0
    style Stage8 fill:#fff3e0
    style Metadata fill:#e0f7fa
```

### 2.2 分段合并流程

```mermaid
flowchart TD
    Start([触发合并]) --> MergeJob[SegmentMergeJob.execute]
    MergeJob --> Read[读取待合并的Segment<br/>从HDFS读取Parquet/Delta文件<br/>验证兼容性]
    Read --> Merge[合并数据<br/>合并相同Layout的数据<br/>重新计算聚合结果<br/>处理数据重叠]
    Merge --> Write[写入新Segment<br/>生成新的Parquet/Delta文件<br/>写入HDFS]
    Write --> Update[更新元数据<br/>添加新Segment到Dataflow<br/>标记旧Segment为已合并<br/>清理旧Segment数据]
    Update --> End([合并完成])

    style Start fill:#e1f5ff
    style End fill:#e1f5ff
    style MergeJob fill:#fce4ec
    style Read fill:#fff3e0
    style Merge fill:#fff3e0
    style Write fill:#fff3e0
    style Update fill:#e0f7fa
```

## 3. 模型管理流程

### 3.1 模型创建流程

```mermaid
flowchart TD
    Start([用户创建模型]) --> Controller[REST 控制器层<br/>NModelController.createModel]
    Controller --> Service[建模服务层<br/>ModelService.createModel<br/>业务逻辑处理/验证]
    Service --> ModelMgr[元数据管理器<br/>NDataModelManager.createModel<br/>持久化到ResourceStore]
    ModelMgr --> IndexMgr[创建索引计划<br/>NIndexPlanManager.createIndexPlan<br/>定义索引布局/参数]
    IndexMgr --> DataflowMgr[创建数据流<br/>NDataflowManager.createDataflow<br/>关联模型和索引计划]
    DataflowMgr --> End([模型创建完成])

    style Start fill:#e1f5ff
    style End fill:#e1f5ff
    style Controller fill:#fff4e1
    style Service fill:#e8f5e9
    style ModelMgr fill:#f3e5f5
    style IndexMgr fill:#f3e5f5
    style DataflowMgr fill:#f3e5f5
```

### 3.2 推荐引擎流程

```mermaid
flowchart TD
    Start([导入SQL/查询历史]) --> Analyze["推荐服务层<br/>RecService.analyzeQueries<br/>分析查询历史/识别模式"]
    Analyze --> Extract["提取维度和度量<br/>• 识别GROUP BY列(维度)<br/>• 识别聚合函数(度量)<br/>• 识别过滤条件<br/>• 识别连接关系"]
    Extract --> RecommendModel["推荐模型结构<br/>• 推荐事实表<br/>• 推荐维度表<br/>• 推荐连接关系<br/>• 推荐计算列"]
    RecommendModel --> RecommendIndex["推荐索引<br/>IndexPlanService.recommendIndex<br/>• 生成聚合索引布局<br/>• 生成表索引布局<br/>• 估算存储成本<br/>• 排序推荐结果"]
    RecommendIndex --> End([展示推荐结果])

    style Start fill:#e1f5ff
    style End fill:#e1f5ff
    style Analyze fill:#fce4ec
    style Extract fill:#fff3e0
    style RecommendModel fill:#fff3e0
    style RecommendIndex fill:#fff3e0
```

## 4. 流式数据处理流程

### 4.1 流式数据加载流程

```mermaid
flowchart LR
    Data[Kafka流式数据] --> Write[1. 数据写入Kafka<br/>按Topic分区]
    Write --> Consume[2. Spark Streaming消费<br/>StreamingJob持续消费]
    Consume --> Process[3. 数据处理和写入<br/>• 解析JSON数据<br/>• 数据清洗和转换<br/>• 写入Delta Lake<br/>• 支持ACID事务]
    Process --> Refresh[4. 刷新模型<br/>• 更新内部表元数据<br/>• 刷新缓存<br/>• 新数据立即可查询]
    Refresh --> Query([数据可查询])

    style Data fill:#e1f5ff
    style Query fill:#e1f5ff
    style Write fill:#fff4e1
    style Consume fill:#e8f5e9
    style Process fill:#f3e5f5
    style Refresh fill:#fce4ec
```

## 5. 典型用户场景

### 5.1 场景1：创建模型并构建索引

```mermaid
sequenceDiagram
    participant User as 用户
    participant UI as Web UI
    participant Ctrl as NModelController
    participant ModelSvc as ModelService
    participant ModelMgr as NDataModelManager
    participant JobCtrl as JobController
    participant JobSvc as JobService
    participant Engine as NSparkCubingEngine

    User->>UI: 1. 登录并创建模型
    UI->>Ctrl: 2. POST /api/model
    Ctrl->>ModelSvc: 3. createModel()
    ModelSvc->>ModelMgr: 4. 持久化模型
    ModelMgr-->>ModelSvc: 5. 模型已保存
    ModelSvc-->>Ctrl: 6. 返回模型ID
    Ctrl-->>UI: 7. 模型创建成功

    User->>UI: 8. 创建索引
    UI->>JobCtrl: 9. POST /api/jobs/build
    JobCtrl->>JobSvc: 10. submitBuildJob()
    JobSvc->>Engine: 11. 提交Spark作业
    Engine-->>JobSvc: 12. 作业已提交
    JobSvc-->>JobCtrl: 13. 作业ID
    JobCtrl-->>UI: 14. 作业已启动

    Engine->>Engine: 15. 执行构建阶段<br/>[参见数据构建流程]
    Engine-->>JobSvc: 16. 构建完成
    JobSvc-->>UI: 17. 通知用户

    User->>UI: 18. 验证索引状态
    UI-->>User: 19. 显示READY状态
    User->>UI: 20. 执行测试查询
```

### 5.2 场景2：执行SQL查询

```mermaid
sequenceDiagram
    participant User as 用户
    participant UI as Web UI
    participant QueryCtrl as NQueryController
    participant QuerySvc as QueryService
    participant Routing as QueryRoutingEngine
    participant Exec as QueryExec
    participant Storage as Storage Layer

    User->>UI: 1. 输入SQL查询
    UI->>QueryCtrl: 2. POST /api/query
    QueryCtrl->>QuerySvc: 3. query(sql)
    QuerySvc->>QuerySvc: 4. 权限检查
    QuerySvc->>Routing: 5. queryWithSqlMassage()
    Routing->>Routing: 6. SQL预处理
    Routing->>Exec: 7. execute()

    Exec->>Exec: 8. SQL解析
    Exec->>Exec: 9. 语义验证
    Exec->>Exec: 10. 查询优化
    Exec->>Exec: 11. 实现路由
    Exec->>Exec: 12. 生成执行计划
    Exec->>Storage: 13. 读取索引数据
    Storage-->>Exec: 14. 返回数据
    Exec-->>Routing: 15. QueryResult
    Routing-->>QuerySvc: 16. 查询结果
    QuerySvc->>QuerySvc: 17. 记录查询历史
    QuerySvc-->>QueryCtrl: 18. 返回结果
    QueryCtrl-->>UI: 19. JSON结果
    UI-->>User: 20. 显示查询结果
```

### 5.3 场景3：增量数据加载

```mermaid
sequenceDiagram
    participant User as 用户
    participant UI as Web UI
    participant JobCtrl as JobController
    participant JobSvc as JobService
    participant Engine as NSparkCubingEngine
    participant DataflowMgr as NDataflowManager

    User->>UI: 1. 确定增量数据范围
    User->>UI: 2. 选择增量构建
    UI->>JobCtrl: 3. POST /api/jobs/incremental
    JobCtrl->>JobSvc: 4. submitBuildJob(增量参数)
    JobSvc->>DataflowMgr: 5. 获取现有Segments
    DataflowMgr-->>JobSvc: 6. 返回Segment列表
    JobSvc->>Engine: 7. 提交增量构建
    Engine->>Engine: 8. 执行增量构建<br/>[仅构建增量数据]
    Engine-->>JobSvc: 9. 增量Segment完成
    JobSvc->>DataflowMgr: 10. 添加新Segment
    DataflowMgr-->>JobSvc: 11. 元数据已更新
    JobSvc-->>UI: 12. 增量构建完成

    User->>UI: 13. 验证增量数据
    UI->>UI: 14. 执行查询
    UI-->>User: 15. 显示包含新数据的结果
```

### 5.4 场景4：使用推荐引擎

```mermaid
sequenceDiagram
    participant User as 用户
    participant UI as Web UI
    participant RecCtrl as RecController
    participant RecSvc as RecService
    participant ModelSvc as ModelService
    participant JobSvc as JobService

    User->>UI: 1. 进入推荐页面
    User->>UI: 2. 上传SQL或使用历史
    UI->>RecCtrl: 3. POST /api/rec/analyze
    RecCtrl->>RecSvc: 4. analyzeQueries()
    RecSvc->>RecSvc: 5. 分析查询模式
    RecSvc->>RecSvc: 6. 提取维度/度量
    RecSvc->>RecSvc: 7. 推荐模型结构
    RecSvc->>RecSvc: 8. 推荐索引
    RecSvc-->>RecCtrl: 9. 推荐结果
    RecCtrl-->>UI: 10. 显示推荐
    UI-->>User: 11. 查看推荐

    User->>UI: 12. 接受推荐并创建模型
    UI->>ModelSvc: 13. createModel(推荐配置)
    ModelSvc-->>UI: 14. 模型已创建

    User->>UI: 15. 构建推荐索引
    UI->>JobSvc: 16. submitBuildJob()
    JobSvc-->>UI: 17. 作业已提交

    JobSvc->>JobSvc: 18. 等待构建完成
    JobSvc-->>UI: 19. 构建完成

    User->>UI: 20. 测试查询性能
```

### 5.5 场景5：融合模型（批流一体）

```mermaid
flowchart LR
    subgraph Batch["批数据流程"]
        BatchSource[批数据源<br/>Hive/HDFS] --> BatchJob[Spark批构建<br/>NSparkCubingEngine]
        BatchJob --> BatchStorage[Parquet/Delta<br/>预计算索引]
    end

    subgraph Streaming["流数据流程"]
        StreamSource[Kafka流数据] --> StreamJob[Spark Streaming<br/>StreamingJob]
        StreamJob --> StreamStorage[Delta Lake<br/>实时数据]
    end

    subgraph Query["融合查询"]
        QueryEngine[查询引擎<br/>QueryRoutingEngine] --> Routing{查询类型}
        Routing --> |历史数据| BatchIndex[批索引<br/>Parquet/Delta]
        Routing --> |实时数据| StreamTable[流表<br/>Delta Lake]
        Routing --> |混合查询| Hybrid[批流融合查询<br/>合并结果]
    end

    BatchStorage --> QueryEngine
    StreamStorage --> QueryEngine

    BatchIndex --> Result[统一查询结果]
    StreamTable --> Result
    Hybrid --> Result

    style Batch fill:#e1f5ff
    style Streaming fill:#e8f5e9
    style Query fill:#fff4e1
    style Result fill:#fce4ec
```

## 6. 完整的用户操作生命周期

```mermaid
stateDiagram-v2
    [*] --> 模型设计: 用户创建模型
    模型设计 --> 索引设计: 定义维度/度量
    索引设计 --> 首次构建: 创建索引
    首次构建 --> 查询验证: 构建完成
    查询验证 --> 增量加载: 定期数据更新
    查询验证 --> 查询执行: 日常使用

    增量加载 --> 查询执行: 增量完成
    增量加载 --> Segment合并: 需要合并
    Segment合并 --> 查询执行: 合并完成

    查询执行 --> 推荐优化: 性能分析
    推荐优化 --> 索引设计: 优化索引
    推荐优化 --> 模型设计: 优化模型

    查询执行 --> [*]: 结束
```

## 7. 关键文件路径索引

### 7.1 入口点
- 主启动器: `src/server/src/main/java/org/apache/kylin/rest/BootstrapServer.java`
- 查询启动器: `src/query-booter/src/main/java/org/apache/kylin/rest/QueryBootstrapServer.java`
- 数据加载启动器: `src/data-loading-booter/src/main/java/org/apache/kylin/rest/DataLoadingBootstrapServer.java`

### 7.2 核心服务
- 查询服务: `src/query-service/src/main/java/org/apache/kylin/rest/service/QueryService.java`
- 作业服务: `src/data-loading-service/src/main/java/org/apache/kylin/rest/service/JobService.java`
- 模型服务: `src/modeling-service/src/main/java/org/apache/kylin/rest/service/ModelService.java`

### 7.3 查询引擎
- 查询执行器: `src/query/src/main/java/org/apache/kylin/query/engine/QueryExec.java`
- 查询路由: `src/query/src/main/java/org/apache/kylin/query/engine/QueryRoutingEngine.java`

### 7.4 构建引擎
- Spark构建引擎: `src/spark-project/engine-spark/src/main/java/org/apache/kylin/engine/spark/NSparkCubingEngine.java`
- Segment构建作业: `src/spark-project/engine-spark/src/main/java/org/apache/kylin/engine/spark/job/SegmentBuildJob.java`

### 7.5 元数据管理
- 项目管理器: `src/core-metadata/src/main/java/org/apache/kylin/metadata/project/NProjectManager.java`
- 模型管理器: `src/core-metadata/src/main/java/org/apache/kylin/metadata/model/NDataModelManager.java`
- 索引计划管理器: `src/core-metadata/src/main/java/org/apache/kylin/metadata/cube/model/NIndexPlanManager.java`
- 数据流管理器: `src/core-metadata/src/main/java/org/apache/kylin/metadata/datflow/NDataflowManager.java`