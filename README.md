# rts
Real time sync data to elasticsearch

### 简介
Elasticsearch connector是一个将其他数据源(如数据库系统)实时同步数据到Elasticsearch的通用平台。通过全量同步和实时增量同步配合，实现高效的x2es的实时数据同步。
同时支持数据筛选，支持同步过程中对数据进行加工处理。

##### 支持数据源
- mongodb

### 应用场景举例
1. 数据镜像备份到Elasticsearch
2. 大数据量查询转移到Elasticsearch中进行，减轻数据库压力
3. 利用Elasticsearch进行实时全局搜索。


### 功能介绍
将数据从数据源准确、一致、高效的复制到Elasticsearch。
1. 全量同步：将全部数据从数据源完整同步到ElasticSearch。用于初始化数据，或者定期同步。如果数据量较大，需要确保两次同步之间有足够的时间间隔。
2. 实时同步：即增量同步。数据初始化之后，通过在Elasticsearch重放数据源操作日志，实现实时增量同步。理论精度可达毫秒级，实际有效精度秒级。该功能需要基于数据源的操作日志系统，如mongodb的oplog机制。

    - todo:配图
    
### 高性能机制
1. async: 基于python3 协程和asyncio，实现异步I/O并发。充分利用网络和磁盘I/O资源。
2. action block：将actions打成区块，通过Elasticsearch的bulk操作接口实现批量操作，减少网络请求资源消耗。
3. chunk: 支持通过action count和bytes两个维度chunk，保证请求效率和成功率。
4. action merge：支持action本地合并，同一个block中对同一条数据(或文档)的多次操作，可以在本地合并为一条操作，减少最终重放压力。
5. stream: 使用异步生成器和迭代器将数据读取后置，减少数据在模块间的传递记忆在内存中的驻留时间，有效控制内存占用。
6. traffic limit: 支持流量控制。

### 数据一致性保证机制
同步过程中的数据一致性主要体现在三个方面：初始化完整数据，增量数据完整性，操作日志重放顺序一致性。
1. 初始化完整数据：
    1. full index：全量同步保证完整初始化数据：大部分数据库系统的操作日志机制都只能保存最近一段时间内的日志，全量同步可以保证初始数据完整性。
    2. ts mark: 全量同步启动时记录当前timestamp，作为实时同步的起点。保证全量同步过程中的操作不丢失。（依赖操作日志幂等性）
2. 增量数据完整性：
    1. block chain：对action block引入区块链机制，每一个action block都包含福区块的id，所有区块形成链式结构，保证任何区块缺失都可侦测。
    2. action block ack：Elasticsearch数据重放成功后，将该action log加入block chain。
    3. double retry：支持应用和网络两级重试，根据需求灵活选择。（依赖操作日志幂等性）
    4. failed action log：多次重试仍然失败的操作，记录日志，供问题分析和手动修复之用。注：该平台无法处理数据类错误，如mongodb中的对字段没有强制类型要求，而Elasticsearch则需要同一个字段保持类型一致，如果同一字段先后出现不同的数据类型，则需要在同步中进行数据清洗，甚至修改数据源数据。
3. 操作日志重放顺序一致性
    
   操作日志顺序主要包括两方面(数据/文档级)：一是同一条数据insert、update、delete操作的相对顺序，二是同一条数据的多次udpate操作的相对顺序。
    1. action block：将操作日志依序批量打成区块，然后依次进行重放。(性能模式无法严格保证重放顺序)
    2. action no：根据action到达的顺序进行全局编号，保证block内部action顺序。
    3. action merge：同一block内的同一条数据的actions融合为一条action，确保局部相对顺序一致。
    4. update_by_query和delete_by_query只支持同步方式，Elasticsearch bulk不支持这两个操作，且这两个方法通常较为耗时，无法保证并发顺序。

### 高可用机制
本平台没有内置Hypervisor，可以通过成熟方案解决，如supervisor。设计时遵循的原则是尽量处理和记录异常和错误，除非该错误严重影响数据一致性或无法处理。
1. 实时同步状态存储
    1. action block ack：成功重放的block，实时写入block，持久化状态。
2. 宕机恢复
    1. ts mark: 全量同步启动时记录当前timestamp，作为实时同步的起点。保证全量同步过程中的操作不丢失。（依赖操作日志幂等性）
    2. block chain：重启时使用block chain最后一个成功区块的last aciton的timestamp作为启动时间点。（依赖操作日志幂等性）
    
    
### 基本操作命令
推荐使用虚拟环境
1. 全量同步
    1. 同步全部索引：python full_sync.py index_all
    2. 同步部分索引(tracks和users)：python full_sync.py index tracks users
2. 实时同步
    1. python rts.py
    
    
    
# 完善中...
    


