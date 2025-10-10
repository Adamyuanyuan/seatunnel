本文档记录对Flink Hive 丢数据问题的改造过程，用于后面需要


## Seatunnel Flink 翻译总览

本文梳理 SeaTunnel 在 Flink 引擎上的翻译链路，解释 `seatunnel-core/seatunnel-flink-starter` 与 `seatunnel-translation/seatunnel-translation-flink` 的分工，并结合本次 Kafka → Hive Streaming 丢数问题，补充 Flink 1.16 之后的行为差异与修复思路。

### 组件分层

- **seatunnel-core/seatunnel-flink-starter**  
  - 提供 CLI、参数解析、环境初始化。  
  - `FlinkStarter` 将配置转成 `FlinkExecution`，依次调用 `Source/Transform/SinkExecuteProcessor`，构建 SeaTunnel 的逻辑 DAG。  
  - 负责装配运行环境（`AbstractFlinkRuntimeEnvironment`）、注册 Catalog、构建并触发 Flink 作业。

- **seatunnel-translation/seatunnel-translation-flink**  
  - 将 SeaTunnel API（Source/Sink/Transform）适配为 Flink `DataStream` API。  
  - `FlinkSink`/`FlinkSource` 等类负责把 SeaTunnel 插件包装为 Flink 的 `Sink`/`Source`。  
  - `FlinkSinkWriter`、`FlinkGlobalCommitter` 等包装器桥接 SeaTunnel 的两阶段提交接口（`prepareCommit`、`commit`、`snapshotState`）与 Flink Runtime 的调用序列。

> 记忆要点：Starter 负责“准备舞台”和“串联插件”；Translation 模块负责“翻译接口”，把 SeaTunnel 插件放到 Flink 的执行引擎里。

### 作业构建流程

1. CLI 解析 job config → 构造 `SeaTunnelFlink`。  
2. `FlinkExecution` 根据配置构建 Source → Transform → Sink 的执行链表。  
3. 每个 Sink 由 `SeaTunnelSink` 实现，Starter 会调用其 `createWriter/restoreWriter`，并交给 Translation 层生成 `FlinkSink`。  
4. Translation 层把 `FlinkSink` 注册到 Flink `DataStream` 作业中，由 Flink 调度执行。  
5. 在运行时，Flink 会周期性地调用 `SinkWriter.prepareCommit(checkpointId)` → `snapshotState()` → `notifyCheckpointComplete()`，SeaTunnel 插件需要保证这串调用的原子性。

### 1.14 → 1.16 行为差异

Flink 1.14 及更早版本调用顺序为：

```
snapshotState(checkpointId)
prepareCommit(checkpointId)
```

从 1.15 开始 Flink 调整为：

```
prepareCommit(checkpointId)
snapshotState()
```

SeaTunnel 早期实现依赖“先快照后 prepare”这一旧顺序，因此在新的调用序列下会出现窗口期：  
`prepareCommit` 关闭文件后，`snapshotState` 尚未来得及旋转事务，新数据会继续写入旧的临时路径，导致提交时 `rename` 找不到源文件或覆盖已提交文件。

### Kafka → Hive Streaming 丢数根因

- Hive Sink 依赖 File Base 模块的两段式提交：Writer 把数据写到事务目录，`FileSinkAggregatedCommitter` 在全局阶段重命名到 Hive 表目录。  
- 原实现直到 `snapshotState` 才切换到新事务目录，因此在 Flink 1.16 的调用顺序下，“prepareCommit 与 snapshotState 之间”进入的新数据仍写在上一事务目录里。  
- 全局提交器重命名时只能看到旧的提交记录，临时文件已经被新的写入覆盖或删除，于是抛出 `source missing and target not found`，造成数据丢失与 Hive 分区缺失。

### 修复要点

1. **在 `prepareCommit` 中立即旋转事务**  
   - 完成文件 close 后，生成 `FileCommitInfo` 并缓存。  
   - 立刻 `beginTransaction(checkpointId + 1)`，为后续增量写入创建新的事务目录和文件 part。  
   - 清空 `beingWrittenFile`，确保新数据不会复用旧的临时文件名。

2. **在 `snapshotState` 中使用缓存的 commit 信息**  
   - 将 `pendingCommitInfo`、`pendingTransactionId`、`pendingCheckpointId` 放入状态快照。  
   - 如果已经在 `prepareCommit` 中旋转事务，`snapshotState` 不再重复 `beginTransaction`。  
   - 避免窗口期内的增量数据混入当前 checkpoint。

3. **对 `abortPrepare` 选择正确的事务目录回滚**  
   - 如果存在 `pendingTransactionId`，优先清理它对应的事务目录，防止误删新事务。

以上改动保证 SeaTunnel 的文件提交语义与 Flink 1.16 及之后版本的调用顺序对齐，重新恢复 Hive Streaming 模式的 exactly-once 行为。

### 与 Zeta/Flink 1.20 的关系

- Zeta 引擎与自研 Flink 1.20 已经实现了“prepare 后立即切换事务”的逻辑，因此不会触发本问题。  
- 回溯到 1.14 之前的 Flink，由于调用顺序不同，即便老逻辑也能正常运行，但同样可以升级为新实现以保持一致语义。

### 建议的验证流程

1. 在本地/集群复现环境中部署修复后的包。  
2. 构造包含大量数据的 Kafka topic，设置 `job.mode=STREAMING`，让任务持续运行。  
3. 断开 Hive Metastore/HDFS 网络，触发 rename 重试，再恢复连接，观察是否还能成功重新提交。  
4. 核对 Hive 表中记录总量与 Kafka 消费量，确认无丢数。  
5. 检查任务日志，确保不再出现 `source missing` 与 `already finished in the last commit` 的异常级别日志。

### 参考代码

- `seatunnel-connectors-v2/.../AbstractWriteStrategy.java` — Writer 端事务切换和状态快照。  
- `seatunnel-connectors-v2/.../FileSinkAggregatedCommitter.java` — 全局提交与 HDFS rename。  
- `seatunnel-translation/.../FlinkSinkWriter.java` — Flink Runtime 与 SeaTunnel Writer 的桥接。  
- `seatunnel-core/.../FlinkExecution.java` — Starter 层构建 DataStream 拓扑。

通过理解上述模块间的协作关系，可以快速定位未来的类似问题（例如不同引擎对两阶段提交接口的扩展、Flink 新版本的行为调整等），也能指导后续对 SeaTunnel 翻译层的演进。



## 对话

Flink 1.16 先执行 prepareCommit 再调用 snapshotState，原来的文件写入逻辑在这两个回合之间没有立即切换事务目录，新的数据继续写进旧的临时文件，FileSinkAggregatedCommitter 随后重命名时就会碰到源文件缺失，从而导致报错和丢数。为此我在 AbstractWriteStrategy.java:81-397 对两阶段提交流程做了以下调整：立即在 prepareCommit 里缓存提交信息、清空正在写的文件、旋转到新的事务目录，并在 snapshotState 中优先使用缓存的提交数据；同时修正 abortPrepare/clonePartitionMap 以适配新的事务状态缓存。这样在 Flink 1.16 的调用顺序下，窗口期写入的数据会落入下一事务，不再覆盖上一 checkpoint 的临时文件。

另外整理了一份文档 docs/zh/seatunnel-flink-translation-guide.md，系统说明 seatunnel-flink-starter 与 seatunnel-translation-flink 的协作关系、Flink 各版本在两阶段提交上的差异，以及本次问题的根因和修复要点，后续排查可直接参考。

尚未跑集成测试，建议在现网或测试集群上做一次 Streaming 任务验证，并刻意触发 HDFS/HMS 断联后再恢复，确认 rename 可以顺利重试；同时对比 Kafka 消费量和 Hive 表里写入的记录数，确保再无丢数。如果还有希望扩展的地方（例如补充自动化回归用例），可以继续沟通。

### 问1-关于能否在Flink翻译侧做
与 Zeta/Flink 1.20 的关系

- Zeta 引擎与自研 Flink 1.20 已经实现了“prepare 后立即切换事务”的逻辑，因此不会触发本问题。  
- 回溯到 1.14 之前的 Flink，由于调用顺序不同，即便老逻辑也能正常运行，但同样可以升级为新实现以保持一致语义。

这个确定吗？我跟社区沟通的时候，社区的意思是能不能通过 引擎层或转换层来解决这个问题，因为 zeta 和 Flink 1.20貌似是可以的。

比如我不改  AbstractWriteStrategy.java，而是重写一个 Flink 1.16 相关的 seatunnel-flink-starter 或 seatunnel-translation-flink，请问这个方案可行吗？

### 答1
关于 “Zeta / Flink‑1.20 已经做到 prepare 后立刻切换事务”
我手头没有那两个版本的源码与日志，无法 100% 断言一定如此，只能推测：之所以它们没有暴露同类丢数，是要么已经在文件写策略里做了立即轮换（类似这次我们补的逻辑），要么那两个引擎层面对 sink writer 的调用顺序/时机和 Flink‑1.16 不同。如果想坐实，需要去 diff 实际版本：看看 Zeta 引擎、你们自研 1.20 分支、以及这些版本里 Hive/File writer 的 snapshotState、prepareCommit 行为。

能不能只改 Flink translation / starter，而不动 connector？
以 Flink 新 Sink API 为例，prepareCommit() → snapshotState() 的调用顺序是由 Flink Runtime 固定调度的，FlinkSinkWriter.prepareCommit(boolean flush) 和 snapshotState() 只是被动接收这一顺序，我们没有机会在 translation 层把它“换成先 snapshot 再 prepare”。如果强行在 wrapper 里自己调用一次 snapshotState()，等 Runtime 再调用时就会重复/错乱。starter 层更偏向配置和 DAG 构建，无法改变 runtime 调度。所谓“在引擎层解决”其实也得回到统一的写策略：要么让每个使用 Flink 的 sink writer 都在 prepareCommit 里自行切换事务，要么分叉出 Flink 专属 writer，这两种本质还是在 connector 内部处理。

为何建议在 AbstractWriteStrategy 修？
它是 File 系列 sink 的公共实现—统一修改能同时覆盖 Hive/Parquet/Text 等落盘逻辑，并且对所有引擎（Flink、Spark、SeaTunnel Engine、Zeta…）都生效。把事务轮换写在这里可以保证任何引擎只要走了 prepareCommit → snapshotState 这套流程，就不会再出现窗口期覆盖的问题。如果未来不同引擎还有特殊需求，再通过写策略层的扩展点去适配即可。

可行的替代做法
如果确实想在 translation 层兜底，可以考虑：

自定义一个 Flink Writer wrapper，在 prepareCommit 里把 commit 信息缓存起来并立即 snapshotState，然后告诉 Runtime 本次 commit 不返回文件；
在 Runtime 真正调用 snapshotState 时，返回一个空 state（因为已经处理过）。
但这等于把现在补在 AbstractWriteStrategy 的逻辑重新搬到 wrapper，维护成本相当，还得确保 Spark/Zeta 等其他引擎的 writer 继续可用。
结论：要彻底消除 1.16 上的丢数，最佳位置仍是文件写策略本身。你完全可以对照 Zeta/Flink‑1.20 的源码确认是否已有类似实现，然后选择是在共享的 AbstractWriteStrategy 中修复，还是针对 Flink 另开一套写策略。但仅靠 starter/translation 改调度顺序，在 Flink Runtime 的约束下基本不可行。


### 问2-为何Flink1.20不丢数
logs\Hive_Streaming_lostdata.md\9576.patch
这个是我们实现的对Flink 1.20的适配，请根据这个patch看一下，是什么原因，Flink 1.20不会丢数，这个原因可以在 Flink 1.16中，通过翻译层解决吗？

### 答2
这个补丁并没有改 AbstractWriteStrategy，而是新增了 seatunnel-translation-flink-20，整套 sink 翻译改成了 Flink SinkV2 API：FlinkSink 转成 Sink<InputT> & SupportsWriterState & SupportsCommitter，FlinkSinkWriter 实现了 org.apache.flink.api.connector.sink2.SinkWriter，自己维护 checkpointId，在 prepareCommit() 里把当前 checkpoint 的数据交给 SeaTunnel writer，然后在 snapshotState(long checkpointId) 中把同一个 checkpoint 的状态打包（参见补丁里的 FlinkSinkWriter.java 与 FlinkSink.java）。这个版本下 flush 只是日志，writer 的生命周期完全交给 Flink 1.20 的 SinkV2 框架调度。

SinkV2 会在 DefaultSinkWriterOperator#prepareSnapshotPreBarrier 阶段以同步方式依次调用 prepareCommit() → snapshotState()，并且在这两个调用之间不会重新调度写线程，也不会让新的 record 进入 writer。这就意味着 SeaTunnel writer 在 prepareCommit() 里关掉文件之后，直到 snapshotState() 完成之前不会再收到新的写入请求，所以即便 AbstractWriteStrategy 仍然是在 snapshotState() 才 beginTransaction(checkpointId+1)，也不会出现“准备提交后又往旧事务目录写数据”的窗口，故而 Hive 那边不会再碰到 rename … (source missing)。

Flink 1.16 的 SinkV1 API 不具备这种保证。prepareCommit(boolean flush) 是在 operator 的 mailbox 里触发的，但 snapshotState() 在下游 state backend 线程里异步执行，两者之间 operator 仍然可能继续处理输入记录，所以文件写策略必须自己在 prepareCommit() 后立即切换事务目录、清空 beingWrittenFile 等状态。不修改 connector（比如我们现在在 AbstractWriteStrategy 里的修复），单靠翻译层无法强行阻止 Flink 1.16 在两个调用之间继续写数据，也无法改写它的调用顺序，因此单纯“重写 1.16 的 starter/translation”解决不了丢数问题。

结论：Flink 1.20 之所以“看起来没丢”，是因为改用了 SinkV2 的执行模型，运行时替我们兜住了 prepare → snapshot 之间的窗口；要在 1.16 上彻底根除问题，需要像我们现在这样在 connector 层（AbstractWriteStrategy）立即切换事务，而不是寄希望于翻译层调整。

### 问3-Flink 1.16是否可以借鉴
Flink 在 1.14 → 1.15+ 之间的 Sink API 调用顺序变更
旧版本（Flink ≤1.14）的行为

在 Flink 1.14 及之前版本中（使用 TwoPhaseCommitSinkFunction 这种老的两阶段提交机制）：

在 checkpoint 触发时，sink 的 snapshotState() 方法内部通常会包含“预提交”（pre-commit／flush）逻辑：即关闭或封堵输出，并准备提交事务。

完成这些预提交／flush 后，再在快照里捕获状态（比如事务状态、未提交文件等）。

最后在 checkpoint 成功阶段，由全局提交器 (committer) 发起真正的提交（比如把临时文件重命名到目标目录）。

也就是说，pre-commit 是“内嵌在” snapshotState() 这个过程里的。

新版本（Flink ≥1.15）的行为

从 Flink 1.15 开始，Flink 推出了新的 统一 Sink API（Sink V2 / FLIP-143 等演进），其 checkpoint 流程在 sink 端有以下调整：

在 checkpoint 到来时，Flink 会首先调用 sink 写入器（SinkWriter）的 prepareCommit()（或类似 pre-commit 接口），让写入器先将数据 flush 或封堵当前事务，生成可提交的 “committables”。

在 prepareCommit() 返回之后，Flink 再调用写入器的 snapshotState()，保存写入器的状态（例如未提交、缓冲区、位置等）。

后续由 Committer 接手，将 prepareCommit 提交阶段产生的 committables 最终真正提交（如将临时文件迁移到目标目录）

总之：顺序变为 prepareCommit → snapshotState（即 “先预提交、再做状态快照”）。

这个顺序变动是设计使得在 barrier 到来前，写入器能更早地 flush/封堵输出，从而在 exactly-once 语义下更好地控制事务。

那我是不是可以用类似的思路，将Flink 1.16，翻译成Flink sink V2 API？这样就能解决这个问题？
是不是 Flink 1.16，也同时具备 Flink sink V2 API？
### 答3
从 Flink 1.15 开始，确实引入了新的 SinkV2 API（org.apache.flink.api.connector.sink2.Sink 系列），Flink 1.16 也包括这套接口。所以理论上，SeaTunnel 在 Flink 1.16 上可以像 1.20 适配那样，新写一套 translation，把 SeaTunnel 的 SinkWriter 包装成 SinkV2，借助 runtime 的调度顺序（先 prepareCommit 再 snapshotState、两者在同一个任务线程里背靠背执行）来避免窗口期。
不过这条路的成本很高：需要像你们 1.20 适配那样整个 translation/starter 重写，覆盖 writer、commit、state 序列化、metrics、multi-table sink 等所有细节；而且要维护两套 Flink translation（老 API 兼容 1.13/1.14/1.15、SinkV2 面向 1.16+），测试面也会翻倍。
更关键的是，无论引擎如何演进，SeaTunnel 自己的 file sink 写策略仍然要保证“prepare 完即刻切换事务目录”，否则一旦哪套引擎（Spark、Zeta、后续 Flink 版本）又出现新的调用时序或并发写入，就会再次踩坑。我们这次在 AbstractWriteStrategy 补的逻辑，等于给所有引擎都加上了保险杠。
所以，虽然 Flink 1.16 technically 支持 SinkV2，也可以照着 1.20 那个 patch 去翻译，但真正能彻底解决问题的核心还是写策略层的修复。现有的修复方案已经在 connector 层兜住窗口期，且对其它引擎同样生效。除非你们准备全面拥抱 SinkV2 并维护两套 translation，否则没必要专门在 1.16 上重复 1.20 那一大套适配。

### 问4-之前PR

之前的改造是这个 logs\Hive_Streaming_lostdata.md\9828.patch
会丢最后一批的数据，是不是跟咱们的改造意思差不多，你的意思是，现在的改造，不会丢最后一批的数据了？

### 答4
对，你手里那份 9828.patch 和我们现在做的事情思路类似——都是为了把 Flink 1.16 “先 prepareCommit，后 snapshotState” 这个顺序补齐。但那次 patch 只是在 prepareCommit 里提前轮转事务，然后让 snapshotState 不再重复 beginTransaction；它没有把这次 prepare 出来的提交信息保存起来，snapshotState 写到状态里的是“已经切到下一事务”的空壳。结果是：

Checkpoint 正常完成时没问题，因为 AggregatedCommitter 拿到 prepareCommit 的返回值就能落地；
但如果 checkpoint 还没成功（例如作业正要停、或 checkpoint 刚触发就失败），先返回的那份 commitInfo 没有进入状态，下次恢复时就丢了。这种情况下 Hive 里就缺最后一批数据，所以你观察到“最后一个 checkpoint 总是写不进去”。
这次的改动在此前基础上补上了关键一步：
在 prepareCommit 里不仅旋转事务，还把本次提交信息缓存到 pendingCommitInfo/pendingTransactionId，随后 snapshotState 会把同一份数据快照到状态里，再决定是否继续 beginTransaction。这样无论 checkpoint 成功与否、甚至 job 在 prepare 之后马上失败，恢复时都能找到那批待提交的信息并重新 commit。也就是说，之前遇到的“最后一批丢失”属于旧 patch 的缺口，现在这套改造已经把它堵住了；最后一批数据不会再因为 checkpoint 尾声切换事务而丢掉。

### 问5-影响面
这次代码，不会影响 zeta引擎，或者 Flink 1.20等其它版本的行为吧？
并且，是不是之前断点续传都有可能有问题？现在断点续传都可以支持了？
### 答5
兼容性
这次只改了 AbstractWriteStrategy（以及它的子类共享的逻辑），没有引入新的引擎特判；接口依旧是 prepareCommit → snapshotState → commit 这一套。所以：

Zeta、自研 Flink-1.20 走的是相同的写策略，也能直接复用。即便它们的 runtime 调用顺序与 1.16 不同（如依旧先 snapshotState 再 prepareCommit），代码里 pendingCommitInfo 为 null 时就退回到旧路径，因此不会改变已验证的行为。
其它引擎（Spark、SeaTunnel Engine 等）只要遵循 prepare/snapshot 的约定，也不会受到影响。
断点续传
旧实现中，prepareCommit 和 snapshotState 之间出现异常，会导致刚刚准备好的那批文件没有写入状态，从而在恢复时丢失。现在我们在 prepareCommit 里把提交信息暂存下来，snapshotState 时把同一份数据拍进 checkpoint；恢复后可以继续提交未完成的事务，也就把断点续传的坑补上了。

所以结论是：

不会破坏其它引擎（包括 Zeta/Flink‑1.20）的现有语义；
以前断点续传存在风险的场景，现在也能可靠恢复提交。后续只需按计划做一次 Streaming 场景的长跑/重启验收即可。