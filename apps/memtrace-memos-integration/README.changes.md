# SmartComment 集成优化说明

基线：`feat/smartcomment-integration` 分支的 `e80b7ced`。本文仅记录在该版本之上的修改，不重复介绍基线已有的 Hook 和插件功能。

## 1. 修改范围

- 在现有架构内精简插件实现，合入 Hook 测试优化，修正文档与实现不一致的描述。
- 保持公开接口、配置及默认值、Hook 位置与顺序、异常处理边界不变。
- 保留图的业务身份、来源关系、顺序、分类和输出说明，以及脱敏、快照隔离、锁、弱引用、队列、缓存、原子落盘和旧图恢复机制。
- 不修改 MemTrace，不修改依赖约束或锁文件。MemOS 核心仅纠正一行 Reader 分组注释，可执行逻辑未改。

## 2. 插件代码修改

以下路径相对本目录的 `memos_smartcomment/`。可执行修改集中在 4 个文件、9 个私有函数。

| 文件 / 函数 | 修改内容 | 等价依据 |
| --- | --- | --- |
| `handlers/add_handler.py` · `_fine_transfer_values` | 将按索引切片取输入改为 `zip(..., strict=True)` 直接配对。 | 仅在 `per_input` 且数量相等时执行；顺序和空组保留。写入结果仍按唯一 ID 匹配。 |
| `handlers/add_handler.py` · `_on_scheduler_memory_operation` | 直接区分归档和删除；在添加状态节点前计算持久化来源状态，删除事后两次筛选。 | 分类和说明字符串不变；完整、部分、未知来源及空结果的判断相同。 |
| `handlers/base.py` · `_hook_subject` | 删除 `trace_id` 的重复提取。 | 保留显式 `None` 的语义；内部字典键顺序变化，但现有调用方仅按字段名读取，不影响业务输出。 |
| `handlers/base.py` · `_unique_values` | 用字典推导替代逐项赋值循环。 | 同身份保留最后一份值，位置仍按首次出现顺序排列。 |
| `handlers/search_handler.py` · `_search_memory_value` | 直接读取已规范化字段，删除重复默认值和字符串转换。 | 唯一构造入口 `_search_result_items` 已填齐字段并完成类型转换；外部输入检查保留。 |
| `handlers/search_handler.py` · `_search_memory_match_key` | 直接读取规范化字段。 | ID / 原始正文的选择、cube 范围、JSON 编码和 SHA-256 算法均未变。 |
| `handlers/search_handler.py` · `_match_search_nodes` | 接收已构造节点，复用 `match_key`；删除重复计算和缺失键兜底，调整私有参数及类型标注。 | 候选缓存仅写入已计算匹配键的节点；重复候选 FIFO 配对及过滤规则不变。 |
| `handlers/search_handler.py` · `_on_search_results` | 删除本地列表的多余 tuple 转换；传递当前节点；直接拼接输出 tuple。 | 内容和顺序不变；过滤节点不进入候选缓存，业务值的快照复制仍保留。 |
| `recorder.py` · `_normalize_memory_units` | 将嵌套字典推导改为直接更新边的来源和目标 ID。 | 使用相同别名映射，保留其他边属性、边顺序和快照恢复规则。 |

其余插件 Python 文件仅整理注释、文档字符串或导入分组标记，没有修改执行逻辑。`adapter.py`、`handlers/base.py`、`plugin.py` 中的三处 `isort: split` 用于兼容根目录与插件的 Ruff 配置，原有绝对导入及执行顺序不变。

## 3. Hook 测试修改

以下路径相对仓库根目录。以 `93e40823` 核对上游测试归属，仅优化本次集成新增的测试。

| 文件 | 修改内容 |
| --- | --- |
| `tests/conftest.py` | 新增显式启用的 `clean_hooks` fixture，替代重复清理代码；不使用全局 autouse。 |
| `tests/plugins/test_hook_context.py` | 注册表列齐 14 个 Hook；合并上下文字段及复制检查；保留新旧签名兼容、单次调用、同步和异步上下文复用；删除 UUID 格式、类型提示等低价值检查。 |
| `tests/api/test_search_pipeline_hooks.py` | 撤出追加的 Hook 测试，保留上游六个测试及辅助代码；相对上游仅保留 Hook 接口所需的两处 `hook_context` 参数断言调整。 |
| `tests/api/test_search_hook_context.py` | 独立验证七个成功 Hook 的顺序与共享上下文、请求/响应替换、五个结果管道对最终响应的影响及失败传播。 |
| `tests/mem_reader/test_hook_pipeline.py` | 用一个成功用例检查参数、上下文和结果替换；用一个失败用例检查原异常及后续成功 Hook 不执行。 |
| `tests/multi_mem_cube/test_single_cube_hooks.py` | 使用固定有效 ID；验证写入参数、返回 ID 替换及后续调度消费；精简失败路径重复断言。 |
| `tests/mem_scheduler/test_operation_observer.py` | 保留 Observer 成功管道与失败传播；执行 MemRead 处理逻辑，覆盖 fine-transfer、增强写入、归档、delete、soft-delete，并验证替换结果被后续步骤使用。 |
| `tests/mem_scheduler/test_mem_read_grouping.py` | 删除“显式声明优先”的过期预期，按实际实现验证 `per_input`、`batch`、`unknown`；保留输入顺序、失败空组及异常传播检查。 |

其余上游原有测试未修改、跳过或放宽。七个共享 Hook 测试文件已同步到独立 Hook 工作树，Hook PR 不依赖插件即可验证。

## 4. 文档纠偏

更新插件 `README.md` 和 `memos_smartcomment/CONVENTIONS.md`：

- Reader 分组由绑定的方法实现自动识别，不读取显式分组声明。
- 成功 Hook 表示被观察的方法返回，不独立证明数据库写入成功；失败 Hook 只能观察传播至该边界的异常。
- 当前 MemRead refresh 调用没有独立观测点，不单独生成刷新事件。
- 移除未随仓库提供的插件测试命令和过时失败数量，保留安装、启用及 JSON 产物检查说明。

`src/memos/mem_scheduler/task_schedule_modules/handlers/mem_read_handler.py` 仅删除注释中“除非显式声明”的过期表述，自动分组和 Reader 顺序保持逻辑未改。

## 5. 验证结果与边界

使用 Python 3.12.14、Poetry 2.1.3、现有锁文件及 SmartComment 0.1.2。原版与优化版在同一依赖环境的独立进程中运行相同插件回归，并核对模块加载路径，避免混用工作树代码。

| 验证范围 | 结果 |
| --- | --- |
| 独立 Hook 工作树 | 41 passed |
| 集成工作树 Hook + Reader 顺序/分组 | 49 passed |
| 本地插件回归：原版 / 优化版 | 31 passed / 31 passed |
| 相关离线 MemOS 模块 | 266 passed，1 skipped（上游已有） |
| 根目录与插件配置的定点 Ruff、格式检查 | 通过 |
| 两工作树 `git diff --check` | 通过 |

上述测试集合存在重叠，不应累加通过数量。插件回归包含真实 SmartComment 图构造和 JSON 写入/读回；数据库、LLM 等外部边界使用替身。插件本地回归覆盖添加关联、来源配对、搜索阶段匹配、旧图恢复、脱敏、快照隔离及生命周期。

等价结论针对现有公开使用路径和业务输出；源码位置字段 `filename`、`lineno`、`trigger_point` 可随路径和行号变化，不承诺 JSON 字节完全一致。

完整 `make test` 在 Ollama 外部调用处达到 180 秒诊断上限后终止，未完成全量测试，也未进行真实部署环境的端到端验收。此限制不应写成“全量通过”。

## 6. 提交范围

提交内容为上述插件清理、Hook 测试优化、文档及本说明。插件目录下本地维护的 `tests/`、`tmp/` 和日志继续被 Git 忽略，不纳入提交。通用 Hook 与独立插件仍按两个上游 PR 的定位准备，不在本轮重新划分基线已有提交。
