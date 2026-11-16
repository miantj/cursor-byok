# Cursor 代理执行流程

本文按当前源码描述服务端目录、运行所有权和完整执行链路；流程中的 `Run` 指一次通用引擎执行，`cycle` 指其中的一次模型流调用。一个会话可以包含多个 Run，一个 Run 可以包含多个 cycle 和工具轮。

## 完整目录

行数为 2026-11-16 按当前工作区重新核对的实测值。统计口径：每个 `.rs` 文件的物理行数，包含空行、注释、文件内测试和独立 Rust 测试文件；排除构建产物和生成代码。目录行数是其下递归 Rust 文件的总行数，不含非 Rust 资产。`protocol/proto.rs` 是手写的生成代码接入及补充类型，计入统计；其 `OUT_DIR` 中的生成文件不计入。`conversation/runtime_tests.rs` 由 `runtime.rs` 的 `#[path]` 挂载，计入统计。下树完整列出 `server/` 内纳入统计的 Rust 文件。

```text
server/                                         # 60,401 行；服务端 Rust 源码、构建脚本和测试
├── build.rs                                    # 54 行；生成 agent Protobuf 并检查 aiserver 协议字段
├── src/                                        # 49,605 行；服务端业务实现及模块内测试
│   ├── lib.rs                                  # 19 行；服务端库的模块导出
│   ├── app.rs                                  # 212 行；依赖组装、HTTP 启动和服务关闭
│   ├── config.rs                               # 170 行；进程配置和受管数据目录
│   ├── error.rs                                # 101 行；公共错误类型及转换
│   ├── network.rs                              # 225 行；按代理设置复用出站 HTTP 客户端
│   │
│   ├── bin/                                    # 16 行；可执行程序入口
│   │   └── cursor-server.rs                    # 16 行；加载配置并启动 App
│   │
│   ├── api/                                    # 3,870 行；面向 Cursor 与外部调用者的 HTTP API
│   │   ├── mod.rs                              # 7 行；API 模块导出
│   │   ├── router.rs                           # 7 行；Cursor 总路由入口
│   │   ├── cursor/                             # 2,295 行；Cursor HTTP/Connect 接入与官方服务转发
│   │   │   ├── mod.rs                          # 9 行；Cursor API 模块导出
│   │   │   ├── handlers.rs                     # 464 行；端点注册及本地/官方服务选路
│   │   │   ├── bidi.rs                         # 233 行；Bidi 解码、输入校验和追加接纳
│   │   │   ├── run_sse.rs                      # 539 行；下行订阅、官方流转交和断开清理
│   │   │   ├── proxy.rs                        # 232 行；官方 Cursor HTTP 请求和响应转发
│   │   │   ├── bugbot_fallback.rs              # 205 行；官方额度错误后的未启动 Bugbot 本地接管
│   │   │   ├── bugbot_tests.rs                 # 400 行；Bugbot 官方错误与本地接管协议测试
│   │   │   └── model_selection_tests.rs        # 213 行；模型选择及官方额度响应测试
│   │   └── byok/                               # 1,561 行；独立的 OpenAI/Anthropic 兼容外部 API
│   │       ├── mod.rs                          # 201 行；API 开关、密钥认证与生成端点
│   │       ├── models.rs                       # 66 行；配置模型和插件模型的公开目录
│   │       ├── protocol.rs                     # 471 行；外部协议请求转通用 ModelRequest
│   │       ├── output.rs                       # 444 行；通用模型事件转外部流式/完整响应
│   │       └── direct.rs                       # 379 行；同协议内置模型的原生 HTTP 转发
│   │
│   ├── cursor/                                 # 20,987 行；Cursor Agent 协议和执行适配层
│   │   ├── mod.rs                              # 13 行；Cursor 子模块导出
│   │   │
│   │   ├── transport/                          # 888 行；request_id 级输入输出通道
│   │   │   ├── mod.rs                          # 15 行；通道公共类型导出
│   │   │   ├── registry.rs                     # 330 行；本地通道及官方路由的注册和代次清理
│   │   │   ├── handle.rs                       # 195 行；通道输入、下行订阅和生命周期入口
│   │   │   ├── lifecycle.rs                    # 170 行；追加接纳计数与关闭排空状态
│   │   │   ├── inbox.rs                        # 30 行；按 append_seqno 去重并顺序释放输入
│   │   │   ├── output.rs                       # 79 行；进程内帧缓存、重放、广播及订阅计数
│   │   │   └── bugbot_replay.rs                # 69 行；暂存尚未启动的官方 Bugbot 上行消息
│   │   │
│   │   ├── conversation/                       # 2,572 行；通道运行代次和会话活跃运行协调
│   │   │   ├── mod.rs                          # 15 行；协调层公共类型导出
│   │   │   ├── registry.rs                     # 208 行；conversation_id → 活跃 RunHandle 与 pending
│   │   │   ├── runtime.rs                      # 886 行；每个 request_id 的 actor 及当前运行代次
│   │   │   ├── runtime_tests.rs                # 163 行；runtime actor 代次与接纳行为测试
│   │   │   ├── command.rs                      # 29 行；Append、RunFinished、Disconnect 及结束类型
│   │   │   ├── delivery.rs                     # 11 行；重导出投递类型并提供目标 Run 校验函数
│   │   │   ├── pending.rs                      # 26 行；按 event_id 去重的跨 Run 待处理队列
│   │   │   └── output.rs                       # 1,234 行；RunEvent 投影、运行时追加、工具和检查点协调
│   │   │
│   │   ├── compile/                            # 2,423 行；Cursor 请求转通用运行数据
│   │   │   ├── mod.rs                          # 16 行；编译入口和类型导出
│   │   │   ├── run.rs                          # 779 行；完整请求、历史和模型编译为 PreparedRun
│   │   │   ├── context.rs                      # 714 行；恢复并编译 rules、skills、MCP 和环境
│   │   │   ├── action.rs                       # 76 行；投递类型、CompiledMessages 和 Action 分类函数
│   │   │   ├── insert_messages.rs              # 210 行；后台任务完成信息转运行初始消息
│   │   │   ├── break_messages.rs               # 393 行；用户消息、上下文注入及消息身份编译
│   │   │   ├── images.rs                       # 75 行；用户消息的图片和 Blob 内容恢复
│   │   │   └── model.rs                        # 160 行；Cursor 模型选择和子任务模型配置
│   │   │
│   │   ├── checkpoint/                         # 2,107 行；通用消息与 Cursor 会话状态的互转
│   │   │   ├── mod.rs                          # 14 行；检查点公共类型导出
│   │   │   ├── builder.rs                      # 303 行；组装会话状态并发出 CheckpointUpdate
│   │   │   ├── steps.rs                        # 143 行；暂存文本、思考和工具展示步骤
│   │   │   ├── turns.rs                        # 127 行；追加和恢复 Cursor turn Blob
│   │   │   ├── roots.rs                        # 114 行；稳定根消息 Blob 的追加和显式替换
│   │   │   ├── recovery.rs                     # 49 行；预取 Blob 校验及根消息恢复
│   │   │   ├── summary.rs                      # 107 行；压缩后的根消息及展示状态重建
│   │   │   ├── derived.rs                      # 222 行；Todo、Plan 和子任务进度的派生状态
│   │   │   ├── worker.rs                       # 212 行；串行执行检查点任务并响应提交屏障
│   │   │   └── messages/                       # 816 行；Cursor 根消息及待执行工具的编解码
│   │   │       ├── mod.rs                      # 12 行；消息编解码入口
│   │   │       ├── decode.rs                   # 259 行；Cursor JSON 根消息转 CanonicalMessage
│   │   │       ├── encode.rs                   # 274 行；CanonicalMessage 转稳定/待定根消息
│   │   │       └── tests.rs                    # 271 行；消息编解码和身份稳定测试
│   │   │
│   │   ├── tools/                              # 6,845 行；工具执行、Cursor 展示和结果关联
│   │   │   ├── mod.rs                          # 306 行；ToolDispatcher 和批次执行协调
│   │   │   ├── registry.rs                     # 91 行；静态工具 Schema 及模式工具选择
│   │   │   ├── runtime.rs                      # 367 行；Exec/交互编号、运行状态和中断清理
│   │   │   ├── stream.rs                       # 288 行；流式工具参数转 Cursor 展示事件
│   │   │   ├── edit.rs                         # 304 行；编辑前读取及后续写入状态
│   │   │   ├── schedule.rs                     # 74 行；后台工具任务调度
│   │   │   ├── compat.rs                       # 105 行；不支持工具的失败结果和安全展示
│   │   │   │
│   │   │   ├── codec/                          # 1,901 行；工具的 Cursor 协议编解码
│   │   │   │   ├── mod.rs                      # 93 行；工具事件、完成和中断的统一入口
│   │   │   │   ├── request.rs                  # 569 行；工具调用转 ExecServer 请求
│   │   │   │   ├── response.rs                 # 372 行；ExecClient 消息和流关闭事件解码
│   │   │   │   ├── query.rs                    # 261 行；工具审批和用户交互请求编码
│   │   │   │   └── render.rs                   # 606 行；工具卡片和完成事件渲染
│   │   │   │
│   │   │   ├── tool_call_dispatch/             # 425 行；按工具类型选择实际执行入口
│   │   │   │   ├── mod.rs                      # 156 行；工具路由及执行参数校验
│   │   │   │   ├── exec.rs                     # 73 行；Cursor Exec 和动态 MCP 执行
│   │   │   │   ├── edit.rs                     # 22 行；编辑工具的隐藏读取阶段
│   │   │   │   ├── interaction.rs              # 115 行；交互请求及审批后的继续执行
│   │   │   │   ├── local.rs                    # 21 行；服务端同步完成的本地工具
│   │   │   │   └── search.rs                   # 38 行；服务端 Semble 搜索任务启动
│   │   │   │
│   │   │   └── tool_call_result/               # 2,984 行；完成事件转通用结果及 Cursor 状态
│   │   │       ├── mod.rs                      # 177 行；ToolCompletion 类型和结果通道
│   │   │       ├── gate.rs                     # 897 行；Exec 完成关联、去重和结果门控
│   │   │       ├── interaction.rs              # 414 行；审批、提问、模式切换等交互结果
│   │   │       ├── local.rs                    # 178 行；Todo 和进度等本地结果
│   │   │       ├── mcp.rs                      # 61 行；MCP 执行前失败结果
│   │   │       ├── mcp_state.rs                # 144 行；MCP 响应和资源状态
│   │   │       ├── search.rs                   # 112 行；搜索结果和对应 Cursor 卡片
│   │   │       └── exec/                       # 1,001 行；命令、读文件和子任务等 Exec 结果
│   │   │           ├── mod.rs                  # 175 行；执行结果解码和后续阶段协调
│   │   │           ├── output.rs               # 577 行；执行输出解析和持久化
│   │   │           └── render.rs               # 249 行；执行输出的 Cursor 展示
│   │   │
│   │   ├── protocol/                           # 687 行；Protobuf 接入及非工具下行协议
│   │   │   ├── mod.rs                          # 6 行；协议子模块导出
│   │   │   ├── proto.rs                        # 111 行；包含生成类型并补充 aiserver 线协议类型
│   │   │   ├── connect.rs                      # 165 行；Connect 帧、结束帧和错误帧
│   │   │   ├── json_stream.rs                  # 233 行；增量 JSON 参数解析
│   │   │   └── events.rs                       # 172 行；模型、注入和轮结束的下行事件
│   │   │
│   │   ├── prompting/                          # 455 行；模式提示词和工具定义编译
│   │   │   ├── mod.rs                          # 10 行；提示词模块导出
│   │   │   ├── compiler.rs                     # 94 行；静态 PromptSpec 和运行时模板渲染
│   │   │   ├── catalog.rs                      # 3 行；提示词模式目录入口
│   │   │   ├── assets.rs                       # 180 行；内嵌/文件提示词资源加载
│   │   │   └── derived_state.rs                # 168 行；运行上下文的派生展示文本
│   │   │
│   │   └── services/                           # 4,997 行；非模型循环的 Cursor 配套服务
│   │       ├── mod.rs                          # 15 行；配套服务模块导出
│   │       ├── account.rs                      # 597 行；账号、订阅和用量接口
│   │       ├── analytics.rs                    # 223 行；Cursor 分析和初始化配置
│   │       ├── blob_sync.rs                    # 328 行；Blob 本地缓存、KV 读取和 SET 确认
│   │       ├── context_sync.rs                 # 230 行；缺失请求上下文的客户端读取和缓存
│   │       ├── model_catalog.rs                # 1,002 行；向 Cursor 发布内置和插件模型目录
│   │       ├── commit_message.rs               # 415 行；Git 提交信息生成
│   │       ├── compatibility.rs                # 119 行；本地响应的配套 Cursor 协议端点
│   │       ├── entitlement.rs                  # 143 行；官方免费额度资格缓存
│   │       ├── server_config.rs                # 51 行；Cursor 服务端功能配置响应
│   │       ├── tab.rs                          # 51 行；Tab 服务配置和路由
│   │       ├── usage.rs                        # 219 行；上下文 Token 用量分解
│   │       ├── observability/                  # 577 行；异步 Cursor 请求和响应追踪
│   │       │   ├── mod.rs                      # 157 行；追踪服务和非阻塞记录入口
│   │       │   ├── event.rs                    # 71 行；内部追踪队列事件
│   │       │   └── worker.rs                   # 349 行；追踪开关、帧解析和数据落库
│   │       └── knowledge/                      # 1,027 行；本地规则知识库及同步
│   │           ├── mod.rs                      # 356 行；知识库 HTTP 端点和服务协调
│   │           ├── store.rs                    # 487 行；本地规则文档和元信息存储
│   │           └── sync.rs                     # 184 行；客户端规则同步
│   │
│   ├── run/                                    # 3,000 行；不依赖 Cursor 协议的通用运行引擎
│   │   ├── mod.rs                              # 20 行；运行引擎公共接口导出
│   │   ├── engine.rs                           # 1,097 行；运行占用、模型循环、收尾和压缩提交
│   │   ├── handle.rs                           # 159 行；RunHandle、阶段检查和取消入口
│   │   ├── command.rs                          # 35 行；RunCommand、MessageBatch 和投递结果
│   │   ├── event.rs                            # 142 行；RunEvent、提交屏障和最终结果
│   │   ├── model_cycle.rs                      # 639 行；单次模型流消费、完整性校验和失败分类
│   │   ├── model_retry.rs                      # 77 行；模型调用的重试预算和重试条件
│   │   ├── history.rs                          # 57 行；摘要请求末尾的独立指令投影
│   │   ├── tool_round.rs                       # 230 行；工具轮创建、结果提交及中断结算
│   │   ├── messages.rs                         # 99 行；运行消息幂等追加和确认等待
│   │   ├── compaction.rs                       # 410 行；上下文预算、压缩输入和替换结果校验
│   │   └── port.rs                             # 35 行；引擎和调用适配层之间的双向通道
│   │
│   ├── model/                                  # 1,854 行；供应商和 Cursor 无关的公共数据类型
│   │   ├── mod.rs                              # 25 行；公共类型导出
│   │   ├── conversation.rs                     # 53 行；会话及 Run 身份
│   │   ├── checkpoint.rs                       # 15 行；内部检查点身份
│   │   ├── message.rs                          # 165 行；规范消息、来源和内容类型
│   │   ├── run.rs                              # 59 行；PreparedRun、RunAction 和子任务关系
│   │   ├── tool.rs                             # 64 行；工具定义、调用、结果和工具轮数据
│   │   ├── inference.rs                        # 50 行；模型调用输入和 Usage
│   │   ├── projection.rs                       # 219 行；规范消息投影为供应商可见历史
│   │   ├── configuration.rs                    # 482 行；模型配置、请求 URL 和推理参数
│   │   ├── observability.rs                    # 279 行；模型调用记录和统计类型
│   │   ├── token_count.rs                      # 248 行；输入 Token 估算和格式化
│   │   └── tool_result_replay.rs               # 195 行；持久化工具结果的供应商输入恢复
│   │
│   ├── provider/                               # 3,205 行；内置/插件模型路由和供应商协议适配
│   │   ├── mod.rs                              # 380 行；Provider 接口和公共 HTTP/SSE 处理
│   │   ├── router.rs                           # 431 行；模型配置路由、调用记录和流空闲超时
│   │   ├── event.rs                            # 53 行；归一化的模型流事件
│   │   ├── normalize.rs                        # 29 行；供应商流事件规范化
│   │   ├── attempt.rs                          # 180 行；单次 HTTP 尝试及错误信息提取
│   │   ├── request_template.rs                 # 67 行；请求参数和 Header 的会话占位符渲染
│   │   ├── recorder.rs                         # 465 行；模型调用载荷、Token、时间和终态记录
│   │   ├── anthropic.rs                        # 547 行；Anthropic Messages 协议
│   │   ├── openai_chat.rs                      # 469 行；OpenAI Chat Completions 协议
│   │   └── openai_responses.rs                 # 584 行；OpenAI Responses 协议
│   │
│   ├── plugin/                                 # 5,773 行；插件发现、Deno 运行时和模型执行扩展
│   │   ├── mod.rs                              # 32 行；插件公共能力导出
│   │   ├── registry.rs                         # 1,374 行；资源、模型目录、认证和调用的协调
│   │   ├── catalog.rs                          # 348 行；发现 manifest 并读取插件能力定义
│   │   ├── manifest.rs                         # 213 行；插件静态声明和权限校验
│   │   ├── definition.rs                       # 217 行；通过宿主虚拟模块读取 TypeScript 定义
│   │   ├── descriptor.rs                       # 315 行；插件能力、资源和模型描述类型
│   │   ├── runtime.rs                          # 229 行；Deno 就绪状态及初始化任务
│   │   ├── installation.rs                     # 273 行；Deno 下载、校验、解压和安装验证
│   │   ├── asset.rs                            # 88 行；平台与固定 Deno 发行资产的映射
│   │   ├── worker.rs                           # 892 行；每个活跃插件的长期 Deno 子进程
│   │   ├── protocol.rs                         # 92 行；宿主与插件之间的逐行 JSON 消息
│   │   ├── wire.rs                             # 300 行；通用模型输入/事件与插件 SDK 协议转换
│   │   ├── state.rs                            # 497 行；宿主持有的插件资源和模型目录状态
│   │   ├── data.rs                             # 254 行；插件私有 JSON 的原子文件存储
│   │   ├── oauth_callback.rs                   # 347 行；宿主持有的本机 OAuth 回调服务
│   │   └── builtin.rs                          # 302 行；内置插件的预安装
│   │
│   ├── store/                                  # 5,342 行；SQLite 事务、会话和观测数据持久化
│   │   ├── mod.rs                              # 28 行；Store 公共接口导出
│   │   ├── sqlite.rs                           # 49 行；连接池、数据库初始化及迁移入口
│   │   ├── migrations.rs                       # 490 行；内嵌迁移执行、进度和启动错误
│   │   ├── writer.rs                           # 15 行；会话写事务的串行锁
│   │   ├── cas.rs                              # 108 行；内容寻址 Blob、引用边和闭包读取
│   │   ├── conversations.rs                    # 100 行；会话创建和当前检查点读取
│   │   ├── checkpoints.rs                      # 373 行；不可变检查点、前缀复用和写入所有权校验
│   │   ├── messages.rs                         # 123 行；规范消息落库、身份冲突检查和读取
│   │   ├── runs.rs                             # 520 行；运行占用、执行关系、调用进度和终态
│   │   ├── tool_rounds.rs                      # 313 行；工具轮、完成顺序和结果结算
│   │   ├── input_anchors.rs                    # 41 行；输入身份与会话基线的去重关联
│   │   ├── llm_calls.rs                        # 606 行；模型调用载荷、计费 Token 和统计
│   │   ├── models.rs                           # 401 行；内置模型配置存储
│   │   ├── settings.rs                         # 714 行；桌面、代理、外部 API 和观测设置
│   │   ├── legacy_config.rs                    # 276 行；旧 YAML 模型配置的显式导入
│   │   ├── cursor_traces.rs                     # 345 行；Cursor 请求追踪和关联资产落库
│   │   ├── overview.rs                         # 221 行；控制台汇总查询
│   │   ├── storage.rs                          # 155 行；可清理观测数据的占用统计与清理
│   │   └── storage/                            # 464 行；存储清理行为测试
│   │       └── tests.rs                        # 464 行；真实 WAL 数据库和物理文件清理测试
│   │
│   ├── control/                                # 2,366 行；管理控制台 HTTP API
│   │   ├── mod.rs                              # 285 行；管理路由、静态页面和开发代理
│   │   ├── service.rs                          # 1,129 行；管理端共享依赖及模型诊断服务
│   │   ├── settings.rs                         # 188 行；设置和统计数据清理接口
│   │   ├── models.rs                           # 98 行；内置模型配置接口
│   │   ├── plugins.rs                          # 160 行；插件运行时、资源和模型管理接口
│   │   ├── overview.rs                         # 36 行；管理概览接口
│   │   ├── calls.rs                            # 34 行；模型调用和 Cursor 追踪查询接口
│   │   ├── ads.rs                              # 408 行；广告配置和请求处理
│   │   └── harness.rs                          # 28 行；本机 Cursor 集成控制接口
│   │
│   ├── search/                                 # 1,420 行；服务端搜索和网页获取
│   │   ├── mod.rs                              # 13 行；搜索公共能力导出
│   │   ├── engine.rs                           # 339 行；HTML/JSON 搜索引擎适配
│   │   ├── catalog.rs                          # 231 行；搜索服务目录与配置
│   │   ├── federation.rs                       # 220 行；多源搜索聚合和排序
│   │   ├── fetch.rs                            # 328 行；网页获取和正文提取
│   │   ├── search_provider.rs                  # 134 行；配置的外部搜索供应商适配
│   │   └── cache.rs                            # 155 行；抓取页面缓存及本地访问路由
│   │
│   └── local_app/                              # 1,045 行；桌面 Cursor 账号、代理和证书集成
│       ├── mod.rs                              # 221 行；CursorHarness 集成状态和启停
│       ├── account.rs                          # 175 行；本地 Cursor 账号状态注入
│       ├── proxy.rs                            # 224 行；本机 HTTPS 代理及拦截域名/路径白名单
│       ├── settings.rs                         # 111 行；Cursor 本地代理设置读写
│       └── ca/                                 # 314 行；本地代理 CA 管理
│           ├── mod.rs                          # 239 行；CA 创建、信任检查和安装
│           └── windows.rs                      # 75 行；Windows 系统证书库集成
│
└── tests/                                      # 10,742 行；跨模块行为测试及测试支持
    ├── conversation_delivery.rs                # 934 行；跨 Run 消息投递和会话协调
    ├── interrupt.rs                            # 2,261 行；追加、替换、工具中断和取消时序
    ├── error_lifecycle.rs                      # 930 行；模型失败分类、重试和错误终态
    ├── cursor_transport_lifecycle.rs           # 197 行；通道接纳、关闭和续接时序
    ├── runtime_context.rs                      # 532 行；运行时请求上下文读取
    ├── checkpoint_recovery.rs                  # 653 行；检查点恢复及工具待定状态
    ├── prefix_stability.rs                     # 635 行；供应商历史和消息前缀稳定
    ├── compaction.rs                           # 884 行；自动/显式压缩和预算
    ├── compaction_safety.rs                    # 316 行；压缩失败及打断的历史安全边界
    ├── tool_round.rs                           # 1,479 行；工具轮提交、完成关联和恢复
    ├── connect_wire.rs                         # 140 行；Connect 帧与 Cursor 线协议
    ├── cursor_trace_queue.rs                   # 129 行；追踪队列和执行路径隔离
    ├── commit_message.rs                       # 251 行；提交信息生成
    ├── external_api.rs                         # 903 行；外部 API 认证、协议和原生转发
    ├── knowledge_rules.rs                      # 216 行；本地规则知识库行为
    ├── local_rules_context.rs                  # 159 行；本地规则进入请求上下文
    └── support/                                # 123 行；行为测试的公共辅助实现
        ├── fixtures.rs                         # 18 行；测试数据库初始化
        ├── fake_provider.rs                    # 95 行；可控模型流
        └── fake_cursor.rs                      # 10 行；假 Cursor 协议辅助类型
```

非 Rust 输入分别由 `server/prompt/cursor/` 提供提示词和工具资源、`server/migrations/` 提供 SQL 迁移、`protocols/cursor/` 提供 Protobuf；插件的 TypeScript 实现与 Rust 宿主通过 `plugin/protocol.rs`、`plugin/wire.rs` 连接，不计入上面的 Rust 行数。

## 顶层架构

```text
Cursor IDE
    │ HTTPS / Connect
    ▼
local_app/proxy.rs                         本机代理边界：域名和路径白名单
    │
    ▼
api/cursor/handlers.rs                     HTTP 边界：解码、端点及模型选路
    ├── 官方模型 ──→ api/cursor/proxy.rs ──→ Cursor 官方服务
    │                                      │
    │                官方 RunSSE ←─────────┘
    │                Bugbot 未启动且额度不足时可转交本地
    │
    └── 本地内置模型 / 插件模型
          │ BidiAppend                          ▲ RunSSE
          ▼                                     │
    ┌───────────────────────────────────────────┴───────┐
    │ TransportRegistry                                 │
    │ request_id → LocalTransport { generation, handle } │
    │ 官方路由另外保存 request_id → upstream generation │
    └───────────────────────┬───────────────────────────┘
                            │ bind_transport
                            ▼
    ┌───────────────────────────────────────────────────┐
    │ ConversationRuntime：每个 request_id 一个 actor    │
    │ OrderedInbox → 当前 RunGeneration                 │
    │                                                   │
    │ RunGeneration：完整请求、代次编号、取消令牌、       │
    │ 可选 RunHandle、工具状态、运行时动作通道           │
    │                                                   │
    │ compile::prepare → PreparedRun + CursorRunContext │
    └──────────────────┬───────────────────┬────────────┘
                       │ activate          │ 建立 Run 双向通道
                       ▼                   ▼
    ┌──────────────────────────┐    ┌─────────────────────────┐
    │ ConversationRegistry     │    │ RunEngine               │
    │ conversation_id          │    │ claim_run → initial     │
    │ → ActiveRun              │    │ → model cycle           │
    │   {run_id, RunHandle}    │    │ → tool round / append   │
    │ + pending[conversation]  │    │ → compaction / finalize │
    │                          │    └────────────┬────────────┘
    │ 替换活跃句柄时取消旧 Run │                 │ RunEvent
    │ 不保存长期会话 Runtime   │                 ▼
    └──────────────────────────┘    ┌─────────────────────────┐
                                    │ ConversationOutput      │
             ModelInvocation        │ 下行事件、工具、追加和   │
    RunEngine ───────────────┐       │ 检查点屏障的适配循环     │
                            ▼       └───────┬──────────┬──────┘
                    ProviderRouter          │          │
                     ├─ 内置 Provider       │          ▼
                     └─ PluginRegistry      │    CheckpointWorker
                           └─ Deno worker   │          │
                                            ▼          ▼
                                      ToolDispatcher  CheckpointBuilder
                                            │          │
                                      Cursor / 本地    Blob SET + ACK
                                            │          │
                                            └────┬─────┘
                                                 ▼
                                        Transport OutputHub
                                                 │ 缓存、广播和重放
                                                 ▼
                                               RunSSE
                                                 │
                                                 ▼
                                             Cursor IDE

持久化边界：RunEngine / Provider 记录 / CheckpointBuilder → Store → SQLite
扩展边界：ProviderRouter → PluginRegistry → plugin/wire → Deno 插件进程
```

所有权按三种身份区分：

```text
request_id                               通道身份
└── TransportHandle + 一个通道 actor
    ├── append_seqno                     本通道输入顺序
    ├── RunGeneration #1 → RunId A        第一次执行
    └── RunGeneration #2 → RunId B        后续完整请求或新轮次执行

conversation_id                          持久化会话身份
├── Store 中的消息、检查点和 Run 记录
└── ConversationRegistry 中的活跃 RunHandle 与待处理消息
    └── 不同 request_id 可以竞争同一会话；激活会替换并取消旧句柄

RunId                                    一次执行身份，不能与 request_id 混用
└── 同一个 Run 内：多个模型 cycle、工具轮和消息追加
```

## 上行主链路

```text
POST /aiserver.v1.BidiService/BidiAppend
        │
        ▼
handlers.rs：读取请求体，Connect 解码 BidiAppendRequest
        │
        ▼
bidi.rs::decode
├── request_id 必须非空
├── data 十六进制 / data_binary 二选一
└── Protobuf 解码 AgentClientMessage
        │
        ▼
handlers.rs：首个模型选择本地配置 / 插件 / 官方路由
        ├── 官方 ──→ 保留官方路由并转发
        └── 本地
              ▼
        TransportRegistry::get_or_create_for_append
              │ 需要时创建通道 actor
              │ 关闭中仅当本条 append 带 model_id 时 replace_closing=true 才替换
              ▼
        TransportHandle::admit
              │ 接纳守卫覆盖命令入队；校验 conversation_id 和 parent
              ▼
        TransportCommand::Append { seqno, message }
              │
              ▼
        runtime.rs → OrderedInbox::push
              ├── 已消费/重复序号：不重复释放
              ├── 存在序号空洞：暂存等待
              └── 连续序号：按序处理
                    ├── RunRequest → start_generation → compile::prepare
                    ├── ExecClient → 上下文响应优先，否则工具协议解码
                    ├── ExecControl → 上下文/工具的关闭或失败响应
                    ├── InteractionResponse → ToolDispatcher
                    ├── KvClient → BlobSynchronizer
                    └── ConversationAction → runtime 内分支处理
```

`compile/action.rs` 定义分类函数和投递数据，但实际运行时 Action 由 `runtime.rs` 分支处理；上行并非统一经过 `delivery(action)` 后自动获得所有 Action 支持。

## 下行主链路

```text
RunEngine / Provider stream
        │ RunEvent
        ▼
conversation/output.rs::run_inner            单个 Run 的适配循环
        │
        ├── Text / Thinking / Usage
        │     ├── protocol/events.rs → AgentServerMessage
        │     └── checkpoint/steps.rs → StepBuffer
        │
        ├── ToolCall 参数事件
        │     ├── tools/stream.rs → Cursor 工具增量卡片
        │     └── ExecuteToolRound → ToolDispatcher
        │
        ├── MessagesCommitted
        │     └── CheckpointJob → CheckpointWorker → Builder → 下行检查点
        │
        └── Ended
              ├── Completed → TurnEnded + 最终会话状态
              ├── Failed → Cursor 错误结束帧
              └── Cancelled → 取消结束帧
                       │
                       ▼
        TransportHandle::emit / emit_frame
                       │ Connect 编码
                       ▼
        OutputHub：history + subscribers
                       │
                       ▼
        run_sse.rs：HTTP body 消费输出队列
                       │ text/event-stream；内容为 Connect 帧
                       ▼
                   Cursor IDE
```

`OutputHub` 缓存属于当前进程和当前通道，不是数据库历史。新订阅会先重放已缓存帧再接收增量；帧结束和 HTTP body 提前释放是不同路径，后者由订阅计数决定是否发送 Disconnect。追踪记录通过 `services/observability/` 的异步队列落库，不由 `OutputHub` 承担数据库事务。

## Message 编译

完整请求和运行中追加是两条实际入口：

```text
完整 AgentRunRequest
        │
        ▼
compile/run.rs::prepare
├── import_prefetched：先校验和缓存预取 Blob
├── conversation_state → recovery → 根消息恢复
├── context::hydrate → 恢复 rules / skills / MCP / environment
├── merge_local_rules → 合并本地规则
├── action → 用户问题 / 后台完成 / 执行计划 / 压缩 / 恢复
├── model → 选择内置或插件模型、推理参数和子任务模型
├── PromptCompiler → 静态 instructions + 工具定义
├── import_checkpoint / anchor_input → 确定历史基线
├── request-context 消息 + runtime 用户消息 → initial_messages
└── match_checkpoint_prefix → 复用已提交输入的前缀
        │
        ▼
PreparedRun                              CursorRunContext
├── run_id / conversation_id             ├── request_id / mode
├── base_checkpoint_id                   ├── ExecContext
├── prompt / model                       ├── dynamic_tools
├── initial_messages                     ├── checkpoint_prompt / turn_user
└── action: Start / Resume / Compact      └── compacting / background_completion
```

```text
运行中的 UserMessageAction / InjectContextAction
        │
        ▼
runtime.rs → RuntimeAction 通道 → ConversationOutput
        │
        ├── 用户消息：要求 message_id；编译运行时文本和图片
        └── 上下文注入：要求 injection_id 和 payload
              └── expected_run_id 与当前 Cursor request_id 比较
        │
        ▼
CompiledMessages                         定义位于 compile/action.rs
├── event_id                             待处理队列的事件去重身份
├── target_run_id: Option<RunId>          会话投递的目标执行身份
├── messages: Vec<CanonicalMessage>      通用规范消息
└── delivery: Ignore / InsertMessages / BreakMessages
        │
        ▼
ConversationRegistry::deliver
        │
        ▼
RunHandle → RunCommand → run/messages.rs → Store
```

消息的持久化身份由 `message_id`、`runtime_event_id` 参与校验；`MessageBatch.event_id` 本身不是替代每条消息身份的数据库去重键。初始完整请求会单独投影 `request-context:*`，并与最近一次请求上下文比较内容；运行中用户追加和注入的编译入口应按各自源码核对，不能由初始请求推断它们已经实现完全相同的上下文更新行为。

`PromptSpec` 和工具目录在准备 Run 时编译；运行时追加消息并不等同于重新配置整个 Run 的模型、模式或工具集合。

## Message 投递

下表描述 `ConversationRegistry::deliver` 和 `RunHandle` 的实际结果；准备阶段的 Cursor Action 路由另外由通道 actor 决定。

```text
会话活跃状态             Ignore             InsertMessages           BreakMessages
───────────────────────────────────────────────────────────────────────────────────
无活跃 Run               Applied            放入 pending             放入 pending
                                            返回 RunEnded            返回 RunEnded

Running                  Applied            当前 cycle/工具轮        中断当前 cycle/
                                            边界后提交               工具轮后提交

Finalizing               Applied            放入 pending             放入 pending
                                            返回 RunClosing          返回 RunClosing

活跃句柄已 Ended         Applied            放入 pending             放入 pending
或命令通道已关闭                            返回 RunEnded            返回 RunEnded
```

Running 主路径如上。若命令已成功 `send`，但在等待提交确认的 oneshot 返回前 Run 进入 Finalizing，调用方仍可能收到 `RunClosing`，随后由 registry 写入 pending。

```text
CompiledMessages.target_run_id
        │
        ├── 有活跃 Run 且目标不同 → StaleTarget，不向活跃句柄提交
        └── 无目标 / 目标相同 → 按 delivery 调用 RunHandle

pending[conversation_id]
        │ PendingMessages：按批次 event_id 去重
        ▼
下一次 spawn_run_request → take_pending
        │ 将 pending 消息放在本次 initial_messages 前
        ▼
新 Run 的初始消息提交
```

待处理队列在内存中；`deliver` 保存消息并不独立创建一个永久消费它的会话任务。后台完成完整请求会先尝试向活跃 Run 插入；若处于关闭边界，会等待会话空闲、刷新检查点并走后续执行。`delivery.rs` 只有目标校验辅助函数，真正投递与 pending 决策在 `registry.rs`。

## RunEngine

```text
PreparedRun
    │
    ▼
Store::claim_run                          数据库中的会话运行占用
    │
    ▼
提交 initial_messages → MessagesCommitted → 等待适配层确认
    │
    ├── Resume 存在 pending_tool_round → 恢复工具轮入口
    │
    ▼
┌─────────────────────── model loop ───────────────────────┐
│ load_checkpoint_messages → project_messages              │
│       │                                                 │
│       ├── 超出上下文预算 → auto_compact → 新检查点 → 循环 │
│       ├── 末尾为 Assistant → 持久化继续指令 → 循环       │
│       ▼                                                 │
│ begin_provider_call → ModelInvocation → consume_model_cycle│
│       │                                                 │
│       ├── BreakMessages → 取消本 cycle → 提交追加 → 循环  │
│       ├── 有工具调用 → tool_round → 结果/追加提交 → 循环  │
│       ├── 无工具调用且有新插入 → 提交回答及消息 → 循环    │
│       └── 无后续输入 → Finalizing → FinalTurn barrier    │
└─────────────────────────────┬───────────────────────────┘
                              ▼
Store::finish_run → phase = Ended → RunEvent::Ended(outcome)
```

```text
RunPhase
├── Running
│   ├── InsertMessages / BreakMessages → 命令入队，等待提交确认
│   ├── ToolResult → 命令入队
│   └── Cancel → 取消令牌 + Cancel 命令
├── Finalizing
│   ├── 新消息 / 新结果 → RunClosing
│   ├── 已接纳消息在收尾边界排空
│   │     └── 普通运行存在追加时可恢复 Running，仍是同一 Run
│   └── 最终检查点确认后完成
└── Ended
    └── 新消息 / 新结果 → RunEnded

RunCommand                               CommandResult
├── InsertMessages(MessageBatch)         ├── Applied       本次提交产生了新消息
├── BreakMessages(MessageBatch)          ├── Duplicate     消息已存在，本次未新增
├── ToolResult(ToolResult)               ├── RunClosing    正在收尾，不再接纳
└── Cancel                               ├── RunEnded      无法继续投递
                                         └── StaleTarget   会话目标 Run 不匹配

RunOutcome
├── Completed
├── Cancelled
└── Failed(Protocol / Provider / Store / Client)
```

重试策略位于 `run/model_retry.rs`，单次 HTTP 尝试位于 `provider/attempt.rs`，仓库没有 `provider/retry.rs`。模型周期失败会携带可重试性和是否已经向外输出的信息，RunEngine 在预算内决定是否重试；协议错误和已经输出的失败周期不能作为普通无输出瞬时失败处理。Provider 负责请求与流错误，RunEngine 负责一次逻辑模型调用的重试与运行循环。

## InsertMessages

```text
时间向下；整个过程仍属于同一 RunId

ConversationRegistry        RunEngine / model cycle       CheckpointWorker / Cursor
        │                             │                              │
        │ insert_messages             │ LLM Call #1 进行中           │
        ├────────────────────────────→│                              │
        │                             │ pending_insertions           │
        │                             │ 继续消费当前模型流           │
        │                             │                              │
        │                             │ #1 完成                      │
        │                             ├─ 无工具：提交 Assistant      │
        │                             └─ 有工具：先完成工具轮        │
        │                             │                              │
        │                             │ append_batches               │
        │                             │ → append_message_once        │
        │                             ├── MessagesCommitted ────────→│
        │                             │                         Blob SET / ACK
        │                             │←──────── barrier ready ──────┤
        │←── Applied / Duplicate ─────┤                              │
        │                             │                              │
        │                             │ LLM Call #2（有新增消息时）  │
```

Insert 不取消正在进行的模型周期或工具轮。后台任务完成信息主要通过完整 `BackgroundTaskCompletionAction` 请求编译后进入这条投递路径；不能把 Action 分类函数列出的所有后台 Action 都视为运行时已经接通。

## BreakMessages

```text
时间向下；取消的是当前 cycle / 工具等待，不是整个 Run

Cursor Action       ConversationOutput        RunEngine                Store / Checkpoint
      │                     │                     │                            │
      ├── User / Inject ───→│                     │                            │
      │                     │ 校验身份和载荷      │                            │
      │                     │ 标记 queued         │                            │
      │                     │ interrupt_execs     │                            │
      │←── Exec abort ──────┤                     │                            │
      │                     ├── BreakMessages ───→│                            │
      │                     │                     ├─ 模型中：取消子令牌        │
      │                     │                     │  等待模型周期停止          │
      │                     │                     └─ 工具中：未完成调用        │
      │                     │                        写 interrupted 结果 ─────→│
      │                     │                     │                            │
      │                     │                     │ 先提交已有 Insert          │
      │                     │                     │ 再提交 Break 消息 ────────→│
      │                     │←─ MessagesCommitted ┤                            │
      │←─ delivered/appended┤                     │                            │
      │                     ├── CheckpointJob ─────────────────────────────────→│
      │                     │                     │←──── barrier ready ────────┤
      │                     │                     │                            │
      │                     │                     │ 下一次 model cycle         │
```

工具轮中的未完成调用会结算为 `is_error=true` 的明确中断结果；已完成结果不重新执行。Cursor 工具取消还有执行器差异：`interrupt_for_message` 不对正在执行的 `Task` 发 Abort，但仍把它的 exec id 记入 `interrupted`，迟到完成会被丢掉；其他 Exec 会 Abort。不能概括成“每次用户追加都会终止所有子任务”，也不能概括成“子任务结果仍回写当前 Run”。完整 Run 替换使用另一套 `interrupt_for_run_replacement` 清理全部 exec/interaction。

## Tool 链路

```text
Provider → 模型工具流事件
        │
        ▼
RunEngine：完成参数校验，建立持久化 ToolRound
        │ ExecuteToolRound { round_id, calls }
        ▼
ConversationOutput → ToolDispatcher::start_batch
        │
        ├── 动态 MCP → Exec 请求（优先按动态定义匹配）
        ├── Shell / Read / Grep / Glob / Task 等 → Cursor Exec
        ├── Write / StrReplace / EditNotebook → 先读后写的编辑链路
        ├── AskQuestion / WebSearch / WebFetch / SwitchMode 等 → Interaction
        ├── TodoWrite / UpdateCurrentStep → 服务端本地完成
        ├── SembleSearch / SembleFindRelated → 服务端搜索任务
        └── 未识别工具 → 显式失败 ToolCompletion
        │
        ▼
CursorToolRuntime
├── Exec/交互 id → PendingExec / PendingInteraction
├── 每个运行代次拥有独立待执行状态
├── 同一通道内跨代次共享执行 id 递增器
└── completed / interrupted 状态用于识别完成和迟到响应
        │
        ▼
Cursor Exec / 用户交互 / 本地任务
        │ ExecClientMessage / ExecControl / InteractionResponse
        ▼
runtime.rs → tools/codec/response.rs → tool_call_result/gate.rs
        │
        ▼
ToolCompletion：通用 ToolResult + Cursor 专用展示状态
        │
        ▼
ConversationOutput::forward_completion
├── 图片结果 → Store Blob → 保存图片引用
├── 已中断调用 → 不再提交迟到完成
├── 校验 call_id 及重复完成
└── RunHandle::tool_result
        │
        ▼
RunEngine / tool_round.rs → Store::commit_tool_result
        │ MessagesCommitted(ToolResult)
        ▼
ConversationOutput：工具完成卡片和 StepBuffer
        │ 最后一条结果结算后
        ▼
CheckpointWorker → Blob 确认 → barrier → 下一次模型调用
```

工具的 Cursor 协议职责：

```text
tools/codec/
├── request.rs     ToolCall → ExecServerMessage
├── query.rs       ToolCall → InteractionQuery
├── response.rs    ExecClientMessage / StreamClose → 执行事件
└── render.rs      ToolCall / ToolCompletion → Cursor 工具卡片

控制与数据回传
Cursor ←── ExecServer / InteractionQuery ── 服务端
Cursor ─── ExecClient / InteractionResponse → 服务端
Cursor ←── Abort（按执行 id）────────────── 服务端
```

`Task` 执行可由 Cursor 发起新的子任务通道；父请求/工具关系通过 `TransportParent` 和 `RunKind::Subagent` 记录。它与服务端运行 Deno 的模型供应商插件是不同扩展边界。

## Checkpoint 链路

内部数据库检查点和 Cursor 会话检查点是两种表示：前者记录通用消息历史及写入所有权，后者记录根消息 Blob、turn、展示步骤和待执行工具，二者由检查点适配层连接。

```text
持久化与对外发布

RunEngine
    │ append_checkpoint / append_message_once / commit_tool_result
    ▼
Store（SQLite 事务）
├── messages：不可变内容及身份校验
├── conversation_checkpoints + checkpoint_messages：历史链和顺序
├── conversations：当前 head 与活跃运行所有权
└── tool_rounds：调用、结果和完成版本
    │ MessagesCommitted { checkpoint_id, cause, barrier }
    ▼
ConversationOutput
    │ 当前 StepBuffer → PendingSteps
    ▼
CheckpointWorker（有界任务队列，串行构建）
├── Settled / ToolSettled → 构建并发布稳定状态
├── ToolStarted → 稳定根消息 + pending_tool_calls
├── Final → staged + settled 两份最终状态
└── Compaction → 压缩后的状态
    │
    ▼
CheckpointBuilder
├── messages/encode.rs → 模型历史根消息
├── roots.rs → 稳定根 Blob 列表
├── turns.rs + steps.rs → turn/展示 Blob
├── derived.rs → Todo / Plan / 进度
└── builder.rs → read_paths / Token / 子任务状态
    │
    ▼
BlobSynchronizer::persist
    ├── Store::put_blob → 本地内容寻址存储
    └── SetBlobArgs → Cursor → SetBlobResult
                              │ 关联 KV id，校验成功后记 ACK
                              ▼
ConversationStateStructure → ConversationCheckpointUpdate
    │
    ▼
worker 返回 ready / 最终状态 → CommitBarrier.complete
    │
    ▼
RunEngine 继续模型调用或完成本 Run
```

最终普通回答的屏障会先构建 `staged` 和 `settled`，由 `ConversationOutput` 在 Completed 分支发送 TurnEnded 和最终状态；并非每一种 CheckpointJob 都在 worker 内直接发布相同帧序列。

```text
恢复

Cursor AgentRunRequest
├── pre_fetched_blobs → 哈希校验 → Store
└── conversation_state.root_prompt_messages_json
        │
        ▼
checkpoint/recovery.rs::hydrate_messages
        │ 按 Blob id 读取；本地没有则 KV GetBlob
        ▼
checkpoint/messages/decode.rs → CanonicalMessage[]
        │
        ▼
compile/run.rs
├── 校验系统提示词根，剥离规范历史中的旧系统 Prompt
├── import_checkpoint → 恢复数据库基线
├── input anchor + checkpoint prefix → 输入去重和前缀复用
└── pending_tool_calls → decode_pending → RunAction::Resume
        │
        ▼
PreparedRun → claim_run → 恢复工具轮或进入模型循环
```

```text
稳定性边界

普通追加
└── Store 追加检查点和规范消息
    └── roots.rs 追加新生成根，并检查本次已生成根未缩短或改变

静态系统根变化
└── roots.rs 会比较并更新系统根 Blob
    └── 不能将 Cursor wire 的所有根一概描述成绝不替换

压缩
└── 显式 replace_checkpoint / replace_roots
    ├── 旧检查点与新 head 区分
    └── 下一次模型请求从新根投影，不属于普通 append
```

自动压缩由预算估算或供应商上下文溢出触发；显式 Summarize 使用 `RunAction::Compact`。`replace_checkpoint` 只在摘要成功后执行；失败、Break 打断、空摘要或摘要含 tool calls 时不替换历史（见 `run/engine.rs`、`run/compaction.rs` 与 `tests/compaction_safety.rs`）。

## Cancel 链路

取消入口分散在通道、会话注册表和引擎，不能归结为“只有 ConversationRuntime 能取消”。

```text
入口                                      所有权和动作
───────────────────────────────────────────────────────────────────────────
运行中 CancelAction                       runtime → RunHandle.cancel
准备中 CancelAction                       runtime → superseded 取消令牌
完整 RunRequest 替换                      start_generation → 取消旧代次/Run
同 conversation_id 新 Run 激活            ConversationRegistry::activate
                                          → 取消被替换的旧 RunHandle
RunSSE 最后一个未完成订阅释放              LocalRunGuard → Disconnect
服务关闭                                  TransportRegistry::shutdown
                                          → 会话活跃句柄取消 + 通道 Disconnect
适配输出失败                              ConversationOutput → 记录失败并取消 Run
Completed 空闲窗内 CancelAction           已无 RunHandle 时基本空操作；
                                          关通道靠 2s idle 或新 Action
```

```text
运行中取消

RunHandle.cancel
    ├── cancellation.cancel
    └── RunCommand::Cancel
              │
              ▼
RunEngine：停止模型 cycle / 退出工具等待或屏障等待
              │
              ├── Store::finish_run(Cancelled)
              └── RunEvent::Ended(Cancelled)

ConversationOutput / runtime
    ├── 清理运行中的 Exec，向 Cursor 发出 Abort
    ├── RunFinish::Transport(Cancelled)
    └── Connect canceled 结束帧 → OutputHub.close
              │
              ▼
通道 actor 退出 → TransportActorGuard → lifecycle Closed
              │
              ▼
TransportRegistry：仅删除仍属于本代次的 request_id 条目
```

取消路径保留已提交的数据库状态，但没有“所有取消都生成一次最终完整 Checkpoint”的统一保证；普通成功收尾和取消不能画成同一最终检查点步骤。

```text
正常轮结束与通道关闭

Run Completed
    │ TurnEnded + 最终检查点
    ▼
等待后续 Action（当前空闲窗口为 2 秒）
    ├── 新用户 Action → 新运行代次 / 新 RunId，复用会话检查点
    └── 空闲超时 → begin_close → 等已接纳 append 完成入队
                       │
                       ▼
                    Draining → 消费已入队命令 → 结束通道

TransportLifecycle
Open ── begin_close ──→ Closing ── 接纳计数归零 ──→ Draining ──→ Closed
 ▲                         │                         │
 └────────────── reopen（调用方明确执行时）───────────┘
```

已接纳消息如何触发 reopen 或代次替换，取决于 `runtime.rs` 的对应消息分支；`lifecycle.rs` 提供状态能力本身不意味着所有续接路径都自动重开。一个 Run 内的 Break/Insert 是原生命周期延续，TurnEnded 后创建新 Run 是新的执行生命周期。

## 模块依赖

以下是当前主要运行依赖，不把所有模块强行画成单向分层；公共配置、网络和错误依赖省略。

```text
app
├── store                           初始化数据库
├── plugin                          插件目录和 Deno 运行时
├── provider                        模型路由
├── cursor/transport                通道和会话运行协调
├── api/cursor                      Cursor 协议接入
├── api/byok                        外部兼容 API
├── control                         管理端
└── local_app                       本机代理、账号和证书

api/cursor
├── cursor/transport                request_id 选路和输入输出
├── cursor/services                 非运行端点
└── plugin                          插件模型 ID 识别

cursor/transport
├── cursor/conversation             为新通道创建 actor
└── cursor/services/observability   异步追踪

cursor/conversation
├── cursor/transport                输入和帧输出句柄
├── cursor/compile                  请求与运行时消息编译
├── cursor/checkpoint               Cursor 状态投影
├── cursor/tools                    工具执行适配
├── cursor/protocol                 Cursor 消息和结束帧
├── run                             通用运行引擎及命令/事件
└── store                           历史、工具轮和运行状态读取

run
├── model                           PreparedRun、规范消息和模型输入
├── provider                        Provider 流接口
└── store                           运行占用、消息提交和检查点

cursor/checkpoint
├── model                           规范消息和工具轮
├── store                           消息、Blob 和工具状态
├── cursor/services/blob_sync       客户端 Blob 确认
└── cursor/protocol                 Cursor 检查点表示

cursor/tools
├── model                           工具调用和结果
├── store                           图片/结果和查询所需状态
├── search                          服务端搜索
└── cursor/protocol                 Cursor Exec 和交互协议

provider/router ──→ plugin/registry  插件模型调用入口
plugin ───────────→ provider         复用 ModelEvent、ProviderStream 和 CallRecorder
provider/plugin ──→ model + store    通用数据和调用记录

api/byok
├── provider                        跨协议转换后的模型调用
├── plugin                          插件模型目录
└── store                           认证设置及模型配置

store ──→ model                     持久化公共领域类型
model                               公共数据及投影，不持有 Cursor actor
```

```text
当前通用执行边界
run / provider / store / model ──X──→ cursor

Cursor wire 适配位于 cursor/*；外部兼容 wire 适配位于 api/byok/*。
transport ↔ conversation、provider ↔ plugin 存在实际双向类型/调用依赖，
不能把它们描述成已经完成拆分的严格单向层。
```

## 当前支持边界

```text
完整请求 AgentRunRequest.action
├── UserMessageAction                初始用户输入；/summarize 进入压缩
├── BackgroundTaskCompletionAction    编译后台完成并尝试 Insert 投递
├── ExecutePlanAction                编译已批准计划及启动消息
├── SummarizeAction                  显式压缩
├── ResumeAction                     显式恢复投影（event_id = resume:{request_id}）
└── 缺少新运行时消息的其他情况        按准备代码进入恢复路径

运行中 AgentClientMessage.ConversationAction
├── UserMessageAction                准备中或 Running 进入本代次追加；轮结束后开新代次并 reopen
├── InjectContextAction              进入运行时注入通道（可带 request-context）
├── CancelAction                     取消当前执行或准备；Completed 空闲窗且无句柄时基本空操作
├── CancelSubagentAction             按子任务执行 id 发出 Abort
└── 其他 Action                      当前分支记录不支持警告并忽略
```

Action 在枚举或分类函数中出现，不代表完整请求入口和运行时入口都已实现其语义。请求上下文取消与身份、准备期续接、收尾接纳、自动压缩失败不替换历史、可见输出后不重试，以及重叠订阅取消归属，已有对应回归覆盖；内存队列上限、进程重启收敛旧 running、真实重连压力仍未覆盖。

外部 `/byok/v1/*` 是独立入口：先检查开关和 API Key，再选择原生同协议转发或通用 Provider 转换；它直接调用模型流，不经过 Cursor Transport、ConversationRuntime、RunEngine 工具循环或 Cursor 检查点。

## 最终核心

```text
Cursor 上行
  → 本机代理白名单
  → api/cursor 模型选路
  → Transport(request_id)：接纳 + 输入排序
  → 通道 ConversationRuntime：当前运行代次
  → Compile：历史/上下文/模型 → PreparedRun
  → ConversationRegistry(conversation_id)：活跃句柄 + pending
  → RunEngine(RunId)：占用 → 模型周期 ↔ 工具轮 / 消息追加 / 压缩
      ├── ProviderRouter → 内置 HTTP Provider / Deno 插件
      ├── Store → SQLite：规范消息、工具轮、检查点和调用记录
      └── ConversationOutput → ToolDispatcher → Cursor / 本地工具
  → CheckpointWorker → Blob SET/ACK → 提交屏障
  → ConversationOutput → Transport OutputHub
  → RunSSE → Cursor 下行

三条边界
request_id      负责一条通道及其 actor
conversation_id 负责持久化会话、活跃句柄协调和待处理消息
RunId           负责一次执行；Insert/Break 续用，下一次执行重新创建
```
