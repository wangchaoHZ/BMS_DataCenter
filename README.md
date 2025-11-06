# BMS_DataCenter

BMS_DataCenter 是一个面向工业/能源场景的轻量级数据采集与时序存储服务，支持通过配置 Excel 文件批量采集多种支持 Modbus TCP 接口的设备数据，并写入 InfluxDB。系统提供简单的 Web/API 接口实现采集启停、配置热更新、数据导出与历史查询。

---

## ✨ 核心特性

- 📦 Excel 配置驱动采集：无需改代码，直接修改 `config.xlsx` 即可新增/调整采集点位。
- 🔄 采集任务自动批处理：同 IP/端口/Slave/功能码的寄存器自动聚合连续地址，减少网络轮询次数。
- 🕒 周期轮询：按 `request_cycle` 秒为周期轮询所有配置点位。
- 💾 InfluxDB 时序存储：每个变量作为一个字段记录到统一 measurement 中，可按变量标签查询。
- 🔐 支持多数据类型解析：`int16` / `uint16` / `int32` / `uint32` / `float32` / `float64`
- 🧮 Decimal 精度处理：整数类型支持按小数位缩放，浮点类型支持四舍五入。
- 🚀 采集热重载：上传新的 Excel 配置后自动停止并重启采集。
- 📤 历史数据导出：将指定时间范围数据导出为 Excel 文件。
- 🔍 单变量时间点附近查询：便于定位某个时刻的真实值。
- 📊 简易数据库占用统计：快速查看本地存储目录大小（用于未来扩展）。
- 🌐 CORS 支持：所有 API 默认允许跨域调用。
- 📝 滚动日志：每天切分日志文件，保留一定天数。

---

## 🗂 项目结构（核心文件）

```
main.go           主程序入口（采集 + API）
sysconfig.json    系统运行配置（启动时加载）
config.xlsx       采集点位配置（Excel，可热更新）
build.ps1         Windows/PowerShell 构建脚本
web/              前端静态页面目录（index.html 等）
logs/             运行产生的日志（自动生成）
```

---

## ⚙️ 系统配置 (`sysconfig.json`)

示例字段说明：

| 字段 | 说明 |
|------|------|
| server_ip | Web 服务监听 IP |
| server_port | Web 服务监听端口 |
| influx_url | InfluxDB 服务地址 |
| influx_token | InfluxDB 访问 Token |
| influx_org | InfluxDB 组织 |
| influx_bucket | 写入的 Bucket 名 |
| influx_measurement | 原始采集写入的 measurement 名称 |
| request_cycle | 轮询采集周期（秒） |
| downsampled_measurement | （预留）降采样写入 measurement |
| downsample_step_minutes | （预留）降采样步长（分钟） |
| raw_keep_days | （预留）原始数据保留天数 |
| downsample_keep_days | （预留）降采样数据保留天数 |
| config_excel | Excel 配置文件路径（默认 config.xlsx） |
| modbus_max_regs | 单次批量读取最大寄存器数量（拆分长区段） |
| db_path | 本地数据目录（目前用于尺寸统计等扩展） |

> 修改 `sysconfig.json` 后，通过配置 API 或重启服务生效（代码中含热重载逻辑）。

---

## 📑 采集配置 Excel (`config.xlsx`)

Excel 使用 `Sheet1`，首行为表头，后续每行一个点位。列含义：

| 列序 | 列名 | 说明 |
|------|------|------|
| A | VarName | 变量内部名称（写入 Influx 的标签 var） |
| B | Label | 展示用名称（导出 Excel 表头优先使用） |
| C | SlaveID | Modbus 从站地址（整数） |
| D | IP | 设备 IP |
| E | Port | 设备端口 |
| F | FuncCode | 功能码：`03`/`HoldingReg`、`04`/`InputReg`、`01`/`Coil`、`02`/`DiscreteInput` |
| G | Address | 起始寄存器地址 |
| H | Quantity | 寄存器数量（仅决定跳过长度，用于连续批处理） |
| I | DataType | 数据类型：`int16` / `uint16` / `int32` / `uint32` / `float32` / `float64` |
| J | Decimal | 小数位控制：整数类型按 10^Decimal 缩放，浮点类型四舍五入（范围 0~4） |

示例首行（表头）：
```
VarName | Label | SlaveID | IP | Port | FuncCode | Address | Quantity | DataType | Decimal
```

### Decimal 处理规则
- 整数型：实际值 = 原始值 / 10^Decimal（不再次四舍五入）
- 浮点型：按 Decimal 位四舍五入
- 限制：小于 0 自动归零，大于 4 上限为 4

---

## 🧠 采集逻辑概述

1. 启动加载 `sysconfig.json` 和 `config.xlsx`
2. 将 Excel 中同 (IP, Port, SlaveID, FuncCode) 且地址连续的配置自动合并成批任务，减少多次请求
3. 每批任务按 `modbus_max_regs` 拆分多次读取，拼接结果
4. 按变量定义的数据类型与 Decimal 处理值
5. 每个变量写入 InfluxDB：
    - measurement：`influx_measurement`
    - tag：`var=VarName`
    - field：`value=<数值>`
6. 周期控制：一次完成后 sleep = request_cycle - 本轮耗时
7. 可通过上传新 Excel 或修改系统配置触发重载

---

## 📡 API 接口

> 监听地址：`http://{server_ip}:{server_port}`

| 接口 | 方法 | 说明 | 参数示例 |
|------|------|------|---------|
| `/start` | GET | 启动采集（如果未在运行） | 无 |
| `/stop` | GET | 停止采集 | 无 |
| `/collect-status` | GET | 查询采集是否在运行 | 无 |
| `/export` | GET | 导出时间范围数据为 Excel | `start=2025-01-01 00:00:00&end=2025-01-01 01:00:00` 支持 `YYYY-MM-DD HH:MM:SS`、`YYYYMMDDHHMMSS`、RFC3339 |
| `/query` | GET | 查询某变量指定时间点附近 ±30 秒内的所有值 | `var=Temp01&ts=2025-01-01 12:00:00` |
| `/dbsize` | GET | 获取本地数据目录大小（用于后续扩展） | 无 |
| `/upload-config` | POST | 上传新的 Excel 配置并热重启采集 | `form-data: file=<config.xlsx>` |
| `/sysconfig` | GET/POST | 读取/更新系统配置（代码里已实现 handler，需确认是否在路由注册中添加） | POST body 为完整 JSON |

> 注意：当前 `main.go` 中实际注册的路由为：`/start /stop /export /query /dbsize /collect-status`，日志提示中列出了 `/sysconfig /upload-config`，如需使用请确认在启动时添加路由注册。

---

## 🔐 时间参数支持格式

解析函数支持以下格式（自动识别）：
- `YYYY-MM-DD HH:MM:SS` （本地时区）
- `YYYYMMDDHHMMSS`
- RFC3339：`2025-01-01T00:00:00Z`

---

## 📤 数据导出形式

导出 Excel：
- 第一列：`time`（格式：`YYYY-MM-DD HH:MM:SS`）
- 后续列：按配置顺序使用 `Label`（为空则用 `VarName`）
- 行：时间戳对应的变量值集合（按 Influx 记录聚合）

文件命名：`export_{start}_{end}.xlsx` （时间格式：`YYYYMMDD_HHMMSS`）

---

## 🛠 构建与运行

### 环境要求
- Go 1.20+（参考 go.mod）
- 可访问的 InfluxDB 实例（具体参考: https://docs.influxdata.com/influxdb/v2/get-started/）
- 设备 Modbus TCP 网络互通

### 快速开始

```bash
# 克隆仓库
git clone https://github.com/wangchaoHZ/BMS_DataCenter.git
cd BMS_DataCenter

# 编辑 sysconfig.json 与 config.xlsx

# 直接构建
go build -o bms_datacenter .

# 或使用 PowerShell 脚本（Windows）
pwsh ./build.ps1

# 运行
./bms_datacenter
```

启动后访问：
```
http://{server_ip}:{server_port}/
```

---

## 🧪 典型调用示例

```bash
# 启动采集
curl http://127.0.0.1:8080/start

# 查询采集状态
curl http://127.0.0.1:8080/collect-status

# 查询变量指定时刻附近值
curl "http://127.0.0.1:8080/query?var=Temp01&ts=2025-01-01 12:00:00"

# 导出一小时数据
curl -OJ "http://127.0.0.1:8080/export?start=2025-01-01 00:00:00&end=2025-01-01 01:00:00"

# 上传新配置（热重载）
curl -F "file=@config.xlsx" http://127.0.0.1:8080/upload-config
```

---

## 🧩 扩展点建议

- 增加降采样与数据清理任务（利用现有 `downsample_*` 字段）
- 增加设备在线状态心跳与告警
- 支持更多协议（如 OPC UA / MQTT）
- 前端展示实时曲线与导出进度
- 增加批量写入优化（当前逐点写，可按行统一写入加速）
- 增加认证与访问控制（目前开放 CORS + 无鉴权）

---

## 🛡 稳定性与错误处理

- 连接/读取失败会按任务内所有变量更新 LastError，并重试（1 秒间隔）
- 每个变量成功解析后更新 LastSuccess，并清空错误计数
- 采集失败不会阻断其它批任务
- Excel 行不足/格式错误自动跳过并记录日志
- Decimal 超限自动钳制，避免异常精度

---

## 🗒 日志

- 日志文件：`./logs/app.YYYY-MM-DD.log`
- 每日滚动，保留最长（配置中使用 `WithMaxAge` 控制）
- 默认包含短文件路径与时间戳，便于快速排查

---

## ❓常见问题

| 问题 | 排查建议 |
|------|----------|
| 变量无数据 | 确认设备在线、功能码正确、Address 与 Quantity 正确 |
| 导出为空 | 时间区间是否正确、变量是否在该时段有写入 |
| 浮点精度不符 | 检查 Decimal 设置（浮点四舍五入 / 整数缩放） |
| 上传 Excel 不生效 | 确认路由是否已注册 `/upload-config` |
| 查询报时间格式错误 | 使用支持的三种格式之一 |

---

## ✅ 结语

欢迎根据现场需求进行定制扩展。如果你在使用过程中遇到问题或希望新增功能，可以在仓库提交 Issue 或直接改造。
