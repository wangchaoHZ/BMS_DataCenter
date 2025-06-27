package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/goburrow/modbus"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/xuri/excelize/v2"
)

const (
	SysConfigFile = "sysconfig.json"
)

type SysConfig struct {
	ServerIP               string `json:"server_ip"`
	ServerPort             int    `json:"server_port"`
	InfluxURL              string `json:"influx_url"`
	InfluxToken            string `json:"influx_token"`
	InfluxOrg              string `json:"influx_org"`
	InfluxBucket           string `json:"influx_bucket"`
	InfluxMeasurement      string `json:"influx_measurement"`
	WriteCycle             int    `json:"write_cycle"`
	DownsampledMeasurement string `json:"downsampled_measurement"`
	DownsampleStepMinutes  int    `json:"downsample_step_minutes"`
	RawKeepDays            int    `json:"raw_keep_days"`
	DownsampleKeepDays     int    `json:"downsample_keep_days"`
	ConfigExcel            string `json:"config_excel"`
	ModbusMaxRegs          int    `json:"modbus_max_regs"`
}

var sysConfig SysConfig

type Config struct {
	VarName  string
	Label    string // 新增Label字段
	SlaveID  byte
	IP       string
	Port     string
	FuncCode string
	Address  uint16
	Quantity uint16
	DataType string
	Decimal  int
}

type TaskStatus struct {
	LastSuccess time.Time
	LastError   error
	RetryCount  int
	Addr        string
}

type BatchTask struct {
	GroupKey  string
	IP        string
	Port      string
	SlaveID   byte
	FuncCode  string
	StartAddr uint16
	TotalQty  uint16
	Items     []Config // 已按Address排序
}

var (
	varMap        sync.Map
	influxW       *InfluxWriter
	collectWg     sync.WaitGroup
	collectCtx    context.Context
	collectCancel context.CancelFunc
	collectLock   sync.Mutex
	collecting    bool
	configs       []Config
	statusMap     map[string]*TaskStatus
)

// ========== 工具函数 ==========

func LoadSysConfig(filename string) (*SysConfig, error) {
	log.Printf("Loading system config from %s", filename)
	b, err := os.ReadFile(filename)
	if err != nil {
		log.Printf("Failed to read config file: %v", err)
		return nil, err
	}
	var cfg SysConfig
	err = json.Unmarshal(b, &cfg)
	if err != nil {
		log.Printf("Failed to unmarshal config: %v", err)
	}
	return &cfg, err
}

func SaveSysConfig(filename string, cfg *SysConfig) error {
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal config: %v", err)
		return err
	}
	err = os.WriteFile(filename, b, 0644)
	if err != nil {
		log.Printf("Failed to write config file: %v", err)
	}
	return err
}

func loadGlobalConfig(cfg *SysConfig) {
	log.Printf("Loading global config: %+v", *cfg)
	sysConfig = *cfg
}

// parseExcelConfig 支持Label新字段
func parseExcelConfig(filename string) ([]Config, error) {
	log.Printf("Parsing Excel config: %s", filename)
	f, err := excelize.OpenFile(filename)
	if err != nil {
		log.Printf("Failed to open Excel file: %v", err)
		return nil, err
	}
	rows, err := f.GetRows("Sheet1")
	if err != nil {
		log.Printf("Failed to get rows from Excel: %v", err)
		return nil, err
	}
	var configs []Config
	for i, row := range rows {
		if i == 0 {
			continue
		}
		// 为了兼容原有行为，最低8列，但Label在第二列，所以要>=9列
		if len(row) < 9 {
			log.Printf("Row %d too short: %v", i, row)
			continue
		}
		slaveID, _ := strconv.Atoi(row[2])
		address, _ := strconv.Atoi(row[6])
		quantity, _ := strconv.Atoi(row[7])
		decimal := 0
		if len(row) >= 10 {
			decimal, _ = strconv.Atoi(row[9])
		}
		if decimal < 0 {
			decimal = 0
		}
		if decimal > 4 {
			decimal = 4
		}
		cfg := Config{
			VarName:  row[0],
			Label:    row[1],
			SlaveID:  byte(slaveID),
			IP:       row[3],
			Port:     row[4],
			FuncCode: row[5],
			Address:  uint16(address),
			Quantity: uint16(quantity),
			DataType: row[8],
			Decimal:  decimal,
		}
		configs = append(configs, cfg)
	}
	log.Printf("Parsed %d configs from Excel", len(configs))
	return configs, nil
}

func groupConfigsForBatch(configs []Config) []BatchTask {
	groupMap := make(map[string][]Config)
	for _, c := range configs {
		key := fmt.Sprintf("%s:%s:%d:%s", c.IP, c.Port, c.SlaveID, c.FuncCode)
		groupMap[key] = append(groupMap[key], c)
	}
	var tasks []BatchTask
	for key, group := range groupMap {
		sort.Slice(group, func(i, j int) bool { return group[i].Address < group[j].Address })
		i := 0
		for i < len(group) {
			start := i
			end := i
			lastAddr := group[i].Address + group[i].Quantity
			for end+1 < len(group) && group[end+1].Address == lastAddr {
				lastAddr += group[end+1].Quantity
				end++
			}
			batch := BatchTask{
				GroupKey:  key,
				IP:        group[i].IP,
				Port:      group[i].Port,
				SlaveID:   group[i].SlaveID,
				FuncCode:  group[i].FuncCode,
				StartAddr: group[start].Address,
				TotalQty:  lastAddr - group[start].Address,
				Items:     group[start : end+1],
			}
			tasks = append(tasks, batch)
			i = end + 1
		}
	}
	log.Printf("Grouped configs into %d batch tasks", len(tasks))
	return tasks
}

func parseRegisterData(data []byte, dtype string) (interface{}, error) {
	switch dtype {
	case "int16":
		if len(data) < 2 {
			return nil, fmt.Errorf("data too short for int16")
		}
		return int16(binary.BigEndian.Uint16(data[:2])), nil
	case "uint16":
		if len(data) < 2 {
			return nil, fmt.Errorf("data too short for uint16")
		}
		return binary.BigEndian.Uint16(data[:2]), nil
	case "int32":
		if len(data) < 4 {
			return nil, fmt.Errorf("data too short for int32")
		}
		return int32(binary.BigEndian.Uint32(data[:4])), nil
	case "uint32":
		if len(data) < 4 {
			return nil, fmt.Errorf("data too short for uint32")
		}
		return binary.BigEndian.Uint32(data[:4]), nil
	case "float32":
		if len(data) < 4 {
			return nil, fmt.Errorf("data too short for float32")
		}
		bits := binary.BigEndian.Uint32(data[:4])
		return float32frombits(bits), nil
	case "float64":
		if len(data) < 8 {
			return nil, fmt.Errorf("data too short for float64")
		}
		bits := binary.BigEndian.Uint64(data[:8])
		return float64frombits(bits), nil
	default:
		return nil, fmt.Errorf("unsupported DataType: %s", dtype)
	}
}
func float32frombits(bits uint32) float32 { return *(*float32)(unsafe.Pointer(&bits)) }
func float64frombits(bits uint64) float64 { return *(*float64)(unsafe.Pointer(&bits)) }

func toFloat64(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int16:
		return float64(t)
	case uint16:
		return float64(t)
	case int32:
		return float64(t)
	case uint32:
		return float64(t)
	default:
		return 0
	}
}

func roundFloat(val float64, dec int) float64 {
	pow := math.Pow10(dec)
	return math.Round(val*pow) / pow
}

// Decimal规则处理：见下
func processValueForDecimal(raw interface{}, dtype string, decimal int) float64 {
	if decimal < 0 {
		decimal = 0
	}
	if decimal > 4 {
		decimal = 4
	}
	switch dtype {
	case "int16", "uint16", "int32", "uint32":
		val := toFloat64(raw)
		if decimal > 0 {
			val = val / math.Pow10(decimal)
		}
		return val // 不做四舍五入
	case "float32", "float64":
		val := toFloat64(raw)
		return roundFloat(val, decimal)
	default:
		return 0
	}
}

// ========== 采集核心 ==========

func runBatchTask(ctx context.Context, batch BatchTask, statusMap map[string]*TaskStatus, wg *sync.WaitGroup) {
	defer wg.Done()
	handler := modbus.NewTCPClientHandler(fmt.Sprintf("%s:%s", batch.IP, batch.Port))
	handler.Timeout = 3 * time.Second
	handler.SlaveId = batch.SlaveID
	client := modbus.NewClient(handler)
	retry := 0

	for {
		select {
		case <-ctx.Done():
			handler.Close()
			log.Printf("Batch task %s stopped", batch.GroupKey)
			return
		default:
			if err := handler.Connect(); err != nil {
				for _, cfg := range batch.Items {
					statusMap[cfg.VarName].LastError = err
					statusMap[cfg.VarName].RetryCount = retry
				}
				log.Printf("MODBUS connect error: %v", err)
				retry++
				time.Sleep(1 * time.Second)
				continue
			}
			totalBytes := int(batch.TotalQty) * 2
			buffer := make([]byte, totalBytes)
			addr := batch.StartAddr
			remain := batch.TotalQty
			offset := 0
			var err error
			for remain > 0 {
				qty := remain
				if qty > uint16(sysConfig.ModbusMaxRegs) {
					qty = uint16(sysConfig.ModbusMaxRegs)
				}
				log.Printf("[MODBUS] Read %s:%s slave=%d func=%s addr=%d qty=%d",
					batch.IP, batch.Port, batch.SlaveID, batch.FuncCode, addr, qty)
				var data []byte
				switch batch.FuncCode {
				case "HoldingReg", "03":
					data, err = client.ReadHoldingRegisters(addr, qty)
				case "InputReg", "04":
					data, err = client.ReadInputRegisters(addr, qty)
				case "Coil", "01":
					data, err = client.ReadCoils(addr, qty)
				case "DiscreteInput", "02":
					data, err = client.ReadDiscreteInputs(addr, qty)
				default:
					for _, cfg := range batch.Items {
						statusMap[cfg.VarName].LastError = fmt.Errorf("unsupported FuncCode: %s", batch.FuncCode)
					}
					log.Printf("Unsupported FuncCode: %s", batch.FuncCode)
					break
				}
				if err != nil {
					for _, cfg := range batch.Items {
						statusMap[cfg.VarName].LastError = err
						statusMap[cfg.VarName].RetryCount = retry
					}
					handler.Close()
					log.Printf("MODBUS read error: %v", err)
					retry++
					time.Sleep(1 * time.Second)
					continue
				}
				copy(buffer[offset:offset+len(data)], data)
				offset += len(data)
				addr += qty
				remain -= qty
			}
			curReg := 0
			for _, cfg := range batch.Items {
				byteLen := 0
				switch cfg.DataType {
				case "int16", "uint16":
					byteLen = 2
				case "int32", "uint32", "float32":
					byteLen = 4
				case "float64":
					byteLen = 8
				default:
					statusMap[cfg.VarName].LastError = fmt.Errorf("unsupported data type")
					curReg += int(cfg.Quantity)
					log.Printf("Unsupported data type: %s", cfg.DataType)
					continue
				}
				byteStart := curReg * 2
				byteEnd := byteStart + byteLen
				if byteEnd > len(buffer) {
					statusMap[cfg.VarName].LastError = fmt.Errorf("response too short: got %d, need %d", len(buffer), byteEnd)
					curReg += int(cfg.Quantity)
					log.Printf("MODBUS response too short for %s", cfg.VarName)
					continue
				}
				value, err := parseRegisterData(buffer[byteStart:byteEnd], cfg.DataType)
				if err != nil {
					statusMap[cfg.VarName].LastError = err
					log.Printf("parseRegisterData error for %s: %v", cfg.VarName, err)
				} else {
					statusMap[cfg.VarName].LastSuccess = time.Now()
					statusMap[cfg.VarName].LastError = nil
					statusMap[cfg.VarName].RetryCount = 0
					// ========== Decimal规则在这里应用 ==========
					v := processValueForDecimal(value, cfg.DataType, cfg.Decimal)
					varMap.Store(cfg.VarName, v)
				}
				curReg += int(cfg.Quantity)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// ========== Influx写入 ==========

type InfluxWriter struct {
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
}

func NewInfluxWriter() *InfluxWriter {
	log.Printf("Creating InfluxWriter: url=%s, org=%s, bucket=%s", sysConfig.InfluxURL, sysConfig.InfluxOrg, sysConfig.InfluxBucket)
	client := influxdb2.NewClient(sysConfig.InfluxURL, sysConfig.InfluxToken)
	writeAPI := client.WriteAPIBlocking(sysConfig.InfluxOrg, sysConfig.InfluxBucket)
	return &InfluxWriter{client: client, writeAPI: writeAPI}
}

func (iw *InfluxWriter) WriteVars(vars map[string]interface{}, t time.Time) error {
	for k, v := range vars {
		val := toFloat64(v)
		point := influxdb2.NewPoint(
			sysConfig.InfluxMeasurement,
			map[string]string{"var": k},
			map[string]interface{}{"value": val},
			t.UTC(),
		)
		if err := iw.writeAPI.WritePoint(context.Background(), point); err != nil {
			log.Printf("Influx write error: %v", err)
			return err
		}
	}
	return nil
}

func (iw *InfluxWriter) Close() {
	iw.client.Close()
	log.Printf("Closed InfluxWriter")
}

// ========== 采集控制/接口 ==========

func startCollectHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("startCollectHandler called")
	collectLock.Lock()
	defer collectLock.Unlock()
	if collecting {
		log.Printf("Already collecting")
		writeJSON(w, http.StatusOK, map[string]interface{}{"status": "already collecting"})
		return
	}
	cfgs, err := parseExcelConfig(sysConfig.ConfigExcel)
	if err != nil {
		log.Printf("parseExcelConfig failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": "parse config failed", "detail": err.Error()})
		return
	}
	configs = cfgs
	statusMap = make(map[string]*TaskStatus)
	for _, cfg := range configs {
		statusMap[cfg.VarName] = &TaskStatus{Addr: fmt.Sprintf("%s:%s[%d]", cfg.IP, cfg.Port, cfg.SlaveID)}
	}
	varMap = sync.Map{}
	collectCtx, collectCancel = context.WithCancel(context.Background())
	batches := groupConfigsForBatch(configs)
	for _, batch := range batches {
		collectWg.Add(1)
		go runBatchTask(collectCtx, batch, statusMap, &collectWg)
	}
	if influxW != nil {
		influxW.Close()
	}
	influxW = NewInfluxWriter()
	go func(ctx context.Context) {
		ticker := time.NewTicker(time.Duration(sysConfig.WriteCycle) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case t := <-ticker.C:
				row := make(map[string]interface{})
				varMap.Range(func(k, v interface{}) bool {
					row[k.(string)] = v
					return true
				})
				if len(row) > 0 {
					err := influxW.WriteVars(row, t)
					if err != nil {
						log.Println("写入InfluxDB失败:", err)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}(collectCtx)
	collecting = true
	log.Printf("采集已启动")
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "started"})
}

func stopCollectHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("stopCollectHandler called")
	collectLock.Lock()
	defer collectLock.Unlock()
	if !collecting {
		log.Printf("Not running")
		writeJSON(w, http.StatusOK, map[string]interface{}{"status": "not running"})
		return
	}
	collectCancel()
	collectWg.Wait()
	collecting = false
	log.Printf("采集已停止")
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "stopped"})
}

// ========== 数据导出接口 ==========

func exportHandler(w http.ResponseWriter, r *http.Request) {
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	log.Printf("exportHandler called, start=%s, end=%s", startStr, endStr)
	if startStr == "" || endStr == "" {
		http.Error(w, "start/end required", 400)
		return
	}
	start, err := parseFlexibleTime(startStr)
	if err != nil {
		log.Printf("parseFlexibleTime(start) failed: %v", err)
		http.Error(w, "bad start format", 400)
		return
	}
	end, err := parseFlexibleTime(endStr)
	if err != nil {
		log.Printf("parseFlexibleTime(end) failed: %v", err)
		http.Error(w, "bad end format", 400)
		return
	}
	client := influxdb2.NewClient(sysConfig.InfluxURL, sysConfig.InfluxToken)
	defer client.Close()
	q := fmt.Sprintf(
		`from(bucket: "%s") |> range(start: %s, stop: %s) |> filter(fn: (r) => r._measurement == "%s")`,
		sysConfig.InfluxBucket,
		start.Format(time.RFC3339),
		end.Format(time.RFC3339),
		sysConfig.InfluxMeasurement)
	log.Printf("Influx query: %s", q)
	result, err := client.QueryAPI(sysConfig.InfluxOrg).Query(r.Context(), q)
	if err != nil {
		log.Printf("Influx query failed: %v", err)
		http.Error(w, "query failed: "+err.Error(), 500)
		return
	}

	rows := map[string]map[string]interface{}{}
	for result.Next() {
		t := result.Record().Time().In(time.Local).Format("2006-01-02 15:04:05")
		v := fmt.Sprintf("%v", result.Record().ValueByKey("var"))
		val := result.Record().Value()
		if rows[t] == nil {
			rows[t] = map[string]interface{}{}
		}
		rows[t][v] = val
	}
	if result.Err() != nil {
		log.Printf("Influx result error: %v", result.Err())
		http.Error(w, "result error: "+result.Err().Error(), 500)
		return
	}
	// 时间排序
	var times []string
	for t := range rows {
		times = append(times, t)
	}
	sort.Strings(times)

	f := excelize.NewFile()
	sheet := "Sheet1"
	f.SetSheetName(f.GetSheetName(0), sheet)
	// 设置第一列宽度为20
	f.SetColWidth(sheet, "A", "A", 20)
	// 冻结第一行
	f.SetPanes(sheet, &excelize.Panes{
		Freeze:      true,
		Split:       false,
		XSplit:      0,
		YSplit:      1,
		TopLeftCell: "A2",
		ActivePane:  "bottomLeft",
	})
	// 写表头（用Label字段）
	f.SetCellValue(sheet, "A1", "time")
	for i, cfg := range configs {
		col, _ := excelize.ColumnNumberToName(i + 2)
		label := cfg.Label
		if label == "" {
			label = cfg.VarName
		}
		f.SetCellValue(sheet, fmt.Sprintf("%s1", col), label)
	}

	// 数据部分
	for i, t := range times {
		f.SetCellValue(sheet, fmt.Sprintf("A%d", i+2), t)
		for j, cfg := range configs {
			col, _ := excelize.ColumnNumberToName(j + 2)
			val := rows[t][cfg.VarName]
			f.SetCellValue(sheet, fmt.Sprintf("%s%d", col, i+2), val)
		}
	}
	var buf bytes.Buffer
	if err := f.Write(&buf); err != nil {
		log.Printf("Excel write failed: %v", err)
		http.Error(w, "excel write failed: "+err.Error(), 500)
		return
	}
	fname := fmt.Sprintf("export_%s_%s.xlsx", start.Format("20060102_150405"), end.Format("20060102_150405"))
	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+fname+"\"")
	_, err = w.Write(buf.Bytes())
	if err != nil {
		log.Printf("Failed to write response: %v", err)
	}
}

// ========== 数据查询接口 ==========

func queryHandler(w http.ResponseWriter, r *http.Request) {
	varName := r.URL.Query().Get("var")
	tsStr := r.URL.Query().Get("ts")
	log.Printf("queryHandler called, var=%s ts=%s", varName, tsStr)
	if varName == "" || tsStr == "" {
		http.Error(w, "var/ts required", 400)
		return
	}
	ts, err := parseFlexibleTime(tsStr)
	if err != nil {
		log.Printf("parseFlexibleTime error: %v", err)
		http.Error(w, "bad ts format", 400)
		return
	}
	start := ts.Add(-30 * time.Second)
	end := ts.Add(30 * time.Second)
	q := fmt.Sprintf(
		`from(bucket: "%s") |> range(start: %s, stop: %s) |> filter(fn: (r) => r._measurement == "%s" and r.var == %q)`,
		sysConfig.InfluxBucket,
		start.Format(time.RFC3339),
		end.Format(time.RFC3339),
		sysConfig.InfluxMeasurement, varName)
	log.Printf("Influx query: %s", q)
	client := influxdb2.NewClient(sysConfig.InfluxURL, sysConfig.InfluxToken)
	defer client.Close()
	result, err := client.QueryAPI(sysConfig.InfluxOrg).Query(r.Context(), q)
	if err != nil {
		log.Printf("Influx QueryAPI failed: %v", err)
		http.Error(w, "query failed: "+err.Error(), 500)
		return
	}
	var rows []map[string]interface{}
	for result.Next() {
		rows = append(rows, map[string]interface{}{
			"ts":    result.Record().Time().In(time.Local).Format("2006-01-02 15:04:05"),
			"value": result.Record().Value(),
		})
	}
	if result.Err() != nil {
		log.Printf("Influx result error: %v", result.Err())
		http.Error(w, "result error: "+result.Err().Error(), 500)
		return
	}
	log.Printf("queryHandler returned %d rows", len(rows))
	writeJSON(w, 200, rows)
}

// ========== 其它接口 ==========

func dbsizeHandler(w http.ResponseWriter, r *http.Request) {
	dbPath := "./influxdb_data"
	log.Printf("dbsizeHandler called, dbPath=%s", dbPath)
	var total int64
	filepath.Walk(dbPath, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	log.Printf("dbsizeHandler result: %.2f MB", float64(total)/1024.0/1024.0)
	writeJSON(w, 200, map[string]interface{}{
		"size": fmt.Sprintf("%.2f MB", float64(total)/1024.0/1024.0),
	})
}

func getSysConfigHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("getSysConfigHandler called")
	cfg, err := LoadSysConfig(SysConfigFile)
	if err != nil {
		log.Printf("getSysConfigHandler error: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, 200, cfg)
}
func postSysConfigHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("postSysConfigHandler called")
	var cfg SysConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		log.Printf("postSysConfigHandler decode error: %v", err)
		http.Error(w, err.Error(), 400)
		return
	}
	if err := SaveSysConfig(SysConfigFile, &cfg); err != nil {
		log.Printf("postSysConfigHandler save error: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, 200, map[string]string{"msg": "ok"})
}
func reloadConfigAndRestartHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("reloadConfigAndRestartHandler called")
	var cfg SysConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		log.Printf("reloadConfigAndRestartHandler decode error: %v", err)
		http.Error(w, "Invalid config", 400)
		return
	}
	if err := SaveSysConfig(SysConfigFile, &cfg); err != nil {
		log.Printf("reloadConfigAndRestartHandler save error: %v", err)
		http.Error(w, "Failed to save config", 500)
		return
	}
	loadGlobalConfig(&cfg)
	stopCollectHandler(w, r)
	startCollectHandler(w, r)
	writeJSON(w, 200, map[string]string{"msg": "采集服务已重启并按新配置运行"})
}

func parseFlexibleTime(str string) (time.Time, error) {
	log.Printf("parseFlexibleTime input: %s", str)
	if len(str) == 19 && strings.Index(str, "T") == -1 {
		t, err := time.ParseInLocation("2006-01-02 15:04:05", str, time.Local)
		log.Printf("parseFlexibleTime result: %v, err: %v", t, err)
		return t, err
	}
	if len(str) == 14 && strings.IndexAny(str, "-:T") == -1 {
		t, err := time.ParseInLocation("20060102150405", str, time.Local)
		log.Printf("parseFlexibleTime result: %v, err: %v", t, err)
		return t, err
	}
	t, err := time.Parse(time.RFC3339, str)
	log.Printf("parseFlexibleTime result: %v, err: %v", t, err)
	return t, err
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	err := json.NewEncoder(w).Encode(v)
	if err != nil {
		log.Printf("writeJSON encode error: %v", err)
	}
}

// ========== main ==========
func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	cfg, err := LoadSysConfig(SysConfigFile)
	if err != nil {
		log.Fatalf("启动时未能加载配置文件(%v): %v", SysConfigFile, err)
	}
	loadGlobalConfig(cfg)
	influxW = NewInfluxWriter()
	defer influxW.Close()
	configs, err = parseExcelConfig(sysConfig.ConfigExcel)
	if err != nil {
		log.Printf("警告：启动时未能加载Excel配置: %v", err)
		configs = []Config{}
	}
	statusMap = make(map[string]*TaskStatus)

	http.HandleFunc("/start", startCollectHandler)
	http.HandleFunc("/stop", stopCollectHandler)
	http.HandleFunc("/export", exportHandler)
	http.HandleFunc("/query", queryHandler)
	http.HandleFunc("/dbsize", dbsizeHandler)
	http.HandleFunc("/sysconfig", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getSysConfigHandler(w, r)
		case http.MethodPost:
			postSysConfigHandler(w, r)
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	http.HandleFunc("/reload-config", reloadConfigAndRestartHandler)

	fs := http.FileServer(http.Dir("./web"))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Join("./web", r.URL.Path)
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			fs.ServeHTTP(w, r)
		} else {
			http.ServeFile(w, r, "./web/index.html")
		}
	})

	addr := fmt.Sprintf("%s:%d", sysConfig.ServerIP, sysConfig.ServerPort)
	fmt.Printf("[INFO] 数据中心服务已启动，监听 %s\n", addr)
	fmt.Println("API: /start /stop /export /query /dbsize /sysconfig /reload-config")
	log.Fatal(http.ListenAndServe(addr, nil))
}
