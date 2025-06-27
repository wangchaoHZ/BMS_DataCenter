// 新增结构体
type BatchTask struct {
	GroupKey string       // ip:port:slaveid:func:dataType
	IP       string
	Port     string
	SlaveID  byte
	FuncCode string
	DataType string
	StartAddr uint16
	TotalQty  uint16
	Items    []Config     // 参与批量读取的变量配置
}

// 分组，将configs分为多个BatchTask
func groupConfigsForBatch(configs []Config) []BatchTask {
	groupMap := make(map[string][]Config)
	for _, c := range configs {
		key := fmt.Sprintf("%s:%s:%d:%s:%s", c.IP, c.Port, c.SlaveID, c.FuncCode, c.DataType)
		groupMap[key] = append(groupMap[key], c)
	}

	var tasks []BatchTask
	for key, group := range groupMap {
		// 按地址排序
		sort.Slice(group, func(i, j int) bool { return group[i].Address < group[j].Address })
		// 连续区间拆分
		i := 0
		for i < len(group) {
			start := i
			end := i
			lastAddr := group[i].Address + group[i].Quantity
			for end+1 < len(group) &&
				group[end+1].Address == lastAddr &&
				group[end+1].Quantity == group[end].Quantity {
				lastAddr += group[end+1].Quantity
				end++
			}
			batch := BatchTask{
				GroupKey: key,
				IP:       group[i].IP,
				Port:     group[i].Port,
				SlaveID:  group[i].SlaveID,
				FuncCode: group[i].FuncCode,
				DataType: group[i].DataType,
				StartAddr: group[start].Address,
				TotalQty:  lastAddr - group[start].Address,
				Items:    group[start : end+1],
			}
			tasks = append(tasks, batch)
			i = end + 1
		}
	}
	return tasks
}

// 批量采集协程
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
			return
		default:
			if err := handler.Connect(); err != nil {
				for _, cfg := range batch.Items {
					statusMap[cfg.VarName].LastError = err
					statusMap[cfg.VarName].RetryCount = retry
				}
				retry++
				time.Sleep(1 * time.Second)
				continue
			}

			var data []byte
			var err error
			switch batch.FuncCode {
			case "HoldingReg", "03":
				data, err = client.ReadHoldingRegisters(batch.StartAddr, batch.TotalQty)
			case "InputReg", "04":
				data, err = client.ReadInputRegisters(batch.StartAddr, batch.TotalQty)
			case "Coil", "01":
				data, err = client.ReadCoils(batch.StartAddr, batch.TotalQty)
			case "DiscreteInput", "02":
				data, err = client.ReadDiscreteInputs(batch.StartAddr, batch.TotalQty)
			default:
				for _, cfg := range batch.Items {
					statusMap[cfg.VarName].LastError = fmt.Errorf("unsupported FuncCode: %s", batch.FuncCode)
				}
				continue
			}
			if err != nil {
				for _, cfg := range batch.Items {
					statusMap[cfg.VarName].LastError = err
					statusMap[cfg.VarName].RetryCount = retry
				}
				retry++
				handler.Close()
				time.Sleep(1 * time.Second)
				continue
			}

			// 逐变量切片数据
			offset := 0
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
					continue
				}
				if offset+byteLen > len(data) {
					statusMap[cfg.VarName].LastError = fmt.Errorf("response too short: got %d, need %d", len(data), offset+byteLen)
					continue
				}
				value, err := parseRegisterData(data[offset:offset+byteLen], cfg.DataType)
				if err != nil {
					statusMap[cfg.VarName].LastError = err
				} else {
					statusMap[cfg.VarName].LastSuccess = time.Now()
					statusMap[cfg.VarName].LastError = nil
					statusMap[cfg.VarName].RetryCount = 0
					varMap.Store(cfg.VarName, value)
				}
				offset += byteLen
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}