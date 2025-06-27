package main

import (
	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

const (
	InfluxURL              = "http://localhost:8086"
	InfluxToken            = "your-influxdb-token"
	InfluxOrg              = "your-org"
	InfluxBucket           = "modbus_data"
	DownsampledMeasurement = "modbus_data_downsampled"
	DownsampleStepMinutes  = 1              // 步长（分钟）
	RawKeepDays            = 30             // 高频数据保留天数
	DownsampleKeepDays     = 200            // 降采样数据保留天数
	CleanPeriod            = 24 * time.Hour // 每隔多久执行一次清理
)

func main() {
	client := influxdb2.NewClient(InfluxURL, InfluxToken)
	defer client.Close()
	for {
		fmt.Println("开始定期清理和降采样...")
		cleanAndDownsample(client)
		fmt.Printf("清理和降采样完成，等待%v后再次执行...\n", CleanPeriod)
		time.Sleep(CleanPeriod)
	}
}

func cleanAndDownsample(client influxdb2.Client) {
	now := time.Now()
	rawStart := now.Add(-time.Duration(DownsampleKeepDays) * 24 * time.Hour)
	rawEnd := now.Add(-time.Duration(RawKeepDays) * 24 * time.Hour)

	// 1. 降采样：将rawStart到rawEnd的数据以每N分钟采样到新measurement
	downsampleFlux := fmt.Sprintf(`
from(bucket: "%s")
|> range(start: %s, stop: %s)
|> filter(fn: (r) => r._measurement == "modbus_data")
|> aggregateWindow(every: %dm, fn: first)
|> to(bucket: "%s", org: "%s", measurement: "%s")
`, InfluxBucket, rawStart.Format(time.RFC3339), rawEnd.Format(time.RFC3339), DownsampleStepMinutes, InfluxBucket, InfluxOrg, DownsampledMeasurement)
	queryAPI := client.QueryAPI(InfluxOrg)
	_, err := queryAPI.Query(context.Background(), downsampleFlux)
	if err != nil {
		fmt.Println("降采样写入失败:", err)
	} else {
		fmt.Println("降采样写入完成")
	}

	// 2. 删除原始measurement中过了rawKeepDays的数据（一个月外但还没downsampleKeepDays那么久）
	deleteFlux := fmt.Sprintf(`
import "influxdata/influxdb/v1"
v1.delete(
  bucket: "%s",
  predicate: (r) => r._measurement == "modbus_data",
  start: %s,
  stop: %s
)
`, InfluxBucket, rawEnd.Format(time.RFC3339), rawStart.Format(time.RFC3339))
	_, err = queryAPI.Query(context.Background(), deleteFlux)
	if err != nil {
		fmt.Println("删除原始高频数据失败:", err)
	} else {
		fmt.Println("原始高频数据已删除")
	}

	// 3. 删除所有超过200天的数据（包括原始和降采样measurement）
	oldDeleteFlux := fmt.Sprintf(`
import "influxdata/influxdb/v1"
v1.delete(
  bucket: "%s",
  predicate: (r) => (r._measurement == "modbus_data" or r._measurement == "%s"),
  start: 0,
  stop: %s
)
`, InfluxBucket, DownsampledMeasurement, rawStart.Format(time.RFC3339))
	_, err = queryAPI.Query(context.Background(), oldDeleteFlux)
	if err != nil {
		fmt.Println("删除超200天数据失败:", err)
	} else {
		fmt.Println("超200天数据已删除")
	}
}
