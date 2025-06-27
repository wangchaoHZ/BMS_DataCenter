package main

import (
	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

// InfluxDB配置
const (
	InfluxURL    = "http://localhost:8086" // 修改为你的地址
	InfluxToken  = "your-influxdb-token"
	InfluxOrg    = "your-org"
	InfluxBucket = "modbus_data"
)

// InfluxDB写入器结构体
type InfluxWriter struct {
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
}

// 初始化InfluxDB写入器
func NewInfluxWriter() *InfluxWriter {
	client := influxdb2.NewClient(InfluxURL, InfluxToken)
	writeAPI := client.WriteAPIBlocking(InfluxOrg, InfluxBucket)
	return &InfluxWriter{
		client:   client,
		writeAPI: writeAPI,
	}
}

// 写入一条数据（变量名，值，时间戳）
func (iw *InfluxWriter) WriteVar(varName string, value interface{}, t time.Time) error {
	p := influxdb2.NewPoint(
		"modbus_data", // measurement
		map[string]string{
			"var": varName,
		},
		map[string]interface{}{
			"value": value,
		},
		t,
	)
	return iw.writeAPI.WritePoint(context.Background(), p)
}

// 写入一批变量（同一时刻的多变量，组成一行）
func (iw *InfluxWriter) WriteVars(vars map[string]interface{}, t time.Time) error {
	points := make([]*influxdb2.Point, 0, len(vars))
	for k, v := range vars {
		points = append(points, influxdb2.NewPoint(
			"modbus_data",
			map[string]string{"var": k},
			map[string]interface{}{"value": v},
			t,
		))
	}
	return iw.writeAPI.WritePoint(context.Background(), points...)
}

// 关闭
func (iw *InfluxWriter) Close() {
	iw.client.Close()
}
