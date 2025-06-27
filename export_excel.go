package main

import (
	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/xuri/excelize/v2"
)

// 查询一段时间内所有变量，导出Excel
func ExportToExcel(start, end time.Time, influxClient influxdb2.Client, filename string) error {
	queryAPI := influxClient.QueryAPI(InfluxOrg)
	flux := fmt.Sprintf(`
from(bucket: "%s")
|> range(start: %s, stop: %s)
|> filter(fn: (r) => r._measurement == "modbus_data")
|> pivot(rowKey:["_time"], columnKey: ["var"], valueColumn: "_value")
|> sort(columns: ["_time"])
`, InfluxBucket, start.Format(time.RFC3339), end.Format(time.RFC3339))

	result, err := queryAPI.Query(context.Background(), flux)
	if err != nil {
		return err
	}

	f := excelize.NewFile()
	sheet := "Sheet1"
	f.NewSheet(sheet)
	headers := []string{"时间"}
	var colNames []string
	rows := make([][]interface{}, 0, 1000)

	// 解析结果，每次循环是一行（同一时刻所有变量）
	for result.Next() {
		if len(colNames) == 0 {
			// 第一次解析字段，确定所有变量名
			for k := range result.Record().Values() {
				if k != "_time" && k != "result" && k != "table" {
					colNames = append(colNames, k)
				}
			}
			headers = append(headers, colNames...)
			f.SetSheetRow(sheet, "A1", &headers)
		}
		row := make([]interface{}, 0, len(colNames)+1)
		timeVal := result.Record().Time().Format("2006-01-02 15:04:05")
		row = append(row, timeVal)
		for _, c := range colNames {
			val := result.Record().ValueByKey(c)
			row = append(row, val)
		}
		rows = append(rows, row)
	}
	for i, row := range rows {
		cell, _ := excelize.CoordinatesToCellName(1, i+2)
		f.SetSheetRow(sheet, cell, &row)
	}
	return f.SaveAs(filename)
}
