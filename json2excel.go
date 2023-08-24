package exceljson

import (
	"github.com/suifengpiao14/logchan/v2"
	"github.com/xuri/excelize/v2"
)

type _json2excel struct{}

// NewJson2excel 实例化 excel服务
func NewJson2excel() *_json2excel {
	return &_json2excel{}
}

//GetRowNumber  获取下一次可以写入的行号
func (export *_json2excel) GetRowNumber(fd *excelize.File, sheet string) (rowNumber int, err error) {
	rows, err := fd.Rows(sheet)
	if err != nil {
		return
	}
	cur := 0
	for rows.Next() {
		cur++
	}
	rowNumber = cur + 1
	return
}

// 移除一行
func (export *_json2excel) RemoveRow(fd *excelize.File, sheet string, row int) (err error) {
	err = fd.RemoveRow(sheet, row)
	return
}

//WriteDataToFile  写入数据
func (export *_json2excel) WriteDataToFile(fd *excelize.File, sheet string, rowNumber int, fields []string, data []map[string]interface{}) (nextRowNumber int, err error) {
	streamWriter, err := export.GetStream(fd, sheet)
	if err != nil {
		return
	}
	defer func() {
		err = streamWriter.Flush()
		if err != nil {
			return
		}
	}()
	for _, record := range data {
		// 组装一行数据
		row := make([]interface{}, len(fields))
		for colIndex, field := range fields {
			row[colIndex] = record[field]
		}
		// 获取当前行开始写入单元地址
		cell, err := excelize.CoordinatesToCellName(0, rowNumber)
		if err != nil {
			return 0, err
		}
		//写入一行数据
		err = streamWriter.SetRow(cell, row)
		if err != nil {
			return 0, err
		}
		rowNumber++ // 增加行号
	}
	nextRowNumber = rowNumber
	return
}

//Write2stream 向写入流中写入数据
func (export *_json2excel) Write2stream(streamWriter *excelize.StreamWriter, rowNumber int, fields []string, data []map[string]interface{}) (nextRowNumber int, err error) {
	for _, record := range data {
		// 组装一行数据
		row := make([]interface{}, len(fields))
		for colIndex, field := range fields {
			row[colIndex] = record[field]
		}

		// 获取当前行开始写入单元地址
		cell, err := excelize.CoordinatesToCellName(0, rowNumber)
		if err != nil {
			return 0, err
		}
		//写入一行数据
		err = streamWriter.SetRow(cell, row)
		if err != nil {
			return 0, err
		}
		rowNumber++ // 增加行号
	}
	nextRowNumber = rowNumber
	return
}

//GetStream 打开文件流，将已有的数据填写到流内，返回写入流
func (export *_json2excel) GetStream(fd *excelize.File, sheet string) (streamWriter *excelize.StreamWriter, err error) {
	streamWriter, err = fd.NewStreamWriter(sheet)
	if err != nil {
		return
	}

	rows, err := fd.GetRows(sheet) //获取行内容
	if err != nil {
		return
	}
	//将源文件内容先写入excel
	for rowindex, oldRow := range rows {
		colLen := len(oldRow)
		newRow := make([]interface{}, colLen)
		for colIndex := 0; colIndex < colLen; colIndex++ {
			if oldRow == nil {
				newRow[colIndex] = nil
			} else {
				newRow[colIndex] = oldRow[colIndex]
			}
		}
		beginCell, _ := excelize.CoordinatesToCellName(1, rowindex+1)
		err = streamWriter.SetRow(beginCell, newRow)
		if err != nil {
			return
		}
	}
	return
}

type DataWrap struct {
	Data      []map[string]interface{}
	RowNumber int
}

type LogInfoName string

func (l LogInfoName) String() string {
	return string(l)
}

const (
	LOG_INFO_READ_CHAN_DATA LogInfoName = "excel_export_read_chan_data"
)

type LogInfoReadChanData struct {
	logchan.EmptyLogInfo
	err error
}

func (l *LogInfoReadChanData) GetName() (name logchan.LogName) {
	name = LOG_INFO_READ_CHAN_DATA
	return name
}

func (l *LogInfoReadChanData) Error() (err error) {
	err = l.err
	return err
}

//ReadChanData 接收chan中的数据,写入文件,完成后输出完成信号，并退出
func (export *_json2excel) ReadChanData(dataChan chan *DataWrap, fd *excelize.File, sheet string, fields []string) (finishSignal chan struct{}, err error) {
	streamWriter, err := export.GetStream(fd, sheet)
	if err != nil {
		return nil, err
	}
	finishSignal = make(chan struct{}, 1)
	go func() {
		logInfo := LogInfoReadChanData{}
		defer func() {
			finishSignal <- struct{}{} // 发送完成信号
		}()
		for dataWrap := range dataChan { //  阻塞读取数据写入文件
			_, err = export.Write2stream(streamWriter, dataWrap.RowNumber, fields, dataWrap.Data)
			if err != nil {
				logInfo.err = err
				return
			}
		}

		// 保存文件
		err = streamWriter.Flush()
		if err != nil {
			logInfo.err = err
			return
		}
		err = fd.Save()
		if err != nil {
			logInfo.err = err
			return
		}
	}()
	return finishSignal, nil
}
