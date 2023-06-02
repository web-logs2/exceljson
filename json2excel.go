package exceljson

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"github.com/pkg/errors"

	"github.com/suifengpiao14/logchan/v2"
	"github.com/xuri/excelize/v2"
)

const (
	FIELDMAP_TYPE_STRING   = "string"
	FIELDMAP_TYPE_TEMPLATE = "template"
	TEMPLATE_SEPARATOR     = "{{"
	EXCEL_CELL_TYPE_FUNC   = "function"
	EXCEL_CELL_TYPE_STRING = "string"
)

type FieldMap struct {
	Type     string
	CellType string
	Value    interface{}
}

var TplFuncMap = template.FuncMap{
	"fen2yuan": func(fen interface{}) string {
		var yuan float64
		intFen, ok := fen.(int)
		if ok {
			yuan = float64(intFen) / 100
			return strconv.FormatFloat(yuan, 'f', 2, 64)
		}
		strFen, ok := fen.(string)
		if ok {
			intFen, err := strconv.Atoi(strFen)
			if err == nil {
				yuan = float64(intFen) / 100
				return strconv.FormatFloat(yuan, 'f', 2, 64)
			}
		}
		return strFen
	},
}

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

func (export *_json2excel) GetFieldMap(fd *excelize.File, sheet string) (fieldMap map[int]*FieldMap, err error) {
	fieldMap = make(map[int]*FieldMap)
	rows, err := fd.GetRows(sheet)
	if err != nil {
		return
	}
	if len(rows) < 2 {
		err = errors.New("excel file must have more than tow row")
		return
	}
	row := rows[0]
	for colIndex, colCell := range row {
		index := colIndex + 1 // excel索引从1开始
		if strings.Contains(colCell, TEMPLATE_SEPARATOR) {
			tplName := fmt.Sprintf("field_%d_tpl", index)
			tpl, err := template.New(tplName).Funcs(TplFuncMap).Parse(colCell)
			if err != nil {
				return nil, err
			}
			cellType := EXCEL_CELL_TYPE_STRING
			if colCell[0:1] == "=" {
				cellType = EXCEL_CELL_TYPE_FUNC
			}

			fieldMap[index] = &FieldMap{
				Type:     FIELDMAP_TYPE_TEMPLATE,
				CellType: cellType,
				Value:    tpl,
			}
		} else {
			fieldMap[index] = &FieldMap{
				Type:     FIELDMAP_TYPE_STRING,
				CellType: EXCEL_CELL_TYPE_STRING,
				Value:    colCell,
			}
		}
	}
	return
}

//WriteDataToFile  写入数据
func (export *_json2excel) WriteDataToFile(fd *excelize.File, sheet string, rowNumber int, fieldMap map[int]*FieldMap, data []map[string]interface{}) (nextRowNumber int, err error) {
	streamWriter, err := export.GetStreamWriter(fd, sheet)
	if err != nil {
		return
	}
	defer func() {
		err = streamWriter.Flush()
		if err != nil {
			return
		}
	}()

	minColIndex := -1
	// 找到每行开始写入的列序号
	for colIndex := range fieldMap {
		if minColIndex == -1 {
			minColIndex = colIndex
			continue
		}
		if colIndex < minColIndex {
			minColIndex = colIndex // 找到最小的列
		}
	}

	colLen := len(fieldMap)
	for _, record := range data {
		// 组装一行数据
		row := make([]interface{}, colLen)
		for colIndex, field := range fieldMap {
			k := colIndex - 1
			val, err := export.GetValue(field, record)
			if err != nil {
				return 0, err
			}
			row[k] = val
		}

		// 获取当前行开始写入单元地址
		cell, err := excelize.CoordinatesToCellName(minColIndex, rowNumber)
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

func (export *_json2excel) GetValue(field *FieldMap, data map[string]interface{}) (value interface{}, err error) {
	if field.Type == FIELDMAP_TYPE_STRING {
		fieldKey, ok := field.Value.(string)
		if !ok {
			err = fmt.Errorf("%v can not convert to string", field.Value)
			return
		}
		value, ok := data[fieldKey]
		if ok {
			return value, nil
		}
		return fieldKey, nil // 对于数据中不存在的列，使用模板中列标题填充，确保数据能对应上
	}
	// 处理go模板
	if field.Type == FIELDMAP_TYPE_TEMPLATE {
		tpl, ok := field.Value.(*template.Template)
		if !ok {
			err = fmt.Errorf("%v can not convert to text/template", field.Value)
			return
		}
		var rw bytes.Buffer
		if err := tpl.Execute(&rw, data); err != nil {
			return nil, err
		}
		str := rw.String()
		return str, nil
	}
	return field.Value, nil
}

//Write2streamWriter 向写入流中写入数据
func (export *_json2excel) Write2streamWriter(streamWriter *excelize.StreamWriter, rowNumber int, fieldMap map[int]*FieldMap, data []map[string]interface{}) (nextRowNumber int, err error) {
	minColIndex := 999999999 // 默认从最右边写入
	// 找到每行开始写入的列序号
	for colIndex := range fieldMap {
		if colIndex < minColIndex {
			minColIndex = colIndex // 找到最小的列
		}
	}

	colLen := len(fieldMap)
	for _, record := range data {
		// 组装一行数据
		row := make([]interface{}, colLen)
		for colIndex, field := range fieldMap {
			k := colIndex - 1
			val, err := export.GetValue(field, record)
			if err != nil {
				return 0, err
			}
			row[k] = val
		}

		// 获取当前行开始写入单元地址
		cell, err := excelize.CoordinatesToCellName(minColIndex, rowNumber)
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

//GetStreamWriter 打开文件流，将已有的数据填写到流内，返回写入流
func (export *_json2excel) GetStreamWriter(fd *excelize.File, sheet string) (streamWriter *excelize.StreamWriter, err error) {
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
func (export *_json2excel) ReadChanData(dataChan chan *DataWrap, fd *excelize.File, sheet string, fieldMap map[int]*FieldMap) (finishSignal chan struct{}, err error) {
	streamWriter, err := export.GetStreamWriter(fd, sheet)
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
			_, err = export.Write2streamWriter(streamWriter, dataWrap.RowNumber, fieldMap, dataWrap.Data)
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
