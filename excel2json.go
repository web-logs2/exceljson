package exceljson

import (
	"github.com/xuri/excelize/v2"
)

type _excel2json struct {
}

// NewExcel2json 实例化 excel服务
func NewExcel2json() *_excel2json {
	return &_excel2json{}
}

// Read 读取excel 表中所有数据
func (instance *_excel2json) Read(f *excelize.File, sheet string, fieldMap map[string]string, rowIndex int, isUnmergeCell bool) ([]map[string]string, error) {
	if isUnmergeCell {
		err := instance.UnmergeCell(f, sheet)
		if err != nil {
			return nil, err
		}
	}
	// 获取 Sheet 上所有单元格
	rows, err := f.GetRows(sheet)
	if err != nil {
		return nil, err
	}
	output := make([]map[string]string, 0)

	for index, row := range rows {
		if index < rowIndex-1 { // 从指定行开始读取
			continue
		}
		record := make(map[string]string, 0)
		for colIndex, colCell := range row {
			colName, err := excelize.ColumnNumberToName(colIndex + 1)
			if err != nil {
				return nil, err
			}
			if fieldMap != nil { // 如果定制了列名和字段映射关系，替换字段映射
				field, ok := fieldMap[colName]
				if ok {
					record[field] = colCell
				}
			} else {
				record[colName] = colCell
			}
		}
		output = append(output, record)
	}
	return output, nil
}

//UnmergeCell 将合并单元格展开，值填充到每个展开的单元内
func (instance *_excel2json) UnmergeCell(f *excelize.File, sheet string) (err error) {
	mergeCells, err := f.GetMergeCells(sheet)
	if err != nil {
		return err
	}
	if len(mergeCells) == 0 {
		return nil
	}
	for _, mergeCell := range mergeCells {
		startAxis := mergeCell.GetStartAxis()
		endAxis := mergeCell.GetEndAxis()
		value := mergeCell.GetCellValue()
		err = f.UnmergeCell(sheet, startAxis, endAxis)
		if err != nil {
			return err
		}
		cells, err := expandCellRegion(startAxis, endAxis)
		if err != nil {
			return err
		}
		for _, cell := range cells {
			err = f.SetCellValue(sheet, cell, value)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

// expandCellRegion 将合并区域展开成单个单元地址集合
func expandCellRegion(startAxis string, endAxis string) (cells []string, err error) {
	startColumn, startRow, err := excelize.CellNameToCoordinates(startAxis)
	if err != nil {
		return nil, err
	}
	endColumn, endRow, err := excelize.CellNameToCoordinates(endAxis)
	if err != nil {
		return nil, err
	}

	cells = make([]string, 0)
	for row := startRow; row <= endRow; row++ {
		for column := startColumn; column <= endColumn; column++ {
			cell, err := excelize.CoordinatesToCellName(column, row)
			if err != nil {
				return nil, err
			}
			cells = append(cells, cell)
		}
	}
	return cells, nil
}
