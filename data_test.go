package gorm

import (
	"fmt"
	"gitops.sudytech.cn/guolei/gorm/driver"
	"gitops.sudytech.cn/guolei/gorm/testdata"
	"testing"
)

func TestData_GetInt(t *testing.T) {
	data := make(map[string]interface{})
	data["Enable"] = 1
	row := NewRow(data)
	resp := row.GetString("eNable")
	t.Logf("GetInt=%s", resp)
	row.Put("EnabLE", "abc")
	t.Logf("GetInt=%s", row.GetString("eNable"))
	row.Delete("ENAbLE")
	t.Logf("GetInt=%s", row.GetString("eNable"))
	fmt.Println("m=", row.GetString("1111"))
}

func TestData_GetBool(t *testing.T) {
	data := make(map[string]interface{})
	data["enable"] = 1
	resp := NewRow(data).GetBool("enable")
	t.Logf("GetBool=%t", resp)
}

func TestRow_GetTime(t *testing.T) {
	opts, _ := options()
	for _, v := range opts {
		gorm, err := v.GetInit()
		if err != nil {
			t.Fatalf(fmt.Sprintf("%s.GetInit.err=", driver.GetDbName(v.DbType)), err)
		} else {
			resp, rErr := gorm.QueryRow(fmt.Sprintf("SELECT * FROM %s", gorm.GetTable(&testdata.Algorithm{})))
			if rErr != nil {
				t.Fatalf(fmt.Sprintf("%s.query.err=", driver.GetDbName(v.DbType)), rErr)
			} else {
				for _, rv := range resp {
					fmt.Println("updateTime=", rv.GetTime("updateTime"))
					fmt.Println("createTime=", rv.GetTime("createTime"))
				}
			}
		}
	}
}
