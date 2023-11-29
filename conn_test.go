package gorm

import (
	"gitops.sudytech.cn/guolei/gorm/testdata"
	"testing"
)

func TestOption_GetInsertSQL(t *testing.T) {
	opts, _ := options()
	algorithm := &testdata.Algorithm{}
	data := make(map[string]interface{})
	//data["code"] = "aa"
	data["NAME"] = "aa1"
	for _, v := range opts {
		sql := v.GetInsertSQL(algorithm, data)
		if sql == nil {
			t.Log("sql is nil")
		} else {
			t.Logf("sql=%s,len=%v", sql.SQL, &sql.Params)
		}
	}
}
