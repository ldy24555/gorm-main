package gorm

import (
	"gitops.sudytech.cn/guolei/gorm/driver"
	"testing"
)

func TestGorm_TranSQL(t *testing.T) {
	var sqls []TranSQL
	sqls = append(sqls, TranSQL{SQL: "INSERT INTO T1 VALUES(?,?)", Params: []interface{}{"0111", "测试"}})
	sqls = append(sqls, TranSQL{SQL: "INSERT INTO T2 SELECT * FROM T1"})
	opts, _ := options()
	for _, v := range opts {
		gorm, err := v.GetInit()
		if err != nil {
			t.Fatalf("%s.connDB.err=%v", driver.GetDbName(v.DbType), err)
		} else {
			err = gorm.TranSQL(sqls)
			if err != nil {
				t.Fatalf("%s.tran.err=%v", driver.GetDbName(v.DbType), err)
			}
		}
	}
}
