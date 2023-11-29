package gorm

import (
	"bufio"
	"fmt"
	"gitops.sudytech.cn/guolei/gorm/driver"
	"gitops.sudytech.cn/guolei/gorm/testdata"
	"gorm.io/gorm/logger"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestOption_InitDB(t *testing.T) {
	opts, _ := options()
	if len(opts) > 0 {
		for _, v := range opts {
			err := v.InitDB()
			if err != nil {
				t.Fatalf(fmt.Sprintf("%s.initDB.err=", driver.GetDbName(v.DbType)), err)
			}
		}
	}
	for _, v := range opts {
		gorm, err := v.GetInit()
		if err != nil {
			t.Fatalf("%s.connDB.err=%v", driver.GetDbName(v.DbType), err)
		} else {
			err = migrate(gorm)
			if err != nil {
				t.Fatalf("%s.migrate.err=%v", driver.GetDbName(v.DbType), err)
			}
		}
	}
}

func TestOption_InitDB2(t *testing.T) {
	opts, _ := options()
	//now := time.Now()
	alg := &testdata.Algorithm{Code: "test", Name: "测试"}
	for _, v := range opts {
		gorm, err := v.GetInit()
		if err != nil {
			t.Fatalf("%s.connDB.err=%v", driver.GetDbName(v.DbType), err)
		} else {
			//gorm.DB.Create(alg)
			//gorm.ExecSQL(fmt.Sprintf("UPDATE %s t SET t.updateTime=? WHERE t.code=?", gorm.GetTable(&algorithm{})), now, alg.Code)
			resp, _ := gorm.QueryRow(fmt.Sprintf("SELECT * FROM %s t WHERE t.code=?", gorm.GetTable(&testdata.Algorithm{})), alg.Code)
			if len(resp) > 0 {
				for _, rv := range resp {
					fmt.Println(fmt.Sprintf("DB(%s).v0=", driver.GetDbName(v.DbType)), rv.data["updateTime"])
				}
			}
			var a testdata.Algorithm
			gorm.DB.Find(&a, "1")
			fmt.Println(fmt.Sprintf("DB(%s).v1=", driver.GetDbName(v.DbType)), a.UpdateTime)
		}
	}
}

func TestOption_DropDB(t *testing.T) {
	opts, _ := options()
	if len(opts) > 0 {
		for _, v := range opts {
			err := v.DropDB()
			if err != nil {
				t.Fatalf("%s.deleteDB.err=%v", driver.GetDbName(v.DbType), err)
			}
		}
	}
}

func migrate(gm *Gorm) error {
	var table []interface{}
	table = append(table, &testdata.Algorithm{})
	return gm.DB.AutoMigrate(table...)
}

func options() ([]*Option, error) {
	file, err := os.Open("/Users/guolei/Documents/F/项目资料/001苏迪/信创/conn.txt")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var opts []*Option
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
		if line == "" {
			opt := toOptions(lines)
			if opt != nil {
				opts = append(opts, opt)
			}
			lines = []string{}
		}
	}
	opt := toOptions(lines)
	if opt != nil {
		opts = append(opts, opt)
	}
	return opts, scanner.Err()
}

func toOptions(req []string) *Option {
	sMap := make(map[string]string)
	for _, v := range req {
		bIndex := strings.Index(v, ":")
		if bIndex != -1 {
			sMap[strings.TrimSpace(v[0:bIndex])] = strings.TrimSpace(v[bIndex+1:])
		}
	}
	dbType := sMap["dbType"]
	dbName := sMap["dbName"]
	dataSourceName := sMap["dataSourceName"]
	if dbType == "" {
		if dbName == "" && dataSourceName == "" {
			return nil
		}
	}
	schema := sMap["schema"]
	dbTypeInt, _ := strconv.Atoi(dbType)
	return &Option{DbType: dbTypeInt, DbName: dbName, DataSourceName: dataSourceName, Schema: schema, LogLevel: logger.Info}
}
