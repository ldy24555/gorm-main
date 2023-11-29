package gorm

import (
	"errors"
	"fmt"
	"github.com/samber/lo"
	"gitops.sudytech.cn/guolei/gorm/driver"
	"gitops.sudytech.cn/guolei/gorm/driver/dm"
	"gitops.sudytech.cn/guolei/gorm/driver/ux"
	"gitops.sudytech.cn/guolei/gorm/driver/vb"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"strings"
)

func (opt *Option) InitDB() error {
	switch opt.DbType {
	case driver.DBTypeUxDB:
		return opt.initUxDB()
	case driver.DBTypeDmDB:
		return opt.initDmDB()
	case driver.DBTypeVbDB:
		return opt.initVbDB()
	default:
		return opt.initMySQLDB()
	}
}

func (opt *Option) initUxDB() error {
	dsn := toPgDsn("postgres", opt.DataSourceName)
	db, err := gorm.Open(ux.New(ux.Config{DSN: dsn}))
	if err != nil {
		return err
	}
	var result []map[string]interface{}
	db.Raw("SELECT datname FROM ux_database").Scan(&result)
	if containKv(result, "datname", opt.DbName) {
		return nil
	}
	sql := fmt.Sprintf("CREATE DATABASE \"%s\"", opt.DbName)
	resp := db.Exec(sql)
	if resp != nil {
		if resp.Error != nil {
			err = resp.Error
		}
	}
	return err
}

func (opt *Option) initDmDB() error {
	dsn := toPgDsn("dm", opt.DataSourceName)
	db, err := gorm.Open(dm.New(dm.Config{DSN: dsn}))
	if err != nil {
		return err
	}
	var result []map[string]interface{}
	db.Raw("select tablespace_name \"datname\" from dba_data_files").Scan(&result)
	if containKv(result, "datname", opt.DbName) {
		return nil
	}
	creSpace := fmt.Sprintf("CREATE tablespace \"%s\" datafile '/dm/data/DMDB/%s.DBF' size 128 autoextend on maxsize 67108863 CACHE = NORMAL", opt.DbName, opt.DbName)
	resp := db.Exec(creSpace)
	if resp != nil {
		if resp.Error != nil {
			return resp.Error
		}
	}
	pass, pErr := parsePass(opt.DataSourceName)
	if pass == "" {
		return lo.Ternary(pErr != nil, pErr, errors.New("未解析到数据库连接密码"))
	}
	creUser := fmt.Sprintf("CREATE USER \"%s\" IDENTIFIED BY %s HASH WITH SHA512 NO SALT PASSWORD_POLICY 2 ENCRYPT BY %s \n LIMIT FAILED_LOGIN_ATTEMPS 3, PASSWORD_LOCK_TIME 1, PASSWORD_GRACE_TIME 10 DEFAULT TABLESPACE \"%s\" DEFAULT INDEX TABLESPACE \"%s\"", opt.DbName, pass, pass, opt.DbName, opt.DbName)
	resp = db.Exec(creUser)
	if resp != nil {
		if resp.Error != nil {
			return resp.Error
		}
	}
	creGrant := fmt.Sprintf("grant\"DBA\" to\"%s\"", opt.DbName)
	resp = db.Exec(creGrant)
	if resp != nil {
		if resp.Error != nil {
			err = resp.Error
		}
	}
	return err
}

func (opt *Option) initVbDB() error {
	dsn := toPgDsn("postgres", opt.DataSourceName)
	dsn = fmt.Sprintf("%s%s?%s", dsn, "vastbase", "sslmode=disable")
	db, err := gorm.Open(vb.New(vb.Config{DSN: dsn}))
	if err != nil {
		return err
	}
	var result []map[string]interface{}
	db.Raw("SELECT datname FROM pg_database").Scan(&result)
	if containKv(result, "datname", opt.DbName) {
		return nil
	}
	user, pErr := parseUser(dsn)
	if user == "" {
		return lo.Ternary(pErr != nil, pErr, errors.New("未解析到数据库连接账号"))
	}
	sql := fmt.Sprintf("CREATE DATABASE \"%s\"\nWITH\nOWNER = %s\nENCODING = 'UTF-8'\nTEMPLATE = template0\nDBCOMPATIBILITY = 'B'\nTABLESPACE = pg_default\nLC_COLLATE = 'en_US.utf8'\nLC_CTYPE = 'en_US.utf8'\nCONNECTION LIMIT = -1", opt.DbName, user)
	resp := db.Exec(sql)
	if resp != nil {
		if resp.Error != nil {
			err = resp.Error
		}
	}
	return err
}

func (opt *Option) initMySQLDB() error {
	dsn := toMyDsn(opt.DataSourceName)
	db, err := gorm.Open(mysql.New(mysql.Config{DSN: dsn}))
	if err == nil {
		resp := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", opt.DbName))
		if resp != nil {
			err = resp.Error
		}
	}
	return err
}

func parseUser(dsn string) (string, error) {
	bIndex := strings.Index(dsn, "://")
	if bIndex != -1 {
		eIndex := strings.LastIndex(dsn, "@")
		if eIndex > bIndex {
			return strings.Split(dsn[bIndex+3:eIndex], ":")[0], nil
		}
	}
	kvs := strings.Split(dsn, " ")
	for _, v := range kvs {
		users := strings.Split(v, "=")
		if users[0] == "user" {
			return users[1], nil
		}
	}
	return "", errors.New("解析数据库连接账号失败")
}

func parsePass(dsn string) (string, error) {
	bIndex := strings.Index(dsn, "://")
	if bIndex != -1 {
		eIndex := strings.LastIndex(dsn, "@")
		if eIndex > bIndex {
			users := dsn[bIndex+3 : eIndex]
			speIndex := strings.Index(users, ":")
			return users[speIndex+1:], nil
		}
	}
	kvs := strings.Split(dsn, " ")
	for _, v := range kvs {
		users := strings.Split(v, "=")
		if users[0] == "password" {
			return users[1], nil
		}
	}
	return "", errors.New("解析数据库连接密码失败")
}

func (opt *Option) DropDB() error {
	switch opt.DbType {
	case driver.DBTypeUxDB:
		return opt.dropUxDB()
	case driver.DBTypeDmDB:
		return opt.dropDmDB()
	case driver.DBTypeVbDB:
		return opt.dropVbDB()
	default:
		return opt.dropMySQLDB()
	}
}

func (opt *Option) dropUxDB() error {
	dsn := toPgDsn("postgres", opt.DataSourceName)
	db, err := gorm.Open(ux.New(ux.Config{DSN: dsn}))
	if err != nil {
		return err
	}
	var result []map[string]interface{}
	db.Raw("SELECT datname FROM ux_database").Scan(&result)
	if !containKv(result, "datname", opt.DbName) {
		return nil
	}
	sql := fmt.Sprintf("DROP DATABASE \"%s\"", opt.DbName)
	resp := db.Exec(sql)
	if resp != nil {
		if resp.Error != nil {
			err = resp.Error
		}
	}
	return err
}

func (opt *Option) dropVbDB() error {
	dsn := toPgDsn("postgres", opt.DataSourceName)
	dsn = fmt.Sprintf("%s%s?%s", dsn, "vastbase", "sslmode=disable")
	db, err := gorm.Open(vb.New(vb.Config{DSN: dsn}))
	if err != nil {
		return err
	}
	var result []map[string]interface{}
	db.Raw("SELECT datname FROM pg_database").Scan(&result)
	if !containKv(result, "datname", opt.DbName) {
		return nil
	}
	sql := fmt.Sprintf("DROP DATABASE \"%s\"", opt.DbName)
	resp := db.Exec(sql)
	if resp != nil {
		if resp.Error != nil {
			err = resp.Error
		}
	}
	return err
}

func (opt *Option) dropDmDB() error {
	dsn := toPgDsn("dm", opt.DataSourceName)
	db, err := gorm.Open(dm.New(dm.Config{DSN: dsn}))
	if err != nil {
		return err
	}
	var result []map[string]interface{}
	db.Raw("select tablespace_name \"datname\" from dba_data_files").Scan(&result)
	if !containKv(result, "datname", opt.DbName) {
		return nil
	}
	creSpace := fmt.Sprintf("DROP USER \"%s\" cascade", opt.DbName)
	resp := db.Exec(creSpace)
	if resp != nil {
		if resp.Error != nil {
			return resp.Error
		}
	}
	creUser := fmt.Sprintf("DROP tablespace \"%s\"", opt.DbName)
	resp = db.Exec(creUser)
	if resp != nil {
		if resp.Error != nil {
			return resp.Error
		}
	}
	return err
}

func (opt *Option) dropMySQLDB() error {
	dsn := toMyDsn(opt.DataSourceName)
	db, err := gorm.Open(mysql.New(mysql.Config{DSN: dsn}))
	if err == nil {
		resp := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", opt.DbName))
		if resp != nil {
			err = resp.Error
		}
	}
	return err
}

func containKv(req []map[string]interface{}, ky, value string) bool {
	if len(req) > 0 {
		for _, v := range req {
			if v[ky] == value {
				return true
			}
		}
	}
	return false
}
