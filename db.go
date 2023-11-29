package gorm

import (
	"fmt"
	"github.com/samber/lo"
	"gitops.sudytech.cn/guolei/gorm/driver"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strconv"
	"strings"
	"sync"
)

type TranSQL struct {
	SQL    string
	Params []interface{}
}

func ToError(req *gorm.DB) error {
	if req != nil {
		if err := req.Error; err != nil {
			if !strings.Contains(err.Error(), "record not found") {
				return err
			}
		}
	}
	return nil
}

func (gm *Gorm) ExecSQL(sql string, params ...interface{}) error {
	opt := gm.Option
	exp := driver.Exp{DbType: opt.DbType, Schema: opt.Schema}
	nSQL := exp.ExecSQL(sql)
	resp := gm.DB.Exec(nSQL, params...)
	if resp != nil {
		if err := resp.Error; err != nil {
			zap.S().Errorf("exec sql:src=%s,new=%s,param=%v,err=%v", sql, nSQL, params, err)
		}
	}
	return ToError(resp)
}

func (gm *Gorm) ExecuteSQL(sql string, params ...interface{}) (int64, error) {
	opt := gm.Option
	exp := driver.Exp{DbType: opt.DbType, Schema: opt.Schema}
	nSQL := exp.ExecSQL(sql)
	resp := gm.DB.Exec(nSQL, params...)
	var rows int64 = 0
	if resp != nil {
		rows = resp.RowsAffected
		if err := resp.Error; err != nil {
			zap.S().Errorf("execute sql:src=%s,new=%s,param=%v,err=%v", sql, nSQL, params, err)
		}
	}
	return rows, ToError(resp)
}

func (gm *Gorm) TranSQL(sql []TranSQL, callback ...func() error) error {
	opt := gm.Option
	exp := driver.Exp{DbType: opt.DbType, Schema: opt.Schema}
	err := gm.DB.Transaction(func(tx *gorm.DB) error {
		for _, v := range sql {
			nSQL := exp.ExecSQL(v.SQL)
			if err := tx.Exec(nSQL, v.Params...).Error; err != nil {
				zap.S().Errorf("tran sql:src=%s,new=%s,param=%v,err=%v", v.SQL, nSQL, v.Params, err)
				return err
			}
		}
		return nil
	})
	if err == nil {
		for _, v := range callback {
			if cErr := v(); cErr != nil {
				zap.S().Error("tran callback err:", v, cErr)
				return cErr
			}
		}
	}
	return err
}

func (gm *Gorm) TransactionSQL(sql []TranSQL, callback ...func(tx *gorm.DB, explain driver.Exp) error) error {
	opt := gm.Option
	exp := driver.Exp{DbType: opt.DbType, Schema: opt.Schema}
	return gm.DB.Transaction(func(tx *gorm.DB) error {
		for _, v := range sql {
			nSQL := exp.ExecSQL(v.SQL)
			if err := tx.Exec(nSQL, v.Params...).Error; err != nil {
				zap.S().Errorf("tran sql:src=%s,new=%s,param=%v,err=%v", v.SQL, nSQL, v.Params, err)
				return err
			}
		}
		for _, v := range callback {
			if cErr := v(tx, exp); cErr != nil {
				zap.S().Error("tran callback err:", v, cErr)
				return cErr
			}
		}
		return nil
	})
}

func (gm *Gorm) QueryRow(sql string, params ...interface{}) ([]Row, error) {
	var result []map[string]interface{}
	opt := gm.Option
	exp := driver.Exp{DbType: opt.DbType, Schema: opt.Schema}
	nSQL := exp.QuerySQL(sql)
	resp := gm.DB.Raw(nSQL, params...).Scan(&result)
	if resp != nil {
		if err := resp.Error; err != nil {
			zap.S().Errorf("query row:src=%s,new=%s,param=%v,err=%v", sql, nSQL, params, err)
		}
	}
	return Rows(result), ToError(resp)
}

func (gm *Gorm) QueryTotal(sql string, params ...interface{}) (int64, error) {
	var sMap map[string]interface{}
	opt := gm.Option
	exp := driver.Exp{DbType: opt.DbType, Schema: opt.Schema}
	nSQL := exp.QuerySQL(sql)
	resp := gm.DB.Raw(nSQL, params...).Scan(&sMap)
	var result int64
	if len(sMap) > 0 {
		for _, v := range sMap {
			switch v.(type) {
			case int64:
				result = v.(int64)
			case int32:
				result = int64(v.(int32))
			default:
				result, _ = strconv.ParseInt(fmt.Sprint(v), 10, 64)
			}
		}
	}
	if resp != nil {
		if err := resp.Error; err != nil {
			zap.S().Errorf("query total:src=%s,new=%s,param=%v,err=%v", sql, nSQL, params, err)
		}
	}
	return result, ToError(resp)
}

func (gm *Gorm) QueryRows(pageNo int32, pageSize int32, sql string, params ...interface{}) ([]Row, error) {
	var result []map[string]interface{}
	if pageNo > 0 {
		if pageSize > 0 {
			sql = fmt.Sprintf("%s LIMIT %d,%d", sql, (pageNo-1)*pageSize, pageSize)
		}
	}
	opt := gm.Option
	exp := driver.Exp{DbType: opt.DbType, Schema: opt.Schema}
	nSQL := exp.QuerySQL(sql)
	resp := gm.DB.Raw(nSQL, params...).Scan(&result)
	if resp != nil {
		if err := resp.Error; err != nil {
			zap.S().Errorf("query rows:src=%s,new=%s,param=%v,err=%v", sql, nSQL, params, err)
		}
	}
	return Rows(result), ToError(resp)
}

func (gm *Gorm) GetTable(model interface{}) string {
	return gm.Option.GetTable(model)
}

func (gm *Gorm) GetTableName(table string) string {
	return gm.Option.GetTableName(table)
}

func (gm *Gorm) GetInsertSQL(model interface{}, data map[string]interface{}) *TranSQL {
	return gm.Option.GetInsertSQL(model, data)
}

func (gm *Gorm) GetUpdateSQL(model interface{}, pks map[string]interface{}, data map[string]interface{}) *TranSQL {
	return gm.Option.GetUpdateSQL(model, pks, data)
}

func (gm *Gorm) FindPageTotal(model interface{}, cons []ConsWrapper) (int64, error) {
	table := gm.GetTable(model)
	var build strings.Builder
	build.WriteString(fmt.Sprintf("SELECT COUNT(1) FROM %s t WHERE 1=1", table))
	params := GenWhereSQL(&build, cons)
	return gm.QueryTotal(build.String(), params...)
}

func (gm *Gorm) FindPageRows(model interface{}, pageNo int32, pageSize int32, cons []ConsWrapper, orders []QueryOrder) ([]Row, error) {
	table := gm.GetTable(model)
	var build strings.Builder
	build.WriteString(fmt.Sprintf("SELECT t.* FROM %s t WHERE 1=1", table))
	params := GenWhereSQL(&build, cons)
	GenOrderSQL(&build, orders)
	return gm.QueryRows(pageNo, pageSize, build.String(), params...)
}

func (gm *Gorm) FindPageSelectTotal(selectSQL string, selectParams []interface{}, cons []ConsWrapper) (int64, error) {
	var build strings.Builder
	build.WriteString(selectSQL)
	var params []interface{}
	params = append(params, selectParams...)
	params = append(params, GenWhereSQL(&build, cons)...)
	return gm.QueryTotal(build.String(), params...)
}

func (gm *Gorm) FindPageSelectRows(selectSQL string, selectParams []interface{}, pageNo int32, pageSize int32, cons []ConsWrapper, orders []QueryOrder) ([]Row, error) {
	var build strings.Builder
	build.WriteString(selectSQL)
	var params []interface{}
	params = append(params, selectParams...)
	params = append(params, GenWhereSQL(&build, cons)...)
	GenOrderSQL(&build, orders)
	return gm.QueryRows(pageNo, pageSize, build.String(), params...)
}

func (gm *Gorm) FindPageFromTotal(fromSQL string, fromParams []interface{}, cons []ConsWrapper) (int64, error) {
	var build strings.Builder
	build.WriteString(fmt.Sprintf("SELECT COUNT(1) %s", fromSQL))
	var params []interface{}
	params = append(params, fromParams...)
	params = append(params, GenWhereSQL(&build, cons)...)
	return gm.QueryTotal(build.String(), params...)
}

func (gm *Gorm) FindPageFromRows(fromSQL string, fromParams []interface{}, pageNo int32, pageSize int32, cons []ConsWrapper, orders []QueryOrder) ([]Row, error) {
	var build strings.Builder
	build.WriteString(fmt.Sprintf("SELECT * %s", fromSQL))
	var params []interface{}
	params = append(params, fromParams...)
	params = append(params, GenWhereSQL(&build, cons)...)
	GenOrderSQL(&build, orders)
	return gm.QueryRows(pageNo, pageSize, build.String(), params...)
}

func (gm *Gorm) FindPageList(model interface{}, pageNo int32, pageSize int32, cons []ConsWrapper, orders []QueryOrder) ([]Row, int64, error) {
	var count int64
	var err1, err2 error
	var resp []Row
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if pageNo != 0 {
			count, err1 = gm.FindPageTotal(model, cons)
		}
	}()
	go func() {
		defer wg.Done()
		if pageSize != 0 {
			resp, err2 = gm.FindPageRows(model, pageNo, pageSize, cons, orders)
		}
	}()
	wg.Wait()
	return resp, count, lo.Ternary(err1 != nil, err1, err2)
}

func (gm *Gorm) FindPageFromList(fromSQL string, fromParams []interface{}, pageNo int32, pageSize int32, cons []ConsWrapper, orders []QueryOrder) ([]Row, int64, error) {
	var count int64
	var err1, err2 error
	var resp []Row
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if pageNo != 0 {
			count, err1 = gm.FindPageFromTotal(fromSQL, fromParams, cons)
		}
	}()
	go func() {
		defer wg.Done()
		if pageSize != 0 {
			resp, err2 = gm.FindPageFromRows(fromSQL, fromParams, pageNo, pageSize, cons, orders)
		}
	}()
	wg.Wait()
	return resp, count, lo.Ternary(err1 != nil, err1, err2)
}

func (gm *Gorm) findPageSelectTotal(selectSQL string, selectParams []interface{}, cons []ConsWrapper) (int64, error) {
	var build strings.Builder
	sql := strings.ToUpper(selectSQL)
	index := strings.Index(sql, "FROM ")
	build.WriteString(fmt.Sprintf("SELECT COUNT(1) %s", selectSQL[index:]))
	var params []interface{}
	params = append(params, selectParams...)
	params = append(params, GenWhereSQL(&build, cons)...)
	return gm.QueryTotal(build.String(), params...)
}

func (gm *Gorm) FindPageSelectList(selectSQL string, selectParams []interface{}, pageNo int32, pageSize int32, cons []ConsWrapper, orders []QueryOrder) ([]Row, int64, error) {
	var count int64
	var err1, err2 error
	var resp []Row
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if pageNo != 0 {
			count, err1 = gm.findPageSelectTotal(selectSQL, selectParams, cons)
		}
	}()
	go func() {
		defer wg.Done()
		if pageSize != 0 {
			resp, err2 = gm.FindPageSelectRows(selectSQL, selectParams, pageNo, pageSize, cons, orders)
		}
	}()
	wg.Wait()
	return resp, count, lo.Ternary(err1 != nil, err1, err2)
}
