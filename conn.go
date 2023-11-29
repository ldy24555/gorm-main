package gorm

import (
	"fmt"
	"github.com/samber/lo"
	"github.com/spf13/viper"
	"gitops.sudytech.cn/guolei/gorm/driver"
	"gitops.sudytech.cn/guolei/gorm/driver/dm"
	"gitops.sudytech.cn/guolei/gorm/driver/ux"
	"gitops.sudytech.cn/guolei/gorm/driver/vb"
	"gitops.sudytech.cn/guolei/gorm/glog"
	"gitops.sudytech.cn/guolei/gorm/sys"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/dbresolver"
	"reflect"
	"strings"
	"sync"
)

var gormDB *Gorm
var gormOnce sync.Once

type Gorm struct {
	DB     *gorm.DB
	Option *Option
}

type Option struct {
	DbName         string
	Schema         string
	DataSourceName string
	DbType         int
	MaxConnections int
	LogLevel       logger.LogLevel
}

func GetConn() *Gorm {
	return gormDB
}

// 初始化后，调用GetConn()获取GORM连接
func (opt *Option) InitConn() {
	gormOnce.Do(func() {
		var err error
		gormDB, err = opt.GetInit()
		if err != nil {
			glog.PrintPanic(err, "")
		}
	})
}

func (opt *Option) GetInit() (*Gorm, error) {
	switch opt.DbType {
	case driver.DBTypeUxDB:
		return opt.initUx()
	case driver.DBTypeDmDB:
		return opt.initDm()
	case driver.DBTypeVbDB:
		return opt.initVb()
	default:
		return opt.initMySQL()
	}
}

func (opt *Option) GetTableName(table string) string {
	exp := driver.Exp{DbType: opt.DbType, Schema: opt.Schema}
	return exp.TableName(table)
}

func (opt *Option) GetTable(model interface{}) string {
	switch model.(type) {
	case string:
		return opt.GetTableName(model.(string))
	}
	fv := reflect.ValueOf(model)
	method := fv.MethodByName("TableName")
	tableName := fmt.Sprint(method.Call(nil)[0])
	return opt.GetTableName(tableName)
}

func (opt *Option) GetInsertSQL(model interface{}, data map[string]interface{}) *TranSQL {
	if model == nil || len(data) == 0 {
		return nil
	}
	keys := make(map[string]string)
	for k := range data {
		keys[strings.ToLower(k)] = k
	}
	var build strings.Builder
	build.WriteString(fmt.Sprintf("INSERT INTO %s(", opt.GetTable(model)))
	var params []interface{}
	first := true
	for _, tag := range sys.GetTags(model) {
		if v, ok := keys[strings.ToLower(tag.Name)]; ok {
			if g := tag.GormP; g != nil && g.Column != "" {
				params = append(params, toData(tag, data[v]))
				build.WriteString(lo.Ternary(first, g.Column, ","+g.Column))
				first = false
			}
		} else {
			if g := tag.GormP; g != nil && g.Column != "" {
				if s := tag.SormP; s != nil && s.Default != "" {
					params = append(params, s.Default)
					build.WriteString(lo.Ternary(first, g.Column, ","+g.Column))
					first = false
				}
			}
		}
	}
	build.WriteString(") VALUES")
	if len(params) == 0 {
		return nil
	}
	build.WriteString(toSqlIn(len(params)))
	return &TranSQL{SQL: build.String(), Params: params}
}

func (opt *Option) GetUpdateSQL(model interface{}, pks map[string]interface{}, data map[string]interface{}) *TranSQL {
	if model == nil || len(data) == 0 {
		return nil
	}
	keys := make(map[string]string)
	for k := range data {
		keys[strings.ToLower(k)] = k
	}
	var build strings.Builder
	build.WriteString(fmt.Sprintf("UPDATE %s t SET", opt.GetTable(model)))
	var params []interface{}
	first := true
	for _, tag := range sys.GetTags(model) {
		if v, ok := keys[strings.ToLower(tag.Name)]; ok {
			if g := tag.GormP; g != nil && g.Column != "" {
				params = append(params, toData(tag, data[v]))
				build.WriteString(fmt.Sprintf("%st.%s=?", lo.Ternary(first, "", ","), g.Column))
				first = false
			}
		}
	}
	first = true
	build.WriteString(" WHERE ")
	if len(pks) > 0 {
		for k, v := range pks {
			params = append(params, v)
			build.WriteString(fmt.Sprintf(" %st.%s=?", lo.Ternary(first, "", " AND "), k))
			first = false
		}
	}
	return &TranSQL{SQL: build.String(), Params: params}
}

func toData(prop sys.Prop, value interface{}) interface{} {
	var format string
	if v := prop.SormP; v != nil {
		format = v.Format
	}
	if v := prop.GormP; v != nil {
		switch v.Type {
		case "int":
			format = lo.Ternary(format == "bool", "boolInt", v.Type)
		default:
			format = v.Type
		}
	}
	switch format {
	case "bool":
		return intToBool(value)
	case "boolInt":
		return boolToInt(value)
	default:
		return value
	}
}

func boolToInt(value interface{}) int {
	v := fmt.Sprint(value)
	return lo.Ternary(v == "true" || v == "1", 1, 0)
}

func intToBool(value interface{}) bool {
	v := fmt.Sprint(value)
	return lo.Ternary(v == "true" || v == "1", true, false)
}

func toSqlIn(size int) string {
	var build strings.Builder
	build.WriteString("(")
	for i := 0; i < size; i++ {
		if i == 0 {
			build.WriteString("?")
		} else {
			build.WriteString(",?")
		}
	}
	build.WriteString(")")
	return build.String()
}

// 多次初始化，会覆盖上一个Gorm，最后一个起作用
func (opt *Option) ManyInitConn() {
	gormOnce.Do(func() {})
	var err error
	gormDB, err = opt.GetInit()
	if err != nil {
		glog.PrintPanic(err, "")
	}
}

func (opt *Option) initUx() (*Gorm, error) {
	dsn := toPgDsn("postgres", opt.DataSourceName)
	dsn = fmt.Sprintf("%s%s?%s", dsn, opt.DbName, "sslmode=disable")
	gorm, err := initDB(ux.New(ux.Config{DSN: dsn}), opt.MaxConnections, opt.LogLevel)
	return &Gorm{DB: gorm, Option: opt}, err
}

func (opt *Option) initDm() (*Gorm, error) {
	user, pErr := parseUser(opt.DataSourceName)
	if pErr != nil {
		return nil, pErr
	}
	dsn := toPgDsn("dm", opt.DataSourceName)
	dsn = strings.Replace(dsn, "://"+user+":", "://"+opt.DbName+":", 1)
	gorm, err := initDB(dm.New(dm.Config{DSN: dsn}), opt.MaxConnections, opt.LogLevel)
	return &Gorm{DB: gorm, Option: opt}, err
}

func (opt *Option) initVb() (*Gorm, error) {
	dsn := toPgDsn("postgres", opt.DataSourceName)
	dsn = fmt.Sprintf("%s%s?%s", dsn, opt.DbName, "sslmode=disable")
	gorm, err := initDB(vb.New(vb.Config{DSN: dsn}), opt.MaxConnections, opt.LogLevel)
	return &Gorm{DB: gorm, Option: opt}, err
}

func (opt *Option) initMySQL() (*Gorm, error) {
	dsn := toMyDsn(opt.DataSourceName)
	dsn = fmt.Sprintf("%s%s?%s", dsn, opt.DbName, "charset=utf8mb4&parseTime=true&loc=Asia%2fShanghai")
	gorm, err := initDB(mysql.New(mysql.Config{DSN: dsn}), opt.MaxConnections, opt.LogLevel)
	return &Gorm{DB: gorm, Option: opt}, err
}

func toMyDsn(dsn string) string {
	sMap := toDsnMap(dsn)
	host := sMap["host"]
	port := sMap["port"]
	user := sMap["user"]
	password := sMap["password"]
	if host != "" && port != "" {
		if user != "" && password != "" {
			dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/", user, password, host, port)
		}
	}
	return dsn
}

func toPgDsn(prefix, dsn string) string {
	sMap := toDsnMap(dsn)
	host := sMap["host"]
	port := sMap["port"]
	user := sMap["user"]
	password := sMap["password"]
	if host != "" && port != "" {
		if user != "" && password != "" {
			dsn = fmt.Sprintf("%s://%s:%s@%s:%s/", prefix, user, password, host, port)
		}
	}
	return dsn
}

func toDsnMap(dsn string) map[string]string {
	sMap := make(map[string]string)
	kvs := strings.Split(dsn, " ")
	for _, v := range kvs {
		bIndex := strings.Index(v, "=")
		if bIndex != -1 {
			sMap[v[0:bIndex]] = v[bIndex+1:]
		}
	}
	return sMap
}

func initDB(director gorm.Dialector, maxConn int, level logger.LogLevel) (*gorm.DB, error) {
	conn, err := gorm.Open(director,
		&gorm.Config{
			Logger: logger.Default.LogMode(level),
		})
	if err != nil {
		return nil, err
	}
	max := lo.Ternary(maxConn > 0, maxConn, 100)
	return conn, conn.Use(
		dbresolver.Register(dbresolver.Config{}).
			SetMaxOpenConns(max),
	)
}

func toLogLevel(level int32) logger.LogLevel {
	switch level {
	case 1:
		return logger.Silent
	case 2:
		return logger.Error
	case 3:
		return logger.Warn
	case 4:
		return logger.Info
	default:
		return logger.Error
	}
}

func GetDbType(dbType string) int {
	switch dbType {
	case "ux", "uxdb", "uxres", "5432":
		return driver.DBTypeUxDB
	case "dm", "dmdb", "5236", "5237":
		return driver.DBTypeDmDB
	case "vb", "vast", "vastbase", "5433":
		return driver.DBTypeVbDB
	default:
		return driver.DBTypeMySQL
	}
}

func GetOption(key, format string) *Option {
	switch format {
	case "yaml":
		return GetYamlOption(key)
	default:
		return GetKvFormatOption(key)
	}
}

func GetYamlOption(key string) *Option {
	configs := viper.GetStringMap(key)
	config := NewRow(configs)
	host := config.GetString("url")
	if host == "" {
		host = config.GetString("host")
	}
	dbType := strings.ToLower(config.GetString("dbType"))
	if dbType == "auto" {
		bIndex := strings.LastIndex(host, ":")
		if bIndex != -1 {
			dbType = host[bIndex+1:]
		}
	}
	dbt := GetDbType(dbType)
	user := config.GetString("user")
	if user == "" {
		user = config.GetString("username")
	}
	password := config.GetString("password")
	database := config.GetString("database")
	maxConn := config.GetInt32("maxConnections")
	logLevel := config.GetInt32("logLevel")
	maxConn = lo.Ternary(maxConn > 0, maxConn, 600)
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/", user, password, host)
	switch dbt {
	case driver.DBTypeUxDB:
		dsn = fmt.Sprintf("postgres://%s:%s@%s/", user, password, host)
	case driver.DBTypeDmDB:
		dsn = fmt.Sprintf("dm://%s:%s@%s/", user, password, host)
	case driver.DBTypeVbDB:
		dsn = fmt.Sprintf("postgres://%s:%s@%s/", user, password, host)
	}
	return &Option{DbType: dbt, DataSourceName: dsn, DbName: database, MaxConnections: int(maxConn), LogLevel: toLogLevel(logLevel)}
}

func GetKvFormatOption(key string) *Option {
	dbType := analyzeKvType(key)
	switch GetDbType(dbType) {
	case driver.DBTypeUxDB:
		return analyzeKvUxOption(key)
	case driver.DBTypeDmDB:
		return analyzeKvDmOption(key)
	case driver.DBTypeVbDB:
		return analyzeKvVbOption(key)
	default:
		return analyzeKvMySQLOption(key)
	}
}

func analyzeKvType(key string) string {
	prefix := lo.Ternary(key == "", "", key+".")
	dbType := strings.ToLower(viper.GetString(prefix + "dbType"))
	if dbType != "auto" {
		return dbType
	}
	dsn := viper.GetString(prefix + "dataSourceName")
	bIndex := strings.LastIndex(dsn, ":")
	eIndex := strings.LastIndex(dsn, ")")
	if bIndex != -1 {
		if eIndex > bIndex {
			dbType = dsn[bIndex+1 : eIndex]
		}
	}
	return dbType
}

func analyzeKvUxOption(key string) *Option {
	prefix := lo.Ternary(key == "", "", key+".")
	dbName := viper.GetString(prefix + "dbName")
	maxConn := viper.GetInt(prefix + "dbMaxConnections")
	dsn := viper.GetString(prefix + "dataSourceName.ux")
	logLevel := viper.GetInt32(prefix + "logLevel")
	return &Option{DbType: driver.DBTypeUxDB, DataSourceName: dsn, DbName: dbName, MaxConnections: maxConn, LogLevel: toLogLevel(logLevel)}
}

func analyzeKvDmOption(key string) *Option {
	prefix := lo.Ternary(key == "", "", key+".")
	dbName := viper.GetString(prefix + "dbName")
	maxConn := viper.GetInt(prefix + "dbMaxConnections")
	dsn := viper.GetString(prefix + "dataSourceName.dm")
	logLevel := viper.GetInt32(prefix + "logLevel")
	return &Option{DbType: driver.DBTypeDmDB, DataSourceName: dsn, DbName: dbName, MaxConnections: maxConn, LogLevel: toLogLevel(logLevel)}
}

func analyzeKvVbOption(key string) *Option {
	prefix := lo.Ternary(key == "", "", key+".")
	dbName := viper.GetString(prefix + "dbName")
	maxConn := viper.GetInt(prefix + "dbMaxConnections")
	dsn := viper.GetString(prefix + "dataSourceName.vb")
	logLevel := viper.GetInt32(prefix + "logLevel")
	return &Option{DbType: driver.DBTypeVbDB, DataSourceName: dsn, DbName: dbName, MaxConnections: maxConn, LogLevel: toLogLevel(logLevel)}
}

func analyzeKvMySQLOption(key string) *Option {
	prefix := lo.Ternary(key == "", "", key+".")
	dbName := viper.GetString(prefix + "dbName")
	maxConn := viper.GetInt(prefix + "dbMaxConnections")
	dsn := viper.GetString(prefix + "dataSourceName")
	logLevel := viper.GetInt32(prefix + "logLevel")
	return &Option{DbType: driver.DBTypeMySQL, DataSourceName: dsn, DbName: dbName, MaxConnections: maxConn, LogLevel: toLogLevel(logLevel)}
}
