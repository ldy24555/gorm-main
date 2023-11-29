package driver

import (
	"github.com/samber/lo"
	"regexp"
	"strings"
)

const (
	DBTypeMySQL = 0
	DBTypeUxDB  = 11 //优炫
	DBTypeDmDB  = 12 //达梦
	DBTypeVbDB  = 13 //海量
)

func GetDbName(dbType int) string {
	switch dbType {
	case DBTypeUxDB:
		return "ux"
	case DBTypeDmDB:
		return "dm"
	case DBTypeVbDB:
		return "vast"
	default:
		return "MySQL"
	}
}

func GetDbDisplayName(dbType int) string {
	switch dbType {
	case DBTypeUxDB:
		return "优炫"
	case DBTypeDmDB:
		return "达梦"
	case DBTypeVbDB:
		return "海量"
	default:
		return "MySQL"
	}
}

type Exp struct {
	DbType int
	Schema string
}

type Explain interface {
	ExecSQL(sql string) string
	QuerySQL(sql string) string
	TableName(tableName string) string
}

func (exp Exp) ExecSQL(sql string) string {
	switch exp.DbType {
	case DBTypeUxDB, DBTypeDmDB, DBTypeVbDB:
		return parseExec(sql)
	default:
		return sql
	}
}

func (exp Exp) QuerySQL(sql string) string {
	switch exp.DbType {
	case DBTypeUxDB, DBTypeDmDB, DBTypeVbDB:
		return parseQuery(sql)
	default:
		return sql
	}
}

func (exp Exp) TableName(tableName string) string {
	switch exp.DbType {
	case DBTypeUxDB, DBTypeDmDB, DBTypeVbDB:
		if exp.Schema == "" {
			return "\"" + tableName + "\""
		} else {
			return "\"" + exp.Schema + "\"." + "\"" + tableName + "\""
		}
	}
	return tableName
}

var regexQ = []*regexp.Regexp{regexQ1, regexQ2}
var regexQ1 = regexp.MustCompile("[ ,][TtSsVv]_\\w+[( ]")    // 用于处理SQL中T_、t_、V_、v_、S_、s_前缀的表名
var regexQ2 = regexp.MustCompile("\\.\\w+[)!=+-><,&|^*/% ]") // 用于处理SQL中.前缀的字段名，即表(别名).前缀的字段

func parseQuery(sql string) string {
	sql = sql + " "
	for _, regex := range regexQ {
		match := regex.FindAllString(sql, -1)
		if len(match) > 0 {
			for _, v := range match {
				vLen := len(v)
				sql = strings.Replace(sql, v, v[0:1]+"\""+v[1:vLen-1]+"\""+v[vLen-1:], -1)
			}
		}
	}
	return strings.TrimSpace(sql)
}

func parseExec(sql string) string {
	uSQL := strings.ToUpper(strings.TrimSpace(sql))
	if strings.HasPrefix(uSQL, "INSERT INTO ") {
		sql = parseInsert(sql)
	} else {
		if strings.HasPrefix(uSQL, "DELETE FROM ") {
			sql = parseDelete(sql)
		}
	}
	return parseQuery(sql)
}

func parseInsert(sql string) string {
	uSQL := strings.ToUpper(sql)
	bIndex := strings.Index(uSQL, "(")
	if bIndex == -1 {
		return sql
	}
	eIndex := strings.Index(uSQL, ") VALUES")
	if eIndex == -1 {
		eIndex = strings.Index(uSQL, ") SELECT ")
	}
	if eIndex < bIndex {
		return sql
	}
	return sql[0:bIndex+1] + parseProps(sql[bIndex+1:eIndex], ",") + sql[eIndex:]
}

func parseProps(prop, sep string) string {
	var resp strings.Builder
	props := strings.Split(prop, sep)
	for k, v := range props {
		if v != "" {
			resp.WriteString(lo.Ternary(k == 0, "", sep))
			resp.WriteString("\"" + v + "\"")
		}
	}
	return resp.String()
}

var regexD1 = regexp.MustCompile("\\s+\\w+[=!<>]")
var regexD2 = regexp.MustCompile("\\s+\\w+\\s+(([=!<>])|((?i)in\\s+)|((?i)like\\s+)|((?i)not\\s+))")

func parseDelete(sql string) string {
	mh := regexD1.FindAllString(sql, -1)
	if len(mh) > 0 {
		for _, v := range mh {
			bIndex := strings.LastIndex(v, " ")
			sql = strings.Replace(sql, v, v[0:bIndex+1]+"\""+v[bIndex+1:len(v)-1]+"\""+v[len(v)-1:], -1)
		}
	}
	match := regexD2.FindAllString(sql, -1)
	if len(match) > 0 {
		for _, v := range match {
			nv := strings.TrimSpace(v)
			prop := nv[0:strings.Index(nv, " ")]
			bIndex := strings.Index(v, prop)
			sql = strings.Replace(sql, v, v[0:bIndex]+"\""+prop+"\""+v[bIndex+len(prop):], -1)
		}
	}
	return sql
}
