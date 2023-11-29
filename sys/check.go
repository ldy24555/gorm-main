package sys

import (
	"fmt"
	"github.com/samber/lo"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type Prop struct {
	Name  string
	GormP *GormP
	SormP *SormP
}

type GormP struct {
	Column   string //字段
	Type     string //类型
	Pk       bool   //主键
	AutoInc  bool   //自增
	Required bool   //必须
	Default  string //默认值
}

type SormP struct {
	Min         string   //最小值
	Max         string   //最大值
	MinLen      string   //最小长度
	MaxLen      string   //最大长度
	Format      string   //格式校验
	Pattern     string   //正则校验
	Default     string   //默认值
	Generate    string   //值生成方式
	Enumeration []string //值枚举范围
}

func toGormP(tag string) *GormP {
	gorm := &GormP{}
	tags := strings.Split(tag, ";")
	for _, v := range tags {
		vs := strings.Split(v, ":")
		if len(vs) == 1 {
			switch vs[0] {
			case "autoIncrement":
				gorm.AutoInc = true
			case "not null":
				gorm.Required = true
			case "primaryKey":
				gorm.Pk = true
				gorm.Required = true
			}
		} else {
			if len(vs) == 2 {
				switch vs[0] {
				case "type":
					if i := strings.Index(vs[1], "("); i == -1 {
						gorm.Type = vs[1]
					} else {
						gorm.Type = vs[1][0:i]
					}
				case "column":
					gorm.Column = vs[1]
				case "default":
					gorm.Default = vs[1]
				case "autoIncrement":
					gorm.AutoInc = "true" == vs[1]
				}
			}
		}
	}
	return lo.Ternary(gorm.Column == "", nil, gorm)
}

func toArray(req string) []string {
	reqS := strings.Split(req, ",")
	var result []string
	for _, v := range reqS {
		if v != "" {
			result = append(result, v)
		}
	}
	return result
}

func toSormP(tag string) *SormP {
	gorm := &SormP{}
	tags := strings.Split(tag, ";")
	for _, v := range tags {
		vs := strings.Split(v, ":")
		if len(vs) == 1 {
			switch vs[0] {
			case "generate":
				gorm.Generate = "generate"
			}
		} else {
			if len(vs) == 2 {
				switch vs[0] {
				case "min":
					gorm.Min = vs[1]
				case "max":
					gorm.Max = vs[1]
				case "minLen":
					gorm.MinLen = vs[1]
				case "maxLen":
					gorm.MaxLen = vs[1]
				case "format":
					gorm.Format = vs[1]
				case "pattern":
					gorm.Pattern = vs[1]
				case "default":
					gorm.Default = vs[1]
				case "generate":
					gorm.Generate = vs[1]
				case "enumeration":
					gorm.Enumeration = toArray(vs[1])
				}
			}
		}
	}
	return lo.Ternary(tag == "", nil, gorm)
}

var propMap sync.Map

func GetPk(model interface{}) Prop {
	props := GetTags(model)
	for _, v := range props {
		if p := v.GormP; p != nil {
			if p.Pk {
				return v
			}
		}
	}
	return Prop{}
}

func GetPks(model interface{}) []Prop {
	var result []Prop
	props := GetTags(model)
	for _, v := range props {
		if p := v.GormP; p != nil {
			if p.Pk {
				result = append(result, v)
			}
		}
	}
	return result
}

func GetTags(model interface{}) []Prop {
	key := fmt.Sprint(reflect.ValueOf(model).Type())
	if v, ok := propMap.Load(key); ok {
		return v.([]Prop)
	}
	var props []Prop
	if t := reflect.TypeOf(model); t != nil {
		e := t.Elem()
		for i := 0; i < e.NumField(); i++ {
			props = append(props, GetTagByField(e.Field(i)))
		}
	}
	propMap.Store(key, props)
	return props
}

func GetTag(model interface{}, name string) (Prop, bool) {
	e := reflect.TypeOf(model).Elem()
	v, ok := e.FieldByName(name)
	if !ok {
		return Prop{}, false
	}
	field := v.Name
	gorm := toGormP(v.Tag.Get("gorm"))
	sorm := toSormP(v.Tag.Get("sorm"))
	return Prop{Name: field, GormP: gorm, SormP: sorm}, true
}

func GetTagByField(field reflect.StructField) Prop {
	name := field.Name
	gorm := toGormP(field.Tag.Get("gorm"))
	sorm := toSormP(field.Tag.Get("sorm"))
	return Prop{Name: name, GormP: gorm, SormP: sorm}
}

func CheckRequire(value interface{}, check Prop) IGormErr {
	if v := check.GormP; v != nil {
		if v.Required {
			if value == nil || fmt.Sprint(value) == "" {
				return NewMessage(PropNoExistCode, fmt.Sprintf("%s不存在或者为空", check.Name))
			}
		}
	}
	return nil
}

func CheckRequired(value interface{}, check Prop) IGormErr {
	ck := false
	if v := check.GormP; v != nil {
		if v.Required {
			if value == nil || fmt.Sprint(value) == "" {
				ck = true
			}
		}
	}
	if !ck {
		return nil
	}
	if check.GormP.AutoInc {
		return nil
	}
	if check.GormP.Default != "" {
		return nil
	}
	if v := check.SormP; v != nil {
		if v.Default != "" || v.Generate != "" {
			return nil
		}
	}
	return NewMessage(PropNoExistCode, fmt.Sprintf("%s不存在或者为空", check.Name))
}

func CheckFormat(value interface{}, check Prop) IGormErr {
	var format string
	if v := check.SormP; v != nil {
		format = v.Format
	}
	if format == "" {
		if v := check.GormP; v != nil {
			switch v.Type {
			case "bool":
				format = "bool"
			case "int", "bigint":
				format = "number"
			}
		}
	}
	switch format {
	case "bool":
		src := fmt.Sprint(value)
		if src != "1" && src != "0" {
			if src != "true" && src != "false" {
				return NewMessage(PropErrorCode, fmt.Sprintf("%s必须为：1/0,true/false", check.Name))
			}
		}
	case "number":
		src, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		if err != nil {
			return NewMessage(PropNoNumberCode, fmt.Sprintf("%s非数值类型", check.Name))
		}
		if s := check.SormP; s != nil {
			if s.Min != "" {
				min, _ := strconv.ParseInt(s.Min, 10, 64)
				if src < min {
					return NewMessage(PropErrorCode, fmt.Sprintf("%s值不能小于%s", check.Name, s.Min))
				}
			}
			if s.Max != "" {
				max, _ := strconv.ParseInt(s.Max, 10, 64)
				if src > max {
					return NewMessage(PropErrorCode, fmt.Sprintf("%s值不能大于%s", check.Name, s.Max))
				}
			}
		}
	}
	return nil
}

func VerifyCreateInfo(model interface{}, data map[string]interface{}) IGormErr {
	reqData := make(map[string]interface{})
	if len(data) > 0 {
		for k, v := range data {
			reqData[strings.ToLower(k)] = v
		}
	}
	check0 := []func(interface{}, Prop) IGormErr{
		CheckRequired,
	}
	check1 := []func(interface{}, Prop) IGormErr{
		CheckFormat,
	}
	for _, tag := range GetTags(model) {
		value := reqData[strings.ToLower(tag.Name)]
		for _, cf := range check0 {
			if err := cf(value, tag); err != nil {
				return err
			}
		}
		if value != nil && fmt.Sprint(value) != "" {
			for _, cf := range check1 {
				if err := cf(value, tag); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func VerifyUpdateInfo(model interface{}, data map[string]interface{}) IGormErr {
	if len(data) == 0 {
		return nil
	}
	reqData := make(map[string]interface{})
	for k, v := range data {
		reqData[strings.ToLower(k)] = v
	}
	check0 := []func(interface{}, Prop) IGormErr{
		CheckRequire,
	}
	check1 := []func(interface{}, Prop) IGormErr{
		CheckFormat,
	}
	e := reflect.TypeOf(model).Elem()
	for i := 0; i < e.NumField(); i++ {
		name := strings.ToLower(e.Field(i).Name)
		if value, ok := reqData[name]; ok {
			tag := GetTagByField(e.Field(i))
			for _, cf := range check0 {
				if err := cf(value, tag); err != nil {
					return err
				}
			}
			if value != "" {
				for _, cf := range check1 {
					if err := cf(value, tag); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
