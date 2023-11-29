package gorm

import (
	"fmt"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"time"
)

type QueryOrder struct {
	Asc       bool
	FieldName string
}

type ConsWrapper interface {
	IsCons() bool
	AsConstraint() *Constraint
	IsOr() bool
	AsOrConstraint() *OrConstraint
	IsAnd() bool
	AsAndConstraint() *AndConstraint
}

type Constraint struct {
	Compare int
	Name    string
	Value   interface{}
}

func (Constraint) IsCons() bool {
	return true
}

func (cons Constraint) AsConstraint() *Constraint {
	return &cons
}

func (Constraint) IsOr() bool {
	return false
}

func (Constraint) AsOrConstraint() *OrConstraint {
	return nil
}

func (Constraint) IsAnd() bool {
	return false
}

func (Constraint) AsAndConstraint() *AndConstraint {
	return nil
}

type OrConstraint struct {
	Wrapper []ConsWrapper
}

func (OrConstraint) IsCons() bool {
	return false
}

func (OrConstraint) AsConstraint() *Constraint {
	return nil
}

func (OrConstraint) IsOr() bool {
	return true
}

func (cons OrConstraint) AsOrConstraint() *OrConstraint {
	return &cons
}

func (OrConstraint) IsAnd() bool {
	return false
}

func (OrConstraint) AsAndConstraint() *AndConstraint {
	return nil
}

type AndConstraint struct {
	Wrapper []ConsWrapper
}

func (AndConstraint) IsCons() bool {
	return false
}

func (AndConstraint) AsConstraint() *Constraint {
	return nil
}

func (AndConstraint) IsOr() bool {
	return false
}

func (cons AndConstraint) AsOrConstraint() *OrConstraint {
	return nil
}

func (AndConstraint) IsAnd() bool {
	return true
}

func (cons AndConstraint) AsAndConstraint() *AndConstraint {
	return &cons
}

const (
	CompareEqual              = 0  //=
	CompareLessThan           = 1  //<
	CompareLike               = 2  //Like
	CompareLessThanOrEqual    = 3  //<=
	CompareGreaterThan        = 4  //>
	CompareGreaterThanOrEqual = 5  //>=
	CompareIn                 = 6  //In
	CompareNotEqual           = 7  //!=
	CompareNotIn              = 8  //Not In
	CompareNotLike            = 9  //Not Like
	CompareIsNull             = 10 //Is Null
	CompareNotNull            = 11 //Is Not Null
)

func GetSymbol(compare int) string {
	switch compare {
	case CompareEqual:
		return "="
	case CompareLessThan:
		return "<"
	case CompareLike:
		return "LIKE"
	case CompareLessThanOrEqual:
		return "<="
	case CompareGreaterThan:
		return ">"
	case CompareGreaterThanOrEqual:
		return ">="
	case CompareIn:
		return "IN"
	case CompareNotEqual:
		return "!="
	case CompareNotIn:
		return "NOT IN"
	case CompareNotLike:
		return "NOT LIKE"
	case CompareIsNull:
		return "IS NULL"
	case CompareNotNull:
		return "IS NOT NULL"
	default:
		return "="
	}
}

func GenCons(name string, value interface{}, compare int) Constraint {
	if compare != CompareLike {
		return Constraint{Name: name, Value: value, Compare: compare}
	}
	return Constraint{Name: name, Value: "%" + fmt.Sprint(value) + "%", Compare: compare}
}

func GenConsWrapper(constraint map[string]interface{}, compareMatch map[string]int, orMatch map[string]interface{}) []ConsWrapper {
	var or []ConsWrapper
	var wrapper []ConsWrapper
	if len(constraint) == 0 {
		return wrapper
	}
	if orMatch == nil {
		orMatch = make(map[string]interface{}, 0)
	}
	if compareMatch == nil {
		compareMatch = make(map[string]int, 0)
	}
	for k, v := range constraint {
		if _, ok0 := orMatch[k]; ok0 {
			if compare, ok := compareMatch[k]; ok {
				or = append(or, GenCons(k, v, compare))
			} else {
				or = append(or, GenCons(k, v, CompareEqual))
			}
		} else {
			if compare, ok1 := compareMatch[k]; ok1 {
				wrapper = append(wrapper, GenCons(k, v, compare))
			} else {
				wrapper = append(wrapper, GenCons(k, v, CompareEqual))
			}
		}
	}
	if len(or) == 0 {
		return wrapper
	}
	return append(wrapper, OrConstraint{Wrapper: or})
}

func GenOrderSQL(build *strings.Builder, orders []QueryOrder) {
	if len(orders) > 0 {
		build.WriteString(" ORDER BY ")
		for k, v := range orders {
			build.WriteString(lo.Ternary(k == 0, "", ","))
			build.WriteString(fmt.Sprintf("%s %s", v.FieldName, lo.Ternary(v.Asc, "ASC", "DESC")))
		}
	}
}

func GenWhereSQL(build *strings.Builder, cons []ConsWrapper) []interface{} {
	var params []interface{}
	if len(cons) == 0 {
		return params
	}
	flag := false
	for k, v := range cons {
		if k != 0 {
			build.WriteString(" AND ")
		} else {
			if !v.IsCons() {
				build.WriteString(" AND ")
			} else {
				flag = true
				build.WriteString(" AND (")
			}
		}
		param := genConsSQL(build, v)
		params = append(params, param...)
	}
	if flag {
		build.WriteString(")")
	}
	return params
}

func genConsSQL(build *strings.Builder, wp ConsWrapper) []interface{} {
	var params []interface{}
	if wp.IsCons() {
		cons := wp.AsConstraint()
		params = append(params, cons.Value)
		build.WriteString(fmt.Sprintf("%s %s ?", cons.Name, GetSymbol(cons.Compare)))
	} else if wp.IsOr() {
		or := wp.AsOrConstraint()
		build.WriteString("(")
		if or != nil {
			for k, v := range or.Wrapper {
				build.WriteString(lo.Ternary(k == 0, "", " OR "))
				orParam := genConsSQL(build, v)
				params = append(params, orParam...)
			}
		}
		build.WriteString(")")
	} else if wp.IsAnd() {
		and := wp.AsAndConstraint()
		build.WriteString("(")
		if and != nil {
			for k, v := range and.Wrapper {
				build.WriteString(lo.Ternary(k == 0, "", " AND "))
				andParam := genConsSQL(build, v)
				params = append(params, andParam...)
			}
		}
		build.WriteString(")")
	}
	return params
}

func StrCondition(constraint map[string]interface{}, name string, value string) {
	if value != "" {
		constraint[name] = value
	}
}

func IntCondition(constraint map[string]interface{}, name string, value interface{}) {
	flag := false
	switch value.(type) {
	case bool:
		flag = true
		constraint[name] = lo.Ternary(value.(bool), 1, 0)
	case string:
		if value.(string) == "true" {
			flag = true
			constraint[name] = 1
		} else {
			if value.(string) == "false" {
				flag = true
				constraint[name] = 0
			}
		}
	}
	if !flag {
		newValue := cast.ToInt64(value)
		if newValue != 0 {
			if newValue == -1 {
				constraint[name] = 0
			} else {
				constraint[name] = newValue
			}
		}
	}
}

func Int32Condition(constraint map[string]interface{}, name string, value int32) {
	if value != 0 {
		if value == -1 {
			constraint[name] = 0
		} else {
			constraint[name] = value
		}
	}
}

func Int64Condition(constraint map[string]interface{}, name string, value int64) {
	if value != 0 {
		if value == -1 {
			constraint[name] = 0
		} else {
			constraint[name] = value
		}
	}
}

func TimeConstraint(wrapper []ConsWrapper, name string, value *timestamppb.Timestamp, compare int) []ConsWrapper {
	if value == nil {
		return wrapper
	}
	dateTime := value.AsTime().In(time.Local).Format("2006-01-02 15:04:05")
	return append(wrapper, Constraint{Name: name, Value: dateTime, Compare: compare})
}

func IntThreshold(constraint map[string]interface{}, name string, value interface{}, threshold int) {
	flag := false
	switch value.(type) {
	case bool:
		flag = true
		constraint[name] = lo.Ternary(value.(bool), 1, 0)
	case string:
		if value.(string) == "true" {
			flag = true
			constraint[name] = 1
		} else {
			if value.(string) == "false" {
				flag = true
				constraint[name] = 0
			}
		}
	}
	if !flag {
		newValue := cast.ToInt64(value)
		if newValue > int64(threshold) {
			constraint[name] = newValue
		}
	}
}

func Int32Threshold(constraint map[string]interface{}, name string, value int32, threshold int32) {
	if value > threshold {
		constraint[name] = value
	}
}

func Int64Threshold(constraint map[string]interface{}, name string, value int64, threshold int64) {
	if value > threshold {
		constraint[name] = value
	}
}
