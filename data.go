package gorm

import (
	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reflect"
	"strings"
	"time"
)

type Row struct {
	km   map[string]string
	data map[string]interface{}
}

func (r Row) IsEmpty() bool {
	return len(r.data) == 0
}

func (r Row) GetInt(key string) int {
	key = r.rKey(key)
	return cast.ToInt(r.data[key])
}

func (r Row) GetInt32(key string) int32 {
	key = r.rKey(key)
	return cast.ToInt32(r.data[key])
}

func (r Row) GetInt64(key string) int64 {
	key = r.rKey(key)
	return cast.ToInt64(r.data[key])
}

func (r Row) GetBool(key string) bool {
	key = r.rKey(key)
	return cast.ToBool(r.data[key])
}

func (r Row) GetString(key string) string {
	key = r.rKey(key)
	return cast.ToString(r.data[key])
}

func (r Row) GetData() map[string]interface{} {
	return r.data
}

func (r Row) GetInterface(key string) interface{} {
	key = r.rKey(key)
	return r.data[key]
}

func (r Row) Delete(key string) {
	key = r.rKey(key)
	delete(r.data, key)
	delete(r.km, strings.ToLower(key))
}

func (r Row) Put(key string, value interface{}) {
	key = r.rKey(key)
	delete(r.data, key)
	r.data[key] = value
	r.km[strings.ToLower(key)] = key
}

func (r Row) GetKeys() []string {
	var keys []string
	if len(r.data) > 0 {
		for k := range r.data {
			keys = append(keys, k)
		}
	}
	return keys
}

func (r Row) ContainsKey(key string) bool {
	_, ok := r.data[r.rKey(key)]
	return ok
}

func (r Row) GetTime(key string) *time.Time {
	key = r.rKey(key)
	if v, ok := r.data[key]; ok {
		if v != nil {
			resp := cast.ToTime(v)
			return &resp
		}
	}
	return nil
}

func (r Row) GetTimestamp(key string) *timestamppb.Timestamp {
	t := r.GetTime(key)
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func (r Row) Decode(resp interface{}) error {
	if len(r.data) == 0 {
		return nil
	}
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata:   nil,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(decodeHookFunc()),
		Result:     resp,
		TagName:    "json",
	})
	if err != nil {
		return err
	}
	newReq := make(map[string]interface{})
	for k, v := range r.data {
		var flag = true
		if v != nil {
			switch v.(type) {
			case string:
				flag = v.(string) != ""
			}
		}
		if flag {
			newReq[k] = v
		}
	}
	return decoder.Decode(newReq)
}

func Rows(req []map[string]interface{}) []Row {
	var resp []Row
	if len(req) > 0 {
		for _, v := range req {
			resp = append(resp, NewRow(v))
		}
	}
	return resp
}

func NewRow(data map[string]interface{}) Row {
	if data == nil {
		data = make(map[string]interface{})
	}
	km := make(map[string]string)
	if len(data) > 0 {
		for k := range data {
			km[strings.ToLower(k)] = k
		}
	}
	return Row{data: data, km: km}
}

func (r Row) rKey(key string) string {
	lKey := strings.ToLower(key)
	if len(r.km) > 0 {
		if v, ok := r.km[lKey]; ok {
			key = v
		}
	}
	return key
}

func decodeHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		switch t.Kind() {
		case reflect.Bool:
			return cast.ToBool(data), nil
		default:
			if !isDataTime(t) {
				return data, nil
			} else {
				return toDataTime(data)
			}
		}
	}
}

func isDataTime(t reflect.Type) bool {
	return reflect.DeepEqual(t, reflect.TypeOf(timestamppb.Timestamp{}))
}

func toTimestamp(dateTime string) (*timestamppb.Timestamp, error) {
	resp, err := parseTime(dateTime)
	if resp == nil {
		return nil, err
	}
	return timestamppb.New(*resp), err
}

func toDataTime(v interface{}) (*timestamppb.Timestamp, error) {
	switch v.(type) {
	case int64:
		return timestamppb.New(time.Unix(0, v.(int64)*int64(time.Millisecond))), nil
	case float64:
		return timestamppb.New(time.Unix(0, int64(v.(float64))*int64(time.Millisecond))), nil
	case time.Time:
		return timestamppb.New(v.(time.Time)), nil
	case []uint8:
		return toTimestamp(string(v.([]uint8)))
	default:
		return toTimestamp(v.(string))
	}
}

const (
	dateFormatPattern         = "2006-01-02"
	dateTimeFormatPattern     = "2006-01-02 15:04:05"
	dateTimeHHFormatPattern   = "2006-01-02 15"
	dateTimeHHmmFormatPattern = "2006-01-02 15:04"
)

func parseTime(datetime string) (*time.Time, error) {
	if datetime == "" {
		return nil, nil
	}
	index := strings.Index(datetime, ".")
	if index != -1 {
		datetime = strings.Replace(datetime[0:index], "T", " ", -1)
	} else {
		sIndex := strings.Index(datetime, "+")
		if sIndex != -1 {
			datetime = strings.Replace(datetime[0:sIndex], "T", " ", -1)
		}
	}
	if len(datetime) == 10 {
		showTime, pErr := time.ParseInLocation(dateFormatPattern, datetime, time.Local)
		return lo.Ternary(pErr != nil, nil, &showTime), pErr
	} else {
		if len(datetime) == 13 {
			showTime, pErr := time.ParseInLocation(dateTimeHHFormatPattern, datetime, time.Local)
			return lo.Ternary(pErr != nil, nil, &showTime), pErr
		}
	}
	if len(datetime) == 16 {
		showTime, pErr := time.ParseInLocation(dateTimeHHmmFormatPattern, datetime, time.Local)
		return lo.Ternary(pErr != nil, nil, &showTime), pErr
	} else {
		showTime, pErr := time.ParseInLocation(dateTimeFormatPattern, datetime, time.Local)
		return lo.Ternary(pErr != nil, nil, &showTime), pErr
	}
}
