package sys

import (
	"fmt"
	"gitops.sudytech.cn/guolei/gorm/testdata"
	"testing"
)

func TestGetTag(t *testing.T) {
	alg := &testdata.Algorithm{}
	p, _ := GetTag(alg, "Name")
	fmt.Println(p)
	fmt.Println(p.GormP)
	fmt.Println(p.SormP)
}

func TestCheckRequired(t *testing.T) {
	var value interface{}
	alg := &testdata.Algorithm{}
	p, _ := GetTag(alg, "Name")
	fmt.Println(p.GormP)
	value = 1
	fmt.Println(CheckFormat(value, p))
}

func TestCheckBaseInfo(t *testing.T) {
	value := make(map[string]interface{})
	value["name"] = "11"
	alg := &testdata.Algorithm{}
	fmt.Println(CheckBaseInfo(alg, value))
}

func TestCheckDataInfo(t *testing.T) {
	value := make(map[string]interface{})
	value["name"] = "11"
	alg := &testdata.Algorithm{}
	fmt.Println(CheckDataInfo(alg, value))
}
