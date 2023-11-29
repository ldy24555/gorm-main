package driver

import (
	"fmt"
	"testing"
)

func TestExp_ExecSQL(t *testing.T) {
	explain := Exp{DbType: DBTypeUxDB, Schema: ""}
	var exec []string
	exec = append(exec, "DELETE FROM T_USER")
	exec = append(exec, "INSERT INTO T_USER VALUE(?,?)")
	exec = append(exec, "INSERT INTO T_USER(id,name) VALUES(?,?)")
	exec = append(exec, "INSERT INTO T_USER SELECT * FROM T_USER t")
	exec = append(exec, "INSERT INTO T_USER(id,name)  SELECT * FROM T_USER t")
	exec = append(exec, "DELETE from T_USER")
	exec = append(exec, "DELETE from T_USER where LoginName=? and Field6>0 or Field8 = 1")
	exec = append(exec, "=﻿INSERT INTO T_CUC_ORG(id,code,name,sort,enable,levelCode,parentId,pinyin,firstLetter,firstLetters,path,origin,owner) VALUES('1','sys_001','系统顶层机构','100','1','01','-1','xitongdingcengjigou','x','xtdcjg','/1/','sys','sys')")
	for _, v := range exec {
		fmt.Println(explain.ExecSQL(v))
	}
	var query []string
	query = append(query, "SELECT * FROM \"T_USER\" t INNERT JOIN T_ORG t2 WHERE t.LoginName=?")
	for _, v := range query {
		fmt.Println(explain.QuerySQL(v))
	}
}
