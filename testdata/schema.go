package testdata

import "time"

const (
	TableAlgorithm = "T_TEST_ALGORITHM"
)

func (*Algorithm) TableName() string {
	return TableAlgorithm
}

type Algorithm struct {
	Id         int64      `gorm:"column:id;type:bigint;primaryKey;autoIncrement:true;" json:"id"`
	Code       string     `gorm:"column:code;type:varchar(40);uniqueIndex:idx_code;" json:"code" sorm:"default:测试"`
	Name       string     `gorm:"column:name;type:varchar(60);not null" json:"name"`
	Type       int32      `gorm:"column:type;type:int;not null;default:1;" json:"type"`
	Impl       string     `gorm:"column:impl;type:varchar(60);" json:"impl"`
	Sort       int64      `gorm:"column:sort;type:bigint;not null;default:10000;" json:"sort"`
	Audit      int32      `gorm:"column:audit;type:int;not null;default:0;" json:"audit"`
	Enable     int32      `gorm:"column:enable;type:int;not null;default:0;" json:"enable"`
	Config     string     `gorm:"column:config;type:varchar(2000);" json:"config"`
	Version    string     `gorm:"column:version;type:varchar(20);" json:"version"`
	UpdateTime *time.Time `gorm:"column:updateTime;type:datetime;" json:"updateTime"`
	CreateTime *time.Time `gorm:"column:createTime;type:datetime;not null;default:CURRENT_TIMESTAMP;" json:"createTime"`
}
