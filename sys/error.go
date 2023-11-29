package sys

import "errors"

const (
	FailCode         = 1    //失败
	SuccessCode      = 0    //成功
	NoExistCode      = 4000 //不存在
	PropNoExistCode  = 4100 //属性值为空或不存在
	PropNoNumberCode = 4110 //属性值非数字类型
	PropErrorCode    = 4120 //属性值错误，超过属性值范围
	NoPermitCode     = 3500 //无权限操作
	MarkDeleteCode   = 3002 //被标记删除
)

type GormErr struct {
	Code  int32
	Error error
}

type IGormErr interface {
	GetCode() int32
	GetError() error
	GetMessage() string
}

func (se GormErr) GetCode() int32 {
	return se.Code
}

func (se GormErr) GetError() error {
	return se.Error
}

func (se GormErr) GetMessage() string {
	if v := se.Error; v == nil {
		return ""
	} else {
		return v.Error()
	}
}

func ErrIF(err error) IGormErr {
	if err == nil {
		return nil
	} else {
		return NewErr(err)
	}
}

func NewErr(err error) IGormErr {
	return NewError(FailCode, err)
}

func NewError(code int32, err error) IGormErr {
	return GormErr{Code: code, Error: err}
}

func NewMessage(code int32, message string) IGormErr {
	return NewError(code, errors.New(message))
}
