package glog

func PrintPanic(err error, msg string) {
	if err != nil {
		if msg == "" {
			panic(err)
		} else {
			panic(msg)
		}
	}
}
