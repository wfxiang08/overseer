package main

import (
	"net/http"
	"time"

	"fmt"
	"github.com/wfxiang08/overseer"
)

var BuildID = "1"

func server(state overseer.State) {
	// 核心入口？

	// 对外提供服务
	http.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d, _ := time.ParseDuration(r.URL.Query().Get("d"))
		fmt.Printf("BuildID: %s\n", BuildID)
		time.Sleep(d)
	}))

	http.Serve(state.Listener, nil)
}

func main() {
	overseer.Run(overseer.Config{
		Program: server, // 执行的函数体
		Address: ":5001",
		Debug:   false,
	})
}
