package main

import (
	bitcask "bitcask-kv"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

var db *bitcask.DB

func init() {
	// 初始化 db 实例
	var err error
	options := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-http")
	options.DirPath = dir
	db, err = bitcask.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db : %v", err))
	}

}

func handlePut(ctx *gin.Context) {
	var data map[string]string
	if err := ctx.ShouldBind(&data); err != nil {
		ctx.String(http.StatusBadRequest, fmt.Sprintf("parse error:%v", err))
	}
	
	for k, v := range data {
		err := db.Put([]byte(k), []byte(v))
		fmt.Println("key: " + k + " " + "value: " + v)
		if err != nil {
			ctx.String(http.StatusInternalServerError, err.Error())
			return
		}
	}
	ctx.String(http.StatusOK, "OK")
}

func handleGet(ctx *gin.Context) {
	key := ctx.Query("key")
	value, err := db.Get([]byte(key))
	if err != nil {
		ctx.String(http.StatusInternalServerError, err.Error())
	}
	ctx.Header("Content-Type", "application/json")
	ctx.JSON(http.StatusOK, gin.H{
		"key":   key,
		"value": string(value),
	})
}

func handleDelete(ctx *gin.Context) {
	key := ctx.Query("key")
	err := db.Delete([]byte(key))
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		ctx.String(http.StatusInternalServerError, err.Error())
		return
	}
	ctx.Header("Content-Type", "application/json")
	ctx.String(http.StatusOK, "OK")
}

func main() {
	engine := gin.Default()

	// 注册处理方法
	engine.POST("bitcask/put", handlePut)
	engine.GET("bitcask/get", handleGet)
	engine.DELETE("bitcask/delete", handleDelete)
	if err := engine.Run(":8089"); err != nil {
		panic(err)
	}
}
