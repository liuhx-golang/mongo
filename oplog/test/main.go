package main

import (
	"flag"
	log "github.com/liuhx-golang/log4go"
	"github.com/liuhx-golang/mongo/oplog"
	"os"
	"os/signal"
)

var confPath = flag.String("conf", "./conf/", "The conf path.")

func main() {
	flag.Parse()
	Log4goConf := *confPath + "log4g.xml"
	log.LoggerConfiguration(Log4goConf)
	var mgo oplog.Mgo
	mgo.Database = "data-cleansing"
	mgo.URI = "mongodb://10.2.6.220:27017/data-cleansing"
	mgo.Collection = "user_task"
	data := make(chan string)
	var operationTypes []string
	operationTypes = append(operationTypes, "delete")
	operationTypes = append(operationTypes, "update")
	operationTypes = append(operationTypes, "insert")
	go oplog.WatchCollection(mgo, data, "825F606DED000000012B022C0100296E5A10049CEA4BA3BC224005A31C98A52CF02B5C46645F696400645F3506F784E8E924FE95F27C0004", operationTypes)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

Loop:
	for {
		select {
		case msg, ok := <-data:
			if ok {
				log.GetLogger().Info(msg)
			}
		case <-signals:
			log.GetLogger().Info("退出")
			break Loop
		}

	}

}
