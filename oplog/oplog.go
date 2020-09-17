package oplog

import (
	"context"
	"encoding/json"
	log "github.com/liuhx-golang/log4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// Mgo  mongo链接参数
type Mgo struct {
	URI        string //数据库网络地址
	Database   string //要连接的数据库
	Collection string //要连接的集合
}

func (m *Mgo) connect() *mongo.Collection {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel() //养成良好的习惯，在调用WithTimeout之后defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(m.URI))
	if err != nil {
		log.GetLogger().Error(err)
	}
	collection := client.Database(m.Database).Collection(m.Collection)
	return collection
}

//WatchCollection 监控collection
func WatchCollection(m Mgo, messageChan chan<- string, resumeAfter string,operationTypes []string) {
	maps := make([]map[string]interface{}, 0)
	for _, operationType := range operationTypes {
		maps = append(maps, map[string]interface{}{"operationType": operationType})
	}
	pipeline := []bson.M{{"$match": bson.M{"$or": maps}}}
	c := m.connect()
	optionList := make([]*options.ChangeStreamOptions, 0)
	if resumeAfter != "" {
		optionList = append(optionList, options.ChangeStream().SetResumeAfter(bson.M{"_data": resumeAfter}))
	}
	optionList = append(optionList, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	changeStream, err := c.Watch(context.Background(), pipeline, optionList...)
	if err != nil {
		// Handle err
		log.GetLogger().Error(err)
		return
	}
	defer changeStream.Close(context.Background())
	var doc bson.M
	for changeStream.Next(context.Background()) {
		if err = changeStream.Decode(&doc); err != nil {
			log.GetLogger().Error(err)
		}
		a, _ := json.Marshal(doc)
		message := string(a)
		log.GetLogger().Info("发送%v", string(a))
		messageChan <- message
	}
	if err = changeStream.Err(); err != nil {
		log.GetLogger().Error(err)
	}
}
