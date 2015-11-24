package service

import (
	"golang.org/x/net/websocket"
	"net/http"
	"net/url"
	"github.com/surgemq/message"
	"github.com/HiFX/surgemq/persistence"
	"strconv"
	"encoding/json"
)

type ServiceWrapper struct {
	server *Server
	mux *http.ServeMux
	webSocketPort string
	redis *persistence.Redis
}

//NewService returns a server
//Params :
//		auth : authentication function;
//		topic_authorization : topic authorization function;
func NewService(auth func(string) (string, error), webSocketPort, redisHost, redisPass string, redisDB int) (*ServiceWrapper, error) {
	//todo : validate webSocket port to follow the format of :1234

	persistence, err := persistence.NewRedis(redisHost, redisPass, redisDB)
	if err != nil {
		return nil, err
	}

	svr := &Server{
		KeepAlive:        DefaultKeepAlive,
		ConnectTimeout:   DefaultConnectTimeout,
		AckTimeout:       DefaultAckTimeout,
		TimeoutRetries:   DefaultTimeoutRetries,
		SessionsProvider: DefaultSessionsProvider,
		TopicsProvider:   DefaultTopicsProvider,
		redis:    persistence,
	}
	svr.RegisterAuthenticator(auth)
	wrapper := &ServiceWrapper{server : svr}
	wrapper.mux = http.NewServeMux()
	wrapper.webSocketPort = webSocketPort

	//handler for proxying websocket
	soketProxy := func(ws *websocket.Conn) {
		address := "tcp://127.0.0.1:1883"
		scheme, _ := url.Parse(address)
		WebsocketTcpProxy(ws, scheme.Scheme, scheme.Host)
	}

	//register all created handlers
	history := func(w http.ResponseWriter, req *http.Request) {
		//authentication needed
		var (
			user_id	string = "user_id"
			offset	string = "offset"
			count	string = "count"
			group	string = "group"
		)
		userId := req.FormValue(user_id)
		offsetStr := req.FormValue(offset)
		countStr := req.FormValue(count)
		groupStr	:= req.FormValue(group)

		offsetInt, convErr := strconv.Atoi(offsetStr)
		if convErr != nil {
			//todo : deal error
			offsetInt = 0
		}
		countInt, convErr := strconv.Atoi(countStr)
		if convErr != nil {
			//todo : deal error
			countInt = 25
		}
		list, scanErr := svr.redis.Scan(userId, groupStr, offsetInt, countInt)
		if scanErr != nil {
			//todo : deal error
		}
		j, _ := json.Marshal(list)
		w.Write(j)
//		w.WriteHeader(200)
		return
	}
	wrapper.mux.Handle("/mqtt", websocket.Handler(soketProxy))
	wrapper.mux.Handle("/history", http.HandlerFunc(history))
	//wrapper.mux.Handle("/topic", http.HandlerFunc(topicGen))
	return wrapper, nil
}

func (this *ServiceWrapper) Start() {
	go http.ListenAndServe(this.webSocketPort, this.mux)
	this.server.ListenAndServe("tcp://:1883")
}

func (this *ServiceWrapper) Publish(msg *message.PublishMessage, onComplete OnCompleteFunc) error {
	return this.server.Publish(msg, onComplete)
}

func (this *ServiceWrapper) Close() error {
	return this.server.Close()
}
