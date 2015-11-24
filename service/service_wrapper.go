package service

import (
	"golang.org/x/net/websocket"
	"net/http"
	"net/url"
	"github.com/surgemq/message"
	"github.com/HiFX/surgemq/persistence"
	"github.com/zenazn/goji/web"
	"github.com/HiFX/surgemq/handlers"
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

	persist, err := persistence.NewRedis(redisHost, redisPass, redisDB)
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
		redis:    persist,
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

	mux := web.New()
	var (
		history handlers.History
	)
	history.Redis = persist
	mux.Get("/chat/history", history.History)
	mux.Get("/chat/rooms", history.ChatRooms)
	wrapper.mux.Handle("/mqtt", websocket.Handler(soketProxy))
	wrapper.mux.Handle("/", mux)
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
