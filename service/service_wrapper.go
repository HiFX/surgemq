package service

import (
	"golang.org/x/net/websocket"
	"net/http"
	"net/url"
)

type ServiceWrapper struct {
	server *Server
	mux *http.ServeMux
	webSocketPort string
}

//NewService returns a server
//Params :
//		auth : authentication function;
//		topic_authorization : topic authorization function;
func NewService(auth func(string) error, topicGenerator func() string, webSocketPort string) (*ServiceWrapper, error) {

	//todo : validate webSocket port to follow the format of :1234
	svr := &Server{
		KeepAlive:        DefaultKeepAlive,
		ConnectTimeout:   DefaultConnectTimeout,
		AckTimeout:       DefaultAckTimeout,
		TimeoutRetries:   DefaultTimeoutRetries,
		SessionsProvider: DefaultSessionsProvider,
		TopicsProvider:   DefaultTopicsProvider,
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

	//handler for topic generator
	topicGen := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(topicGenerator()))
		w.WriteHeader(200)
		return
	}

	//register all created handlers
	wrapper.mux.Handle("/mqtt", websocket.Handler(soketProxy))
	wrapper.mux.Handle("/topic", http.HandlerFunc(topicGen))
	return wrapper, nil
}

func (this *ServiceWrapper) Start() {
	go http.ListenAndServe(this.webSocketPort, this.mux)
	this.server.ListenAndServe("tcp://:1883")
}

func (this *ServiceWrapper) Publish(msg *message.PublishMessage, onComplete OnCompleteFunc) error {
	return this.server.Publish(msg , onComplete )
}

func (this *ServiceWrapper) Close() error {
	return this.server.Close()
}
