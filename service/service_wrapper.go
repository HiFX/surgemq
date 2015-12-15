package service

import (
	"golang.org/x/net/websocket"
	"net/http"
	"net/url"
	"github.com/surgemq/message"
	"github.com/HiFX/surgemq/persistence"
	"github.com/zenazn/goji/web"
	"github.com/HiFX/surgemq/handlers"
	"io/ioutil"
	gmiddleware "github.com/zenazn/goji/web/middleware"
)

type ServiceWrapper struct {
	server *Server
	mux *http.ServeMux
	webSocketPort string
	redis *persistence.Redis
}

//NewService returns a server
//Params :
func NewService(webSocketPort, redisHost, redisPass string, redisDB int, keyFilePath string,
			authorizer func(...string)(bool)) (*ServiceWrapper, error) {
	//todo : validate webSocket port to follow the format of :1234

	//get authenticator
	persist, err := persistence.NewRedis(redisHost, redisPass, redisDB)
	if err != nil {
		return nil, err
	}
	keyFile, readError := readPublicKey(keyFilePath)
	if readError != nil {
		return nil, readError
	}
	var (
		historyHandler handlers.History
		authHandlder    handlers.Authenticator
		userHandler handlers.OnlineUsers
	)
	//set up handlers
	historyHandler.Redis = persist
	authHandlder = handlers.Authenticator{KeyFile : keyFile, ClientId : "some_valid_client", Persist : persist, ModeProd : false}
	userHandler = handlers.OnlineUsers{Persist : persist}
	authenticator, err := authHandlder.NewTokenAuthenticator()
	if err != nil {
		//todo : deal error
	}
	svr := &Server{
		KeepAlive:        DefaultKeepAlive,
		ConnectTimeout:   DefaultConnectTimeout,
		AckTimeout:       DefaultAckTimeout,
		TimeoutRetries:   DefaultTimeoutRetries,
		SessionsProvider: DefaultSessionsProvider,
		TopicsProvider:   DefaultTopicsProvider,
		redis:    persist,
		authorization : authorizer,
		authenticate : authenticator,
	}

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
	//register middleware
	mux.Use(gmiddleware.EnvInit)
	mux.Use(authHandlder.Authenticate)
	//todo : this is a hack
	mux.Options("/*", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request){
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "X-Requested-With, Authorization")
		}))
	mux.Get("/chat/history", historyHandler.History)
	mux.Get("/chat/rooms", historyHandler.ChatRooms)
	mux.Get("/chat/timeLine", historyHandler.ChatTimeLine)
	mux.Get("/chat/online/buddies", userHandler.OnlineBuddies)
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

func readPublicKey(keyFilePath string) ([]byte , error) {
	return ioutil.ReadFile(keyFilePath)
}

