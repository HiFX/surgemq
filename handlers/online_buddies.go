package handlers

import (
	"github.com/HiFX/surgemq/persistence"
	"github.com/zenazn/goji/web"
	"net/http"
	"github.com/HiFX/surgemq/models"
	"fmt"
)

type OnlineUsers struct {
	Base
	Persist *persistence.Redis
}

func (this *OnlineUsers) OnlineBuddies(c web.C, w http.ResponseWriter, req *http.Request) {
	dToken, _ := c.Env["token"]
	oToken, _ := dToken.(models.Token)
	oToken = oToken
	buddies, err := this.Persist.BuddiesOnline(oToken.Sub)
	if err != nil {
		//todo : deal error
		fmt.Println("error in reading online buddies, ", err)
		return
	}
	this.Respond(w, 200, buddies)
	return
}
