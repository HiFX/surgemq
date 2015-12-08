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

//OnlineBuddies : list of all buddies online.
func (this *OnlineUsers) OnlineBuddies(c web.C, w http.ResponseWriter, req *http.Request) {
	dToken, _ := c.Env["token"]
	oToken, _ := dToken.(models.Token)
	buddies, err := this.Persist.BuddiesOnline(oToken.Sub)
	if err != nil {
		//todo : deal error
		fmt.Println("error in reading online buddies, ", err)
		return
	}
	this.Respond(w, 200, buddies)
	return
}

//IsUserOnline : Online status of a single user
func (this *OnlineUsers) IsUserOnline(c web.C, w http.ResponseWriter, req *http.Request) {
	userID := req.FormValue("user_id")
	//todo : decide up on whether to implement authorization here for preventing a
	//user from getting the online status of another not connnected user;
	status, err := this.Persist.UserOnlineStatus(userID)
	if err != nil {
		//todo : deal error
	}
	type response struct {
		Status    bool    `json:"status"`
	}
	resp := response{}
	resp.Status = status
	this.Respond(w, 200, resp)
}
