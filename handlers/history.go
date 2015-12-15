//package handlers implements additional handler functionalities
//to be implemented along with the plain MQTT chat spec.
//Additional functionalities includes chat history and so on

//author :  shalin LK <shalinlk@hifx.co.in>

package handlers

import (
	"github.com/zenazn/goji/web"
	"net/http"
	"github.com/HiFX/surgemq/persistence"
	"strconv"
	"github.com/HiFX/surgemq/models"
)

type History struct {
	Base
	Redis *persistence.Redis
}

//History returns history of a chat among a group
func (this *History) History(c web.C, w http.ResponseWriter, req *http.Request) {
	var (
		offset    string = "offset"
		count    string  = "count"
		group    string  = "topic"
	)

	dToken := c.Env["token"]
	oToken, _ := dToken.(models.Token)
	userId := oToken.Sub
	offsetStr := req.FormValue(offset)
	countStr := req.FormValue(count)
	groupStr := req.FormValue(group)

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
	list, scanErr := this.Redis.Scan(userId, groupStr, offsetInt, countInt)
	if scanErr != nil {
		//todo : deal error
	}
	this.Respond(w, 200, list)
	return
}

//Chats returns all unique chats of the user
func (this *History) ChatRooms(c web.C, w http.ResponseWriter, req *http.Request) {
	var (
		offset    string = "offset"
		count    string  = "count"
	)
	offsetStr := req.FormValue(offset)
	countStr := req.FormValue(count)
	dToken, _ := c.Env["token"]
	oToken, _ := dToken.(models.Token)

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

	list, err := this.Redis.ChatList(oToken.Sub, int64(offsetInt), int64(countInt))
	if err != nil {
		//todo : deal error
		return
	}
	this.Respond(w, 200, list)
}

//Read user time line
func (this *History) ChatTimeLine(c web.C, w http.ResponseWriter, req *http.Request) {
	dToken, _ := c.Env["token"]
	oToken, _ := dToken.(models.Token)

	var (
		offset    string = "offset"
		count    string  = "count"
	)
	offsetStr := req.FormValue(offset)
	countStr := req.FormValue(count)

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
	timeLine, err := this.Redis.ChatTimeLine(oToken.Sub, offsetInt, countInt)
	if err != nil {
		//todo : deal error
		return
	}
	this.Respond(w, 200, timeLine)
}
