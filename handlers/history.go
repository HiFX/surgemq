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
	"fmt"
)

type History struct {
	Base
	Redis *persistence.Redis
}

//History returns history of a chat among a group
func (this *History) History(c web.C, w http.ResponseWriter, req *http.Request) {
	fmt.Println("History invoked")
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
	list, scanErr := this.Redis.Scan(userId, groupStr, offsetInt, countInt)
	if scanErr != nil {
		//todo : deal error
	}
	this.Respond(w, 200, list)
	return
}

//Chats returns all unique chats of the user
func (this *History) ChatRooms(c web.C, w http.ResponseWriter, req *http.Request){
	fmt.Println("Chat Rooms Invoked")
	var (
		user_id	string = "user_id"
		offset	string = "offset"
		count	string = "count"
	)
	userId := req.FormValue(user_id)
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

	list, err := this.Redis.ChatList(userId)
	if err != nil {
		//todo : deal error
		return
	}
	length := len(list)
	if offsetInt > length {
		this.Respond(w, 200, []string{})
		return
	}
	if offsetInt + countInt > length {
		countInt = length
	}
	this.Respond(w, 200, list[offsetInt:offsetInt + countInt])
}

