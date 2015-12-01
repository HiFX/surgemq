package models

import (
	"encoding/json"
	"fmt"
)

type Message struct{
	//type determines the type of the message
	Type	MessageType		`json:"type,omitempty"`
	//Id : id of the one who speaks
	Id	string	`json:"id,omitempty"`
	//Pic : profile image of the speaker
	Pic	string	`json:"profile_pic,omitempty"`
	//Who : name of the one who speaks
	Who string    `json:"who,omitempty"`
	//When : time in utc unix
	When    int64    `json:"when,omitempty"`
	//What : the actual message
	What    string    `json:what,omitempty"`
}

func (this *Message) Serialize() ([]byte) {
	flat, _ := json.Marshal(this)
	return flat
}

func (this *Message) String() string {
	return fmt.Sprintf("%s : %s", this.Who, this.What)
}
