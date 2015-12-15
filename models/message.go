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
	What    string    `json:"what,omitempty"`
	//On : topic on which the message is made
	On string	`json:"topic,omitempty"`
	//todo : remove buddies from message struct
	//Buddies : who are the pariticipants
	Buddies *[]UserProfileBasics		`json:"buddies,omitempty"`
}

func (this *Message) Serialize() ([]byte) {
	flat, _ := json.Marshal(this)
	return flat
}

func (this *Message) String() string {
	return fmt.Sprintf("%s : %s", this.Who, this.What)
}

type Messages struct {
	MessageList []Message	`json:"messages,omitempty"`
	Meta	*ThreadMeta		`json:"meta,omitempty"`
}

type ThreadMeta struct {
	Buddies	*[]UserProfileBasics	`json:"buddies,omitempty"`
	Topic	string					`json:"topic,omitempty"`
}
