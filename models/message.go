package models

import (
	"time"
	"encoding/json"
	"fmt"
)

type Message struct{
	//Who : id of the one who speaks
	Who string	`json:"who,omitempty"`
	//When : time in utc unix
	When	time.Time	`json:"when,omitempty"`
	//What : the actual message
	What	string	`json:what,omitempty"`
}

func (this *Message) Serialize()([]byte){
	flat, _ := json.Marshal(this)
	return flat
}

func (this *Message) String()string{
	return fmt.Sprintf("%s : %s", this.Who, this.What)
}
