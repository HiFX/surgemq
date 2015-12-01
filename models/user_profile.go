package models

import (
	"encoding/json"
	"fmt"
)

const (

)

type UserProfileBasics struct {
	Id           string  `json:"usr_id,omitempty"`
	FirstName    string  `json:"usr_first_name,omitempty"`
	LastName     string  `json:"usr_last_name,omitempty"`
	ProfileImage string  `json:"usr_image,omitempty"`
}

type UserProfileCore struct {
	UserProfileBasics
	Email        string `json:"email,omitempty"`
}

func (this *UserProfileCore) Flatten() ([]byte) {
	j, _ := json.Marshal(this)
	return j
}
func (this *UserProfileBasics) Name() string {
	return fmt.Sprintf("%s %s", this.FirstName, this.LastName)
}
