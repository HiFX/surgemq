package models

import "encoding/json"

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
	j,_ := json.Marshal(this)
	return j
}
