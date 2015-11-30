package models


type ChatList struct {
	Key		string	`json:"key"`
	Info	ChatInfo	`json:"info"`
}

type ChatInfo struct {
	Members	[]Member	`json:"members,omitempty"`
}

type Member struct {
	Id	string		`json:"id,omitempty"`
	Name string		`json:"name,omitempty"`
}
