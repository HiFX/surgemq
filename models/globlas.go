package models

type MessageType int

var (
	//0 through 9 is reserved for user messages
	MessageTypeUserMessage		MessageType		= 0
	//10 through 19 is reserved for system notifications
	MessageTypeSysNotification	MessageType		= 10
)
