package models

type MessageType int

var (
	//0 through 9 is reserved for user messages
	MsgTypUserMessage			MessageType		= 1
	//10 through 19 is reserved for system notifications
	MsgTypSysNotifyUserDisconnect	MessageType		= 10
	MsgTypSysNotifyUserConnect	MessageType		= 11
	//20 through 29 is reserved for control notifications
	MsgTypCtrlNotifyUserDisconnect MessageType		= 20
	MsgTypCtrlNotifyUserConnect MessageType		= 21
)
