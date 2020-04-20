package event

type EventType int

//大于Sys_Event_User_Define给用户定义
const (
	Sys_Event_Tcp       EventType = 5
	Sys_Event_Http_Event EventType = 4

	Sys_Event_User_Define EventType = 1000
)

