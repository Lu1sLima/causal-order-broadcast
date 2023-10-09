package EagerReliableBroadcast

import (
	BestEffortBroadcast "SD/BEB"
	"fmt"
	"strings"
)

type EagerReliableBroadcast_Req_Message struct {
	Addresses []string
	Message  string
}

type EagerReliableBroadcast_Ind_Message struct {
	From    string
	Message string
}


type EagerReliableBroadcast_Module struct {
	Ind      chan EagerReliableBroadcast_Ind_Message
	Req      chan EagerReliableBroadcast_Req_Message
	Addresses []string
	Beb *BestEffortBroadcast.BestEffortBroadcast_Module
	delivered map[string]string
	dbg      bool
}

func (module *EagerReliableBroadcast_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . [ RB msg : " + s + " ] . . . . . . . . .")
	}
}

func (module *EagerReliableBroadcast_Module) Init(address string) {
	module.InitD(address, true)
}

func (module *EagerReliableBroadcast_Module) InitD(address string, _dbg bool) {
	module.dbg = _dbg
	module.outDbg("Init RB!")
	module.Beb = BestEffortBroadcast.NewBEB(address, _dbg)
	module.Start()
}

func (module *EagerReliableBroadcast_Module) Start() {

	go func() {
		for {
			select {
			case y := <-module.Req:
				module.Broadcast(y)
			case y := <- module.Beb.Ind:
				module.Deliver(BEB2RB(y))
			}
		}
	}()
}

func (module *EagerReliableBroadcast_Module) Broadcast(message EagerReliableBroadcast_Req_Message) {
	module.outDbg("RB BROADCAST: " + message.Message + " through BEB!")
	// fmt.Println(". . . . . . . . . [ RB BROADCAST: " + message.Message + " through BEB! ] . . . . . . . . .")
	for i := 0; i < len(message.Addresses); i++ {
		msg := RB2BEB(message)
		module.Beb.Req <- msg
	}
}

func (module *EagerReliableBroadcast_Module) Deliver(message EagerReliableBroadcast_Ind_Message) {

	msg_concat := message.Message
	// fmt.Println(". . . . . . . . . [ RB DELIVER: '" + message.Message + "' from " + message.From + " ] . . . . . . . . .")
	module.outDbg("RB DELIVER: '" + strings.Split(message.Message, "§")[0] + "' from " + message.From)

	_, ok := module.delivered[msg_concat]
	if !ok {
		module.outDbg("RB DELIVER: Message was not previously delivered!")
		module.delivered[msg_concat] = message.Message
		module.Ind <- message
		module.outDbg("RB DELIVER: Broadcasting message through BEB!")
		beb_msg := IndRB2BEB(message, module.Addresses)
		module.Beb.Broadcast(beb_msg)
	} else {
		module.outDbg("RB DELIVER: Message already delivered!")
	}
}

func IndRB2BEB(message EagerReliableBroadcast_Ind_Message, addresses []string) BestEffortBroadcast.BestEffortBroadcast_Req_Message {
	return BestEffortBroadcast.BestEffortBroadcast_Req_Message {
		Addresses: addresses,
		Message: message.Message}
}
func RB2BEB(message EagerReliableBroadcast_Req_Message) BestEffortBroadcast.BestEffortBroadcast_Req_Message {
	return BestEffortBroadcast.BestEffortBroadcast_Req_Message {
		Addresses: message.Addresses,
		Message: message.Message}
}

func BEB2RB(message BestEffortBroadcast.BestEffortBroadcast_Ind_Message) EagerReliableBroadcast_Ind_Message {
	return EagerReliableBroadcast_Ind_Message{
		From: message.From,
		Message: message.Message}
}

func NewRB(addresses []string, _dbg bool) *EagerReliableBroadcast_Module {
	rb := &EagerReliableBroadcast_Module{
		Req: make(chan EagerReliableBroadcast_Req_Message),
		Ind: make(chan EagerReliableBroadcast_Ind_Message),
		Beb: BestEffortBroadcast.NewBEB(addresses[0], false),
		Addresses: addresses,
		delivered:  make(map[string]string),
		dbg: _dbg,
	}
	rb.outDbg("Init RB!")
	rb.Start()
	return rb
}