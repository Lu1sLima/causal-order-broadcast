package CausalOrderBroadcast

import (
	EagerReliableBroadcast "SD/RB"
	"fmt"
	"strconv"
	"strings"
	"time"
)


type COBroadcast_Req_Message struct {
	Addresses []string
	Message   string
}

type COBroadcast_Ind_Message struct {
	From    string
	Message string
}

type COBroadcast_Pending_Message struct {
	From string
	Message string
}

type COBroadcast_Module struct {
	Ind      chan COBroadcast_Ind_Message
	Req      chan COBroadcast_Req_Message
	Id int
	Addresses []string
	Address string
	RB *EagerReliableBroadcast.EagerReliableBroadcast_Module
	pending map[string]string
	vector []int
	lsn int
	dbg      bool
}

func (module *COBroadcast_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . [ " + s + " ] . . . . . . . . .")
	}
}

func (module *COBroadcast_Module) Init(id int, address string, addresses []string) {
	module.InitD(id, address, addresses, true)
}

func (module *COBroadcast_Module) InitD(id int, address string, addresses []string, _dbg bool) {
	module.dbg = _dbg
	module.Id = id
	module.Address = address
	module.outDbg("Init RB!")
	module.RB = EagerReliableBroadcast.NewRB(addresses, _dbg)
	module.Start()
}

func (module *COBroadcast_Module) Start() {

	go func() {
		for {
			select {
			case y := <-module.Req:
				module.Broadcast(y)
			case y := <- module.RB.Ind:
				module.Deliver(RB2COB(y))
			}
		}
		}()
	}
	
	func (module *COBroadcast_Module) Broadcast(message COBroadcast_Req_Message) {
	if module.dbg {
		fmt.Println("")
		fmt.Println(". . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .")
	}
	module.outDbg("COB BROADCAST: " + message.Message + " through RB!")
	PrintVector(module.vector, module.dbg)
	w := module.vector
	w[module.Id] = module.lsn
	module.lsn = module.lsn + 1
	module.RB.Broadcast(CO2RB(message, module.Id, w))
}

func (module *COBroadcast_Module) Deliver(message COBroadcast_Ind_Message) {
	if module.dbg {
		fmt.Println("")
		fmt.Println(". . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .")
	}
	// index:  0    1    2      3
	// MSG =  MSG § IP § ID § VECTOR

	////// Adding DELAY to the second message //////
	if module.Id == 1 && module.vector[0] == 1 {
		module.outDbg("GOING TO SLEEP zzZzzZzZz!")
		time.Sleep(5 * time.Second)
	}
	if module.Id == 1 {
		module.outDbg("I AM AWAKE! \\o/")
	}
	////////////////////////////////////////////////

	module.outDbg("COB DELIVER: "+message.Message)

	module.pending[message.Message] = message.Message
	exists := true
	k := ""

	PrintPendings(module.pending)
	for exists {
		exists, k = existMessageLessThanOrEqual(module.pending, module.vector)

		if exists {

			delete(module.pending, k)
			keySplit := strings.Split(k, "§")
			fromId, _ := strconv.Atoi(keySplit[2])
			module.vector[fromId] = module.vector[fromId]+1
			module.Ind <- COBDeliverMessage(keySplit[1], k)
			module.outDbg("DELIVERING MESSAGE: >>>>>> " + keySplit[0] + " <<<<<< FROM: "+ keySplit[1])
		}
	}

}

func existMessageLessThanOrEqual(pending map[string]string, vector []int) (bool, string) {
	
	for k, _ := range pending {
		messageSplit := strings.Split(k, "§")
		fmt.Println("O VETOR: " + messageSplit[3] + " É <= " + intArrayToString(vector, ","))
		if isLesserOrEqual(stringToIntArray(messageSplit[3], ","), vector) { 
			return true, k
		}
	}

	return false, ""
}

func isLesserOrEqual(a []int, b []int) bool {
	for i := 0; i < len(a); i++ {
		if a[i] > b[i] {	
			return false
		}
		
	}
	
	return true
}

func COBDeliverMessage(from string, message string) COBroadcast_Ind_Message {
	return COBroadcast_Ind_Message{
		From:    from,
		Message: message,
	}
}

func createVector(addresses []string) []int {
	data := make([]int, len(addresses))

	for i := 0; i < len(data); i++ {
		data[i] = 0
	}

	return data
}

func createPendings(addresses []string) map[string]string {
	var data = make(map[string]string)

	return data
}

func RB2COB(message EagerReliableBroadcast.EagerReliableBroadcast_Ind_Message) COBroadcast_Ind_Message {
	return COBroadcast_Ind_Message{
		From:    message.From,
		Message: message.Message,
	}
}

func CO2RB(message COBroadcast_Req_Message, id int, vector []int) EagerReliableBroadcast.EagerReliableBroadcast_Req_Message {
	vectorString := intArrayToString(vector, ",")

	//         0    1    2      3
	// MSG =  MSG § IP § ID § VECTOR

	return EagerReliableBroadcast.EagerReliableBroadcast_Req_Message{
		Addresses: message.Addresses,
		Message:   message.Message + "§" + strconv.Itoa(id) + "§" + vectorString,
	}
}

func stringToIntArray(str, delimiter string) []int {
    substrings := strings.Split(str, delimiter)

    intArray := make([]int, len(substrings))

    for i, s := range substrings {
        intValue, err := strconv.Atoi(s)
        if err != nil {
            fmt.Printf("Error converting '%s' to int: %v\n", s, err)
            continue
        }
        intArray[i] = intValue
    }

    return intArray
}

func intArrayToString(arr []int, delimiter string) string {
    strArray := make([]string, len(arr))
    for i, v := range arr {
        strArray[i] = fmt.Sprintf("%d", v)
    }

    result := strings.Join(strArray, delimiter)
    return result
}


func PrintVector(vector []int, db bool) {
	if !db {
		return
	}

	for i := 0; i < len(vector); i++ {
		fmt.Println(". . . . . . . . . [ POS:" + strconv.Itoa(i) + " VALUE: " + strconv.Itoa(vector[i]) + " ] . . . . . . . . .")
	}
}

func PrintPendings(pendings map[string]string) {
	for k, v := range pendings {
		fmt.Println("KEY: " + k + " VALUE: "+ v )
	}
}

func NewCOB(id int, address string, addresses []string, _dbg bool) *COBroadcast_Module {
	cob := &COBroadcast_Module{
		Id: id,
		Ind:       make(chan COBroadcast_Ind_Message),
		Req:       make(chan COBroadcast_Req_Message),
		Addresses: addresses,
		Address:   address,
		RB:        EagerReliableBroadcast.NewRB(addresses, false),
		pending:   createPendings(addresses),
		vector:    createVector(addresses),
		lsn:       0,
		dbg:       _dbg,
	}
	cob.outDbg(" Init COB!")
	cob.Start()

	return cob
}