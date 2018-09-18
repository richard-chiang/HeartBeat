/*

This package specifies the API to the failure detector library to be
used in assignment 1 of UBC CS 416 2018W1.

You are *not* allowed to change the API below. For example, you can
modify this file by adding an implementation to Initialize, but you
cannot change its API.

*/

package fdlib

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

//////////////////////////////////////////////////////
// Define the message types fdlib has to use to communicate to other
// fdlib instances. We use Go's type declarations for this:
// https://golang.org/ref/spec#Type_declarations

// Heartbeat message.
type HBeatMessage struct {
	EpochNonce uint64 // Identifies this fdlib instance/epoch.
	SeqNum     uint64 // Unique for each heartbeat in an epoch.
}

// An ack message; response to a heartbeat.
type AckMessage struct {
	HBEatEpochNonce uint64 // Copy of what was received in the heartbeat.
	HBEatSeqNum     uint64 // Copy of what was received in the heartbeat.
}

// Notification of a failure, signal back to the client using this
// library.
type FailureDetected struct {
	UDPIpPort string    // The RemoteIP:RemotePort of the failed node.
	Timestamp time.Time // The time when the failure was detected.
}

//////////////////////////////////////////////////////

// An FD interface represents an instance of the fd
// library. Interfaces are everywhere in Go:
// https://gobyexample.com/interfaces
type FD interface {
	// Tells the library to start responding to heartbeat messages on
	// a local UDP IP:port. Can return an error that is related to the
	// underlying UDP connection.
	StartResponding(LocalIpPort string) (err error)

	// Tells the library to stop responding to heartbeat
	// messages. Always succeeds.
	StopResponding()

	// Tells the library to start monitoring a particular UDP IP:port
	// with a specific lost messages threshold. Can return an error
	// that is related to the underlying UDP connection.
	AddMonitor(LocalIpPort string, RemoteIpPort string, LostMsgThresh uint8) (err error)

	// Tells the library to stop monitoring a particular remote UDP
	// IP:port. Always succeeds.
	RemoveMonitor(RemoteIpPort string)

	// Tells the library to stop monitoring all nodes.
	StopMonitoring()
}

var isInitialized bool = false
var nextNumber uint64 = 0

//===================================================================
//===================================================================
//          Custom Struct
//===================================================================
//===================================================================

type FDImp struct {
	Nonce       uint64
	Notify      chan FailureDetected
	ServerConn  net.PacketConn
	QuitChan    chan bool
	IsServerOn  bool
	MonitorList []Monitor
}

type Monitor struct {
	Mux              sync.Mutex
	Conn             *net.UDPConn
	IsConnOn         bool
	LocalIpPort      string
	RemoteIpPort     string
	LostMsgThresh    uint8
	LostMsgCount     uint8
	HeartBeatRecords map[uint64]PacketTime
	Rtt              float64
	HBQuitChan       chan bool
	AckQuitChan      chan bool
	HBSentChan       chan bool
	TimeOutOrAck     chan bool
}

type PacketTime struct {
	Sent    time.Time
	Receive time.Time
	IsSet   bool
}

//===================================================================
//===================================================================
//          API
//===================================================================
//===================================================================

// The constructor for a new FD object instance. Note that notifyCh
// can only be received on by the client that receives it from
// initialize:
// https://www.golang-book.com/books/intro/10
func Initialize(EpochNonce uint64, ChCapacity uint8) (fd FD, notifyCh <-chan FailureDetected, err error) {

	if isInitialized {
		return nil, nil, errors.New("already initialized, but not anymore after this error")
	}

	notify := make(chan FailureDetected, ChCapacity)

	fd = &FDImp{
		Nonce:  EpochNonce,
		Notify: notify}

	isInitialized = true
	return fd, notify, nil
}

// Receive heartbeat and send acks
func (fd *FDImp) StartResponding(LocalIpPort string) (err error) {
	if fd.IsServerOn {
		return errors.New("already responding")
	}
	fmt.Println("local ip" + LocalIpPort)

	pc, err := net.ListenPacket("udp", LocalIpPort)
	if err != nil {
		fmt.Println(err.Error())
		return errors.New("cannot listen for packet")
	}
	fd.ServerConn = pc
	fd.IsServerOn = true
	fd.QuitChan = make(chan bool)

	go fd.ServerMessenger()
	return
}

func (fd *FDImp) StopResponding() {
	fd.QuitChan <- true
	err := fd.ServerConn.Close()
	if err != nil {
		fmt.Println(err.Error())
	}
	fd.IsServerOn = false
}

// Send heartbeats and receive acks
func (fd *FDImp) AddMonitor(LocalIpPort string, RemoteIpPort string, LostMsgThresh uint8) (err error) {

	// check if it exist
	existed := false
	globalMonitor := new(Monitor)
	for i := range fd.MonitorList {
		m := &fd.MonitorList[i]
		fmt.Println("add monitor check " + m.RemoteIpPort)
		if m.RemoteIpPort == RemoteIpPort && m.LocalIpPort == LocalIpPort {
			existed = true
			if m.LostMsgThresh != LostMsgThresh {
				globalMonitor = m
				m.Mux.Lock()
				m.LostMsgThresh = LostMsgThresh
				m.Mux.Unlock()
			}
			break
		}
	}

	fmt.Println("add monitor, is it existed? " + strconv.FormatBool(existed))

	if !existed {
		// create new monitor
		monitor := new(Monitor)
		monitor.Mux.Lock()
		monitor.LocalIpPort = LocalIpPort
		monitor.RemoteIpPort = RemoteIpPort
		monitor.LostMsgThresh = LostMsgThresh
		monitor.LostMsgCount = 0
		monitor.HeartBeatRecords = make(map[uint64]PacketTime)
		monitor.Rtt = 3
		monitor.AckQuitChan = make(chan bool)
		monitor.HBQuitChan = make(chan bool)
		monitor.HBSentChan = make(chan bool)
		monitor.TimeOutOrAck = make(chan bool)

		lAddr, err := net.ResolveUDPAddr("udp", LocalIpPort)
		if err != nil {
			fmt.Println(err.Error())
			return errors.New("Connection to local ip failed in AddMonitor")
		}

		rAddr, err := net.ResolveUDPAddr("udp", RemoteIpPort)
		if err != nil {
			fmt.Println(err.Error())
			return errors.New("Connection to remote ip failed in AddMonitor")
		}

		conn, err := net.DialUDP("udp", lAddr, rAddr)
		if err != nil {
			fmt.Println(err.Error())
			return errors.New("Connection to client failed in AddMonitor")
		}

		monitor.Conn = conn
		monitor.Mux.Unlock()

		fd.MonitorList = append(fd.MonitorList, *monitor)
		globalMonitor = monitor
	}

	if !globalMonitor.IsConnOn {
		globalMonitor.IsConnOn = true

		fmt.Println("start messenger and routine")
		go fd.HeartBeatMessenger(globalMonitor)
		globalMonitor.TimeOutOrAck <- true
		go fd.ReceiveAckRoutine(globalMonitor)
	}

	return
}

func (fd *FDImp) RemoveMonitor(RemoteIpPort string) {
	fmt.Println("remove monitor " + RemoteIpPort)
	for i := range fd.MonitorList {
		m := &fd.MonitorList[i]
		if m.RemoteIpPort == RemoteIpPort {
			fd.CloseMonitor(m)
		}
	}
	return
}

func (fd *FDImp) StopMonitoring() {
	fmt.Println("Stop Monitor")
	for i := range fd.MonitorList {
		m := &fd.MonitorList[i]
		fd.CloseMonitor(m)
	}
	return
}

//===================================================================
//===================================================================
//          Messager
//===================================================================
//===================================================================

func (fd *FDImp) ServerMessenger() error {
	var err error
	for err == nil {
		select {
		case <-fd.QuitChan:
			fd.IsServerOn = false
			return nil

		default:
			hb, addr, err := fd.ReceiveHeartBeat()

			if err != nil {
				fmt.Println(err.Error())
				return errors.New("cannot receive heartbeat for lib")
			}

			ack := AckMessage{
				HBEatEpochNonce: hb.EpochNonce,
				HBEatSeqNum:     hb.SeqNum}

			go fd.SendAck(ack, addr)
		}
	}

	return nil
}

func (fd *FDImp) HeartBeatMessenger(m *Monitor) error {
	fmt.Println("Heart Beat Messenger " + m.RemoteIpPort)
	for {
		select {
		case <-m.HBQuitChan:
			return nil
		case <-m.TimeOutOrAck:
			m.Mux.Lock()
			id := getNextID()
			hbToSend := HBeatMessage{fd.Nonce, id}

			// Record current time
			if m.HeartBeatRecords == nil {
				m.HeartBeatRecords = make(map[uint64]PacketTime)
			}
			m.HeartBeatRecords[id] = PacketTime{Sent: time.Now(), IsSet: false}
			m.Mux.Unlock()
			// Send heart beat
			err := m.SendHeartBeat(hbToSend)

			if err != nil {
				fmt.Println(err.Error())
				return errors.New("cannot send heart beat")
			}
		}

		m.Mux.Lock()
		sleepTime := time.Duration(m.Rtt)
		m.Mux.Unlock()
		time.Sleep(sleepTime * time.Second)
	}
}

func (fd *FDImp) ReceiveAckRoutine(m *Monitor) error {
	fmt.Println("Receive Ack Routine " + m.RemoteIpPort)
	for {
		select {
		case <-m.AckQuitChan:
			return nil
		case <-m.HBSentChan:
			// Set read dead line
			rttDuration := time.Duration(m.Rtt)
			duration := time.Duration(rttDuration * time.Second)
			newTime := time.Now().Add(duration)

			err := m.Conn.SetReadDeadline(newTime)
			if err != nil {
				fmt.Println(m.Conn)
				fmt.Println(err.Error())
				return errors.New("error with set read deadline in receive ack")
			}
			// wait for ack
			ack, err := m.ReceiveAck()
			m.TimeOutOrAck <- true
			if err != nil {
				fmt.Println("failed: lost msg count ++")
				m.LostMsgCount++
				if m.LostMsgCount == m.LostMsgThresh {
					fmt.Println("failure detected")
					fd.Notify <- FailureDetected{
						UDPIpPort: m.RemoteIpPort,
						Timestamp: time.Now()}
					fd.CloseMonitor(m)
					return nil
				}
				fmt.Println(err.Error())
				continue
			}

			m.Mux.Lock()
			// Yes, reset lost msg to 0
			m.LostMsgCount = 0
			// Yes, update Rtt for this monitor
			// 1. update the packet receive time
			pt := m.HeartBeatRecords[ack.HBEatSeqNum]
			pt.Receive = time.Now()
			pt.IsSet = true
			m.HeartBeatRecords[ack.HBEatSeqNum] = pt
			// 2. use the receive time to calculate rtt
			packet := m.HeartBeatRecords[ack.HBEatSeqNum]
			sentTime := packet.Sent
			receiveTime := packet.Receive
			var elasped = receiveTime.Sub(sentTime).Seconds()
			var average = (elasped + m.Rtt) / 2

			// 3. update rtt
			m.Rtt = average
			m.Mux.Unlock()
		}
	}
}

//===================================================================
//===================================================================
//           Writing       Sending
//===================================================================
//===================================================================

func (fd *FDImp) SendAck(ack AckMessage, addr net.Addr) {
	fmt.Println("Send ack")
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(ack); err != nil {
		fmt.Println(err.Error())
		fmt.Println("cannot encode ack")
	}

	if _, err := fd.ServerConn.WriteTo(buf.Bytes(), addr); err != nil {
		fmt.Println(err.Error())
		fmt.Println("cannot send ack")
	}
}

func (m Monitor) SendHeartBeat(hb HBeatMessage) error {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(hb); err != nil {
		fmt.Println(err.Error())
		return err
	}

	fmt.Println("send heartbeat " + strconv.Itoa(int(hb.SeqNum)))

	if _, err := m.Conn.Write(buf.Bytes()); err != nil {
		fmt.Println(err.Error())
		return err
	}

	m.HBSentChan <- true
	return nil
}

//===================================================================
//===================================================================
//           Reading      	Receiving
//===================================================================
//===================================================================

func (fd *FDImp) ReceiveHeartBeat() (HBeatMessage, net.Addr, error) {
	buf := make([]byte, 1024)
	n, addr, err := fd.ServerConn.ReadFrom(buf)

	if err != nil {
		fmt.Println(err.Error())
		return HBeatMessage{}, nil, err
	}

	fmt.Println("receive heart beat")
	// bytes -> Buffer
	reader := bytes.NewReader(buf[:n])
	msg := new(HBeatMessage)

	decoder := gob.NewDecoder(reader)
	decoder.Decode(msg)
	return *msg, addr, nil
}

func (m Monitor) ReceiveAck() (AckMessage, error) {
	buf := make([]byte, 1024)
	n, _, err := m.Conn.ReadFrom(buf)

	if err != nil {
		fmt.Println("did not receive ack in itme")
		return AckMessage{}, err
	}

	// bytes -> Buffer
	reader := bytes.NewReader(buf[:n])
	msg := new(AckMessage)

	decoder := gob.NewDecoder(reader)
	decoder.Decode(msg)
	fmt.Println("receive ack " + strconv.Itoa(int(msg.HBEatSeqNum)))
	return *msg, nil
}

//===================================================================
//===================================================================
//           Helpers
//===================================================================
//===================================================================
func getNextID() uint64 {
	numHolder := &nextNumber
	atomic.AddUint64(numHolder, 1)
	num := atomic.LoadUint64(numHolder)
	return num
}

func (fd *FDImp) CloseMonitor(ml *Monitor) {
	fmt.Println("close monitor " + ml.RemoteIpPort)

	for i := range fd.MonitorList {
		m := &fd.MonitorList[i]
		fmt.Println("check " + m.RemoteIpPort)
		if ml.RemoteIpPort == m.RemoteIpPort && ml.LocalIpPort == m.LocalIpPort {
			fd.MonitorList = append(fd.MonitorList[:i], fd.MonitorList[i+1:]...)
		}
	}
	fmt.Println("close monitor delete after " + strconv.Itoa(len(fd.MonitorList)))
	ml.IsConnOn = false
	ml.Conn.Close()
	ml.AckQuitChan <- true
	ml.HBQuitChan <- true
}
