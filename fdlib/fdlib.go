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
	ServerConn  net.Conn
	IsServerOn  bool
	MonitorList []Monitor
}

type Monitor struct {
	Mux              sync.Mutex
	Conn             net.Conn
	IsConnOn         bool
	LocalIpPort      string
	RemoteIpPort     string
	LostMsgThresh    uint8
	LostMsgCount     uint8
	HeartBeatRecords map[uint64]PacketTime
	Rtt              float64
	HBQuitChan       chan bool
	AckQuitChan      chan bool
}

type PacketTime struct {
	sent    time.Time
	receive time.Time
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
	listener, err := net.Listen("udp", LocalIpPort)
	if err != nil {
		fmt.Println(err.Error())
		return errors.New("cannot listen to udp in start respond")
	}

	fd.IsServerOn = true

	go fd.ServerMessenger(listener)
	return
}

func (fd *FDImp) StopResponding() {
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
		if m.RemoteIpPort == RemoteIpPort && m.LocalIpPort == LocalIpPort && m.LostMsgThresh != LostMsgThresh {
			globalMonitor = m
			m.LostMsgThresh = LostMsgThresh
			existed = true
			break
		}
	}

	if !existed {
		// create new monitor
		monitor := new(Monitor)
		monitor.LocalIpPort = LocalIpPort
		monitor.RemoteIpPort = RemoteIpPort
		monitor.LostMsgThresh = LostMsgThresh
		monitor.LostMsgCount = 0
		monitor.HeartBeatRecords = make(map[uint64]PacketTime)
		monitor.Rtt = 3
		monitor.AckQuitChan = make(chan bool)
		monitor.HBQuitChan = make(chan bool)

		lAddr, err := net.ResolveUDPAddr("udp", LocalIpPort)
		if err != nil {
			return errors.New("Connection to local ip failed in AddMonitor")
		}

		rAddr, err := net.ResolveUDPAddr("udp", RemoteIpPort)
		if err != nil {
			return errors.New("Connection to remote ip failed in AddMonitor")
		}

		conn, err := net.DialUDP("udp", lAddr, rAddr)
		if err != nil {
			return errors.New("Connection to client failed in AddMonitor")
		}

		monitor.Conn = conn

		fd.MonitorList = append(fd.MonitorList, *monitor)
		globalMonitor = monitor
	}

	if !globalMonitor.IsConnOn {
		globalMonitor.IsConnOn = true
		go fd.HeartBeatMessenger(globalMonitor, globalMonitor.HBQuitChan)
		go fd.ReceiveAckRoutine(globalMonitor, globalMonitor.AckQuitChan)
	}

	return
}

func (fd *FDImp) RemoveMonitor(RemoteIpPort string) {
	for i := range fd.MonitorList {
		m := &fd.MonitorList[i]
		if m.RemoteIpPort == RemoteIpPort {
			m.CloseMonitor()
		}
	}
	return
}

func (fd *FDImp) StopMonitoring() {
	for i := range fd.MonitorList {
		m := &fd.MonitorList[i]
		m.CloseMonitor()
	}
	return
}

//===================================================================
//===================================================================
//          Messager
//===================================================================
//===================================================================

func (fd *FDImp) ServerMessenger(listener net.Listener) error {
	fmt.Println("server messenger open")
	var err error
	for err == nil {
		conn, err := listener.Accept()
		if err != nil {
			conn.Close()
			return errors.New("unable to accept new heartbeat")
		}
		fd.ServerConn = conn

		hb, err := fd.ReceiveHeartBeat()

		if err != nil {
			conn.Close()
			return errors.New("cannot receive heartbeat for lib")
		}

		ack := AckMessage{
			HBEatEpochNonce: hb.EpochNonce,
			HBEatSeqNum:     hb.SeqNum}

		err = fd.SendAck(ack)
		if err != nil {
			conn.Close()
			return errors.New("cannot send ack from lib")
		}

		conn, err = listener.Accept()
	}
	fmt.Println("server messenger closed")

	return nil
}

func (fd *FDImp) HeartBeatMessenger(m *Monitor, quit chan bool) error {
	fmt.Println("start heart beat messenger to " + m.RemoteIpPort)
	for {
		select {
		case <-quit:
			m.Conn.Close()
			return nil
		default:
			id := getNextID()
			hbToSend := HBeatMessage{fd.Nonce, id}

			// Record current time
			m.HeartBeatRecords[id] = PacketTime{sent: time.Now()}

			// Send heart beat
			err := m.SendHeartBeat(hbToSend)

			if err != nil {
				m.Conn.Close()
				return errors.New("cannot send heart beat")
			}
		}
	}
}

func (fd *FDImp) ReceiveAckRoutine(m *Monitor, quit chan bool) error {
	for {
		select {
		case <-quit:
			m.Conn.Close()
			return nil
		default:
			// Set read dead line
			rttDuration := time.Duration(m.Rtt)
			duration := time.Duration(rttDuration * time.Second)
			m.Conn.SetReadDeadline(time.Now().Add(duration))

			// wait for ack
			ack, err := m.ReceiveAck()
			m.Mux.Lock()
			if err == nil {
				// Yes, reset lost msg to 0
				m.LostMsgCount = 0
				// Yes, update Rtt for this monitor
				// 1. update the packet receive time
				pt := m.HeartBeatRecords[ack.HBEatSeqNum]
				pt.receive = time.Now()
				m.HeartBeatRecords[ack.HBEatSeqNum] = pt
				// 2. use the receive time to calculate rtt
				packet := m.HeartBeatRecords[ack.HBEatSeqNum]
				sentTime := packet.sent
				receiveTime := packet.receive
				var elasped = receiveTime.Sub(sentTime).Seconds()
				var average = (elasped + m.Rtt) / 2

				// 3. update rtt
				m.Rtt = average
			} else {
				// No, record one lost msg
				m.LostMsgCount++
			}
			m.Mux.Unlock()

			// timeout if over time out threshhold
			if m.LostMsgCount == m.LostMsgThresh {
				failure := FailureDetected{m.RemoteIpPort, time.Now()}
				fd.Notify <- failure
				m.Conn.Close()
				return errors.New("failure detected")
			}
		}
	}
}

//===================================================================
//===================================================================
//           Writing       Sending
//===================================================================
//===================================================================

func (fd *FDImp) SendAck(ack AckMessage) error {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(ack); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if _, err := fd.ServerConn.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

func (m Monitor) SendHeartBeat(hb HBeatMessage) error {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(hb); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if _, err := m.Conn.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

//===================================================================
//===================================================================
//           Reading      	Receiving
//===================================================================
//===================================================================

func (fd *FDImp) ReceiveHeartBeat() (HBeatMessage, error) {
	buf := make([]byte, 1024)
	n, err := fd.ServerConn.Read(buf)

	if err != nil {
		return HBeatMessage{}, err
	}

	// bytes -> Buffer
	reader := bytes.NewReader(buf[:n])
	msg := new(HBeatMessage)

	decoder := gob.NewDecoder(reader)
	decoder.Decode(msg)
	return *msg, nil
}

func (m Monitor) ReceiveAck() (AckMessage, error) {
	buf := make([]byte, 1024)
	n, err := m.Conn.Read(buf)

	if err != nil {
		return AckMessage{}, err
	}

	// bytes -> Buffer
	reader := bytes.NewReader(buf[:n])
	msg := new(AckMessage)

	decoder := gob.NewDecoder(reader)
	decoder.Decode(msg)
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

func (m *Monitor) CloseMonitor() {
	m.IsConnOn = false
	m.AckQuitChan <- true
	m.HBQuitChan <- true
	m.Conn.Close()
}
