/*

A trivial application to illustrate how the fdlib library can be used
in assignment 1 for UBC CS 416 2018W1.

Usage:
go run client.go
*/

package main

// Expects fdlib.go to be in the ./fdlib/ dir, relative to
// this client.go file
import "./fdlib"

import "fmt"
import "os"
import "time"

func main() {
	// Local (127.0.0.1) hardcoded IPs to simplify testing. Run hostname -i
	localIpPort := "198.162.33.23:8888"
	toMonitorIpPort := "198.162.33.23:9989" // TODO: change this to remote node
	localIpPortMon := "198.162.33.23:3001"  // Add a monitor for a remote node.
	var lostMsgThresh uint8 = 3

	// TODO: generate a new random epoch nonce on each run
	var epochNonce uint64 = 12345
	var chCapacity uint8 = 5

	// Initialize fdlib. Note the use of multiple assignment:
	// https://gobyexample.com/multiple-return-values
	fd, notifyCh, err := fdlib.Initialize(epochNonce, chCapacity)
	if checkError(err) != nil {
		return
	}

	// Stop monitoring and stop responding on exit.
	// Defers are really cool, check out: https://blog.golang.org/defer-panic-and-recover
	defer fd.StopMonitoring()
	defer fd.StopResponding()

	fmt.Println("Started responding to heartbeats.")

	err = fd.AddMonitor(localIpPortMon, toMonitorIpPort, lostMsgThresh)
	if checkError(err) != nil {
		return
	}

	err = fd.StartResponding(localIpPort)
	if checkError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node: ", toMonitorIpPort)

	// Wait indefinitely, blocking on the notify channel, to detect a
	// failure.
	select {
	case notify := <-notifyCh:
		fmt.Println("Detected a failure of", notify)
		// Re-add the remote node for monitoring.
		err := fd.AddMonitor(localIpPortMon, toMonitorIpPort, lostMsgThresh)
		if checkError(err) != nil {
			return
		}
		fmt.Println("Started to monitor node: ", toMonitorIpPort)
	case <-time.After(time.Duration(int(lostMsgThresh)*6) * time.Second):
		// case <-time.After(time.Second):
		fmt.Println("No failures detected")
	}
}

// If error is non-nil, print it out and return it.
func checkError(err error) error {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}
