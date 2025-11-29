package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

var (
	primaryAddr string
	backupAddr  string
	topic       string
	serverType  = "tcp"
)

func init() {
	flag.StringVar(&primaryAddr, "primary-addr", primaryAddr, "Primary Broker Address (host:port)")
	flag.StringVar(&backupAddr, "backup-addr", backupAddr, "Backup Broker Address (host:port)")
	flag.StringVar(&topic, "topic", topic, "The topic name to subscribe to (Default: topicC)")
}

// subscribeToBroker is responsible for connecting to a single Broker and receiving messages.
// It includes logic for continuous reconnection retry to handle Broker failure and failover.
func subscribeToBroker(addr string, topic string) {
	fmt.Printf("[SUB] Starting listener for Broker: %s\n", addr)

	// Continuous connection retry
	for {
		conn, err := net.Dial(serverType, addr)
		if err != nil {
			fmt.Printf("[SUB][%s] Connection failed: %v. Retrying after %v...\n", addr, err, 5*time.Second)
			time.Sleep(5 * time.Second)
			continue
		}

		fmt.Printf("[SUB][%s] Connection successful, attempting to subscribe to topic: %s\n", addr, topic)

		// 1. Send initial SUBSCRIBE request
		subscribeCmd := fmt.Sprintf("SUBSCRIBE %s\n", topic)
		_, err = conn.Write([]byte(subscribeCmd))
		if err != nil {
			fmt.Printf("[SUB][%s] Failed to send SUBSCRIBE: %v. Reconnecting...\n", addr, err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// 2. Read confirmation
		reader := bufio.NewReader(conn)
		response, err := reader.ReadString('\n')
		if err != nil || !strings.HasPrefix(response, "OK") {
			fmt.Printf("[SUB][%s] Subscription confirmation failed: %s | Error: %v. Reconnecting...\n", addr, strings.TrimSpace(response), err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		fmt.Printf("[SUB][%s] Successfully subscribed. Waiting for messages...\n", addr)

		// 3. Loop to read messages indefinitely
		for {
			message, err := reader.ReadString('\n')
			if err != nil {
				// Server closed the connection or error occurred (Primary killed)
				fmt.Printf("\n--- [SUB][%s] Connection interrupted (Error: %v). Retrying connection... ---\n", addr, err)
				conn.Close()
				break // Break inner loop to trigger outer for-loop retry
			}

			// The subscriber prints the received message.
			fmt.Printf("-> [%s] Received: %s (Time Received: %s)\n", addr, strings.TrimSpace(message), time.Now().Format("15:04:05.000"))
		}
	}
}

func main() {
	flag.Parse()

	if topic == "" {
		fmt.Println("Error: Topic must be specified. Please use --topic=<topic_name>.")
		os.Exit(1)
	}

	fmt.Printf("--- Subscriber Started (Dual Subscription) ---\n")
	fmt.Printf("Subscribed Topic: %s | Primary: %s | Backup: %s\n", topic, primaryAddr, backupAddr)

	// *** Key: Start two Goroutines to achieve Dual Subscription (Assignment Requirement 1) ***
	go subscribeToBroker(primaryAddr, topic)
	go subscribeToBroker(backupAddr, topic)

	// Keep the main goroutine alive
	fmt.Println("\nWaiting for messages... Please run Broker and Publisher.")
	select {} // Block forever
}
