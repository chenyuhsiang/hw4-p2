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

	// Connection and Retry Settings
	connectionTimeout = 3 * time.Second
	retryDelay        = 5 * time.Second
)

func init() {
	flag.StringVar(&primaryAddr, "primary-addr", "localhost:5555", "Primary Broker Address")
	flag.StringVar(&backupAddr, "backup-addr", "localhost:5556", "Backup Broker Address")
	flag.StringVar(&topic, "topic", "topicC", "The topic name to subscribe to")
}

// subscribeToBroker is responsible for connecting to a single Broker and receiving messages.
// It includes logic for continuous reconnection retry to handle Broker failure and failover.
func subscribeToBroker(addr string, topic string) {
	fmt.Printf("[SUB] Starting listener for Broker: %s\n", addr)

	// Continuous connection retry
	for {
		// 1. Connection Attempt with Timeout
		conn, err := net.DialTimeout(serverType, addr, connectionTimeout)
		if err != nil {
			fmt.Printf("[SUB][%s] Connection failed: %v. Retrying after %v...\n", addr, err, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		fmt.Printf("[SUB][%s] Connection successful, attempting to subscribe to topic: %s\n", addr, topic)

		// 2. Send initial SUBSCRIBE request
		subscribeCmd := fmt.Sprintf("SUBSCRIBE %s\n", topic)
		_, err = conn.Write([]byte(subscribeCmd))
		if err != nil {
			fmt.Printf("[SUB][%s] Failed to send SUBSCRIBE: %v. Reconnecting...\n", addr, err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// 3. Read confirmation
		reader := bufio.NewReader(conn)
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		response, err := reader.ReadString('\n')
		conn.SetReadDeadline(time.Time{})

		if err != nil || !strings.HasPrefix(response, "OK") {
			fmt.Printf("[SUB][%s] Subscription confirmation failed: %s | Error: %v. Reconnecting...\n", addr, strings.TrimSpace(response), err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		fmt.Printf("[SUB][%s] Successfully subscribed. Waiting for messages...\n", addr)

		// 4. Loop to read messages indefinitely
		for {
			message, err := reader.ReadString('\n')
			if err != nil {
				// Server closed the connection or error occurred (Primary killed/Failover)
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

	if primaryAddr == "" && backupAddr == "" {
		fmt.Println("Error: Both primary-addr and backup-addr are empty. Cannot start subscriber.")
		os.Exit(1)
	}

	fmt.Printf("--- Subscriber Started (Dual Subscription) ---\n")
	fmt.Printf("Subscribed Topic: %s | Primary: %s | Backup: %s\n", topic, primaryAddr, backupAddr)

	if primaryAddr != "" {
		go subscribeToBroker(primaryAddr, topic)
	}
	if backupAddr != "" {
		go subscribeToBroker(backupAddr, topic)
	}

	fmt.Println("\nWaiting for messages... Please run Broker and Publisher.")
	select {}
}
