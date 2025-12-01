package main

import (
	"bufio"
	"container/list"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	primaryAddr string
	backupAddr  string
	serverType  = "tcp"
	topic       string
	ackTimeout  = 500 * time.Millisecond // Required 500ms ACK timeout
)

// Active state for publisher
var (
	currentAddr string
	retryDelay      = 3 * time.Second
	maxRetries      = 3
	seq         int = 0

	// Control Variable for Failover (Primary is assumed alive until ACK timeout)
	primaryAlive bool = true

	// Stores the last 5 sent messages
	recentMessages    *list.List = list.New()
	maxRecentMessages            = 5
	logMutex          sync.Mutex
)

// SentMessage stores the necessary info for resending
type SentMessage struct {
	Seq     int
	Payload string
}

func init() {
	flag.StringVar(&primaryAddr, "primary-addr", "localhost:5555", "Primary Broker Address")
	flag.StringVar(&backupAddr, "backup-addr", "localhost:5556", "Backup Broker Address")
	flag.StringVar(&topic, "topic", "topicC", "Topic name for publishing messages")
}

// storeMessage logs the message and maintains the maxRecentMessages limit.
func storeMessage(seq int, payload string) {
	logMutex.Lock()
	defer logMutex.Unlock()

	// Add new message to the front
	recentMessages.PushFront(SentMessage{Seq: seq, Payload: payload})

	// Remove oldest message if the list exceeds the limit
	if recentMessages.Len() > maxRecentMessages {
		recentMessages.Remove(recentMessages.Back())
	}
}

// connectWithRetry attempts to establish a connection to a broker.
func connectWithRetry() (net.Conn, string, error) {
	var addresses []string

	// Publisher first tries Primary, then Backup (as long as it thinks Primary is alive)
	if primaryAlive {
		addresses = []string{primaryAddr, backupAddr}
	} else {
		// After Primary failure is detected, only try Backup
		addresses = []string{backupAddr}
	}

	// Calculate max attempts (e.g., 3 retries on Primary, then 3 retries on Backup)
	maxAttempts := maxRetries * len(addresses)

	// Initialize or switch currentAddr for the first attempt
	if currentAddr == "" {
		currentAddr = addresses[0]
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Determine which address to try next based on the addresses list
		addrToTry := addresses[attempt%len(addresses)]

		if currentAddr != addrToTry && attempt > 0 {
			currentAddr = addrToTry
			fmt.Printf("[PUB] Attempting to switch connection to standby Broker: %s\n", currentAddr)
		} else if attempt > 0 {
			fmt.Printf("[PUB] Connection failed, retrying connection to %s in %v...\n", currentAddr, retryDelay)
			time.Sleep(retryDelay)
		}

		// Ensure we only try primaryAddr if primaryAlive is true
		if !primaryAlive && currentAddr == primaryAddr {
			continue
		}

		conn, err := net.DialTimeout(serverType, currentAddr, 1*time.Second)
		if err == nil {
			return conn, currentAddr, nil
		}

		fmt.Printf("[PUB] Connection to %s failed: %v\n", currentAddr, err)
	}

	return nil, "", fmt.Errorf("Maximum retry attempts reached, unable to connect to any available Broker")
}

func sendRecentMessagesToBackup() {
	logMutex.Lock()
	defer logMutex.Unlock()

	fmt.Println("\n--- !!! Primary Crash Detected: Resending last 5 messages to Backup Broker !!! ---")

	conn, err := net.DialTimeout(serverType, backupAddr, 1*time.Second)
	if err != nil {
		fmt.Printf("[RESEND] Cannot connect to Backup Broker %s to resend messages: %v\n", backupAddr, err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Iterate from newest to oldest
	for e := recentMessages.Front(); e != nil; e = e.Next() {
		msg := e.Value.(SentMessage)

		// Format the PUBLISH command: PUBLISH <topic> <seq> <message>
		publishCmd := fmt.Sprintf("PUBLISH %s %d %s\n", topic, msg.Seq, msg.Payload)

		_, err := conn.Write([]byte(publishCmd))
		if err != nil {
			fmt.Printf("[RESEND] Failed to send Seq: %d to Backup %s: %v\n", msg.Seq, backupAddr, err)
			continue
		}

		// Wait for ACK/NACK from the backup
		conn.SetReadDeadline(time.Now().Add(ackTimeout))
		response, err := reader.ReadString('\n')

		if strings.HasPrefix(response, "ACK") {
			fmt.Printf("[RESEND] Successfully resent Seq: %d, Response: %s\n", msg.Seq, strings.TrimSpace(response))
		} else if strings.HasPrefix(response, "NACK") {
			fmt.Printf("[RESEND] Sent Seq: %d, Backup NACKed: %s (Expected before failover)\n", msg.Seq, strings.TrimSpace(response))
		} else {
			fmt.Printf("[RESEND] Sent Seq: %d, Unexpected response/error from Backup: %v, Response: %s\n", msg.Seq, err, strings.TrimSpace(response))
		}
	}

	fmt.Println("--- Resend operation complete. ---")

	// Update state after resending: Mark Primary as dead and switch connection
	primaryAlive = false
	currentAddr = backupAddr
}

// printRecentMessagesAndExit is called before the program terminates.
func printRecentMessagesAndExit(reason string) {
	fmt.Printf("\n--- Publisher Shutting Down (%s) ---\n", reason)

	// Print the last 5 messages sent
	logMutex.Lock()
	fmt.Println("\n--- Last 5 Sent Messages (for verification) ---")
	for e := recentMessages.Front(); e != nil; e = e.Next() {
		msg := e.Value.(SentMessage)
		fmt.Println("   ", fmt.Sprintf("Seq: %d, Payload: %s", msg.Seq, msg.Payload))
	}
	logMutex.Unlock()
	os.Exit(0)
}

func main() {
	flag.Parse()

	fmt.Println("--- Publisher Started ---")
	fmt.Printf("Topic: %s | Rate: 10 Hz | Log Size: %d | ACK Timeout: %v\n", topic, maxRecentMessages, 10, ackTimeout)
	fmt.Println("Press Ctrl+C at any time to print log and exit.")

	// Set up the ticker for 10 Hz (every 100 milliseconds)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Total experiment run time
	totalTimer := time.After(30 * time.Second)

	exitChan := make(chan os.Signal, 1)
	// Register to catch interrupt and terminate signals
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {

		// Handle OS termination signal
		case sig := <-exitChan:
			printRecentMessagesAndExit(fmt.Sprintf("Signal Received: %v", sig))
			return

		// Timer for Total Test Time
		case <-totalTimer:
			printRecentMessagesAndExit("Total Test Time Expired (30s)")
			return

		case <-ticker.C:
			// Increment sequence number *before* sending
			seq++
			messagePayload := fmt.Sprintf("Time: %s", time.Now().Format("15:04:05.000"))

			// 1. Establish connection (with retry and failover logic)
			conn, connectedAddr, err := connectWithRetry()
			if err != nil {
				fmt.Println("[WARN] Unable to connect to any Broker, skipping this send attempt.")
				seq-- // Decrement seq because this attempt failed before send
				continue
			}

			publishCmd := fmt.Sprintf("PUBLISH %s %d %s\n", topic, seq, messagePayload)

			// 2. Send PUBLISH request
			_, err = conn.Write([]byte(publishCmd))
			if err != nil {
				fmt.Printf("[PUB] Failed to send Seq: %d to %s: %v\n", seq, connectedAddr, err.Error())
				conn.Close()
				seq-- // Decrement seq because the send failed; we need to retry this sequence number
				time.Sleep(retryDelay)
				continue
			}

			// 3. Wait for ACK
			conn.SetReadDeadline(time.Now().Add(ackTimeout)) // Use the required 500ms timeout
			reader := bufio.NewReader(conn)

			response, err := reader.ReadString('\n')
			conn.Close()

			if err != nil || !strings.HasPrefix(response, "ACK") {
				// Failure: ACK failed/timeout
				fmt.Printf("[PUB] Failed to receive ACK for Seq: %d (Error: %v). Will retry sending this Seq next time.\n", seq, err)

				// Decrement seq because processing failed (ACK not received); need to retry
				seq--

				if connectedAddr == primaryAddr {
					fmt.Printf("[PUB] ACK timeout on Primary %s. Assuming Primary Crash.\n", connectedAddr)
					sendRecentMessagesToBackup()
				}

				continue
			} else {
				storeMessage(seq, messagePayload)
				fmt.Printf("[PUB] Successfully sent Seq: %d, Broker: %s, Response: %s\n", seq, connectedAddr, strings.TrimSpace(response))
			}
		}
	}
}
