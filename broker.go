package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	serverHost string
	serverPort string
	serverType = "tcp"

	// Fault-Tolerance Configuration
	role     string // "primary" or "backup"
	peerAddr string // The address (host:port) of the other broker

	// Polling configuration for Backup
	pollingPeriod = 3 * time.Second
	echoTimeout   = 200 * time.Millisecond

	// Configuration for Primary's Edge Computing
	minProcessingTime = 50 * time.Millisecond
	maxProcessingTime = 150 * time.Millisecond
)

// Global Broker State
type BrokerState struct {
	Role        string // "primary" or "backup"
	PeerAddress string

	// Message Log: map of SequenceNumber (int) -> Full Message Text (string)
	// Used by the Backup to store replicated messages
	MsgLog   map[int]string
	LogMutex sync.RWMutex // Changed from sync.Mutex to sync.RWMutex to support RLock/RUnlock
	NextSeq  int          // Used only when transitioning from backup to primary

	// Connection status to the peer (mostly for Primary's control signals)
	PeerAlive bool
}

var brokerState *BrokerState = &BrokerState{
	MsgLog:    make(map[int]string),
	NextSeq:   1,
	PeerAlive: true,
}

// Message struct is used to pass data between Goroutines via channel
type Message struct {
	conn net.Conn // The connection that sent the message
	text string   // The raw message content from client
}

// Subscriber information
// Note: We don't store the topic here, as the topic is the key in TopicSubscribers map.
type Subscriber struct {
	conn net.Conn
	addr string
}

// Global state for the broker
var (
	// TopicSubscribers stores the subscription map (hash table):
	// TopicName (string) -> List of Subscriber structs ([]Subscriber)
	TopicSubscribers = make(map[string][]Subscriber)

	// Mutex to protect concurrent access to TopicSubscribers from multiple goroutines
	subscribersMutex sync.RWMutex // RWMutex for both reading (subscribing) and writing (delivering)
)

func init() {
	// Define flags with names, default values, and help descriptions
	flag.StringVar(&serverHost, "host", "0.0.0.0", "The IP address to bind to (e.g., 0.0.0.0)")
	flag.StringVar(&serverPort, "port", "5555", "The port to listen on (e.g., 5555)")
	flag.StringVar(&role, "role", "primary", "Broker role: 'primary' or 'backup'")
	flag.StringVar(&peerAddr, "peer-addr", "localhost:5556", "Address of the peer broker (host:port)")

	// Initialize random seed for processing time simulation
	rand.Seed(time.Now().UnixNano())
}

// *** 新增: 模擬邊緣計算的忙碌迴圈 (Busy Loop) ***
// edgeCompute simulates the pseudo edge computing time (50-150ms).
func edgeCompute() {
	// Calculate a random duration between 50ms and 150ms
	minNanos := minProcessingTime.Nanoseconds()
	maxNanos := maxProcessingTime.Nanoseconds()

	// The difference is 100ms
	diff := maxNanos - minNanos

	// rand.Int63n returns [0, diff)
	randomNanos := rand.Int63n(diff + 1) // +1 to include 150ms

	// Total duration
	duration := time.Duration(minNanos + randomNanos)

	start := time.Now()
	// Use a sleep function as an adequate simulation of time consumption
	// and to prevent excessive CPU consumption from an actual busy loop.
	time.Sleep(duration)

	fmt.Printf("[EDGE] Pseudo Edge Computing complete. Duration: %v\n", time.Since(start))
}

// cleanupSubscription removes a connection from all subscription lists.
func cleanupSubscription(conn net.Conn) {
	subscribersMutex.Lock()
	defer subscribersMutex.Unlock()

	clientAddr := conn.RemoteAddr().String()

	// Iterate over all topics
	for topic := range TopicSubscribers {
		newList := []Subscriber{}
		for _, sub := range TopicSubscribers[topic] {
			// If the connection address matches, skip it (do not add to newList)
			if sub.addr != clientAddr {
				newList = append(newList, sub)
			}
		}

		// Update the list for the topic
		TopicSubscribers[topic] = newList

		// Optional: Clean up topic entry if list is empty
		if len(newList) == 0 {
			delete(TopicSubscribers, topic)
		}
	}
	// Close the connection explicitly
	conn.Close()
	fmt.Printf("[BROKER] Cleaned up subscription for %s and closed connection.\n", clientAddr)
}

// deliverMessage sends a message to all subscribers of a specific topic.
// 'message' argument is expected to be in the format "<topic> <seq> <payload>"
func deliverMessage(topic string, message string) {
	subscribersMutex.RLock() // Use RLock for reading the map
	defer subscribersMutex.RUnlock()

	// 1. Check if the topic exists
	subs, found := TopicSubscribers[topic]
	if !found || len(subs) == 0 {
		fmt.Printf("[DELIVER] No subscribers for topic %s. Message dropped.\n", topic)
		return
	}

	fmt.Printf("[DELIVER] Delivering message to %d subscribers on topic %s: %s\n", len(subs), topic, strings.TrimSpace(message))

	// Prepare the final message format to send to subscribers: MESSAGE <topic> <seq> <payload>
	fullMessage := fmt.Sprintf("MESSAGE %s\n", strings.TrimSpace(message))

	// 2. Iterate through all subscribers and send the message
	for _, sub := range subs {
		// Use a temporary variable to hold the connection for write
		conn := sub.conn
		_, err := conn.Write([]byte(fullMessage))
		if err != nil {
			fmt.Printf("[DELIVER] ERROR sending to %s on topic %s: %v. Marking for cleanup.\n", sub.addr, topic, err)
			// For simplicity, we rely on the handleConnection goroutine to eventually detect and clean up the dead connection.
		}
	}
}

// brokerToPeerGoroutine handles sending replicated messages to the peer broker.
// *** 修正一: 已移除 func 上的 "go"，使其成為同步阻塞呼叫 (Synchronous Blocking Call) ***
func brokerToPeerGoroutine(topic string, seq int, message string) {
	fmt.Printf("[REPLICA] Attempting to replicate Seq: %d to peer %s\n", seq, brokerState.PeerAddress)

	conn, err := net.Dial(serverType, brokerState.PeerAddress)
	if err != nil {
		fmt.Printf("[REPLICA] Connection to peer %s failed: %v. Peer is down.\n", brokerState.PeerAddress, err)
		brokerState.PeerAlive = false // Mark peer as dead
		return
	}
	defer conn.Close()

	// Format the REPLICATE command: REPLICATE <topic> <seq> <message>
	replicateCmd := fmt.Sprintf("REPLICATE %s %d %s\n", topic, seq, message)

	_, err = conn.Write([]byte(replicateCmd))
	if err != nil {
		fmt.Printf("[REPLICA] Failed to send REPLICATE command to peer: %v\n", err)
		return
	}

	// Wait for ACK from the peer
	conn.SetReadDeadline(time.Now().Add(echoTimeout))
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')

	if err != nil || !strings.HasPrefix(response, "ACK_REPLICA") {
		fmt.Printf("[REPLICA] Did not receive ACK from peer or error: %v, Response: %s\n", err, strings.TrimSpace(response))
		brokerState.PeerAlive = false // Treat replication failure as peer unreliable/down
	} else {
		fmt.Printf("[REPLICA] Successfully replicated Seq: %d.\n", seq)
		brokerState.PeerAlive = true // Reconfirm peer is alive
	}
}

// *** 新增: 通知 Backup 清除 Log 的 Goroutine ***
// brokerToPeerClearLogGoroutine notifies the backup to delete the log entry after delivery.
func brokerToPeerClearLogGoroutine(seq int) {
	if !brokerState.PeerAlive {
		fmt.Printf("[CLEAR] Peer is down. Skipping log clear for Seq: %d.\n", seq)
		return
	}

	fmt.Printf("[CLEAR] Notifying peer %s to clear log entry Seq: %d\n", brokerState.PeerAddress, seq)

	conn, err := net.Dial(serverType, brokerState.PeerAddress)
	if err != nil {
		fmt.Printf("[CLEAR] Connection to peer %s failed: %v. Peer is down.\n", brokerState.PeerAddress, err)
		brokerState.PeerAlive = false // Mark peer as dead
		return
	}
	defer conn.Close()

	// Format the CLEAR_REPLICATE command: CLEAR_REPLICATE <seq>
	clearCmd := fmt.Sprintf("CLEAR_REPLICATE %d\n", seq)

	_, err = conn.Write([]byte(clearCmd))
	if err != nil {
		fmt.Printf("[CLEAR] Failed to send CLEAR_REPLICATE command to peer: %v\n", err)
		return
	}

	// Wait for ACK from the peer
	conn.SetReadDeadline(time.Now().Add(echoTimeout))
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')

	if err != nil || !strings.HasPrefix(response, "ACK_CLEAR") {
		fmt.Printf("[CLEAR] Did not receive ACK_CLEAR from peer or error: %v, Response: %s\n", err, strings.TrimSpace(response))
	} else {
		fmt.Printf("[CLEAR] Successfully received ACK_CLEAR for Seq: %d.\n", seq)
	}
}

// deliverAllMessages is called by a backup broker when it transitions to primary.
// It sends all un-delivered messages from the log to the subscribers.
func deliverAllMessages() {
	// Use RLock for reading the log data safely
	brokerState.LogMutex.RLock()
	// To ensure ordered delivery, we must iterate by sequence number.
	var seqs []int
	for seq := range brokerState.MsgLog {
		seqs = append(seqs, seq)
	}
	brokerState.LogMutex.RUnlock() // Unlock RLock before calling deliverMessage (which uses subscribersMutex)

	// *** 修改 2: 實際呼叫 sort.Ints 以確保順序遞送 ***
	sort.Ints(seqs)

	if len(seqs) == 0 {
		fmt.Println("[FAILOVER] Message log is empty. No backlog to deliver.")
		return
	}

	fmt.Printf("[FAILOVER] Starting delivery of %d backlog messages from sequence log.\n", len(seqs))

	maxSeq := 0

	for _, seq := range seqs {
		brokerState.LogMutex.RLock()
		fullMsg, found := brokerState.MsgLog[seq]
		brokerState.LogMutex.RUnlock()

		if !found {
			continue // Skip if already cleared during a race condition
		}

		// fullMsg should contain the topic and actual payload in the format:
		// topicC 10 Time: 15:04:05.000
		parts := strings.Fields(fullMsg)
		if len(parts) < 3 {
			fmt.Printf("[FAILOVER] Skipping malformed log entry at seq %d: %s\n", seq, fullMsg)
			continue
		}
		topic := parts[0]
		// The actual message is everything after the topic
		// The `deliverMessage` function expects the full message string for delivery formatting.
		// payload := strings.Join(parts[2:], " ") // Unused, we pass fullMsg to deliverMessage

		// 1. Edge Compute (Simulate the required processing before delivery)
		edgeCompute()

		// 2. Deliver message to subscribers
		// Note: deliverMessage uses its own lock on subscribersMutex
		deliverMessage(topic, fullMsg)
		fmt.Printf("[FAILOVER] Delivered Seq %d: %s\n", seq, strings.TrimSpace(fullMsg))

		if seq > maxSeq {
			maxSeq = seq
		}
	}

	// After delivering, the log can be cleared and NextSeq updated.
	brokerState.LogMutex.Lock()
	brokerState.NextSeq = maxSeq + 1
	brokerState.MsgLog = make(map[int]string) // Clear the log
	brokerState.LogMutex.Unlock()

	fmt.Printf("[FAILOVER] Backlog delivery complete. Log cleared. New NextSeq: %d\n", brokerState.NextSeq)
}

// appLogic is the central processing unit for all incoming client messages.
func appLogic(inputChan <-chan Message, outputChan chan<- string) {
	fmt.Println("Application Logic Goroutine Started.")
	for msg := range inputChan {
		text := strings.TrimSpace(msg.text)
		parts := strings.Fields(text)
		command := ""
		if len(parts) > 0 {
			command = strings.ToUpper(parts[0])
		}

		conn := msg.conn
		clientAddr := conn.RemoteAddr().String()

		switch command {
		case "SUBSCRIBE":
			// ... (Existing SUBSCRIBE logic) ...
			if len(parts) < 2 {
				conn.Write([]byte("ERROR Missing topic\n"))
				break
			}
			topic := parts[1]
			// Lock for writing/modifying the subscribers map
			subscribersMutex.Lock()
			TopicSubscribers[topic] = append(TopicSubscribers[topic], Subscriber{conn: conn, addr: clientAddr})
			subscribersMutex.Unlock()

			fmt.Printf("[BROKER] %s subscribed to topic %s.\n", clientAddr, topic)
			conn.Write([]byte("OK Subscribed\n"))

		case "PUBLISH":
			// Primary broker logic for PUBLISH
			// Format: PUBLISH <topic> <seq> <message>
			if brokerState.Role == "primary" {
				if len(parts) < 4 {
					conn.Write([]byte("ERROR Invalid PUBLISH format. Must include topic, sequence, and message.\n"))
					break
				}
				topic := parts[1]
				payload := strings.Join(parts[3:], " ")
				seqStr := parts[2]
				seq, err := strconv.Atoi(seqStr)
				if err != nil {
					conn.Write([]byte("ERROR Invalid sequence number.\n"))
					break
				}

				// The full message logged/replicated is: <topic> <seq> <payload>
				fullMessage := fmt.Sprintf("%s %d %s", topic, seq, payload)

				// 1. Replicate to Backup (if alive) - 阻塞式呼叫 (Blocking Call)
				if brokerState.PeerAlive && brokerState.PeerAddress != "" {
					brokerToPeerGoroutine(topic, seq, payload) // Synchronous replication
				} else {
					fmt.Println("[WARN] Peer is marked down or not configured. Skipping replication.")
				}

				// 2. *** 新增: 偽邊緣計算 ***
				edgeCompute()

				// 3. Deliver message to subscribers
				deliverMessage(topic, fullMessage)

				// 4. *** 新增: 通知 Backup 清除 Log (非同步呼叫) ***
				// Only clear log if replication was attempted, and delivery succeeded (implicitly assumed)
				if brokerState.PeerAlive && brokerState.PeerAddress != "" {
					go brokerToPeerClearLogGoroutine(seq)
				}

				// 5. Send ACK to publisher (最後一步，符合流程)
				conn.Write([]byte(fmt.Sprintf("ACK %d\n", seq)))

				// 6. Close the connection (Publisher always closes after ACK)
				conn.Close()

			} else { // backup broker received PUBLISH
				// *** 修正二: Backup Broker 收到 PUBLISH 時，先 Log 訊息，再 NACK ***

				// 1. Extract message details
				if len(parts) < 4 {
					conn.Write([]byte("ERROR Invalid PUBLISH format. Must include topic, sequence, and message.\n"))
					conn.Close()
					break
				}
				topic := parts[1]
				payload := strings.Join(parts[3:], " ")
				seqStr := parts[2]
				seq, err := strconv.Atoi(seqStr)
				if err != nil {
					conn.Write([]byte("ERROR Invalid sequence number.\n"))
					conn.Close()
					break
				}

				// The full message logged/replicated is: <topic> <seq> <payload>
				fullMessage := fmt.Sprintf("%s %d %s", topic, seq, payload)

				// 2. Log the message (Handles publisher's resend)
				brokerState.LogMutex.Lock()
				brokerState.MsgLog[seq] = fullMessage
				brokerState.LogMutex.Unlock()
				fmt.Printf("[REPLICA/PUB-RESEND] Backup received PUBLISH from %s, logged Seq: %d. Sending NACK.\n", clientAddr, seq)

				// 3. Send NACK and close (as it's not the Primary)
				conn.Write([]byte("NACK Not Primary Broker\n"))
				conn.Close()
			}

		case "REPLICATE":
			// Backup broker logic for REPLICATE
			// Format: REPLICATE <topic> <seq> <message>
			if brokerState.Role == "backup" {
				// ... (Existing REPLICATE logic) ...
				if len(parts) < 4 {
					fmt.Println("[REPLICA] ERROR Invalid REPLICATE format.")
					conn.Write([]byte("ERROR Invalid REPLICATE format.\n"))
					break
				}
				topic := parts[1]
				seqStr := parts[2]
				payload := strings.Join(parts[3:], " ")

				seq, err := strconv.Atoi(seqStr)
				if err != nil {
					fmt.Println("[REPLICA] ERROR Invalid sequence number in REPLICATE.")
					conn.Write([]byte("ERROR Invalid sequence number.\n"))
					break
				}

				// The full message logged is: <topic> <seq> <payload>
				fullMessage := fmt.Sprintf("%s %d %s", topic, seq, payload)

				// Lock for writing to the message log
				brokerState.LogMutex.Lock()
				brokerState.MsgLog[seq] = fullMessage
				brokerState.LogMutex.Unlock()

				fmt.Printf("[REPLICA] Logged Seq: %d for topic %s.\n", seq, topic)
				conn.Write([]byte("ACK_REPLICA\n"))
				// Close is handled by the primary's brokerToPeerGoroutine defer conn.Close()
			} else {
				// Primary received REPLICATE - unexpected, ignore or log error
				fmt.Printf("[REPLICA] Primary received unexpected REPLICATE command from %s. Ignoring.\n", clientAddr)
			}

		case "CLEAR_REPLICATE":
			// *** 新增: Backup 處理 Log 清除命令 ***
			// Format: CLEAR_REPLICATE <seq>
			if brokerState.Role == "backup" {
				if len(parts) < 2 {
					fmt.Println("[CLEAR] ERROR Missing sequence number in CLEAR_REPLICATE.")
					conn.Write([]byte("ERROR Missing sequence number.\n"))
					break
				}
				seqStr := parts[1]
				seq, err := strconv.Atoi(seqStr)
				if err != nil {
					fmt.Println("[CLEAR] ERROR Invalid sequence number in CLEAR_REPLICATE.")
					conn.Write([]byte("ERROR Invalid sequence number.\n"))
					break
				}

				brokerState.LogMutex.Lock()
				if _, exists := brokerState.MsgLog[seq]; exists {
					delete(brokerState.MsgLog, seq)
					fmt.Printf("[CLEAR] Successfully cleared Seq: %d from message log. Current log size: %d\n", seq, len(brokerState.MsgLog))
				} else {
					fmt.Printf("[CLEAR] Warning: Seq: %d not found in log (already delivered?).\n", seq)
				}
				brokerState.LogMutex.Unlock()

				conn.Write([]byte("ACK_CLEAR\n"))
			} else {
				fmt.Printf("[CLEAR] Primary received unexpected CLEAR_REPLICATE command from %s. Ignoring.\n", clientAddr)
			}

		case "RESEND_LOG":
			// ... (Skipped RESEND_LOG logic as per previous design decision) ...
			conn.Write([]byte("ERROR RESEND_LOG command is not supported. Please use PUBLISH to the current Primary.\n"))

		case "GOODBYE":
			// ... (Existing GOODBYE logic) ...
			fmt.Printf("[BROKER] %s sent GOODBYE. Cleaning up.\n", clientAddr)
			// Cleanup will close the connection.
			cleanupSubscription(conn)

		case "STATUS":
			// Basic status check for polling
			conn.Write([]byte("OK STATUS\n"))

		default:
			conn.Write([]byte("ERROR Unknown command\n"))
		}
	}
}

// brokerControlGoroutine handles internal broker tasks, like failover logic.
func brokerControlGoroutine(inputChan chan<- Message) {
	fmt.Println("Broker Control Goroutine Started.")

	// Backup broker needs a polling mechanism to detect primary failure.
	if brokerState.Role == "backup" {
		ticker := time.NewTicker(pollingPeriod)
		defer ticker.Stop()

		for range ticker.C {
			// Check if we have transitioned to primary inside the loop
			// We must lock to check the role safely
			brokerState.LogMutex.RLock()
			currentRole := brokerState.Role
			brokerState.LogMutex.RUnlock()
			if currentRole != "backup" {
				fmt.Println("[POLL] Role changed to Primary. Stopping polling.")
				return
			}

			fmt.Printf("[POLL] Checking status of Primary at %s...\n", brokerState.PeerAddress)
			conn, err := net.Dial(serverType, brokerState.PeerAddress)

			if err != nil {
				// Primary is down! Initiate Failover
				fmt.Printf("[POLL] Primary %s is DOWN! Error: %v\n", brokerState.PeerAddress, err)

				// Lock the state to update role and execute failover steps
				brokerState.LogMutex.Lock()       // We use the LogMutex to protect role change too
				if brokerState.Role == "backup" { // Double check we are still backup
					fmt.Println("--- !!! INITIATING FAILOVER: PROMOTING TO PRIMARY !!! ---")
					brokerState.Role = "primary"
					brokerState.PeerAlive = false // No longer relevant, but set to false
					fmt.Printf("[FAILOVER] New Next Sequence ID set to %d\n", brokerState.NextSeq)

					// 1. Deliver all backlog messages (MsgLog)
					// We unlock before calling deliverAllMessages to avoid deadlock
					// with the internal RLock/Lock and subscribersMutex.
					brokerState.LogMutex.Unlock()
					deliverAllMessages()
					// Now LogMutex is unlocked and MsgLog is cleared, NextSeq is updated.

					// 2. We now continue as the Primary.
					// We return to exit the polling loop as a backup.
					return
				}
				brokerState.LogMutex.Unlock() // Unlock if the role check failed (shouldn't happen)

			} else {
				// Primary is alive, send STATUS command
				fmt.Printf("[POLL] Connection to Primary %s established. Sending STATUS...\n", brokerState.PeerAddress)
				conn.Write([]byte("STATUS\n"))

				// Wait for a response (OK STATUS)
				conn.SetReadDeadline(time.Now().Add(echoTimeout))
				reader := bufio.NewReader(conn)
				response, err := reader.ReadString('\n')
				conn.Close()

				if err != nil || !strings.HasPrefix(response, "OK STATUS") {
					fmt.Printf("[POLL] Primary did not reply with OK STATUS. Assuming failure: %v\n", err)
					brokerState.LogMutex.Lock()
					if brokerState.Role == "backup" {
						fmt.Println("--- !!! INITIATING FAILOVER: PROMOTING TO PRIMARY (STATUS TIMEOUT) !!! ---")
						brokerState.Role = "primary"
						brokerState.PeerAlive = false
						fmt.Printf("[FAILOVER] New Next Sequence ID set to %d\n", brokerState.NextSeq)

						// 1. Deliver all backlog messages (MsgLog)
						brokerState.LogMutex.Unlock() // Unlock before calling deliverAllMessages
						deliverAllMessages()

						// 2. We break the polling loop as a backup.
						return
					}
					brokerState.LogMutex.Unlock()
				} else {
					fmt.Println("[POLL] Primary is confirmed ALIVE.")
					brokerState.PeerAlive = true // Confirm Primary is alive (from Backup's perspective)
				}
			}
		}
	}

	// This is where any Primary-specific background tasks would go, e.g.,
	// periodically checking if the Peer has come back up to notify publishers, etc.
	if brokerState.Role == "primary" {
		// Primary's background check for peer recovery (optional, but good practice)
		// For simplicity, we skip this check and assume the peer stays backup/down unless manually fixed.
	}
}

// handleConnection manages I/O for a single client connection.
func handleConnection(conn net.Conn, inputChan chan<- Message, wg *sync.WaitGroup) {
	defer wg.Done()
	clientAddr := conn.RemoteAddr().String()
	fmt.Printf("[PROXY] New connection from: %s\n", clientAddr)

	reader := bufio.NewReader(conn)

	for {
		// Read client message (blocking call)
		message, err := reader.ReadString('\n')
		if err != nil {
			// Client disconnected or error occurred
			if err.Error() == "EOF" {
				fmt.Printf("[PROXY] Client %s disconnected gracefully.\n", clientAddr)
			} else {
				fmt.Printf("[PROXY] Client %s disconnected (Error: %v)\n", clientAddr, err)
			}

			// Clean up the subscription, which also closes the connection safely
			cleanupSubscription(conn)
			return
		}

		// The raw message needs to be parsed for command to handle broker-to-broker
		parts := strings.Fields(strings.TrimSpace(message))
		command := ""
		if len(parts) > 0 {
			command = strings.ToUpper(parts[0])
		}

		// Note: We need to include CLEAR_REPLICATE here for the primary to close the connection after the command.
		if command == "REPLICATE" || command == "STATUS" || command == "CLEAR_REPLICATE" {
			// Broker-to-broker communication must be handled by appLogic
			inputChan <- Message{conn: conn, text: message}
			// Important: broker-to-broker connections are single command/response, so we return here.
			return

		} else {
			// Other commands (SUBSCRIBE, PUBLISH, GOODBYE) are handled by appLogic as before
			inputChan <- Message{conn: conn, text: message}
			// For SUBSCRIBE, the connection stays open. For GOODBYE/PUBLISH, it will close later.
		}
	}
}

// proxy Goroutine (Acceptor)
func proxy(listener net.Listener, inputChan chan<- Message) {
	fmt.Println("Proxy Goroutine (Acceptor/IO Handler) Started.")
	var wg sync.WaitGroup

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			return
		}
		wg.Add(1)
		go handleConnection(conn, inputChan, &wg)
	}
}

func main() {
	flag.Parse()
	brokerState.Role = strings.ToLower(role)
	brokerState.PeerAddress = peerAddr

	fmt.Printf("--- Starting %s Message Broker ---\n", strings.ToUpper(brokerState.Role))
	fmt.Printf("Listening on: %s:%s\n", serverHost, serverPort)
	fmt.Printf("Peer address: %s\n", brokerState.PeerAddress)
	fmt.Printf("Broker State initialized. Next Sequence ID: %d\n", brokerState.NextSeq)

	// Start the listener
	listener, err := net.Listen(serverType, serverHost+":"+serverPort)
	if err != nil {
		fmt.Println("Error listening: ", err.Error())
		os.Exit(1)
	}
	defer listener.Close()

	// Channels for communication between Goroutines
	inputChan := make(chan Message)
	outputChan := make(chan string) // Not currently used, but good practice

	// Start Goroutines
	go proxy(listener, inputChan)
	go appLogic(inputChan, outputChan)
	go brokerControlGoroutine(inputChan)

	// Keep main goroutine alive
	select {}
}
