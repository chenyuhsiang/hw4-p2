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
	host     string
	port     string
	role     string
	peerAddr string

	// Timeouts
	pollingPeriod = 3 * time.Second
	echoTimeout   = 200 * time.Millisecond
	minProcTime   = 50 * time.Millisecond
	maxProcTime   = 150 * time.Millisecond
	serverType    = "tcp"
)

// BrokerState holds the shared state for the broker
type BrokerState struct {
	Role        string // "primary" or "backup"
	PeerAddress string
	MsgLog      map[int]string // SequenceNumber -> Full Message Text (topic seq payload)
	LogMutex    sync.RWMutex
	NextSeq     int // Used only when transitioning from backup to primary
	PeerAlive   bool
}

var brokerState *BrokerState = &BrokerState{
	MsgLog:    make(map[int]string),
	NextSeq:   1,
	PeerAlive: true,
}

type Message struct {
	conn net.Conn
	text string
}

type Subscriber struct {
	conn net.Conn
	addr string
}

var (
	TopicSubscribers = make(map[string][]Subscriber)
	subscribersMutex sync.RWMutex
)

func init() {
	flag.StringVar(&host, "host", "0.0.0.0", "IP address to bind to")
	flag.StringVar(&port, "port", "5555", "Port to listen on")
	flag.StringVar(&role, "role", "primary", "Broker role: 'primary' or 'backup'")
	flag.StringVar(&peerAddr, "peer-addr", "localhost:5556", "Address of the peer broker")
	rand.Seed(time.Now().UnixNano())
}

// edgeCompute simulates the pseudo edge computing time (50-150ms).
func edgeCompute() {
	diff := maxProcTime.Nanoseconds() - minProcTime.Nanoseconds()
	duration := time.Duration(minProcTime.Nanoseconds() + rand.Int63n(diff+1))
	time.Sleep(duration)
}

// cleanupSubscription removes a connection from all subscription lists.
func cleanupSubscription(conn net.Conn) {
	subscribersMutex.Lock()
	defer subscribersMutex.Unlock()
	addr := conn.RemoteAddr().String()

	for topic, subs := range TopicSubscribers {
		newList := []Subscriber{}
		for _, sub := range subs {
			if sub.addr != addr {
				newList = append(newList, sub)
			}
		}
		TopicSubscribers[topic] = newList
		if len(newList) == 0 {
			delete(TopicSubscribers, topic)
		}
	}
	conn.Close()
	fmt.Printf("[BROKER] Cleaned up connection for %s.\n", addr)
}

// deliverMessage sends a message to all subscribers of a specific topic.
func deliverMessage(topic string, message string) {
	subscribersMutex.RLock()
	defer subscribersMutex.RUnlock()

	subs := TopicSubscribers[topic]
	if len(subs) == 0 {
		return // No subscribers, exit silently
	}

	fullMessage := fmt.Sprintf("MESSAGE %s\n", strings.TrimSpace(message))

	for _, sub := range subs {
		_, err := sub.conn.Write([]byte(fullMessage))
		if err != nil {
			// Rely on handleConnection for eventual cleanup
			fmt.Printf("[DELIVER] Error sending to %s: %v\n", sub.addr, err)
		}
	}
}

// sendPeerCommand handles communication with the peer broker (Primary -> Backup).
func sendPeerCommand(cmd string) (string, error) {
	conn, err := net.Dial(serverType, brokerState.PeerAddress)
	if err != nil {
		brokerState.PeerAlive = false
		return "", fmt.Errorf("connection failed: %w", err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte(cmd + "\n"))
	if err != nil {
		return "", fmt.Errorf("failed to send command: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(echoTimeout))
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read timeout/error: %w", err)
	}

	return strings.TrimSpace(response), nil
}

// brokerToPeerGoroutine handles sending replicated messages to the peer broker.
func brokerToPeerGoroutine(topic string, seq int, payload string) {
	if !brokerState.PeerAlive {
		return
	}

	replicateCmd := fmt.Sprintf("REPLICATE %s %d %s", topic, seq, payload)

	response, err := sendPeerCommand(replicateCmd)
	if err != nil || !strings.HasPrefix(response, "ACK_REPLICA") {
		fmt.Printf("[REPLICA] Failed Seq %d. Response: %s, Error: %v\n", seq, response, err)
		brokerState.PeerAlive = false
	} else {
		fmt.Printf("[REPLICA] Success Seq %d.\n", seq)
		brokerState.PeerAlive = true
	}
}

// brokerToPeerClearLogGoroutine notifies the backup to delete the log entry after delivery.
func brokerToPeerClearLogGoroutine(seq int) {
	if !brokerState.PeerAlive {
		return
	}

	clearCmd := fmt.Sprintf("CLEAR_REPLICATE %d", seq)

	response, err := sendPeerCommand(clearCmd)
	if err != nil || !strings.HasPrefix(response, "ACK_CLEAR") {
		fmt.Printf("[CLEAR] Failed Seq %d. Response: %s, Error: %v\n", seq, response, err)
	} else {
		fmt.Printf("[CLEAR] Cleared Seq %d.\n", seq)
	}
}

// deliverAllMessages is called by a backup broker when it transitions to primary.
func deliverAllMessages() {
	brokerState.LogMutex.RLock()
	var seqs []int
	for seq := range brokerState.MsgLog {
		seqs = append(seqs, seq)
	}
	brokerState.LogMutex.RUnlock()

	sort.Ints(seqs)

	if len(seqs) == 0 {
		fmt.Println("[FAILOVER] Log empty. No backlog to deliver.")
		return
	}

	fmt.Printf("[FAILOVER] Delivering %d backlog messages.\n", len(seqs))
	maxSeq := 0

	for _, seq := range seqs {
		brokerState.LogMutex.RLock()
		fullMsg, found := brokerState.MsgLog[seq]
		brokerState.LogMutex.RUnlock()

		if !found {
			continue
		}

		parts := strings.Fields(fullMsg)
		if len(parts) < 3 {
			continue
		}
		topic := parts[0]

		edgeCompute()
		deliverMessage(topic, fullMsg)
		fmt.Printf("[FAILOVER] Delivered Seq %d.\n", seq)

		if seq > maxSeq {
			maxSeq = seq
		}
	}

	brokerState.LogMutex.Lock()
	brokerState.NextSeq = maxSeq + 1
	brokerState.MsgLog = make(map[int]string)
	brokerState.LogMutex.Unlock()

	fmt.Printf("[FAILOVER] Delivery complete. New NextSeq: %d\n", brokerState.NextSeq)
}

// appLogic is the central processing unit for all incoming client messages.
func appLogic(inputChan <-chan Message) {
	for msg := range inputChan {
		text := strings.TrimSpace(msg.text)
		parts := strings.Fields(text)
		if len(parts) == 0 {
			continue
		}
		command := strings.ToUpper(parts[0])
		conn := msg.conn
		clientAddr := conn.RemoteAddr().String()

		switch command {
		case "SUBSCRIBE":
			if len(parts) < 2 {
				conn.Write([]byte("ERROR Missing topic\n"))
				break
			}
			topic := parts[1]
			subscribersMutex.Lock()
			TopicSubscribers[topic] = append(TopicSubscribers[topic], Subscriber{conn: conn, addr: clientAddr})
			subscribersMutex.Unlock()
			conn.Write([]byte("OK Subscribed\n"))

		case "PUBLISH":
			if len(parts) < 4 {
				conn.Write([]byte("ERROR Invalid PUBLISH format\n"))
				conn.Close()
				break
			}
			topic := parts[1]
			seqStr := parts[2]
			payload := strings.Join(parts[3:], " ")
			seq, _ := strconv.Atoi(seqStr)
			fullMessage := fmt.Sprintf("%s %d %s", topic, seq, payload)

			if brokerState.Role == "primary" {
				if brokerState.PeerAlive {
					brokerToPeerGoroutine(topic, seq, payload)
				}
				edgeCompute()
				deliverMessage(topic, fullMessage)
				if brokerState.PeerAlive {
					go brokerToPeerClearLogGoroutine(seq)
				}
				conn.Write([]byte(fmt.Sprintf("ACK %d\n", seq)))
				conn.Close() // Primary closes connection after ACK

			} else { // backup broker received PUBLISH (Resend or regular)
				brokerState.LogMutex.Lock()
				brokerState.MsgLog[seq] = fullMessage
				brokerState.LogMutex.Unlock()

				fmt.Printf("[PUB-RESEND] Backup received PUBLISH from %s, logged Seq: %d. NACKed.\n", clientAddr, seq)
				conn.Write([]byte("NACK Not Primary Broker\n"))
				conn.Close() // Backup closes connection after NACK
			}

		case "REPLICATE":
			if brokerState.Role == "backup" {
				if len(parts) < 4 {
					conn.Write([]byte("ERROR Invalid REPLICATE format\n"))
					break
				}
				topic := parts[1]
				seqStr := parts[2]
				payload := strings.Join(parts[3:], " ")
				seq, _ := strconv.Atoi(seqStr)
				fullMessage := fmt.Sprintf("%s %d %s", topic, seq, payload)

				brokerState.LogMutex.Lock()
				brokerState.MsgLog[seq] = fullMessage
				brokerState.LogMutex.Unlock()

				conn.Write([]byte("ACK_REPLICA\n"))
			}

		case "CLEAR_REPLICATE":
			if brokerState.Role == "backup" {
				if len(parts) < 2 {
					conn.Write([]byte("ERROR Missing sequence number\n"))
					break
				}
				seq, _ := strconv.Atoi(parts[1])
				brokerState.LogMutex.Lock()
				delete(brokerState.MsgLog, seq)
				brokerState.LogMutex.Unlock()
				conn.Write([]byte("ACK_CLEAR\n"))
			}

		case "GOODBYE":
			cleanupSubscription(conn) // Cleanup also closes conn

		case "STATUS":
			conn.Write([]byte("OK STATUS\n"))

		default:
			conn.Write([]byte("ERROR Unknown command\n"))
		}
	}
}

// brokerControlGoroutine handles internal broker tasks, like failover logic.
func brokerControlGoroutine() {
	if brokerState.Role == "backup" {
		ticker := time.NewTicker(pollingPeriod)
		defer ticker.Stop()

		for range ticker.C {
			brokerState.LogMutex.RLock()
			currentRole := brokerState.Role
			brokerState.LogMutex.RUnlock()
			if currentRole != "backup" {
				return // Role changed, stop polling
			}

			// 1. Poll Primary with STATUS command
			response, err := sendPeerCommand("STATUS")

			// 2. Evaluate Status
			if err != nil || !strings.HasPrefix(response, "OK STATUS") {
				fmt.Printf("[POLL] Primary %s is DOWN (Error: %v, Response: %s)\n", brokerState.PeerAddress, err, response)

				brokerState.LogMutex.Lock()
				if brokerState.Role == "backup" {
					fmt.Println("--- !!! INITIATING FAILOVER: PROMOTING TO PRIMARY !!! ---")
					brokerState.Role = "primary"
					brokerState.PeerAlive = false

					brokerState.LogMutex.Unlock()
					deliverAllMessages() // Delivers backlog, updates NextSeq, and clears log

					return // Exit polling loop
				}
				brokerState.LogMutex.Unlock()
			} else {
				brokerState.PeerAlive = true
			}
		}
	}
}

// handleConnection manages I/O for a single client connection.
func handleConnection(conn net.Conn, inputChan chan<- Message, wg *sync.WaitGroup) {
	defer wg.Done()
	reader := bufio.NewReader(conn)

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			cleanupSubscription(conn)
			return
		}

		parts := strings.Fields(strings.TrimSpace(message))
		if len(parts) == 0 {
			continue
		}
		command := strings.ToUpper(parts[0])

		inputChan <- Message{conn: conn, text: message}

		// Broker-to-broker commands (REPLICATE, STATUS, CLEAR_REPLICATE) are single-request-response.
		if command == "REPLICATE" || command == "STATUS" || command == "CLEAR_REPLICATE" {
			return
		}
		// PUBLISH and GOODBYE also close the connection on the application side.
		// SUBSCRIBE keeps the connection open, so the loop continues.
	}
}

// proxy Goroutine (Acceptor)
func proxy(listener net.Listener, inputChan chan<- Message) {
	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
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
	fmt.Printf("Listening on: %s:%s\n", host, port)

	listener, err := net.Listen(serverType, host+":"+port)
	if err != nil {
		os.Exit(1)
	}
	defer listener.Close()

	inputChan := make(chan Message)

	go proxy(listener, inputChan)
	go appLogic(inputChan)
	go brokerControlGoroutine()

	select {}
}
