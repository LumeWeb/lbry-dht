package dht

import (
	"encoding/hex"
	"net"
	"strings"
	"sync"
	"time"

	"go.lumeweb.com/lbry-dht/bits"
	"go.lumeweb.com/lbry-dht/extras/errors"
	"go.lumeweb.com/lbry-dht/extras/stop"
	"go.lumeweb.com/lbry-dht/extras/util"

	"github.com/davecgh/go-spew/spew"
	"github.com/lyoshenka/bencode"
)

// packet represents the information receive from udp.
type packet struct {
	data  []byte
	raddr *net.UDPAddr
}

// UDPConn allows using a mocked connection to test sending/receiving data
// TODO: stop mocking this and use the real thing
type UDPConn interface {
	ReadFromUDP([]byte) (int, *net.UDPAddr, error)
	WriteToUDP([]byte, *net.UDPAddr) (int, error)
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	Close() error
}

// RequestHandlerFunc is exported handler for requests.
type RequestHandlerFunc func(addr *net.UDPAddr, request Request)

// Node is a type representation of a node on the network.
type Node struct {
	// the node's id
	id bits.Bitmap
	// UDP connection for sending and receiving data
	conn UDPConn
	// true if we've closed the connection on purpose
	connClosed bool
	// token manager
	tokens *tokenManager

	// map of outstanding transactions + mutex
	txLock       *sync.RWMutex
	transactions map[messageID]*transaction

	// routing table
	rt RoutingTable
	// data store
	store *contactStore

	// config for validation and other settings
	conf *Config

	// overrides for request handlers
	requestHandler RequestHandlerFunc

	// stop the node neatly and clean up after itself
	grp *stop.Group
}

// NewNode returns an initialized Node's pointer.
// ContactExpire behavior:
// - Zero value: uses default tExpire duration
// - Positive value: uses the specified duration
// - Negative value: treated as zero, falls back to default tExpire
func NewNode(id bits.Bitmap, config *Config) *Node {
	contactExpire := tExpire // default
	if config != nil && config.ContactExpire > 0 {
		contactExpire = config.ContactExpire
	}

	return &Node{
		id:    id,
		rt:    newRoutingTable(id),
		store: newStore(contactExpire),
		conf:  config,

		txLock:       &sync.RWMutex{},
		transactions: make(map[messageID]*transaction),

		grp:    stop.New(),
		tokens: &tokenManager{},
	}
}

// Connect connects to the given connection and starts any background threads necessary
func (n *Node) Connect(conn UDPConn) error {
	n.conn = conn

	n.tokens.Start(tokenSecretRotationInterval)

	go func() {
		// stop tokens and close the connection when we're shutting down
		<-n.grp.Ch()
		n.tokens.Stop()
		n.connClosed = true
		err := n.conn.Close()
		if err != nil {
			log.Error("error closing node connection on shutdown - ", err)
		}
	}()

	packets := make(chan packet)

	n.grp.Add(1)
	go func() {
		defer n.grp.Done()

		buf := make([]byte, udpMaxMessageLength)

		for {
			bytesRead, raddr, err := n.conn.ReadFromUDP(buf)
			if err != nil {
				if n.connClosed {
					return
				}
				log.Errorf("udp read error: %v", err)
				continue
			} else if raddr == nil {
				log.Errorf("udp read with no raddr")
				continue
			}

			data := make([]byte, bytesRead)
			copy(data, buf[:bytesRead]) // slices use the same underlying array, so we need a new one for each packet

			select { // needs select here because packet consumer can quit and the packets channel gets filled up and blocks
			case packets <- packet{data: data, raddr: raddr}:
			case <-n.grp.Ch():
				return
			}
		}
	}()

	n.grp.Add(1)
	go func() {
		defer n.grp.Done()

		var pkt packet

		for {
			select {
			case pkt = <-packets:
				n.handlePacket(pkt)
			case <-n.grp.Ch():
				return
			}
		}
	}()

	// TODO: turn this back on when you're sure it works right
	n.grp.Add(1)
	go func() {
		defer n.grp.Done()
		n.startRoutingTableGrooming()
	}()

	return nil
}

// Shutdown shuts down the node
func (n *Node) Shutdown() {
	log.Debugf("[%s] node shutting down", n.id.HexShort())
	n.grp.StopAndWait()
	log.Debugf("[%s] node stopped", n.id.HexShort())
}

// handlePacket handles packets received from udp.
func (n *Node) handlePacket(pkt packet) {
	//log.Debugf("[%s] Received message from %s (%d bytes) %s", n.id.HexShort(), pkt.raddr.String(), len(pkt.data), hex.EncodeToString(pkt.data))

	if !util.InSlice(string(pkt.data[0:5]), []string{"d1:0i", "di0ei"}) {
		log.Errorf("[%s] data is not a well-formatted dict: (%d bytes) %s", n.id.HexShort(), len(pkt.data), hex.EncodeToString(pkt.data))
		return
	}

	// the following is a bit of a hack, but it lets us avoid decoding every message twice
	// it depends on the data being a dict with 0 as the first key (so it starts with "d1:0i") and the message type as the first value
	// TODO: test this more thoroughly

	switch pkt.data[5] {
	case '0' + requestType:
		request := Request{}
		err := bencode.DecodeBytes(pkt.data, &request)
		if err != nil {
			log.Errorf("[%s] error decoding request from %s: %s: (%d bytes) %s", n.id.HexShort(), pkt.raddr.String(), err.Error(), len(pkt.data), hex.EncodeToString(pkt.data))
			return
		}
		log.Debugf("[%s] query %s: received request from %s: %s(%s)", n.id.HexShort(), request.ID.HexShort(), request.NodeID.HexShort(), request.Method, request.argsDebug())
		n.handleRequest(pkt.raddr, request)

	case '0' + responseType:
		response := Response{}
		err := bencode.DecodeBytes(pkt.data, &response)
		if err != nil {
			log.Errorf("[%s] error decoding response from %s: %s: (%d bytes) %s", n.id.HexShort(), pkt.raddr.String(), err.Error(), len(pkt.data), hex.EncodeToString(pkt.data))
			return
		}
		log.Debugf("[%s] query %s: received response from %s: %s", n.id.HexShort(), response.ID.HexShort(), response.NodeID.HexShort(), response.argsDebug())
		n.handleResponse(pkt.raddr, response)

	case '0' + errorType:
		e := Error{}
		err := bencode.DecodeBytes(pkt.data, &e)
		if err != nil {
			log.Errorf("[%s] error decoding error from %s: %s: (%d bytes) %s", n.id.HexShort(), pkt.raddr.String(), err.Error(), len(pkt.data), hex.EncodeToString(pkt.data))
			return
		}
		log.Debugf("[%s] query %s: received error from %s: %s", n.id.HexShort(), e.ID.HexShort(), e.NodeID.HexShort(), e.ExceptionType)
		n.handleError(pkt.raddr, e)

	default:
		log.Errorf("[%s] invalid message type: %s", n.id.HexShort(), string(pkt.data[5]))
		return
	}
}

// handleRequest handles the requests received from udp.
func (n *Node) handleRequest(addr *net.UDPAddr, request Request) {
	if request.NodeID.Equals(n.id) {
		log.Warn("ignoring self-request")
		return
	}

	// if a handler is overridden, call it instead
	if n.requestHandler != nil {
		n.requestHandler(addr, request)
		return
	}

	switch request.Method {
	default:
		//n.sendMessage(addr, Error{ID: request.ID, NodeID: n.id, ExceptionType: "invalid-request-method"})
		log.Errorln("invalid request method")
		return
	case pingMethod:
		err := n.sendMessage(addr, Response{ID: request.ID, NodeID: n.id, Data: pingSuccessResponse})
		if err != nil {
			log.Error("error sending 'pingmethod' response message - ", err)
		}
	case storeMethod:
		// TODO: we should be sending the IP in the request, not just using the sender's IP
		// TODO: should we be using StoreArgs.NodeID or StoreArgs.Value.LbryID ???
		if n.tokens.Verify(request.StoreArgs.Value.Token, request.NodeID, addr) {
			contact := Contact{ID: request.StoreArgs.NodeID, IP: addr.IP, Port: addr.Port, PeerPort: request.StoreArgs.Value.Port}

			// Validate before storing if validator is configured
			if n.validateContactForHash(request.StoreArgs.BlobHash, contact) {
				n.Store(request.StoreArgs.BlobHash, contact)
				err := n.sendMessage(addr, Response{ID: request.ID, NodeID: n.id, Data: storeSuccessResponse})
				if err != nil {
					log.Error("error sending 'storemethod' response message - ", err)
				}
			} else {
				// Send error response when validation fails
				err := n.sendMessage(addr, Error{ID: request.ID, NodeID: n.id, ExceptionType: "invalid-contact"})
				if err != nil {
					log.Error("error sending 'storemethod' error response message - ", err)
				}
			}
		} else {
			err := n.sendMessage(addr, Error{ID: request.ID, NodeID: n.id, ExceptionType: "invalid-token"})
			if err != nil {
				log.Error("error sending 'storemethod'response message for invalid-token - ", err)
			}
		}
	case findNodeMethod:
		if request.Arg == nil {
			log.Errorln("request is missing arg")
			return
		}
		err := n.sendMessage(addr, Response{
			ID:       request.ID,
			NodeID:   n.id,
			Contacts: n.rt.GetClosest(*request.Arg, bucketSize),
		})
		if err != nil {
			log.Error("error sending 'findnodemethod' response message - ", err)
		}

	case findValueMethod:
		if request.Arg == nil {
			log.Errorln("request is missing arg")
			return
		}

		res := Response{
			ID:     request.ID,
			NodeID: n.id,
			Token:  n.tokens.Get(request.NodeID, addr),
		}

		if contacts := n.store.Get(*request.Arg); len(contacts) > 0 {
			// Filter contacts through validator if configured
			var validContacts []Contact
			for _, contact := range contacts {
				if n.validateContactForHash(*request.Arg, contact) {
					validContacts = append(validContacts, contact)
				} else {
					// Remove bad contact from store
					n.store.RemoveContactFromHash(*request.Arg, contact)
				}
			}

			// Only return "value found" if validation didn't prune all contacts
			if len(validContacts) > 0 {
				res.FindValueKey = request.Arg.RawString()
				res.Contacts = validContacts
			} else {
				// Validation removed all contacts, fall back to routing table
				res.Contacts = n.rt.GetClosest(*request.Arg, bucketSize)
			}
		} else {
			res.Contacts = n.rt.GetClosest(*request.Arg, bucketSize)
		}

		err := n.sendMessage(addr, res)
		if err != nil {
			log.Error("error sending 'findvaluemethod' response message - ", err)
		}
	}

	// nodes that send us requests should not be inserted, only refreshed.
	// the routing table must only contain "good" nodes, which are nodes that reply to our requests
	// if a node is already good (aka in the table), its fine to refresh it
	// http://www.bittorrent.org/beps/bep_0005.html#routing-table
	n.rt.Fresh(Contact{ID: request.NodeID, IP: addr.IP, Port: addr.Port})
}

// handleResponse handles responses received from udp.
func (n *Node) handleResponse(addr *net.UDPAddr, response Response) {
	tx := n.txFind(response.ID, Contact{ID: response.NodeID, IP: addr.IP, Port: addr.Port})
	if tx != nil {
		select {
		case tx.res <- response:
		default:
			//log.Errorf("[%s] query %s: response received, but tx has no listener or multiple responses to the same tx", n.id.HexShort(), response.ID.HexShort())
		}
	}

	n.rt.Update(Contact{ID: response.NodeID, IP: addr.IP, Port: addr.Port})
}

// handleError handles errors received from udp.
func (n *Node) handleError(addr *net.UDPAddr, e Error) {
	spew.Dump(e)
	n.rt.Fresh(Contact{ID: e.NodeID, IP: addr.IP, Port: addr.Port})
}

// send sends data to a udp address
func (n *Node) sendMessage(addr *net.UDPAddr, data Message) error {
	encoded, err := bencode.EncodeBytes(data)
	if err != nil {
		return errors.Err(err)
	}

	if req, ok := data.(Request); ok {
		log.Debugf("[%s] query %s: sending request to %s (%d bytes) %s(%s)",
			n.id.HexShort(), req.ID.HexShort(), addr.String(), len(encoded), req.Method, req.argsDebug())
	} else if res, ok := data.(Response); ok {
		log.Debugf("[%s] query %s: sending response to %s (%d bytes) %s",
			n.id.HexShort(), res.ID.HexShort(), addr.String(), len(encoded), res.argsDebug())
	} else {
		log.Debugf("[%s] (%d bytes) %s", n.id.HexShort(), len(encoded), spew.Sdump(data))
	}

	err = n.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		if n.connClosed {
			return nil
		}
		log.Error("error setting write deadline - ", err)
	}

	_, err = n.conn.WriteToUDP(encoded, addr)
	return errors.Err(err)
}

// transaction represents a single query to the dht. it stores the queried contact, the request, and the response channel
type transaction struct {
	contact     Contact
	req         Request
	res         chan Response
	skipIDCheck bool
}

// insert adds a transaction to the manager.
func (n *Node) txInsert(tx *transaction) {
	n.txLock.Lock()
	defer n.txLock.Unlock()
	n.transactions[tx.req.ID] = tx
}

// delete removes a transaction from the manager.
func (n *Node) txDelete(id messageID) {
	n.txLock.Lock()
	defer n.txLock.Unlock()
	delete(n.transactions, id)
}

// Find finds a transaction for the given id and contact
func (n *Node) txFind(id messageID, c Contact) *transaction {
	n.txLock.RLock()
	defer n.txLock.RUnlock()

	t, ok := n.transactions[id]
	if !ok || !t.contact.Equals(c, !t.skipIDCheck) {
		return nil
	}

	return t
}

// SendOptions controls the behavior of send calls
type SendOptions struct {
	skipIDCheck bool
}

// SendAsync sends a transaction and returns a channel that will eventually contain the transaction response
// The response channel is closed when the transaction is completed or times out.
func (n *Node) SendAsync(contact Contact, req Request, options ...SendOptions) <-chan *Response {
	ch := make(chan *Response, 1)

	if contact.ID.Equals(n.id) {
		log.Error("sending query to self")
		close(ch)
		return ch
	}

	go func() {
		defer close(ch)

		req.ID = newMessageID()
		req.NodeID = n.id
		tx := &transaction{
			contact: contact,
			req:     req,
			res:     make(chan Response),
		}

		if len(options) > 0 && options[0].skipIDCheck {
			tx.skipIDCheck = true
		}

		n.txInsert(tx)
		defer n.txDelete(tx.req.ID)

		for i := 0; i < udpRetry; i++ {
			err := n.sendMessage(contact.Addr(), tx.req)
			if err != nil {
				if !strings.Contains(err.Error(), "use of closed network connection") { // this only happens on localhost. real UDP has no connections
					log.Error("send error: ", err)
				}
				continue
			}

			select {
			case res := <-tx.res:
				ch <- &res
				return
			case <-n.grp.Ch():
				return
			case <-time.After(udpTimeout):
			}
		}

		// notify routing table about a failure to respond
		n.rt.Fail(tx.contact)
	}()

	return ch
}

// Send sends a transaction and blocks until the response is available. It returns a response, or nil
// if the transaction timed out.
func (n *Node) Send(contact Contact, req Request, options ...SendOptions) *Response {
	return <-n.SendAsync(contact, req, options...)
}

// CountActiveTransactions returns the number of transactions in the manager
func (n *Node) CountActiveTransactions() int {
	n.txLock.Lock()
	defer n.txLock.Unlock()
	return len(n.transactions)
}

func (n *Node) startRoutingTableGrooming() {
	refreshTicker := time.NewTicker(tRefresh / 5) // how often to check for buckets that need to be refreshed
	for {
		select {
		case <-refreshTicker.C:
			RoutingTableRefresh(n, tRefresh, n.grp.Child())
		case <-n.grp.Ch():
			return
		}
	}
}

// Store stores a node contact in the node's contact store.
func (n *Node) Store(hash bits.Bitmap, c Contact) {
	n.store.Upsert(hash, c)
}

// AddKnownNode adds a known-good node to the routing table
func (n *Node) AddKnownNode(c Contact) {
	n.rt.Update(c)
}

// RemoveBadPeer removes a peer from the contact store (for all hashes)
func (n *Node) RemoveBadPeer(contact Contact) {
	n.store.RemoveContact(contact)
}

// RemoveBadPeerFromHash removes a peer from a specific hash mapping
func (n *Node) RemoveBadPeerFromHash(blobHash bits.Bitmap, contact Contact) {
	n.store.RemoveContactFromHash(blobHash, contact)
}

// CleanupExpiredData removes all expired contacts from the store
func (n *Node) CleanupExpiredData() {
	n.store.CleanupExpired()
}

// validateContactForHash checks if a contact is valid for a specific blob hash using the configured validator
// Returns true if valid or no validator is configured, false if invalid
func (n *Node) validateContactForHash(blobHash bits.Bitmap, contact Contact) bool {
	if n.conf == nil {
		return true
	}
	if n.conf.Validator == nil {
		return true
	}
	return n.conf.Validator.ValidateContactForHash(blobHash, contact)
}

// applyContactFiltering applies filtering to contacts if a validator is configured
func (n *Node) applyContactFiltering(hash bits.Bitmap, contacts []Contact) []Contact {
	if n.conf == nil || n.conf.Validator == nil {
		return contacts
	}

	var filteredContacts []Contact
	for _, contact := range contacts {
		if n.validateContactForHash(hash, contact) {
			filteredContacts = append(filteredContacts, contact)
		}
	}
	return filteredContacts
}
