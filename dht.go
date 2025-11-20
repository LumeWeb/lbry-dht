package dht

import (
	"fmt"
	"math/big"
	"net"
	"sort"
	"strings"
	"time"

	"go.lumeweb.com/lbry-dht/bits"
	"go.lumeweb.com/lbry-dht/extras/errors"
	"go.lumeweb.com/lbry-dht/extras/stop"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
)

var log *logrus.Logger

func UseLogger(l *logrus.Logger) {
	log = l
}

func init() {
	log = logrus.StandardLogger()
	//log.SetFormatter(&log.TextFormatter{ForceColors: true})
	//log.SetLevel(log.DebugLevel)
}

// DHT represents a DHT node.
type DHT struct {
	// config
	conf *Config
	// local contact
	contact Contact
	// node
	node *Node
	// stopGroup to shut down DHT
	grp *stop.Group
	// channel is closed when DHT joins network
	joined chan struct{}
	// cache for store tokens
	tokenCache *tokenCache
	// hashes that need to be put into the announce queue or removed from the queue
	announceAddRemove chan queueEdit
}

// New returns a DHT pointer. If config is nil, then config will be set to the default config.
func New(config *Config) *DHT {
	if config == nil {
		config = NewStandardConfig()
	}

	d := &DHT{
		conf:              config,
		grp:               stop.New(),
		joined:            make(chan struct{}),
		announceAddRemove: make(chan queueEdit),
	}
	return d
}

func (dht *DHT) connect(conn UDPConn) error {
	contact, err := getContact(dht.conf.NodeID, dht.conf.Address)
	if err != nil {
		return err
	}

	dht.contact = contact
	dht.node = NewNode(contact.ID, dht.conf)
	dht.tokenCache = newTokenCache(dht.node, tokenSecretRotationInterval)

	return dht.node.Connect(conn)
}

// Start starts the dht
func (dht *DHT) Start() error {
	listener, err := net.ListenPacket(Network, dht.conf.Address)
	if err != nil {
		return errors.Err(err)
	}
	conn := listener.(*net.UDPConn)

	err = dht.connect(conn)
	if err != nil {
		return err
	}

	dht.join()
	log.Infof("[%s] DHT ready on %s (%d nodes found during join)",
		dht.node.id.HexShort(), dht.contact.Addr().String(), dht.node.rt.Count())

	dht.grp.Add(1)
	go func() {
		dht.runAnnouncer()
		dht.grp.Done()
	}()

	if dht.conf.RPCPort > 0 {
		dht.grp.Add(1)
		go func() {
			dht.runRPCServer(dht.conf.RPCPort)
			dht.grp.Done()
		}()
	}

	return nil
}

// join makes current node join the dht network.
func (dht *DHT) join() {
	defer close(dht.joined) // if anyone's waiting for join to finish, they'll know its done

	log.Infof("[%s] joining DHT network", dht.node.id.HexShort())

	// ping nodes, which gets their real node IDs and adds them to the routing table
	atLeastOneNodeResponded := false
	for _, addr := range dht.conf.SeedNodes {
		err := dht.Ping(addr)
		if err != nil {
			log.Error(errors.Prefix(fmt.Sprintf("[%s] join", dht.node.id.HexShort()), err))
		} else {
			atLeastOneNodeResponded = true
		}
	}

	if !atLeastOneNodeResponded {
		log.Errorf("[%s] join: no nodes responded to initial ping", dht.node.id.HexShort())
		return
	}

	// now call iterativeFind on yourself
	_, _, err := FindContacts(dht.node, dht.node.id, false, dht.grp.Child())
	if err != nil {
		log.Errorf("[%s] join: %s", dht.node.id.HexShort(), err.Error())
	}

	// TODO: after joining, refresh all buckets further away than our closest neighbor
	// http://xlattice.sourceforge.net/components/protocol/kademlia/specs.html#join
}

// WaitUntilJoined blocks until the node joins the network.
func (dht *DHT) WaitUntilJoined() {
	if dht.joined == nil {
		panic("dht not initialized")
	}
	<-dht.joined
}

// Shutdown shuts down the dht
func (dht *DHT) Shutdown() {
	log.Debugf("[%s] DHT shutting down", dht.node.id.HexShort())
	dht.grp.StopAndWait()
	dht.node.Shutdown()
	log.Debugf("[%s] DHT stopped", dht.node.id.HexShort())
}

// Ping pings a given address, creates a temporary contact for sending a message, and returns an error if communication
// fails.
func (dht *DHT) Ping(addr string) error {
	raddr, err := net.ResolveUDPAddr(Network, addr)
	if err != nil {
		return err
	}

	tmpNode := Contact{ID: bits.Rand(), IP: raddr.IP, Port: raddr.Port}
	res := dht.node.Send(tmpNode, Request{Method: pingMethod}, SendOptions{skipIDCheck: true})
	if res == nil {
		return errors.Err("no response from node %s", addr)
	}

	return nil
}

// getOption represents a functional option for Get operations (currently unused but kept for future extensibility)
type getOption func()

// applyContactFiltering applies filtering to contacts if a validator is configured
func (dht *DHT) applyContactFiltering(hash bits.Bitmap, contacts []Contact) []Contact {
	return dht.node.applyContactFiltering(hash, contacts)
}

// getWithOptions returns the list of nodes that have the blob for the given hash with optional filtering
func (dht *DHT) getWithOptions(hash bits.Bitmap, options ...getOption) ([]Contact, error) {
	contacts, found, err := FindContacts(dht.node, hash, true, dht.grp.Child())
	if err != nil {
		return nil, err
	}

	if found {
		// Apply filtering if validator is configured
		return dht.applyContactFiltering(hash, contacts), nil
	}
	return nil, nil
}

// Get returns the list of nodes that have the blob for the given hash
func (dht *DHT) Get(hash bits.Bitmap) ([]Contact, error) {
	return dht.getWithOptions(hash)
}

// PrintState prints the current state of the DHT including address, nr outstanding transactions, stored hashes as well
// as current bucket information.
func (dht *DHT) PrintState() {
	log.Printf("DHT node %s at %s", dht.contact.String(), time.Now().Format(time.RFC822Z))
	log.Printf("Outstanding transactions: %d", dht.node.CountActiveTransactions())
	log.Printf("Stored hashes: %d", dht.node.store.CountStoredHashes())
	log.Printf("Buckets:")
	for _, line := range strings.Split(dht.node.rt.BucketInfo(), "\n") {
		log.Println(line)
	}
}

func (dht DHT) ID() bits.Bitmap {
	return dht.contact.ID
}

// GetNode returns the internal node pointer
func (dht *DHT) GetNode() *Node {
	return dht.node
}

// GetRoutingTable returns the internal routing table pointer
func (dht *DHT) GetRoutingTable() *routingTable {
	return dht.node.rt
}

// GetContacts returns all contacts from the routing table
func (dht *DHT) GetContacts() []Contact {
	return dht.node.rt.GetAllContacts()
}

// FindContacts finds contacts for the given target using the DHT discovery mechanism
func (dht *DHT) FindContacts(target bits.Bitmap, findValue bool) ([]Contact, bool, error) {
	return FindContacts(dht.node, target, findValue, dht.grp.Child())
}

// ExploreKeyspace systematically explores the keyspace around a target using iterative approach
func (dht *DHT) ExploreKeyspace(target bits.Bitmap) ([]Contact, error) {
	return dht.ExploreKeyspaceWithLimit(target, 1000)
}

// ExploreKeyspaceWithLimit systematically explores the keyspace around a target using iterative approach with a configurable iteration limit
func (dht *DHT) ExploreKeyspaceWithLimit(target bits.Bitmap, maxIterations int) ([]Contact, error) {
	var allContacts []Contact
	visited := make(map[bits.Bitmap]bool)

	// Start with the target
	currentTarget := target
	factor := 2048

	for i := 0; i < maxIterations; i++ {
		// Find contacts for current target
		contacts, _, err := dht.FindContacts(currentTarget, false)
		if err != nil {
			return allContacts, err
		}

		// Add new contacts to our collection
		for _, contact := range contacts {
			if !visited[contact.ID] {
				allContacts = append(allContacts, contact)
				visited[contact.ID] = true
			}
		}

		// If no contacts found, we're done
		if len(contacts) == 0 {
			break
		}

		// Sort contacts by distance from current target
		sort.Slice(contacts, func(i, j int) bool {
			return contacts[i].ID.Xor(currentTarget).Cmp(contacts[j].ID.Xor(currentTarget)) < 0
		})

		// Get the farthest contact
		farthestContact := contacts[len(contacts)-1]
		farthestDistance := farthestContact.ID.Xor(currentTarget)
		currentDistance := target.Xor(currentTarget)

		// If we're not getting closer, adjust the target
		if farthestDistance.Cmp(currentDistance) <= 0 {
			// Calculate next jump using the factor approach from Python crawler
			maxDistance := bits.MaxP()
			currentDistanceBig := currentDistance.Big()
			maxDistanceBig := maxDistance.Big()

			// next_jump = current_distance + (max_distance // factor)
			nextJump := new(big.Int).Div(maxDistanceBig, big.NewInt(int64(factor)))
			nextJump.Add(nextJump, currentDistanceBig)

			factor /= 2
			if factor > 8 && nextJump.Cmp(maxDistanceBig) < 0 {
				// key = int.from_bytes(peer.node_id, 'big') ^ next_jump
				currentTargetBig := new(big.Int).Xor(dht.node.id.Big(), nextJump)
				currentTarget = bits.FromBigP(currentTargetBig)
			} else {
				break
			}
		} else {
			// Move to the farthest contact
			currentTarget = farthestContact.ID
			factor = 2048
		}
	}

	return allContacts, nil
}

// GetRandomTarget generates a random target in the keyspace
func (dht *DHT) GetRandomTarget() bits.Bitmap {
	return bits.Rand()
}

// GetDistanceFromTarget calculates the XOR distance from the node's ID to the target and returns as uint64
func (dht *DHT) GetDistanceFromTarget(target bits.Bitmap) uint64 {
	distance := dht.node.id.Xor(target)
	distanceBig := distance.Big()

	// Convert to uint64, taking the lower 64 bits
	if distanceBig.BitLen() > 64 {
		return distanceBig.Uint64()
	}

	return uint64(distanceBig.Int64())
}

// RemoveBadPeer removes a peer from all hash mappings when peer is confirmed bad
func (dht *DHT) RemoveBadPeer(contact Contact) {
	dht.node.RemoveBadPeer(contact)
}

// RemoveBadPeerFromHash removes a peer from a specific hash mapping
func (dht *DHT) RemoveBadPeerFromHash(blobHash bits.Bitmap, contact Contact) {
	dht.node.RemoveBadPeerFromHash(blobHash, contact)
}

func getContact(nodeID, addr string) (Contact, error) {
	var c Contact
	if nodeID == "" {
		c.ID = bits.Rand()
	} else {
		c.ID = bits.FromHexP(nodeID)
	}

	ip, port, err := net.SplitHostPort(addr)
	if err != nil {
		return c, errors.Err(err)
	} else if ip == "" {
		return c, errors.Err("address does not contain an IP")
	} else if port == "" {
		return c, errors.Err("address does not contain a port")
	}

	c.IP = net.ParseIP(ip)
	if c.IP == nil {
		return c, errors.Err("invalid ip")
	}

	c.Port, err = cast.ToIntE(port)
	if err != nil {
		return c, errors.Err(err)
	}

	return c, nil
}
