package dht

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.lumeweb.com/lbry-dht/bits"
)

func TestNodeFinder_FindNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow nodeFinder test")
	}

	bs, dhts := TestingCreateNetwork(t, 3, true, false)
	defer func() {
		for i := range dhts {
			dhts[i].Shutdown()
		}
		bs.Shutdown()
	}()

	contacts, found, err := FindContacts(dhts[2].node, bits.Rand(), false, nil)
	if err != nil {
		t.Fatal(err)
	}

	if found {
		t.Fatal("something was found, but it should not have been")
	}

	if len(contacts) != 3 {
		t.Errorf("expected 3 node, found %d", len(contacts))
	}

	foundBootstrap := false
	foundOne := false
	foundTwo := false

	for _, n := range contacts {
		if n.ID.Equals(bs.id) {
			foundBootstrap = true
		}
		if n.ID.Equals(dhts[0].node.id) {
			foundOne = true
		}
		if n.ID.Equals(dhts[1].node.id) {
			foundTwo = true
		}
	}

	if !foundBootstrap {
		t.Errorf("did not find bootstrap node %s", bs.id.Hex())
	}
	if !foundOne {
		t.Errorf("did not find first node %s", dhts[0].node.id.Hex())
	}
	if !foundTwo {
		t.Errorf("did not find second node %s", dhts[1].node.id.Hex())
	}
}

// assertContactInStore verifies that a contact exists in the store for a given blob hash
func assertContactInStore(t *testing.T, store *contactStore, blobHash bits.Bitmap, contact Contact, shouldExist bool) {
	contacts := store.Get(blobHash)
	found := false
	for _, c := range contacts {
		if c.ID.Equals(contact.ID) {
			found = true
			break
		}
	}

	if shouldExist && !found {
		t.Errorf("Expected contact %s to be found in store for hash %s", contact.ID.HexShort(), blobHash.HexShort())
	} else if !shouldExist && found {
		t.Errorf("Expected contact %s to NOT be found in store for hash %s", contact.ID.HexShort(), blobHash.HexShort())
	}
}

// assertContactInRoutingTable verifies that a contact exists in the routing table
func assertContactInRoutingTable(t *testing.T, rt *routingTable, contact Contact, shouldExist bool) {
	found := false
	for _, bucket := range rt.buckets {
		if bucket.Has(contact) {
			found = true
			break
		}
	}

	if shouldExist && !found {
		t.Errorf("Expected contact %s to be found in routing table", contact.ID.HexShort())
	} else if !shouldExist && found {
		t.Errorf("Expected contact %s to NOT be found in routing table", contact.ID.HexShort())
	}
}

// Test new DHT methods
func TestDHT_GetNode(t *testing.T) {
	// Generate a random node ID and convert to hex string for Config
	nodeID := bits.Rand()
	nodeIDHex := nodeID.Hex()

	// Create DHT with Config-based initialization
	dht := New(&Config{NodeID: nodeIDHex, Address: "127.0.0.1:0", AnnounceRate: DefaultAnnounceRate})

	// Initialize DHT components properly by starting it
	err := dht.Start()
	if err != nil {
		t.Fatalf("Failed to start DHT: %v", err)
	}
	defer dht.Shutdown()

	if dht.GetNode() == nil {
		t.Error("GetNode() returned nil")
	}
}

func TestDHT_GetRoutingTable(t *testing.T) {
	nodeID := bits.Rand().Hex()
	dht := New(&Config{
		NodeID:       nodeID,
		Address:      "127.0.0.1:0",
		AnnounceRate: DefaultAnnounceRate,
	})
	err := dht.Start()
	if err != nil {
		t.Fatalf("Failed to start DHT: %v", err)
	}
	defer dht.Shutdown()

	if dht.GetRoutingTable() == nil {
		t.Error("GetRoutingTable() returned nil")
	}
}

func TestDHT_GetContacts(t *testing.T) {
	// Generate a random node ID and convert to hex string for Config
	nodeID := bits.Rand()
	nodeIDHex := nodeID.Hex()

	// Create DHT with Config-based initialization
	dht := New(&Config{NodeID: nodeIDHex, Address: "127.0.0.1:0", AnnounceRate: DefaultAnnounceRate})

	// Initialize DHT components properly by starting it
	err := dht.Start()
	if err != nil {
		t.Fatalf("Failed to start DHT: %v", err)
	}
	defer dht.Shutdown()

	// Ensure routing table is properly initialized
	if dht.GetRoutingTable() == nil {
		t.Error("Routing table is nil")
		return
	}

	contacts := dht.GetContacts()
	// GetContacts should return an empty slice, not nil, when no contacts exist
	if contacts == nil {
		t.Error("GetContacts() returned nil, expected empty slice")
	}
	if len(contacts) != 0 {
		t.Errorf("GetContacts() returned %d contacts, expected 0", len(contacts))
	}
}

func TestDHT_GetRandomTarget(t *testing.T) {
	// GetRandomTarget is independent of DHT startup - it generates random targets
	// without requiring network initialization or node state
	dht := New(nil)
	target1 := dht.GetRandomTarget()
	target2 := dht.GetRandomTarget()

	if target1.Equals(target2) {
		t.Error("GetRandomTarget() returned same value twice")
	}
}

func TestDHT_GetDistanceFromTarget(t *testing.T) {
	// Create proper 96-character hex strings for Bitmap (48 bytes)
	nodeIDHex := strings.Repeat("01", 48) // 96 characters
	targetHex := strings.Repeat("00", 48) // 96 characters

	// Create DHT with Config-based initialization
	dht := New(&Config{NodeID: nodeIDHex, Address: "127.0.0.1:0", AnnounceRate: DefaultAnnounceRate})

	// Initialize DHT components properly by starting it
	err := dht.Start()
	if err != nil {
		t.Fatalf("Failed to start DHT: %v", err)
	}
	defer dht.Shutdown()

	target := bits.FromHexP(targetHex)

	distance := dht.GetDistanceFromTarget(target)
	if distance == 0 {
		t.Error("GetDistanceFromTarget() returned 0 for different values")
	}

	// Test with same target
	sameDistance := dht.GetDistanceFromTarget(dht.ID())
	if sameDistance != 0 {
		t.Error("GetDistanceFromTarget() should return 0 for same target")
	}
}

func TestDHT_FindContacts(t *testing.T) {
	bs, dhts := TestingCreateNetwork(t, 2, true, false)
	defer func() {
		for i := range dhts {
			dhts[i].Shutdown()
		}
		bs.Shutdown()
	}()

	target := bits.Rand()
	contacts, _, err := dhts[0].FindContacts(target, false)
	if err != nil {
		t.Fatal(err)
	}

	if len(contacts) == 0 {
		t.Error("FindContacts() returned no contacts")
	}

	// Stronger invariants: verify at least one other node is found
	foundOtherNode := false
	for _, contact := range contacts {
		if !contact.ID.Equals(dhts[0].ID()) {
			foundOtherNode = true
			break
		}
	}
	if !foundOtherNode {
		t.Error("FindContacts() should return at least one other node")
	}

	// Verify results are ordered by distance (closest first)
	for i := 1; i < len(contacts); i++ {
		prevDist := contacts[i-1].ID.Xor(target)
		currDist := contacts[i].ID.Xor(target)
		if prevDist.Cmp(currDist) > 0 {
			t.Errorf("FindContacts() results not ordered by distance: contact[%d] distance=%s > contact[%d] distance=%s",
				i-1, prevDist.HexShort(), i, currDist.HexShort())
			break
		}
	}
}

func TestDHT_RemoveBadPeer(t *testing.T) {
	nodeID := bits.Rand().Hex()
	dht := New(&Config{
		NodeID:       nodeID,
		Address:      "127.0.0.1:0",
		AnnounceRate: DefaultAnnounceRate,
	})
	err := dht.Start()
	if err != nil {
		t.Fatalf("Failed to start DHT: %v", err)
	}
	defer dht.Shutdown()

	contact := Contact{ID: bits.Rand(), IP: net.ParseIP("127.0.0.1"), Port: 8080}

	// Add contact to store first
	blobHash := bits.Rand()
	dht.node.Store(blobHash, contact)

	// Also add contact to routing table to test routing table expectations
	dht.node.AddKnownNode(contact)

	// Verify contact is in store and routing table before removal
	assertContactInStore(t, dht.node.store, blobHash, contact, true)
	assertContactInRoutingTable(t, dht.GetRoutingTable(), contact, true)

	dht.RemoveBadPeer(contact)

	// Verify removal from contact store (should be removed)
	assertContactInStore(t, dht.node.store, blobHash, contact, false)

	// Note: RemoveBadPeer only removes from contact store, not routing table
	// This is the current behavior - routing table removal happens through failure mechanisms
	assertContactInRoutingTable(t, dht.GetRoutingTable(), contact, true)
}

func TestDHT_RemoveBadPeerFromHash(t *testing.T) {
	nodeID := bits.Rand().Hex()
	dht := New(&Config{
		NodeID:       nodeID,
		Address:      "127.0.0.1:0",
		AnnounceRate: DefaultAnnounceRate,
	})
	err := dht.Start()
	if err != nil {
		t.Fatalf("Failed to start DHT: %v", err)
	}
	defer dht.Shutdown()

	contact := Contact{ID: bits.Rand(), IP: net.ParseIP("127.0.0.1"), Port: 8080}
	blobHash := bits.Rand()

	// Store contact for the blob hash
	dht.node.Store(blobHash, contact)

	// Verify contact is in store before removal
	assertContactInStore(t, dht.node.store, blobHash, contact, true)

	dht.RemoveBadPeerFromHash(blobHash, contact)

	// Verify removal from store (should be removed)
	assertContactInStore(t, dht.node.store, blobHash, contact, false)
}

func TestDHT_ExploreKeyspace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow ExploreKeyspace test")
	}

	bs, dhts := TestingCreateNetwork(t, 2, true, false)
	defer func() {
		for i := range dhts {
			dhts[i].Shutdown()
		}
		bs.Shutdown()
	}()

	target := bits.Rand()
	limit := 2
	contacts, err := dhts[0].ExploreKeyspaceWithLimit(target, limit) // Use limited version for testing
	if err != nil {
		t.Fatal(err)
	}

	if len(contacts) == 0 {
		t.Error("ExploreKeyspaceWithLimit() returned no contacts")
	}

	// Assert that the limit semantics are respected
	if len(contacts) > limit {
		t.Fatalf("ExploreKeyspaceWithLimit() returned %d contacts, which exceeds the requested limit of %d", len(contacts), limit)
	}
}

func TestNodeFinder_FindNodes_NoBootstrap(t *testing.T) {
	_, dhts := TestingCreateNetwork(t, 3, false, false)
	defer func() {
		for i := range dhts {
			dhts[i].Shutdown()
		}
	}()

	_, _, err := FindContacts(dhts[2].node, bits.Rand(), false, nil)
	if err == nil {
		t.Fatal("contact finder should have errored saying that there are no contacts in the routing table")
	}
}

func TestNodeFinder_FindValue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow nodeFinder test")
	}

	bs, dhts := TestingCreateNetwork(t, 3, true, false)
	defer func() {
		for i := range dhts {
			dhts[i].Shutdown()
		}
		bs.Shutdown()
	}()

	blobHashToFind := bits.Rand()
	nodeToFind := Contact{ID: bits.Rand(), IP: net.IPv4(1, 2, 3, 4), Port: 5678}
	dhts[0].node.store.Upsert(blobHashToFind, nodeToFind)

	contacts, found, err := FindContacts(dhts[2].node, blobHashToFind, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	if !found {
		t.Fatal("node was not found")
	}

	if len(contacts) != 1 {
		t.Fatalf("expected one node, found %d", len(contacts))
	}

	if !contacts[0].ID.Equals(nodeToFind.ID) {
		t.Fatalf("found node id %s, expected %s", contacts[0].ID.Hex(), nodeToFind.ID.Hex())
	}
}

func TestDHT_LargeDHT(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large DHT test")
	}

	nodes := 100
	bs, dhts := TestingCreateNetwork(t, nodes, true, true)
	defer func() {
		for _, d := range dhts {
			go d.Shutdown()
		}
		bs.Shutdown()
		time.Sleep(1 * time.Second)
	}()

	wg := &sync.WaitGroup{}
	ids := make([]bits.Bitmap, nodes)
	for i := range ids {
		ids[i] = bits.Rand()
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			err := dhts[index].announce(ids[index])
			if err != nil {
				t.Error("error announcing random bitmap - ", err)
			}
		}(i)
	}
	wg.Wait()

	// check that each node is in at learst 1 other routing table
	rtCounts := make(map[bits.Bitmap]int)
	for _, d := range dhts {
		for _, d2 := range dhts {
			if d.node.id.Equals(d2.node.id) {
				continue
			}
			c := d2.node.rt.GetClosest(d.node.id, 1)
			if len(c) > 1 {
				t.Error("rt returned more than one node when only one requested")
			} else if len(c) == 1 && c[0].ID.Equals(d.node.id) {
				rtCounts[d.node.id]++
			}
		}
	}

	for k, v := range rtCounts {
		if v == 0 {
			t.Errorf("%s was not in any routing tables", k.HexShort())
		}
	}

	// check that each ID is stored by at least 3 nodes
	storeCounts := make(map[bits.Bitmap]int)
	for _, d := range dhts {
		for _, id := range ids {
			if len(d.node.store.Get(id)) > 0 {
				storeCounts[id]++
			}
		}
	}

	for k, v := range storeCounts {
		if v == 0 {
			t.Errorf("%s was not stored by any nodes", k.HexShort())
		}
	}
}

// TestContactValidator implements ContactValidator interface for testing
type TestContactValidator struct {
	allowedContacts map[bits.Bitmap]bool // contacts that are allowed
	allowedHashes   map[bits.Bitmap]bool // hashes that allow validation
}

func NewTestContactValidator() *TestContactValidator {
	return &TestContactValidator{
		allowedContacts: make(map[bits.Bitmap]bool),
		allowedHashes:   make(map[bits.Bitmap]bool),
	}
}

func (v *TestContactValidator) AllowContact(contact Contact) {
	v.allowedContacts[contact.ID] = true
}

func (v *TestContactValidator) AllowHash(hash bits.Bitmap) {
	v.allowedHashes[hash] = true
}

func (v *TestContactValidator) ValidateContactForHash(blobHash bits.Bitmap, contact Contact) bool {
	// If hash is not in allowed list, allow all contacts for that hash
	if !v.allowedHashes[blobHash] {
		return true
	}

	// If hash is in allowed list, only allow allowed contacts
	return v.allowedContacts[contact.ID]
}

func TestGetWithOptions(t *testing.T) {
	// Create a DHT with a validator
	config := NewStandardConfig()
	validator := NewTestContactValidator()
	config.Validator = validator

	dht := New(config)

	// Initialize the DHT
	nodeID := bits.Rand()
	dht.contact = Contact{ID: nodeID}
	dht.node = NewNode(nodeID, config)

	// Create test contacts
	contact1 := Contact{ID: bits.Rand(), IP: net.ParseIP("127.0.0.1"), Port: 8080}
	contact2 := Contact{ID: bits.Rand(), IP: net.ParseIP("127.0.0.1"), Port: 8081}
	contact3 := Contact{ID: bits.Rand(), IP: net.ParseIP("127.0.0.1"), Port: 8082}

	// Create test hash
	hash := bits.Rand()

	// Allow validation for this hash
	validator.AllowHash(hash)

	// Allow only contact1 and contact3
	validator.AllowContact(contact1)
	validator.AllowContact(contact3)

	// Test applyContactFiltering directly instead of using GetWithOptions
	// to avoid network dependencies
	contacts := []Contact{contact1, contact2, contact3}
	filteredContacts := dht.applyContactFiltering(hash, contacts)

	if len(filteredContacts) != 2 {
		t.Errorf("Expected 2 filtered contacts, got %d", len(filteredContacts))
	}

	// Verify the right contacts are returned
	contactIDs := make(map[bits.Bitmap]bool)
	for _, c := range filteredContacts {
		contactIDs[c.ID] = true
	}

	if !contactIDs[contact1.ID] {
		t.Error("Contact1 should be included in results")
	}
	if contactIDs[contact2.ID] {
		t.Error("Contact2 should be filtered out")
	}
	if !contactIDs[contact3.ID] {
		t.Error("Contact3 should be included in results")
	}
}

func TestContactValidator_StoreValidation(t *testing.T) {
	// Create a DHT with a validator
	config := NewStandardConfig()
	validator := NewTestContactValidator()
	config.Validator = validator

	nodeID := bits.Rand()
	node := NewNode(nodeID, config)

	// Create test contacts
	contact1 := Contact{ID: bits.Rand(), IP: net.ParseIP("127.0.0.1"), Port: 8080}
	contact2 := Contact{ID: bits.Rand(), IP: net.ParseIP("127.0.0.1"), Port: 8081}

	// Create test hash
	hash := bits.Rand()

	// Allow validation for this hash
	validator.AllowHash(hash)

	// Allow only contact1
	validator.AllowContact(contact1)

	// Test storing contacts directly (Store method doesn't validate - only handleRequest does)
	// So both contacts will be stored
	node.Store(hash, contact1)
	node.Store(hash, contact2)

	// Check stored contacts - both should be there since Store doesn't validate
	contacts := node.store.Get(hash)
	if len(contacts) != 2 {
		t.Errorf("Expected 2 stored contacts, got %d", len(contacts))
	}

	// Test validation through validateContactForHash method directly
	valid1 := node.validateContactForHash(hash, contact1)
	valid2 := node.validateContactForHash(hash, contact2)

	if !valid1 {
		t.Error("Contact1 should be valid")
	}
	if valid2 {
		t.Error("Contact2 should be invalid")
	}
}

func TestContactValidator_NoValidation(t *testing.T) {
	// Create a DHT without validator
	config := NewStandardConfig()

	nodeID := bits.Rand()
	node := NewNode(nodeID, config)

	// Create test contacts
	contact1 := Contact{ID: bits.Rand(), IP: net.ParseIP("127.0.0.1"), Port: 8080}
	contact2 := Contact{ID: bits.Rand(), IP: net.ParseIP("127.0.0.1"), Port: 8081}

	// Create test hash
	hash := bits.Rand()

	// Store contacts without validation
	node.Store(hash, contact1)
	node.Store(hash, contact2)

	// Check stored contacts
	contacts := node.store.Get(hash)
	if len(contacts) != 2 {
		t.Errorf("Expected 2 stored contacts, got %d", len(contacts))
	}
}

func TestContactValidator_FindValueFiltering(t *testing.T) {
	// Create a DHT with a validator
	config := NewStandardConfig()
	validator := NewTestContactValidator()
	config.Validator = validator

	nodeID := bits.Rand()
	node := NewNode(nodeID, config)

	// Create test contacts
	contact1 := Contact{ID: bits.Rand(), IP: net.ParseIP("127.0.0.1"), Port: 8080}
	contact2 := Contact{ID: bits.Rand(), IP: net.ParseIP("127.0.0.1"), Port: 8081}
	contact3 := Contact{ID: bits.Rand(), IP: net.ParseIP("127.0.0.1"), Port: 8082}

	// Create test hash
	hash := bits.Rand()

	// Allow validation for this hash
	validator.AllowHash(hash)

	// Allow only contact1 and contact3
	validator.AllowContact(contact1)
	validator.AllowContact(contact3)

	// Store all contacts directly in store (simulating previous storage)
	node.store.hashes[hash] = map[bits.Bitmap]bool{
		contact1.ID: true,
		contact2.ID: true,
		contact3.ID: true,
	}
	node.store.contacts[contact1.ID] = contact1
	node.store.contacts[contact2.ID] = contact2
	node.store.contacts[contact3.ID] = contact3

	// Test findValue filtering using node's applyContactFiltering method
	allContacts := node.store.Get(hash)
	filteredContacts := node.applyContactFiltering(hash, allContacts)
	if len(filteredContacts) != 2 {
		t.Errorf("Expected 2 filtered contacts, got %d", len(filteredContacts))
	}

	// Verify the right contacts are returned
	contactIDs := make(map[bits.Bitmap]bool)
	for _, c := range filteredContacts {
		contactIDs[c.ID] = true
	}

	if !contactIDs[contact1.ID] {
		t.Error("Contact1 should be included in results")
	}
	if contactIDs[contact2.ID] {
		t.Error("Contact2 should be filtered out")
	}
	if !contactIDs[contact3.ID] {
		t.Error("Contact3 should be included in results")
	}
}
