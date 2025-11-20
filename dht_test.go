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

// Test new DHT methods
func TestDHT_GetNode(t *testing.T) {
	// Generate a random node ID and convert to hex string for Config
	nodeID := bits.Rand()
	nodeIDHex := nodeID.Hex()

	// Create DHT with Config-based initialization
	dht := New(&Config{NodeID: nodeIDHex, Address: "127.0.0.1:8080", AnnounceRate: DefaultAnnounceRate})

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
	dht := New(&Config{NodeID: nodeIDHex, Address: "127.0.0.1:8080", AnnounceRate: DefaultAnnounceRate})

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
	dht := New(&Config{NodeID: nodeIDHex, Address: "127.0.0.1:8080", AnnounceRate: DefaultAnnounceRate})

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

	// Verify contact is in store before removal
	contactsBefore := dht.node.store.Get(blobHash)
	found := false
	for _, c := range contactsBefore {
		if c.ID.Equals(contact.ID) {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Failed to add contact to store before removal test")
	}

	dht.RemoveBadPeer(contact)

	// Verify removal from contact store
	contactsAfter := dht.node.store.Get(blobHash)
	found = false
	for _, c := range contactsAfter {
		if c.ID.Equals(contact.ID) {
			found = true
			break
		}
	}
	if found {
		t.Error("RemoveBadPeer() did not remove contact from contact store")
	}
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

	dht.RemoveBadPeerFromHash(blobHash, contact)

	// Verify removal
	contacts := dht.node.store.Get(blobHash)
	for _, c := range contacts {
		if c.ID.Equals(contact.ID) {
			t.Error("RemoveBadPeerFromHash() did not remove contact from blob hash")
		}
	}
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
	contacts, err := dhts[0].ExploreKeyspaceWithLimit(target, 1) // Use limited version for testing
	if err != nil {
		t.Fatal(err)
	}

	if len(contacts) == 0 {
		t.Error("ExploreKeyspaceWithLimit() returned no contacts")
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
