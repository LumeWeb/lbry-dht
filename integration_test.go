package dht

import (
	"testing"

	"go.lumeweb.com/lbry-dht/bits"
)

// Integration tests for cross-component functionality
func TestIntegration_RemoveBadPeerWorkflow(t *testing.T) {
	// Create a DHT instance
	dht := New(nil)

	// Initialize the DHT to set up internal components
	nodeID := bits.Rand()
	dht.contact = Contact{ID: nodeID}
	dht.node = NewNode(nodeID)

	// Create test contacts
	contact1 := Contact{ID: bits.Rand(), IP: nil, Port: 8080}
	contact2 := Contact{ID: bits.Rand(), IP: nil, Port: 8081}

	// Create test hashes
	hash1 := bits.Rand()
	hash2 := bits.Rand()

	// Simulate storing contacts for different hashes
	dht.node.Store(hash1, contact1)
	dht.node.Store(hash1, contact2)
	dht.node.Store(hash2, contact1)

	// Verify contacts are stored
	if len(dht.node.store.Get(hash1)) != 2 {
		t.Error("Contacts not stored properly for hash1")
	}
	if len(dht.node.store.Get(hash2)) != 1 {
		t.Error("Contacts not stored properly for hash2")
	}

	// Remove bad peer from all hashes
	dht.RemoveBadPeer(contact1)

	// Verify contact1 is removed from all hashes
	if len(dht.node.store.Get(hash1)) != 1 {
		t.Error("Contact1 not removed from hash1")
	}
	if len(dht.node.store.Get(hash2)) != 0 {
		t.Error("Contact1 not removed from hash2")
	}

	// Verify contact2 is still there
	contacts1 := dht.node.store.Get(hash1)
	if len(contacts1) != 1 || !contacts1[0].ID.Equals(contact2.ID) {
		t.Error("Contact2 was incorrectly removed")
	}
}

func TestIntegration_GetContactsWorkflow(t *testing.T) {
	// Create a DHT instance
	dht := New(nil)

	// Initialize the DHT to set up internal components
	nodeID := bits.Rand()
	dht.contact = Contact{ID: nodeID}
	dht.node = NewNode(nodeID)

	// Create test contacts
	contact1 := Contact{ID: bits.Rand(), IP: nil, Port: 8080}
	contact2 := Contact{ID: bits.Rand(), IP: nil, Port: 8081}
	contact3 := Contact{ID: bits.Rand(), IP: nil, Port: 8082}

	// Add contacts to routing table
	dht.node.rt.Update(contact1)
	dht.node.rt.Update(contact2)
	dht.node.rt.Update(contact3)

	// Test GetContacts from DHT
	contacts := dht.GetContacts()
	if len(contacts) != 3 {
		t.Errorf("Expected 3 contacts, got %d", len(contacts))
	}

	// Test GetAllContacts from routing table directly
	allContacts := dht.GetRoutingTable().GetAllContacts()
	if len(allContacts) != 3 {
		t.Errorf("Expected 3 contacts from routing table, got %d", len(allContacts))
	}

	// Verify both methods return same contacts
	if len(contacts) != len(allContacts) {
		t.Error("GetContacts() and GetAllContacts() returned different number of contacts")
	}
}

func TestIntegration_DistanceAndExploration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow integration test")
	}

	bs, dhts := TestingCreateNetwork(t, 2, true, false)
	defer func() {
		for i := range dhts {
			dhts[i].Shutdown()
		}
		bs.Shutdown()
	}()

	// Test distance calculation
	target := bits.Rand()
	distance := dhts[0].GetDistanceFromTarget(target)

	if distance == 0 {
		t.Error("Distance should not be 0 for random target")
	}

	// Test exploration
	contacts, err := dhts[0].ExploreKeyspaceWithLimit(target, 1)
	if err != nil {
		t.Fatal(err)
	}

	if len(contacts) == 0 {
		t.Error("ExploreKeyspace should find some contacts")
	}

	// Verify distances are reasonable
	for _, contact := range contacts {
		contactDistance := dhts[0].GetDistanceFromTarget(contact.ID)
		if contactDistance == 0 {
			t.Error("Contact distance should not be 0")
		}
	}
}

func TestIntegration_NodeAndRoutingTableInteraction(t *testing.T) {
	// Create a node and verify routing table interaction
	nodeID := bits.Rand()
	node := NewNode(nodeID)

	// Create test contact
	contact := Contact{ID: bits.Rand(), IP: nil, Port: 8080}

	// Add contact to routing table
	node.rt.Update(contact)

	// Verify contact is in routing table
	contacts := node.rt.GetAllContacts()
	if len(contacts) != 1 {
		t.Error("Contact not added to routing table")
	}

	// Store contact in node's store
	hash := bits.Rand()
	node.Store(hash, contact)

	// Verify contact is in store
	storeContacts := node.store.Get(hash)
	if len(storeContacts) != 1 {
		t.Error("Contact not stored in node's store")
	}

	// Remove bad peer
	node.RemoveBadPeer(contact)

	// Verify contact is removed from store
	storeContactsAfter := node.store.Get(hash)
	if len(storeContactsAfter) != 0 {
		t.Error("Contact not removed from store")
	}

	// Contact should still be in routing table (RemoveBadPeer only affects store)
	contactsAfter := node.rt.GetAllContacts()
	if len(contactsAfter) != 1 {
		t.Error("Contact should still be in routing table")
	}
}
