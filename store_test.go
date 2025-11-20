package dht

import (
	"testing"

	"go.lumeweb.com/lbry-dht/bits"
)

// Test new store methods
func TestContactStore_RemoveContact(t *testing.T) {
	store := newStore()

	contact1 := Contact{ID: bits.Rand(), IP: nil, Port: 8080}
	contact2 := Contact{ID: bits.Rand(), IP: nil, Port: 8081}
	hash1 := bits.Rand()
	hash2 := bits.Rand()

	// Add contacts to store
	store.Upsert(hash1, contact1)
	store.Upsert(hash1, contact2)
	store.Upsert(hash2, contact1)

	// Verify contacts are added
	if len(store.Get(hash1)) != 2 {
		t.Errorf("Expected 2 contacts for hash1, got %d", len(store.Get(hash1)))
	}
	if len(store.Get(hash2)) != 1 {
		t.Errorf("Expected 1 contact for hash2, got %d", len(store.Get(hash2)))
	}

	// Remove contact1 from all hashes
	store.RemoveContact(contact1)

	// Verify contact1 is removed from all hashes
	contacts1 := store.Get(hash1)
	contacts2 := store.Get(hash2)

	if len(contacts1) != 1 {
		t.Errorf("Expected 1 contact for hash1 after removal, got %d", len(contacts1))
	}
	if len(contacts2) != 0 {
		t.Errorf("Expected 0 contacts for hash2 after removal, got %d", len(contacts2))
	}

	// Verify contact2 is still there
	if len(contacts1) == 1 && !contacts1[0].ID.Equals(contact2.ID) {
		t.Error("Wrong contact remaining in hash1")
	}
}

func TestContactStore_RemoveContactFromHash(t *testing.T) {
	store := newStore()

	contact1 := Contact{ID: bits.Rand(), IP: nil, Port: 8080}
	contact2 := Contact{ID: bits.Rand(), IP: nil, Port: 8081}
	hash1 := bits.Rand()
	hash2 := bits.Rand()

	// Add contacts to store
	store.Upsert(hash1, contact1)
	store.Upsert(hash1, contact2)
	store.Upsert(hash2, contact1)

	// Remove contact1 only from hash1
	store.RemoveContactFromHash(hash1, contact1)

	// Verify contact1 is removed from hash1 but still in hash2
	contacts1 := store.Get(hash1)
	contacts2 := store.Get(hash2)

	if len(contacts1) != 1 {
		t.Errorf("Expected 1 contact for hash1 after removal, got %d", len(contacts1))
	}
	if len(contacts2) != 1 {
		t.Errorf("Expected 1 contact for hash2 after removal, got %d", len(contacts2))
	}

	// Verify correct contacts remain
	if len(contacts1) == 1 && !contacts1[0].ID.Equals(contact2.ID) {
		t.Error("Wrong contact remaining in hash1")
	}
	if len(contacts2) == 1 && !contacts2[0].ID.Equals(contact1.ID) {
		t.Error("Wrong contact remaining in hash2")
	}
}

func TestContactStore_RemoveContactFromHash_CleanupEmptyMaps(t *testing.T) {
	store := newStore()

	contact := Contact{ID: bits.Rand(), IP: nil, Port: 8080}
	hash := bits.Rand()

	// Add contact to store
	store.Upsert(hash, contact)

	// Verify hash exists
	if len(store.Get(hash)) != 1 {
		t.Error("Contact not added properly")
	}

	// Remove contact from hash
	store.RemoveContactFromHash(hash, contact)

	// Verify hash is cleaned up (empty maps should be removed)
	contacts := store.Get(hash)
	if len(contacts) != 0 {
		t.Errorf("Expected 0 contacts after removal, got %d", len(contacts))
	}

	// Verify contact is also removed from contacts map since it's not associated with any hashes
	if len(store.contacts) != 0 {
		t.Errorf("Expected 0 contacts in contacts map, got %d", len(store.contacts))
	}
}

func TestContactStore_RemoveContact_NonExistent(t *testing.T) {
	store := newStore()

	contact := Contact{ID: bits.Rand(), IP: nil, Port: 8080}

	// Remove non-existent contact - should not panic
	store.RemoveContact(contact)

	// Verify store is still in valid state
	if store.CountStoredHashes() != 0 {
		t.Error("Store should be empty")
	}
}

func TestContactStore_RemoveContactFromHash_NonExistent(t *testing.T) {
	store := newStore()

	contact := Contact{ID: bits.Rand(), IP: nil, Port: 8080}
	hash := bits.Rand()

	// Remove non-existent contact from non-existent hash - should not panic
	store.RemoveContactFromHash(hash, contact)

	// Verify store is still in valid state
	if store.CountStoredHashes() != 0 {
		t.Error("Store should be empty")
	}
}
