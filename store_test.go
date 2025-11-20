package dht

import (
	"testing"
	"time"

	"go.lumeweb.com/lbry-dht/bits"
)

// testStoreWrapper provides test-only access to contactStore internals
type testStoreWrapper struct {
	*contactStore
}

// getTimestampFor returns the timestamp for a contact in a specific hash.
// This is a test helper that safely accesses internal timestamp data.
func (w *testStoreWrapper) getTimestampFor(blobHash bits.Bitmap, contactID bits.Bitmap) (time.Time, bool) {
	w.contactStore.lock.RLock()
	defer w.contactStore.lock.RUnlock()

	if timestampMap, exists := w.contactStore.timestamps[blobHash]; exists {
		if timestamp, exists := timestampMap[contactID]; exists {
			return timestamp, true
		}
	}
	return time.Time{}, false
}

// Test new store methods
func TestContactStore_RemoveContact(t *testing.T) {
	store := newStore(tExpire)

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
	store := newStore(tExpire)

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
	store := newStore(tExpire)

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
	store := newStore(tExpire)

	contact := Contact{ID: bits.Rand(), IP: nil, Port: 8080}

	// Remove non-existent contact - should not panic
	store.RemoveContact(contact)

	// Verify store is still in valid state
	if store.CountStoredHashes() != 0 {
		t.Error("Store should be empty")
	}
}

func TestContactStore_TimestampResetOnReadd(t *testing.T) {
	store := &testStoreWrapper{newStore(tExpire)}

	contact := Contact{ID: bits.Rand(), IP: nil, Port: 8080}
	hash := bits.Rand()

	// Add contact to store
	store.Upsert(hash, contact)

	// Get the initial timestamp using the test wrapper
	initialTimestamp, exists := store.getTimestampFor(hash, contact.ID)
	if !exists {
		t.Fatal("Initial timestamp should exist")
	}

	// Wait a bit to ensure timestamp difference
	time.Sleep(50 * time.Millisecond)

	// Re-add the same contact (should reset timestamp)
	store.Upsert(hash, contact)

	// Get the updated timestamp using the test wrapper
	updatedTimestamp, exists := store.getTimestampFor(hash, contact.ID)
	if !exists {
		t.Fatal("Updated timestamp should exist")
	}

	// Verify timestamp was updated (should be later than initial)
	if !updatedTimestamp.After(initialTimestamp) {
		t.Error("Timestamp should be reset when contact is re-added")
	}

	// Verify contact is still accessible
	contacts := store.Get(hash)
	if len(contacts) != 1 {
		t.Errorf("Expected 1 contact, got %d", len(contacts))
	}
	if !contacts[0].ID.Equals(contact.ID) {
		t.Error("Wrong contact in store")
	}
}

func TestContactStore_Expiration(t *testing.T) {
	// Use longer expiration time for testing to reduce flakiness
	shortExpire := 200 * time.Millisecond
	store := newStore(shortExpire)

	contact1 := Contact{ID: bits.Rand(), IP: nil, Port: 8080}
	contact2 := Contact{ID: bits.Rand(), IP: nil, Port: 8081}
	hash := bits.Rand()

	// Add contacts to store
	store.Upsert(hash, contact1)
	store.Upsert(hash, contact2)

	// Verify contacts are added
	if len(store.Get(hash)) != 2 {
		t.Errorf("Expected 2 contacts, got %d", len(store.Get(hash)))
	}

	// Wait for expiration with larger safety margin
	time.Sleep(250 * time.Millisecond)

	// Get should clean up expired contacts
	contacts := store.Get(hash)
	if len(contacts) != 0 {
		t.Errorf("Expected 0 contacts after expiration, got %d", len(contacts))
	}

	// Verify internal maps are cleaned up
	store.lock.RLock()
	hashExists := len(store.hashes[hash]) > 0
	timestampExists := len(store.timestamps[hash]) > 0
	store.lock.RUnlock()

	if hashExists {
		t.Error("Hash map should be cleaned up after expiration")
	}
	if timestampExists {
		t.Error("Timestamp map should be cleaned up after expiration")
	}
}

func TestContactStore_ExpirationWithReadd(t *testing.T) {
	// Use longer expiration time for testing to reduce flakiness
	shortExpire := 200 * time.Millisecond
	store := newStore(shortExpire)

	contact := Contact{ID: bits.Rand(), IP: nil, Port: 8080}
	hash := bits.Rand()

	// Add contact to store
	store.Upsert(hash, contact)

	// Wait for near expiration but not quite (with larger margin)
	time.Sleep(100 * time.Millisecond)

	// Re-add contact (should reset timestamp)
	store.Upsert(hash, contact)

	// Wait past original expiration time
	time.Sleep(150 * time.Millisecond) // Slightly less than expiration time

	// Contact should still exist because timestamp was reset
	contacts := store.Get(hash)
	if len(contacts) != 1 {
		t.Errorf("Expected 1 contact after re-add, got %d", len(contacts))
	}

	// Wait for expiration after re-add (with larger margin)
	time.Sleep(250 * time.Millisecond) // Ensure it expires

	// Now it should be expired
	contacts = store.Get(hash)
	if len(contacts) != 0 {
		t.Errorf("Expected 0 contacts after final expiration, got %d", len(contacts))
	}
}

func TestContactStore_RemoveContactFromHash_NonExistent(t *testing.T) {
	store := newStore(tExpire)

	contact := Contact{ID: bits.Rand(), IP: nil, Port: 8080}
	hash := bits.Rand()

	// Remove non-existent contact from non-existent hash - should not panic
	store.RemoveContactFromHash(hash, contact)

	// Verify store is still in valid state
	if store.CountStoredHashes() != 0 {
		t.Error("Store should be empty")
	}
}
