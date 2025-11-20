package dht

import (
	"sync"
	"time"

	"go.lumeweb.com/lbry-dht/bits"
)

type contactStore struct {
	// map of blob hashes to (map of node IDs to bools)
	hashes map[bits.Bitmap]map[bits.Bitmap]bool
	// stores the peers themselves, so they can be updated in one place
	contacts map[bits.Bitmap]Contact
	// stores timestamps for when each contact was added to a specific hash
	timestamps map[bits.Bitmap]map[bits.Bitmap]time.Time
	// time after which contacts expire
	contactExpire time.Duration
	lock          sync.RWMutex
}

func newStore(contactExpire time.Duration) *contactStore {
	return &contactStore{
		hashes:        make(map[bits.Bitmap]map[bits.Bitmap]bool),
		contacts:      make(map[bits.Bitmap]Contact),
		timestamps:    make(map[bits.Bitmap]map[bits.Bitmap]time.Time),
		contactExpire: contactExpire,
	}
}

func (s *contactStore) Upsert(blobHash bits.Bitmap, contact Contact) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Clean up expired entries for this hash before adding new one
	s.cleanupExpiredForHash(blobHash)

	if _, ok := s.hashes[blobHash]; !ok {
		s.hashes[blobHash] = make(map[bits.Bitmap]bool)
	}
	if _, ok := s.timestamps[blobHash]; !ok {
		s.timestamps[blobHash] = make(map[bits.Bitmap]time.Time)
	}

	s.hashes[blobHash][contact.ID] = true
	s.contacts[contact.ID] = contact
	s.timestamps[blobHash][contact.ID] = time.Now()
}

func (s *contactStore) Get(blobHash bits.Bitmap) []Contact {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Clean up expired entries before returning
	s.cleanupExpiredForHash(blobHash)

	var contacts []Contact
	if ids, ok := s.hashes[blobHash]; ok {
		for id := range ids {
			contact, ok := s.contacts[id]
			if !ok {
				panic("node id in IDs list, but not in nodeInfo")
			}
			contacts = append(contacts, contact)
		}
	}
	return contacts
}

// RemoveContact removes a contact from all hashes and from contacts map
func (s *contactStore) RemoveContact(contact Contact) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Remove from all hash mappings
	for hash, idMap := range s.hashes {
		if _, exists := idMap[contact.ID]; exists {
			delete(idMap, contact.ID)
			// Remove from timestamps as well
			s.removeTimestampForContact(hash, contact.ID)
			// Clean up empty hash maps
			if len(idMap) == 0 {
				delete(s.hashes, hash)
			}
		}
	}

	// Remove from contacts map
	delete(s.contacts, contact.ID)
}

// RemoveContactFromHash removes a specific contact from a specific hash
func (s *contactStore) RemoveContactFromHash(blobHash bits.Bitmap, contact Contact) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if idMap, exists := s.hashes[blobHash]; exists {
		delete(idMap, contact.ID)
		// Remove from timestamps as well
		s.removeTimestampForContact(blobHash, contact.ID)
		// Clean up empty hash maps
		if len(idMap) == 0 {
			delete(s.hashes, blobHash)
		}
	}

	// Only remove from contacts map if this contact isn't associated with any other hashes
	stillAssociated := false
	for _, idMap := range s.hashes {
		if _, exists := idMap[contact.ID]; exists {
			stillAssociated = true
			break
		}
	}

	if !stillAssociated {
		delete(s.contacts, contact.ID)
	}
}

// Replace: TODO method:
func (s *contactStore) RemoveTODO(contact Contact) {
	s.RemoveContact(contact)
}

func (s *contactStore) CountStoredHashes() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.hashes)
}

// removeTimestampForContact removes timestamp for a specific contact from a specific hash
// This method assumes the lock is already held
func (s *contactStore) removeTimestampForContact(blobHash bits.Bitmap, contactID bits.Bitmap) {
	if timestampMap, exists := s.timestamps[blobHash]; exists {
		delete(timestampMap, contactID)
		if len(timestampMap) == 0 {
			delete(s.timestamps, blobHash)
		}
	}
}

// cleanupExpiredForHash removes expired contacts for a specific blob hash
// This method assumes the lock is already held
func (s *contactStore) cleanupExpiredForHash(blobHash bits.Bitmap) {
	// Skip cleanup if expiration is disabled (contactExpire <= 0)
	if s.contactExpire <= 0 {
		return
	}

	idMap, exists := s.hashes[blobHash]
	if !exists {
		return
	}

	timestampMap, exists := s.timestamps[blobHash]
	if !exists {
		return
	}

	cutoffTime := time.Now().Add(-s.contactExpire)
	var expiredIDs []bits.Bitmap

	// Find expired contacts
	for id, timestamp := range timestampMap {
		if timestamp.Before(cutoffTime) {
			expiredIDs = append(expiredIDs, id)
		}
	}

	// Remove expired contacts
	for _, id := range expiredIDs {
		delete(idMap, id)
		delete(timestampMap, id)

		// Check if this contact is still associated with other hashes
		stillAssociated := false
		for otherHash, otherIDMap := range s.hashes {
			if otherHash.Equals(blobHash) {
				continue // skip the current hash we're processing
			}
			if _, exists := otherIDMap[id]; exists {
				stillAssociated = true
				break
			}
		}

		// Remove from contacts map if not associated with any other hashes
		if !stillAssociated {
			delete(s.contacts, id)
		}
	}

	// Clean up empty maps
	if len(idMap) == 0 {
		delete(s.hashes, blobHash)
		delete(s.timestamps, blobHash)
	}
}

// CleanupExpired removes all expired contacts from the store
func (s *contactStore) CleanupExpired() {
	s.lock.Lock()
	defer s.lock.Unlock()

	for blobHash := range s.hashes {
		s.cleanupExpiredForHash(blobHash)
	}
}
