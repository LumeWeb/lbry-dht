package dht

import (
	"sync"

	"go.lumeweb.com/lbry-dht/bits"
)

// TODO: expire stored data after tExpire time

type contactStore struct {
	// map of blob hashes to (map of node IDs to bools)
	hashes map[bits.Bitmap]map[bits.Bitmap]bool
	// stores the peers themselves, so they can be updated in one place
	contacts map[bits.Bitmap]Contact
	lock     sync.RWMutex
}

func newStore() *contactStore {
	return &contactStore{
		hashes:   make(map[bits.Bitmap]map[bits.Bitmap]bool),
		contacts: make(map[bits.Bitmap]Contact),
	}
}

func (s *contactStore) Upsert(blobHash bits.Bitmap, contact Contact) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.hashes[blobHash]; !ok {
		s.hashes[blobHash] = make(map[bits.Bitmap]bool)
	}
	s.hashes[blobHash][contact.ID] = true
	s.contacts[contact.ID] = contact
}

func (s *contactStore) Get(blobHash bits.Bitmap) []Contact {
	s.lock.RLock()
	defer s.lock.RUnlock()

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
