package common

import (
	"bytes"
	"crypto"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"hash"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// ConvertPublicationToBytes converts a publication into a byte slice.
// It returns a byte slice. It takes as input the publication.
func ConvertPublicationToBytes(pub *pb.Publication) []byte {
	var buf bytes.Buffer
	pubType := make([]byte, 8)
	publisherID := make([]byte, 8)
	publicationID := make([]byte, 8)
	brokerID := make([]byte, 8)
	topic := make([]byte, 8)

	// Convert publication information to byte slices
	binary.PutVarint(pubType, int64(pub.PubType))
	binary.PutVarint(publisherID, pub.PublisherID)
	binary.PutVarint(publicationID, pub.PublicationID)
	binary.PutVarint(brokerID, pub.BrokerID)
	binary.PutVarint(topic, pub.Topic)

	// Write publication information to buffer
	buf.Write(pubType)
	buf.Write(publisherID)
	buf.Write(publicationID)
	buf.Write(brokerID)
	buf.Write(topic)
	buf.Write(pub.Content)

	return buf.Bytes()
}

// CreatePublicationMAC creates a MAC from a publication and key.
// It returns a byte slice of the MAC. It takes as input the publication,
// the key, and the type of hash algorithm to use.
func CreatePublicationMAC(pub *pb.Publication, key []byte, algorithm crypto.Hash) []byte {

	var mac hash.Hash

	message := ConvertPublicationToBytes(pub)

	switch {
	case algorithm == crypto.MD5:
		mac = hmac.New(md5.New, key)
	case algorithm == crypto.SHA1:
		mac = hmac.New(sha1.New, key)
	case algorithm == crypto.SHA256:
		mac = hmac.New(sha256.New, key)
	case algorithm == crypto.SHA512:
		mac = hmac.New(sha512.New, key)
	default:
		mac = hmac.New(md5.New, key)
	}
	mac.Write(message)
	sum := mac.Sum(nil)

	return sum
}

// CheckPublicationMAC checks whether or not a MAC is valid for a publication and key.
// It returns true if it is valid, false otherwise. It takes as input the publication, the MAC,
// the key, and the type of hash algorithm to use.
func CheckPublicationMAC(pub *pb.Publication, mac []byte, key []byte, algorithm crypto.Hash) bool {

	mac2 := CreatePublicationMAC(pub, key, algorithm)

	return hmac.Equal(mac, mac2)
}
