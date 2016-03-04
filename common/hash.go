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

// Algorithm contains with hash algorithm is used
var Algorithm = crypto.MD5

// ConvertPublicationToBytes converts a publication into a byte slice.
// It returns a byte slice. It takes as input the publication.
func ConvertPublicationToBytes(pub *pb.Publication) []byte {
	var buf bytes.Buffer
	pubType := make([]byte, 8)
	publisherID := make([]byte, 8)
	publicationID := make([]byte, 8)
	brokerID := make([]byte, 8)
	topicID := make([]byte, 8)

	// Convert publication information to byte slices
	binary.PutUvarint(pubType, uint64(pub.PubType))
	binary.PutUvarint(publisherID, pub.PublisherID)
	binary.PutVarint(publicationID, pub.PublicationID)
	binary.PutUvarint(brokerID, pub.BrokerID)
	binary.PutUvarint(topicID, pub.TopicID)

	// Write publication information to buffer
	buf.Write(pubType)
	buf.Write(publisherID)
	buf.Write(publicationID)
	buf.Write(brokerID)
	buf.Write(topicID)
	for i := range pub.Contents {
		buf.Write(pub.Contents[i])
	}

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
