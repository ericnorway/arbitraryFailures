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
	// "fmt"
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
	topicID := make([]byte, 8)

	// Convert publication information to byte slices
	binary.PutUvarint(pubType, uint64(pub.PubType))
	binary.PutUvarint(publisherID, pub.PublisherID)
	binary.PutVarint(publicationID, pub.PublicationID)
	binary.PutUvarint(topicID, pub.TopicID)

	// Write publication information to buffer
	buf.Write(pubType)
	buf.Write(publisherID)
	buf.Write(publicationID)
	buf.Write(topicID)
	for i := range pub.Contents {
		buf.Write(pub.Contents[i])
	}

	return buf.Bytes()
}

// CreatePublicationMAC creates a MAC from a publication and key.
// It returns a byte slice of the MAC. It takes as input the publication,
// the key, and the type of hash algorithm to use.
func CreatePublicationMAC(pub *pb.Publication, key []byte) []byte {

	var mac hash.Hash

	message := ConvertPublicationToBytes(pub)

	switch Algorithm {
	case crypto.MD5:
		mac = hmac.New(md5.New, key)
	case crypto.SHA1:
		mac = hmac.New(sha1.New, key)
	case crypto.SHA256:
		mac = hmac.New(sha256.New, key)
	case crypto.SHA512:
		mac = hmac.New(sha512.New, key)
	default:
		mac = hmac.New(md5.New, key)
	}
	mac.Write(message)

	return mac.Sum(nil)
}

// CheckPublicationMAC checks whether or not a MAC is valid for a publication and key.
// It returns true if it is valid, false otherwise. It takes as input the publication, the MAC,
// the key, and the type of hash algorithm to use.
func CheckPublicationMAC(pub *pb.Publication, mac []byte, key []byte) bool {
	mac2 := CreatePublicationMAC(pub, key)
	return hmac.Equal(mac, mac2)
}

// ConvertSubscriptionToBytes converts a subscription request into a byte slice.
// It returns a byte slice. It takes as input the subscription request.
func ConvertSubscriptionToBytes(sub *pb.SubRequest) []byte {
	var buf bytes.Buffer
	subscriberID := make([]byte, 8)
	topicID := make([]byte, 8)

	binary.PutUvarint(subscriberID, sub.SubscriberID)

	// Write subscription information to buffer
	buf.Write(subscriberID)
	for _, topic := range sub.TopicIDs {
		binary.PutUvarint(topicID, topic)
		buf.Write(topicID)
	}

	return buf.Bytes()
}

// CreateSubscriptionMAC creates a MAC from a subscription request and key.
// It returns a byte slice of the MAC. It takes as input the subscription request,
// the key, and the type of hash algorithm to use.
func CreateSubscriptionMAC(sub *pb.SubRequest, key []byte) []byte {

	var mac hash.Hash

	message := ConvertSubscriptionToBytes(sub)

	switch Algorithm {
	case crypto.MD5:
		mac = hmac.New(md5.New, key)
	case crypto.SHA1:
		mac = hmac.New(sha1.New, key)
	case crypto.SHA256:
		mac = hmac.New(sha256.New, key)
	case crypto.SHA512:
		mac = hmac.New(sha512.New, key)
	default:
		mac = hmac.New(md5.New, key)
	}
	mac.Write(message)

	return mac.Sum(nil)
}

// CheckSubscriptionMAC checks whether or not a MAC is valid for a subscription request and key.
// It returns true if it is valid, false otherwise. It takes as input the subscription, the MAC,
// the key, and the type of hash algorithm to use.
func CheckSubscriptionMAC(sub *pb.SubRequest, mac []byte, key []byte) bool {
	mac2 := CreateSubscriptionMAC(sub, key)
	return hmac.Equal(mac, mac2)
}
