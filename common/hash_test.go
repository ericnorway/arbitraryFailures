package common

import (
	"bytes"
	"crypto"
	"testing"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

func TestConvertPublicationToBytes(t *testing.T) {
	for i, test := range convertPublicationToBytesTests {
		buf := ConvertPublicationToBytes(&test.pub)

		if !bytes.Equal(buf, test.want) {
			t.Errorf("ConvertPublicationToBytes\ntest nr:%d\ndescription: %s\nwant: %v\ngot: %v\n",
				i+1, test.desc, test.want, buf)
		}
	}
}

var convertPublicationToBytesTests = []struct {
	desc string
	pub  pb.Publication
	want []byte
}{
	{
		desc: "Normal Publication",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Contents: [][]byte{
				[]byte("The quick, brown fox jumped over the lazy dog."),
			},
		},
		want: []byte{
			1, 0, 0, 0, 0, 0, 0, 0, // BRB
			7, 0, 0, 0, 0, 0, 0, 0, // 7
			18, 0, 0, 0, 0, 0, 0, 0, // 9 (18 because signed)
			33, 0, 0, 0, 0, 0, 0, 0, // 33
			84, 104, 101, // The
			32,
			113, 117, 105, 99, 107, // quick
			44, // comma
			32,
			98, 114, 111, 119, 110, // brown
			32,
			102, 111, 120, // fox
			32,
			106, 117, 109, 112, 101, 100, // jumped
			32,
			111, 118, 101, 114, // over
			32,
			116, 104, 101, // the
			32,
			108, 97, 122, 121, // lazy
			32,
			100, 111, 103, // dog
			46, // period
		},
	},
	{
		desc: "Publication with empty content.",
		pub: pb.Publication{
			PubType:       AB,
			PublisherID:   1,
			PublicationID: 2,
			TopicID:       3,
			Contents:      [][]byte{},
		},
		want: []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // AB
			1, 0, 0, 0, 0, 0, 0, 0, // 1
			4, 0, 0, 0, 0, 0, 0, 0, // 2 (4 because signed)
			3, 0, 0, 0, 0, 0, 0, 0, // 3
		},
	},
}

func TestCreatePublicationMAC(t *testing.T) {
	for i, test := range createPublicationMACTests {
		buf := CreatePublicationMAC(&test.pub, test.key, test.alg)

		if !bytes.Equal(buf, test.want) {
			t.Errorf("CreatePublicationMAC\ntest nr:%d\ndescription: %s\nwant: %v\ngot: %v\n",
				i+1, test.desc, test.want, buf)
		}
	}
}

var createPublicationMACTests = []struct {
	desc string
	pub  pb.Publication
	key  []byte
	alg  crypto.Hash
	want []byte
}{
	{
		desc: "Normal Publication, MD5",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Contents: [][]byte{
				[]byte("The quick, brown fox jumped over the lazy dog."),
			},
		},
		key:  []byte("12345"),
		alg:  crypto.MD5,
		want: []byte{138, 120, 109, 82, 211, 5, 202, 39, 76, 25, 122, 179, 235, 117, 97, 250},
	},
	{
		desc: "Normal Publication, SHA1",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Contents: [][]byte{
				[]byte("The quick, brown fox jumped over the lazy dog."),
			},
		},
		key:  []byte("Spaceballs"),
		alg:  crypto.SHA1,
		want: []byte{2, 81, 227, 253, 137, 190, 227, 182, 138, 206, 220, 185, 18, 96, 221, 15, 248, 47, 53, 207},
	},
	{
		desc: "Normal Publication, SHA256",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Contents: [][]byte{
				[]byte("The quick, brown fox jumped over the lazy dog."),
			},
		},
		key:  []byte("DarkHelmet"),
		alg:  crypto.SHA256,
		want: []byte{170, 148, 9, 35, 59, 172, 107, 115, 9, 145, 34, 87, 85, 37, 125, 180, 238, 1, 83, 29, 7, 12, 13, 252, 86, 172, 141, 70, 23, 121, 9, 131},
	},
	{
		desc: "Normal Publication, SHA512",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Contents: [][]byte{
				[]byte("The quick, brown fox jumped over the lazy dog."),
			},
		},
		key: []byte("dARKhELMET"),
		alg: crypto.SHA512,
		want: []byte{73, 234, 16, 38, 105, 224, 207, 121, 85, 52, 234, 18, 27, 223, 22, 79, 103, 254, 8, 66, 95, 32, 72, 153, 53, 228, 189, 127, 64, 47, 17, 135,
			42, 122, 55, 209, 185, 14, 130, 104, 90, 231, 5, 151, 52, 36, 123, 198, 90, 99, 122, 112, 167, 55, 30, 211, 82, 148, 209, 208, 132, 35, 176, 110},
	},
}

func TestCheckPublicationMAC(t *testing.T) {
	for i, test := range checkPublicationMACTests {
		valid := CheckPublicationMAC(&test.pub, test.mac, test.key, test.alg)

		if valid != test.want {
			t.Errorf("CheckPublicationMAC\ntest nr:%d\ndescription: %s\nwant: %v\ngot: %v\n",
				i+1, test.desc, test.want, valid)
		}
	}
}

var checkPublicationMACTests = []struct {
	desc string
	pub  pb.Publication
	mac  []byte
	key  []byte
	alg  crypto.Hash
	want bool
}{
	{
		desc: "Normal Publication, MD5",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Contents: [][]byte{
				[]byte("The quick, brown fox jumped over the lazy dog."),
			},
		},
		mac:  []byte{138, 120, 109, 82, 211, 5, 202, 39, 76, 25, 122, 179, 235, 117, 97, 250},
		key:  []byte("12345"),
		alg:  crypto.MD5,
		want: true,
	},
	{
		desc: "Normal Publication, MD5, PubType changed",
		pub: pb.Publication{
			PubType:       AB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Contents: [][]byte{
				[]byte("The quick, brown fox jumped over the lazy dog."),
			},
		},
		mac:  []byte{138, 120, 109, 82, 211, 5, 202, 39, 76, 25, 122, 179, 235, 117, 97, 250},
		key:  []byte("12345"),
		alg:  crypto.MD5,
		want: false,
	},
	{
		desc: "Normal Publication, MD5, PublisherID changed",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   11,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Contents: [][]byte{
				[]byte("The quick, brown fox jumped over the lazy dog."),
			},
		},
		mac:  []byte{138, 120, 109, 82, 211, 5, 202, 39, 76, 25, 122, 179, 235, 117, 97, 250},
		key:  []byte("12345"),
		alg:  crypto.MD5,
		want: false,
	},
	{
		desc: "Normal Publication, MD5, PublicationID changed",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 1,
			BrokerID:      2,
			TopicID:       33,
			Contents: [][]byte{
				[]byte("The quick, brown fox jumped over the lazy dog."),
			},
		},
		mac:  []byte{138, 120, 109, 82, 211, 5, 202, 39, 76, 25, 122, 179, 235, 117, 97, 250},
		key:  []byte("12345"),
		alg:  crypto.MD5,
		want: false,
	},
	{
		desc: "Normal Publication, MD5, topic changed",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       99,
			Contents: [][]byte{
				[]byte("The quick, brown fox jumped over the lazy dog."),
			},
		},
		mac:  []byte{138, 120, 109, 82, 211, 5, 202, 39, 76, 25, 122, 179, 235, 117, 97, 250},
		key:  []byte("12345"),
		alg:  crypto.MD5,
		want: false,
	},
	{
		desc: "Normal Publication, MD5, Content changed",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Contents: [][]byte{
				[]byte("Wrong content."),
			},
		},
		mac:  []byte{138, 120, 109, 82, 211, 5, 202, 39, 76, 25, 122, 179, 235, 117, 97, 250},
		key:  []byte("12345"),
		alg:  crypto.MD5,
		want: false,
	},
	{
		desc: "Normal Publication, MD5, Key changed",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Contents: [][]byte{
				[]byte("The quick, brown fox jumped over the lazy dog."),
			},
		},
		mac:  []byte{138, 120, 109, 82, 211, 5, 202, 39, 76, 25, 122, 179, 235, 117, 97, 250},
		key:  []byte("Spaceballs"),
		alg:  crypto.MD5,
		want: false,
	},
}
