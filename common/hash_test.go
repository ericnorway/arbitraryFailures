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
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		want: []byte{
			1, 0, 0, 0, 0, 0, 0, 0, // BRB
			7, 0, 0, 0, 0, 0, 0, 0, // 7
			18, 0, 0, 0, 0, 0, 0, 0, // 9 (18 because signed)
			2, 0, 0, 0, 0, 0, 0, 0, // 2
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
			BrokerID:      3,
			TopicID:       4,
			Content:       nil,
		},
		want: []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // AB
			1, 0, 0, 0, 0, 0, 0, 0, // 1
			4, 0, 0, 0, 0, 0, 0, 0, // 2 (4 because signed)
			3, 0, 0, 0, 0, 0, 0, 0, // 3
			4, 0, 0, 0, 0, 0, 0, 0, // 4
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
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		key:  []byte("12345"),
		alg:  crypto.MD5,
		want: []byte{36, 127, 152, 131, 226, 176, 168, 141, 12, 162, 91, 99, 134, 27, 155, 83},
	},
	{
		desc: "Normal Publication, SHA1",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		key:  []byte("Spaceballs"),
		alg:  crypto.SHA1,
		want: []byte{44, 223, 131, 126, 19, 219, 233, 218, 35, 155, 91, 26, 11, 239, 188, 90, 139, 245, 51, 175},
	},
	{
		desc: "Normal Publication, SHA256",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		key:  []byte("DarkHelmet"),
		alg:  crypto.SHA256,
		want: []byte{142, 7, 47, 183, 114, 96, 55, 169, 219, 36, 219, 169, 56, 53, 186, 45, 243, 188, 91, 241, 235, 9, 121, 243, 62, 156, 216, 14, 119, 193, 146, 165},
	},
	{
		desc: "Normal Publication, SHA512",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			TopicID:       33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		key: []byte("dARKhELMET"),
		alg: crypto.SHA512,
		want: []byte{1, 229, 203, 231, 251, 143, 55, 96, 245, 27, 237, 39, 85, 90, 188, 175, 144, 79, 79, 87, 100, 25, 250, 208, 196, 182, 110, 252, 244, 147, 217, 8,
			244, 250, 38, 239, 128, 237, 251, 10, 38, 194, 247, 234, 7, 51, 142, 159, 169, 32, 70, 52, 168, 231, 167, 146, 206, 135, 222, 230, 162, 42, 125, 174},
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
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{36, 127, 152, 131, 226, 176, 168, 141, 12, 162, 91, 99, 134, 27, 155, 83},
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
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{36, 127, 152, 131, 226, 176, 168, 141, 12, 162, 91, 99, 134, 27, 155, 83},
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
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{36, 127, 152, 131, 226, 176, 168, 141, 12, 162, 91, 99, 134, 27, 155, 83},
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
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{36, 127, 152, 131, 226, 176, 168, 141, 12, 162, 91, 99, 134, 27, 155, 83},
		key:  []byte("12345"),
		alg:  crypto.MD5,
		want: false,
	},
	{
		desc: "Normal Publication, MD5, BrokerID changed",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      20,
			TopicID:       33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{36, 127, 152, 131, 226, 176, 168, 141, 12, 162, 91, 99, 134, 27, 155, 83},
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
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{36, 127, 152, 131, 226, 176, 168, 141, 12, 162, 91, 99, 134, 27, 155, 83},
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
			Content:       []byte("Wrong content."),
		},
		mac:  []byte{36, 127, 152, 131, 226, 176, 168, 141, 12, 162, 91, 99, 134, 27, 155, 83},
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
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{36, 127, 152, 131, 226, 176, 168, 141, 12, 162, 91, 99, 134, 27, 155, 83},
		key:  []byte("Spaceballs"),
		alg:  crypto.MD5,
		want: false,
	},
}
