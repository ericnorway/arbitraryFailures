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
			Topic:         33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		want: []byte{
			2, 0, 0, 0, 0, 0, 0, 0, // BRB
			14, 0, 0, 0, 0, 0, 0, 0, // 7
			18, 0, 0, 0, 0, 0, 0, 0, // 9
			4, 0, 0, 0, 0, 0, 0, 0, // 2
			66, 0, 0, 0, 0, 0, 0, 0, // 33
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
			Topic:         4,
			Content:       nil,
		},
		want: []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // AB
			2, 0, 0, 0, 0, 0, 0, 0, // 1
			4, 0, 0, 0, 0, 0, 0, 0, // 2
			6, 0, 0, 0, 0, 0, 0, 0, // 3
			8, 0, 0, 0, 0, 0, 0, 0, // 4
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
			Topic:         33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		key:  []byte("12345"),
		alg:  crypto.MD5,
		want: []byte{170, 117, 87, 155, 209, 35, 81, 95, 90, 137, 77, 129, 207, 26, 195, 161},
	},
	{
		desc: "Normal Publication, SHA1",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			Topic:         33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		key:  []byte("Spaceballs"),
		alg:  crypto.SHA1,
		want: []byte{131, 142, 165, 166, 153, 206, 112, 193, 132, 51, 205, 244, 218, 187, 81, 181, 108, 52, 225, 208},
	},
	{
		desc: "Normal Publication, SHA256",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			Topic:         33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		key:  []byte("DarkHelmet"),
		alg:  crypto.SHA256,
		want: []byte{204, 99, 197, 75, 225, 226, 243, 24, 96, 183, 185, 64, 138, 186, 118, 8, 76, 206, 182, 78, 162, 59, 86, 63, 222, 99, 5, 28, 245, 71, 177, 37},
	},
	{
		desc: "Normal Publication, SHA512",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			Topic:         33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		key: []byte("dARKhELMET"),
		alg: crypto.SHA512,
		want: []byte{66, 117, 187, 97, 208, 108, 194, 20, 223, 126, 99, 142, 254, 34, 171, 128, 70, 108, 208, 231, 79, 221, 51, 22, 69, 124, 45, 121, 171, 225, 72, 5,
			242, 29, 237, 4, 188, 170, 119, 142, 170, 134, 202, 48, 89, 25, 131, 181, 126, 140, 206, 128, 120, 167, 255, 244, 38, 138, 172, 247, 178, 172, 18, 139},
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
			Topic:         33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{170, 117, 87, 155, 209, 35, 81, 95, 90, 137, 77, 129, 207, 26, 195, 161},
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
			Topic:         33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{170, 117, 87, 155, 209, 35, 81, 95, 90, 137, 77, 129, 207, 26, 195, 161},
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
			Topic:         33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{170, 117, 87, 155, 209, 35, 81, 95, 90, 137, 77, 129, 207, 26, 195, 161},
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
			Topic:         33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{170, 117, 87, 155, 209, 35, 81, 95, 90, 137, 77, 129, 207, 26, 195, 161},
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
			Topic:         33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{170, 117, 87, 155, 209, 35, 81, 95, 90, 137, 77, 129, 207, 26, 195, 161},
		key:  []byte("12345"),
		alg:  crypto.MD5,
		want: false,
	},
	{
		desc: "Normal Publication, MD5, Topic changed",
		pub: pb.Publication{
			PubType:       BRB,
			PublisherID:   7,
			PublicationID: 9,
			BrokerID:      2,
			Topic:         99,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{170, 117, 87, 155, 209, 35, 81, 95, 90, 137, 77, 129, 207, 26, 195, 161},
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
			Topic:         33,
			Content:       []byte("Wrong content."),
		},
		mac:  []byte{170, 117, 87, 155, 209, 35, 81, 95, 90, 137, 77, 129, 207, 26, 195, 161},
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
			Topic:         33,
			Content:       []byte("The quick, brown fox jumped over the lazy dog."),
		},
		mac:  []byte{170, 117, 87, 155, 209, 35, 81, 95, 90, 137, 77, 129, 207, 26, 195, 161},
		key:  []byte("Spaceballs"),
		alg:  crypto.MD5,
		want: false,
	},
}
