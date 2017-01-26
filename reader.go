package orc

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
)

const (
	TAIL_SIZE_GUESS = 16 * 1024 // 16 KB, must be > 255
)

type reader interface {
	io.Closer
	io.ReadSeeker
	io.ReaderAt
	Name() string
}

type File struct {
	PostScript
	Footer

	length           int64
	postscriptLength int64
	r                reader
}

func (f *File) Close() {
	f.r.Close()
}

func (f *File) Length() int64 {
	return f.length
}

func (f *File) PostScriptLength() int64 {
	return f.postscriptLength
}

var writerVersions = map[uint32]string{
	1: "HIVE-8732",
	2: "HIVE-4243",
	3: "HIVE-12055",
	4: "HIVE-13083",
}

func (f *File) WriterVersion() string {
	v, ok := writerVersions[f.PostScript.GetWriterVersion()]
	if !ok {
		return "original"
	}
	return v
}

// Open an ORC file, ready for reading
func Open(filename string) (*File, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	o := &File{length: info.Size(), r: f}
	if err := load(o); err != nil {
		return nil, err
	}
	return o, nil
}

func load(f *File) error {
	if err := f.loadTail(); err != nil {
		return err
	}
	return nil
}

// load postscript and footer
// The Postscript section provides the necessary information to interpret the rest of the file including the length of
// the file’s Footer and Metadata sections, the version of the file, and the kind of general compression used
func (f *File) loadTail() error {
	readSize := int64(math.Min(float64(TAIL_SIZE_GUESS), float64(f.length)))
	buf := make([]byte, readSize)

	n, err := f.r.ReadAt(buf, f.length-readSize)
	if err != nil {
		return fmt.Errorf("Failed to read %s: %s", f.r.Name(), err)
	}
	if n < 4 {
		return fmt.Errorf("Failed to read enough of %s to load tail", f.r.Name())
	}

	f.postscriptLength = int64(buf[n-1]) // uint8 is actually enough

	if err := proto.Unmarshal(buf[readSize-f.postscriptLength-1:readSize-1], &(f.PostScript)); err != nil {
		return err
	}

	if f.PostScript.GetMagic() != "ORC" {
		return fmt.Errorf("unexpected magic: %s", f.PostScript.GetMagic())
	}

	return f.loadFooter(&buf)
}

// load footer
// Once the Postscript is parsed, the compressed serialized length of the Footer is known and it can be decompressed
// and parsed.
func (f *File) loadFooter(tail *[]byte) error {
	footerLength := int64(f.PostScript.GetFooterLength())
	tailLength := 1 + f.postscriptLength + footerLength
	readLength := int64(len(*tail))

	var footerReader io.Reader
	if tailLength > readLength {
		// did not catch entire tail with first read, get more
		// TODO: revisit reread vs read diff + concat original read
		footerReader = io.NewSectionReader(f.r, f.length-tailLength, footerLength)
	} else {
		footerOffset := readLength - tailLength
		footerReader = bytes.NewReader((*tail)[footerOffset : footerOffset+footerLength])
	}

	if *(f.PostScript.Compression) != CompressionKind_NONE {
		var err error
		footerReader, err = f.compressedReader(footerReader, footerLength)
		if err != nil {
			return err
		}
	}

	footerBuf, err := ioutil.ReadAll(footerReader)
	if err != nil {
		return fmt.Errorf("while consuming footer: %s", err)
	}

	if err := proto.Unmarshal(footerBuf, &(f.Footer)); err != nil {
		return fmt.Errorf("while unmarshaling footer: %s", err)
	}

	return nil
}

func (f *File) GetStripeFooter(i *StripeInformation) (*StripeFooter, error) {
	start := int64(i.GetOffset() + i.GetIndexLength() + i.GetDataLength())
	length := int64(i.GetFooterLength())
	var r io.Reader = io.NewSectionReader(f.r, start, length)

	if *(f.PostScript.Compression) != CompressionKind_NONE {
		var err error
		r, err = f.compressedReader(r, length)
		if err != nil {
			return nil, err
		}
	}

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("while consuming stripe footer: %s", err)
	}

	var footer StripeFooter
	if err := proto.Unmarshal(buf, &footer); err != nil {
		return nil, fmt.Errorf("while unmarshaling stripe footer: %s", err)
	}

	return &footer, nil
}

func (f *File) compressedReader(r io.Reader, l int64) (io.Reader, error) {
	buf := make([]byte, 3)
	// TODO: handle short read
	if _, err := r.Read(buf); err != nil {
		return nil, err
	}
	buf = append(buf, 0)
	header := binary.LittleEndian.Uint32(buf)

	isOriginal := header & 1
	compressedLength := header >> 1 // (header - isOriginal) / 2
	if int64(compressedLength) != l-3 {
		log.Printf("compressed length %d less than expected %d", compressedLength, l-3)
		r = &io.LimitedReader{R: r, N: int64(compressedLength)}
	}

	if isOriginal == 0 {
		switch *(f.PostScript.Compression) {
		case CompressionKind_ZLIB:
			r = flate.NewReader(r)
		case CompressionKind_SNAPPY:
			snap, err := ioutil.ReadAll(r)
			if err != nil {
				return nil, fmt.Errorf("while reading snappy bits: %s", err)
			}

			dst, err := snappy.Decode(nil, snap)
			if err != nil {
				return nil, fmt.Errorf("while decoding snappy bits: %s", err)
			}
			r = bytes.NewReader(dst)
		default:
			return nil, fmt.Errorf("unsupported compression: %s", f.PostScript.Compression.String())
		}
	}

	return r, nil
}
