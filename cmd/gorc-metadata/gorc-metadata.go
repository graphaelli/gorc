package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/graphaelli/gorc"
)

var (
	verbose = flag.Bool("verbose", false, "include additional metadata")
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <filename> [<filename> ...]\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func stripes(infos []*orc.StripeInformation, f *orc.File, verbose bool) []map[string]interface{} {
	var j []map[string]interface{}

	for i, info := range infos {
		offset := info.GetOffset()
		si := map[string]interface{}{
			"data":   info.GetDataLength(),
			"footer": info.GetFooterLength(),
			"index":  info.GetIndexLength(),
			"length": info.GetIndexLength() + info.GetDataLength() + info.GetFooterLength(),
			"offset": offset,
			"rows":   info.GetNumberOfRows(),
			"stripe": i,
		}

		if verbose {
			if footer, err := f.GetStripeFooter(info); err != nil {
				si["encodings"] = err
			} else {
				encodings := []map[string]interface{}{}
				for c, col := range footer.GetColumns() {
					encodings = append(encodings, map[string]interface{}{
						"column":   c,
						"encoding": col.GetKind().String(),
						"count":    col.GetDictionarySize(),
					})
				}
				si["encodings"] = encodings

				streams := []map[string]interface{}{}
				for c, col := range footer.GetStreams() {
					length := col.GetLength()
					streams = append(streams, map[string]interface{}{
						"column": col.GetColumn(),
						"id":     c,
						"kind":   col.GetKind().String(),
						"length": length,
						"offset": offset,
					})
					offset += length
				}
				si["streams"] = streams

				si["timezone"] = footer.GetWriterTimezone()
			}
		}

		j = append(j, si)
	}
	return j
}

func main() {
	flag.Parse()
	if len(flag.Args()) < 1 {
		flag.Usage()
		return
	}

	for _, filename := range flag.Args() {
		o, err := orc.Open(filename)
		if err != nil {
			log.Fatalln(err)
		}

		var version []string
		for _, v := range o.PostScript.GetVersion() {
			version = append(version, strconv.FormatUint(uint64(v), 10))
		}

		metadata := map[string]interface{}{
			"compression":       strings.ToLower(o.PostScript.Compression.String()),
			"compression block": o.PostScript.CompressionBlockSize,
			"content":           o.Footer.GetContentLength(),
			"file length":       o.Length(),
			"footer":            o.PostScript.GetFooterLength(),
			"format":            strings.Join(version, "."),
			"name":              filename,
			"postscript":        o.PostScriptLength(),
			"row index stride":  o.Footer.GetRowIndexStride(),
			"rows":              o.Footer.GetNumberOfRows(),
			"stripe count":      len(o.Footer.GetStripes()),
			"stripe stats":      o.PostScript.GetMetadataLength(),
			"stripes":           stripes(o.Footer.GetStripes(), o, *verbose),
			"type":              fmt.Sprintf("%q", o.Footer.Types), // not identical
			"user metadata":     o.Footer.GetMetadata(),            // not identical
			"writer version":    o.WriterVersion(),
		}

		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(&metadata); err != nil {
			log.Println(err)
		}
	}
}
