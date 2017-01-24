package orc

import "testing"

func TestOpen(t *testing.T) {
	testCases := []struct {
		filename string
		CompressionKind
	}{
		{"examples/TestOrcFile.columnProjection.orc", CompressionKind_NONE},
		{"examples/TestOrcFile.emptyFile.orc", CompressionKind_NONE},
		{"examples/TestOrcFile.metaData.orc", CompressionKind_NONE},
		{"examples/TestOrcFile.test1.orc", CompressionKind_ZLIB},
		{"examples/TestOrcFile.testMemoryManagementV11.orc", CompressionKind_NONE},
		{"examples/TestOrcFile.testMemoryManagementV12.orc", CompressionKind_NONE},
		{"examples/TestOrcFile.testPredicatePushdown.orc", CompressionKind_NONE},
		{"examples/TestOrcFile.testSeek.orc", CompressionKind_ZLIB},
		// Skipped!
		{"examples/TestOrcFile.testSnappy.orc", CompressionKind_SNAPPY},
		{"examples/TestOrcFile.testStringAndBinaryStatistics.orc", CompressionKind_ZLIB},
		{"examples/TestOrcFile.testStripeLevelStats.orc", CompressionKind_ZLIB},
		{"examples/TestOrcFile.testTimestamp.orc", CompressionKind_ZLIB},
		{"examples/TestOrcFile.testUnionAndTimestamp.orc", CompressionKind_NONE},
		{"examples/TestOrcFile.testWithoutIndex.orc", CompressionKind_SNAPPY},
		{"examples/decimal.orc", CompressionKind_NONE},
		{"examples/demo-11-none.orc", CompressionKind_NONE},
		{"examples/demo-11-zlib.orc", CompressionKind_ZLIB},
		{"examples/demo-12-zlib.orc", CompressionKind_ZLIB},
		{"examples/nulls-at-end-snappy.orc", CompressionKind_SNAPPY},
		{"examples/orc-file-11-format.orc", CompressionKind_NONE},
		{"examples/orc_split_elim.orc", CompressionKind_NONE},
		{"examples/over1k_bloom.orc", CompressionKind_ZLIB},
		// LZ4 not supported
		// {"examples/TestVectorOrcFile.testLz4.orc", CompressionKind_LZ4},
		// LZO not supported (test passes because content is not actually compressed)
		// {"examples/TestVectorOrcFile.testLzo.orc", CompressionKind_LZO}, // isOriginal = 1
		{"examples/TestOrcFile.testDate1900.orc", CompressionKind_ZLIB},
		{"examples/TestOrcFile.testDate2038.orc", CompressionKind_ZLIB},
	}

	for _, tc := range testCases {
		t.Run(tc.filename, func(t *testing.T) {
			if tc.filename == "examples/TestOrcFile.testSnappy.orc" {
				t.Skip("skipped", tc.filename)
				return
			}
			o, err := Open(tc.filename)
			if err != nil {
				t.Error(err)
				return
			}
			defer o.Close()
			if *o.PostScript.Compression != tc.CompressionKind {
				t.Errorf("got %s; want %s", *o.PostScript.Compression, tc.CompressionKind)
			}
		})
	}
}
