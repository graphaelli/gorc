package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/graphaelli/gorc"
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <filename> [<filename> ...]\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func printColStats(col *orc.ColumnStatistics) {
	if stats := col.GetBinaryStatistics(); stats != nil {
		fmt.Printf("Data type: Binary\nValues: %d\nTotal Length: %d\n", col.GetNumberOfValues(), stats.GetSum())
	} else if stats := col.GetBucketStatistics(); stats != nil {
		trues := stats.GetCount()[0]
		values := col.GetNumberOfValues()
		fmt.Printf("Data type: Boolean\nValues: %d\n(true: %d; false: %d)\n", values, trues, values-trues)
	} else if stats := col.GetDateStatistics(); stats != nil {
		fmt.Printf("Data type: Date\nValues: %d\nMinimum: %d\nMaximum: %d\n", col.GetNumberOfValues(),
			stats.GetMinimum(), stats.GetMaximum())
	} else if stats := col.GetDecimalStatistics(); stats != nil {
		fmt.Printf("Data type: Decimal\nValues: %d\nMinimum: %s\nMaximum: %s\nSum: %s\n", col.GetNumberOfValues(),
			stats.GetMinimum(), stats.GetMaximum(), stats.GetSum())
	} else if stats := col.GetDoubleStatistics(); stats != nil {
		fmt.Printf("Data type: Double\nValues: %d\nMinimum: %f\nMaximum: %f\nSum: %f\n", col.GetNumberOfValues(),
			stats.GetMinimum(), stats.GetMaximum(), stats.GetSum())
	} else if stats := col.GetIntStatistics(); stats != nil {
		fmt.Printf("Data type: Integer\nValues: %d\nMinimum: %d\nMaximum: %d\nSum: %d\n", col.GetNumberOfValues(),
			stats.GetMinimum(), stats.GetMaximum(), stats.GetSum())
	} else if stats := col.GetStringStatistics(); stats != nil {
		fmt.Printf("Data type: String\nValues: %d\nMinimum: %s\nMaximum: %s\nTotal Length: %d\n", col.GetNumberOfValues(),
			stats.GetMinimum(), stats.GetMaximum(), stats.GetSum())
	} else if stats := col.GetTimestampStatistics(); stats != nil {
		fmt.Printf("Data type: Timestamp\nValues: %d\nMinimum: %d\nMaximum: %d\n", col.GetNumberOfValues(),
			stats.GetMinimum(), stats.GetMaximum())
	} else {
		fmt.Printf("Column has %d values\n", col.GetNumberOfValues())
	}
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

		// TODO: hasCorrectStatistics?
		colStats := o.GetStatistics()
		fmt.Printf("%s has %d columns\n", filename, len(colStats))
		for i, col := range colStats {
			fmt.Printf("*** Column %d ***\n", i)
			printColStats(col)
			fmt.Println()
		}

		metadata := o.GetMetadata()
		if metadata == nil {
			log.Fatalln("failed to get metadata")
		}
		stripeStats := metadata.GetStripeStats()
		fmt.Printf("%s has %d stripes\n", filename, len(stripeStats))

		for i, stripe := range stripeStats {
			fmt.Printf("*** Stripe %d ***\n\n", i)
			for c, col := range stripe.GetColStats() {
				fmt.Printf("--- Column %d ---\n", c)
				printColStats(col)
				fmt.Println()
			}
		}
	}
}
