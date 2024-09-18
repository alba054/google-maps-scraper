package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	// postgres driver
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"

	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/adapters/writers/csvwriter"
	"github.com/gosom/scrapemate/adapters/writers/jsonwriter"
	"github.com/gosom/scrapemate/scrapemateapp"
	"github.com/playwright-community/playwright-go"

	"github.com/gosom/google-maps-scraper/gmaps"
	"github.com/gosom/google-maps-scraper/kafkawriter"
	"github.com/gosom/google-maps-scraper/postgres"
)

func main() {
	// just install playwright
	if os.Getenv("PLAYWRIGHT_INSTALL_ONLY") == "1" {
		if err := installPlaywright(); err != nil {
			os.Exit(1)
		}

		os.Exit(0)
	}

	if err := run(); err != nil {
		os.Stderr.WriteString(err.Error() + "\n")

		os.Exit(1)

		return
	}

	os.Exit(0)
}

func run() error {
	ctx := context.Background()
	args := parseArgs()

	if args.workerenable {
		return consume(ctx, &args)
	}

	if args.workerph2enable {
		return consumePhase2(ctx, &args)
	}

	if args.dsn == "" {
		return runFromLocalFile(ctx, &args)
	}

	if args.kafkaenable {
		return runFromKafka(ctx, &args)
	}

	return runFromDatabase(ctx, &args)
}

func runFromLocalFile(ctx context.Context, args *arguments) error {
	var input io.Reader

	switch args.inputFile {
	case "stdin":
		input = os.Stdin
	default:
		f, err := os.Open(args.inputFile)
		if err != nil {
			return err
		}

		defer f.Close()

		input = f
	}

	var resultsWriter io.Writer

	switch args.resultsFile {
	case "stdout":
		resultsWriter = os.Stdout
	default:
		f, err := os.Create(args.resultsFile)
		if err != nil {
			return err
		}

		defer f.Close()

		resultsWriter = f
	}

	csvWriter := csvwriter.NewCsvWriter(csv.NewWriter(resultsWriter))

	writers := []scrapemate.ResultWriter{}

	if args.json {
		writers = append(writers, jsonwriter.NewJSONWriter(resultsWriter))
	} else {
		writers = append(writers, csvWriter)
	}

	opts := []func(*scrapemateapp.Config) error{
		// scrapemateapp.WithCache("leveldb", "cache"),
		scrapemateapp.WithConcurrency(args.concurrency),
		scrapemateapp.WithExitOnInactivity(args.exitOnInactivityDuration),
	}
	if args.workerenable {
		opts = []func(*scrapemateapp.Config) error{
			// scrapemateapp.WithCache("leveldb", "cache"),
			scrapemateapp.WithConcurrency(args.concurrency),
		}
	}

	if args.debug {
		opts = append(opts, scrapemateapp.WithJS(
			scrapemateapp.Headfull(),
			scrapemateapp.DisableImages(),
		),
		)
	} else {
		opts = append(opts, scrapemateapp.WithJS(scrapemateapp.DisableImages()))
	}

	cfg, err := scrapemateapp.NewConfig(
		writers,
		opts...,
	)
	if err != nil {
		return err
	}

	app, err := scrapemateapp.NewScrapeMateApp(cfg)
	if err != nil {
		return err
	}

	seedJobs, err := createSeedJobs(args.langCode, input, args.maxDepth, args.email, args.nearby, args.lat, args.long, args.link, args.zoomLevel, args.nearbyState)
	if err != nil {
		return err
	}

	return app.Start(ctx, seedJobs...)
}

func runFromKafka(ctx context.Context, args *arguments) error {
	var input io.Reader

	switch args.inputFile {
	case "stdin":
		input = os.Stdin
	default:
		f, err := os.Open(args.inputFile)
		if err != nil {
			return err
		}

		defer f.Close()

		input = f
	}

	// Define the Kafka broker and topic
	brokerAddress := []string{"kafka01.research.ai:9092", "kafka02.research.ai:9092", "kafka03.research.ai:9092"} // Change this to your Kafka broker address
	topic := "sample-data-batch-01"

	// Create a new Kafka writer (producer)
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokerAddress,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Load-balances the messages
	})

	// switch args.resultsFile {
	// case "stdout":
	// 	resultsWriter = os.Stdout
	// default:
	// 	f, err := os.Create(args.resultsFile)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	defer f.Close()

	// 	resultsWriter = f
	// }

	kafkawriter := kafkawriter.NewResultWriter(writer)

	writers := []scrapemate.ResultWriter{}

	writers = append(writers, kafkawriter)

	opts := []func(*scrapemateapp.Config) error{
		// scrapemateapp.WithCache("leveldb", "cache"),
		scrapemateapp.WithConcurrency(args.concurrency),
		scrapemateapp.WithExitOnInactivity(args.exitOnInactivityDuration),
	}

	if args.debug {
		opts = append(opts, scrapemateapp.WithJS(
			scrapemateapp.Headfull(),
			scrapemateapp.DisableImages(),
		),
		)
	} else {
		opts = append(opts, scrapemateapp.WithJS(scrapemateapp.DisableImages()))
	}

	cfg, err := scrapemateapp.NewConfig(
		writers,
		opts...,
	)
	if err != nil {
		return err
	}

	app, err := scrapemateapp.NewScrapeMateApp(cfg)
	if err != nil {
		return err
	}

	seedJobs, err := createSeedJobs(args.langCode, input, args.maxDepth, args.email, args.nearby, args.lat, args.long, args.link, args.zoomLevel, args.nearbyState)
	if err != nil {
		return err
	}

	return app.Start(ctx, seedJobs...)
}

func runFromDatabase(ctx context.Context, args *arguments) error {
	db, err := openPsqlConn(args.dsn)
	if err != nil {
		return err
	}

	provider := postgres.NewProvider(db)

	if args.produceOnly {
		return produceSeedJobs(ctx, args, provider)
	}

	psqlWriter := postgres.NewResultWriter(db)

	writers := []scrapemate.ResultWriter{
		psqlWriter,
	}

	opts := []func(*scrapemateapp.Config) error{
		// scrapemateapp.WithCache("leveldb", "cache"),
		scrapemateapp.WithConcurrency(args.concurrency),
		scrapemateapp.WithProvider(provider),
		scrapemateapp.WithExitOnInactivity(args.exitOnInactivityDuration),
	}

	if args.debug {
		opts = append(opts, scrapemateapp.WithJS(scrapemateapp.Headfull()))
	} else {
		opts = append(opts, scrapemateapp.WithJS())
	}

	cfg, err := scrapemateapp.NewConfig(
		writers,
		opts...,
	)
	if err != nil {
		return err
	}

	app, err := scrapemateapp.NewScrapeMateApp(cfg)
	if err != nil {
		return err
	}

	return app.Start(ctx)
}

func produceSeedJobs(ctx context.Context, args *arguments, provider scrapemate.JobProvider) error {
	var input io.Reader

	switch args.inputFile {
	case "stdin":
		input = os.Stdin
	default:
		f, err := os.Open(args.inputFile)
		if err != nil {
			return err
		}

		defer f.Close()

		input = f
	}

	jobs, err := createSeedJobs(args.langCode, input, args.maxDepth, args.email, args.nearby, args.lat, args.long, args.link, args.zoomLevel, args.nearbyState)
	if err != nil {
		return err
	}

	for i := range jobs {
		if err := provider.Push(ctx, jobs[i]); err != nil {
			return err
		}
	}

	return nil
}

func createSeedJobs(langCode string, r io.Reader, maxDepth int, email bool, nearby string, lat, long float64, link string, zoomLevel string, nearbyState bool) (jobs []scrapemate.IJob, err error) {
	scanner := bufio.NewScanner(r)
	reader := csv.NewReader(r)

	if nearbyState {

		// Create a new CSV reader

		// Read all the lines from the CSV
		// records, err := reader.ReadAll()
		for {
			data, err := reader.Read()
			if data == nil && err != nil {
				break
			}

			if data[0] == "address" {
				continue
			}

			_lat, _ := strconv.ParseFloat(data[1], 64)
			_long, _ := strconv.ParseFloat(data[2], 64)
			_nearby := data[3]

			jobs = append(jobs, gmaps.NewGmapJob("", langCode, "", maxDepth, email, _nearby, _lat, _long, link, zoomLevel))
		}

	} else {
		var query string
		for scanner.Scan() {
			query = strings.TrimSpace(scanner.Text())
			if query == "" {
				continue
			}

			var id string

			if before, after, ok := strings.Cut(query, "#!#"); ok {
				query = strings.TrimSpace(before)
				id = strings.TrimSpace(after)
			}

			jobs = append(jobs, gmaps.NewGmapJob(id, langCode, query, maxDepth, email, nearby, lat, long, link, zoomLevel))
		}
	}

	return jobs, scanner.Err()
}

func installPlaywright() error {
	return playwright.Install()
}

type arguments struct {
	concurrency              int
	cacheDir                 string
	maxDepth                 int
	inputFile                string
	resultsFile              string
	json                     bool
	langCode                 string
	debug                    bool
	dsn                      string
	produceOnly              bool
	exitOnInactivityDuration time.Duration
	email                    bool
	nearby                   string
	link                     string
	zoomLevel                string
	nearbyState              bool
	lat                      float64
	long                     float64
	kafkaenable              bool
	workerenable             bool
	workerph2enable          bool
}

func parseArgs() (args arguments) {
	const (
		defaultDepth      = 10
		defaultCPUDivider = 2
	)

	defaultConcurency := runtime.NumCPU() / defaultCPUDivider
	if defaultConcurency < 1 {
		defaultConcurency = 1
	}

	flag.IntVar(&args.concurrency, "c", defaultConcurency, "sets the concurrency. By default it is set to half of the number of CPUs")
	flag.StringVar(&args.cacheDir, "cache", "cache", "sets the cache directory (no effect at the moment)")
	flag.IntVar(&args.maxDepth, "depth", defaultDepth, "is how much you allow the scraper to scroll in the search results. Experiment with that value")
	flag.StringVar(&args.resultsFile, "results", "stdout", "is the path to the file where the results will be written")
	flag.StringVar(&args.inputFile, "input", "stdin", "is the path to the file where the queries are stored (one query per line). By default it reads from stdin")
	flag.StringVar(&args.langCode, "lang", "en", "is the languate code to use for google (the hl urlparam).Default is en . For example use de for German or el for Greek")
	flag.BoolVar(&args.debug, "debug", false, "Use this to perform a headfull crawl (it will open a browser window) [only when using without docker]")
	flag.StringVar(&args.dsn, "dsn", "", "Use this if you want to use a database provider")
	flag.BoolVar(&args.produceOnly, "produce", false, "produce seed jobs only (only valid with dsn)")
	flag.DurationVar(&args.exitOnInactivityDuration, "exit-on-inactivity", 0, "program exits after this duration of inactivity(example value '5m')")
	flag.BoolVar(&args.json, "json", false, "Use this to produce a json file instead of csv (not available when using db)")
	flag.BoolVar(&args.email, "email", false, "Use this to extract emails from the websites")
	flag.BoolVar(&args.nearbyState, "nearbyState", false, "Enable nearby search from inputfile")
	flag.Float64Var(&args.lat, "lat", 0, "set the latitude to search for nearby places")
	flag.Float64Var(&args.long, "long", 0, "set the longitude to search for nearby places")
	flag.StringVar(&args.nearby, "nearby", "", "set the nearby place")
	flag.StringVar(&args.link, "link", "", "set link of a place")
	flag.StringVar(&args.zoomLevel, "zoomLevel", "14z", "set link of a place")
	flag.BoolVar(&args.kafkaenable, "kafka", false, "enable push data to kafka")
	flag.BoolVar(&args.workerenable, "worker", false, "enable as worker")
	flag.BoolVar(&args.workerph2enable, "workerph2", false, "enable as worker phase 2")

	flag.Parse()

	return args
}

func openPsqlConn(dsn string) (conn *sql.DB, err error) {
	conn, err = sql.Open("pgx", dsn)
	if err != nil {
		return
	}

	err = conn.Ping()

	return
}

func consume(ctx context.Context, args *arguments) error {
	// Define the Kafka broker and topic
	brokerAddress := []string{"kafka01.research.ai:9092", "kafka02.research.ai:9092", "kafka03.research.ai:9092"} // Change this to your Kafka broker address
	topic := "poi-google-map-track-01"
	groupID := "test-0.0.0.1" // Kafka consumer group

	// Create a new Kafka reader (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokerAddress,
		Topic:    topic,
		GroupID:  groupID, // Consumer group ID for coordinated consumption
		MinBytes: 10e3,    // 10KB minimum size per fetch request
		MaxBytes: 10e6,    // 10MB maximum size per fetch request
	})
	defer reader.Close()

	fmt.Println("Starting Kafka consumer...")

	var resultsWriter io.Writer

	switch args.resultsFile {
	case "stdout":
		resultsWriter = os.Stdout
	default:
		f, err := os.Create(args.resultsFile)
		if err != nil {
			return err
		}

		defer f.Close()

		resultsWriter = f
	}

	csvWriter := csvwriter.NewCsvWriter(csv.NewWriter(resultsWriter))

	writers := []scrapemate.ResultWriter{}

	if args.json {
		writers = append(writers, jsonwriter.NewJSONWriter(resultsWriter))
	} else {
		writers = append(writers, csvWriter)
	}

	opts := []func(*scrapemateapp.Config) error{
		// scrapemateapp.WithCache("leveldb", "cache"),
		scrapemateapp.WithConcurrency(args.concurrency),
		scrapemateapp.WithExitOnInactivity(args.exitOnInactivityDuration),
	}
	// if args.workerenable {
	// 	opts = []func(*scrapemateapp.Config) error{
	// 		// scrapemateapp.WithCache("leveldb", "cache"),
	// 		scrapemateapp.WithConcurrency(args.concurrency),
	// 	}
	// }

	if args.debug {
		opts = append(opts, scrapemateapp.WithJS(
			scrapemateapp.Headfull(),
			scrapemateapp.DisableImages(),
		),
		)
	} else {
		opts = append(opts, scrapemateapp.WithJS(scrapemateapp.DisableImages()))
	}

	cfg, err := scrapemateapp.NewConfig(
		writers,
		opts...,
	)
	if err != nil {
		return err
	}

	app, err := scrapemateapp.NewScrapeMateApp(cfg)
	if err != nil {
		return err
	}

	// Consume messages in an infinite loop
	// wg := sync.WaitGroup{}
	counter := make(chan bool, 5)
	for {

		// wg.Add(1)
		counter <- true
		go func() {
			// Read a message from Kafka
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Fatalf("Failed to read message: %v", err)
			}

			var data map[string]string
			json.Unmarshal(message.Value, &data)
			// fmt.Println(data["location_name"])
			// // Optional: Handle the message (process, save, etc.)
			// // handleMessage(message) // Custom processing function
			seedJobs, _ := createSeedJobs(args.langCode, strings.NewReader(data["location_name"]), args.maxDepth, args.email, args.nearby, args.lat, args.long, args.link, args.zoomLevel, args.nearbyState)
			// if err != nil {
			// 	return err
			// }

			app.Start(ctx, seedJobs...)
			<-counter
			// wg.Done()
		}()
	}
	// wg.Wait()

	// return nil
}

func consumePhase2(ctx context.Context, args *arguments) error {
	// Define the Kafka broker and topic
	brokerAddress := []string{"kafka01.research.ai:9092", "kafka02.research.ai:9092", "kafka03.research.ai:9092"} // Change this to your Kafka broker address
	topic := "poi-google-map-track-point"
	groupID := "test-0.0.0.8" // Kafka consumer group

	// Create a new Kafka reader (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokerAddress,
		Topic:    topic,
		GroupID:  groupID, // Consumer group ID for coordinated consumption
		MinBytes: 10e3,    // 10KB minimum size per fetch request
		MaxBytes: 10e6,    // 10MB maximum size per fetch request
	})
	defer reader.Close()

	fmt.Println("Starting Kafka consumer...")

	var resultsWriter io.Writer

	switch args.resultsFile {
	case "stdout":
		resultsWriter = os.Stdout
	default:
		f, err := os.Create(args.resultsFile)
		if err != nil {
			return err
		}

		defer f.Close()

		resultsWriter = f
	}

	csvWriter := csvwriter.NewCsvWriter(csv.NewWriter(resultsWriter))

	writers := []scrapemate.ResultWriter{}

	if args.json {
		writers = append(writers, jsonwriter.NewJSONWriter(resultsWriter))
	} else {
		writers = append(writers, csvWriter)
	}

	opts := []func(*scrapemateapp.Config) error{
		// scrapemateapp.WithCache("leveldb", "cache"),
		scrapemateapp.WithConcurrency(args.concurrency),
		scrapemateapp.WithExitOnInactivity(args.exitOnInactivityDuration),
	}
	// if args.workerenable {
	// 	opts = []func(*scrapemateapp.Config) error{
	// 		// scrapemateapp.WithCache("leveldb", "cache"),
	// 		scrapemateapp.WithConcurrency(args.concurrency),
	// 	}
	// }

	if args.debug {
		opts = append(opts, scrapemateapp.WithJS(
			scrapemateapp.Headfull(),
			scrapemateapp.DisableImages(),
		),
		)
	} else {
		opts = append(opts, scrapemateapp.WithJS(scrapemateapp.DisableImages()))
	}

	cfg, err := scrapemateapp.NewConfig(
		writers,
		opts...,
	)
	if err != nil {
		return err
	}

	app, err := scrapemateapp.NewScrapeMateApp(cfg)
	if err != nil {
		return err
	}

	// Consume messages in an infinite loop
	// wg := sync.WaitGroup{}
	counter := make(chan bool, 5)
	for {

		// wg.Add(1)
		counter <- true
		go func() {
			// Read a message from Kafka
			message, err := reader.ReadMessage(ctx)
			// fmt.Println(message)
			if err != nil {
				log.Fatalf("Failed to read message: %v", err)
			}

			var data map[string]interface{}
			json.Unmarshal(message.Value, &data)
			// fmt.Println(data)
			lat := data["latitude"].(float64)
			long := data["longtitude"].(float64)
			link := data["link"].(string)
			_, _, zoomLevel, _ := extractLatLongZoom(link)
			// // Optional: Handle the message (process, save, etc.)
			// // handleMessage(message) // Custom processing function
			restaurant, _ := createSeedJobs(args.langCode, strings.NewReader("secret"), args.maxDepth, args.email, "restaurant", lat, long, args.link, zoomLevel, args.nearbyState)
			store, _ := createSeedJobs(args.langCode, strings.NewReader("secret"), args.maxDepth, args.email, "convenience store", lat, long, args.link, zoomLevel, args.nearbyState)
			school, _ := createSeedJobs(args.langCode, strings.NewReader("secret"), args.maxDepth, args.email, "school", lat, long, args.link, zoomLevel, args.nearbyState)
			police, _ := createSeedJobs(args.langCode, strings.NewReader("secret"), args.maxDepth, args.email, "police station", lat, long, args.link, zoomLevel, args.nearbyState)
			hospital, _ := createSeedJobs(args.langCode, strings.NewReader("secret"), args.maxDepth, args.email, "hospital", lat, long, args.link, zoomLevel, args.nearbyState)
			bank, _ := createSeedJobs(args.langCode, strings.NewReader("secret"), args.maxDepth, args.email, "bank", lat, long, args.link, zoomLevel, args.nearbyState)
			pasar, _ := createSeedJobs(args.langCode, strings.NewReader("secret"), args.maxDepth, args.email, "pasar", lat, long, args.link, zoomLevel, args.nearbyState)
			toko, _ := createSeedJobs(args.langCode, strings.NewReader("secret"), args.maxDepth, args.email, "toko", lat, long, args.link, zoomLevel, args.nearbyState)
			worship, _ := createSeedJobs(args.langCode, strings.NewReader("secret"), args.maxDepth, args.email, "place of worship", lat, long, args.link, zoomLevel, args.nearbyState)
			market, _ := createSeedJobs(args.langCode, strings.NewReader("secret"), args.maxDepth, args.email, "traditional market", lat, long, args.link, zoomLevel, args.nearbyState)

			seedjobs := []scrapemate.IJob{}
			seedjobs = append(seedjobs, restaurant...)
			seedjobs = append(seedjobs, store...)
			seedjobs = append(seedjobs, school...)
			seedjobs = append(seedjobs, police...)
			seedjobs = append(seedjobs, hospital...)
			seedjobs = append(seedjobs, bank...)
			seedjobs = append(seedjobs, pasar...)
			seedjobs = append(seedjobs, toko...)
			seedjobs = append(seedjobs, worship...)
			seedjobs = append(seedjobs, market...)
			// if err != nil {
			// 	return err
			// }

			app.Start(ctx, seedjobs...)
			<-counter
			// wg.Done()
		}()
	}
	// wg.Wait()

	// return nil
}

// ExtractLatLongZoom extracts latitude, longitude, and zoom level from a string.
func extractLatLongZoom(s string) (latitude, longitude, zoomLevel string, err error) {
	// Define the regex pattern for latitude, longitude, and zoom level
	pattern := `(-?\d+\.\d+),(-?\d+\.\d+),(\d{1,2}z)`

	// Compile the regex pattern
	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", "", "", err
	}

	// Find the first match
	match := re.FindStringSubmatch(s)
	if len(match) < 4 {
		return "", "", "", fmt.Errorf("no match found")
	}

	// Extract latitude, longitude, and zoom level
	latitude = match[1]
	longitude = match[2]
	zoomLevel = match[3]

	return latitude, longitude, zoomLevel, nil
}
