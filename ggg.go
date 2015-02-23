package main

import (
	"code.google.com/p/go-charset/charset"
	_ "code.google.com/p/go-charset/data"
	"encoding/xml"
	"flag"
	"strconv"
	//"fmt"
	"github.com/amir/raidman"
	"github.com/spf13/viper"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

type config struct {
	RiemanHost string
	RiemanPort int
	Clusters   []ClusterConfig
}

type ClusterConfig struct {
	Host     string
	Port     int
	Interval int
}

type GangliaXml struct {
	XMLName xml.Name  `xml:"GANGLIA_XML"`
	Version string    `xml:"VERSION,attr"`
	Source  string    `xml:"SOURCE,attr"`
	Grid    []Grid    `xml:"GRID"`
	Cluster []Cluster `xml:"CLUSTER"`
}

type Grid struct {
	XMLName xml.Name `xml:"GRID"`
	Cluster []Cluster
}

type Cluster struct {
	XMLName xml.Name `xml:"CLUSTER"`
	Name    string   `xml:"NAME,attr"`
	Owner   string   `xml:"OWNER,attr"`
	Host    []Host   `xml:"HOST"`
}

type Host struct {
	XMLName  xml.Name `xml:"HOST"`
	Name     string   `xml:"NAME,attr"`
	Ip       string   `xml:"IP,attr"`
	Location string   `xml:"LOCATION,attr"`
	Metric   []Metric `xml:"METRIC"`
}

type Metric struct {
	XMLName   xml.Name       `xml:"METRIC"`
	Name      string         `xml:"NAME,attr"`
	Val       string         `xml:"VAL,attr"`
	Type      string         `xml:"TYPE,attr"`
	Units     string         `xml:"UNIT,attr"`
	TN        string         `xml:"TN,attr"`
	TMax      string         `xml:"TMAX,attr"`
	DMax      string         `xml:"DMAX,attr"`
	Slope     string         `xml:"SLOPE,attr"`
	ExtraData []ExtraElement `xml:"extra_data>extra_element"`
}

type ExtraElement struct {
	XMLName xml.Name `xml:"EXTRA_ELEMENT"`
	Name    string   `xml:"attr"`
	Val     string   `xml:"attr"`
}

var ganglia_addr = flag.String("ganglia_addr", "foreman.voidetoile.net:8649", "ganglia address")
var metric_prefix = flag.String("prefix", "ggg.", "prefix for metric names")

var runeMap = map[rune]rune{
	46: 95, // '.' -> '_'
	20: 95, // ' ' -> '_'
}

func graphiteStringMap(char rune) (ret rune) {
	ret, ok := runeMap[char]
	if !ok {
		ret = char
	}
	return
}

func readXmlFromFile(in io.Reader) (gmeta GangliaXml, err error) {
	log.Print("Reading XML file")
	p := xml.NewDecoder(in)
	p.CharsetReader = charset.NewReader

	gmeta = GangliaXml{}
	err = p.Decode(&gmeta)
	return
}

func printClusterMetrics(riemann raidman.Client, cl *Cluster, ret chan int) {
	ch := make(chan int)
	log.Print("Reading hosts")
	for _, hst := range cl.Host {
		log.Printf("Reading host %s", hst.Name)
		go printHostMetrics(riemann, *cl, hst, ch)
	}
	for _ = range cl.Host {
		<-ch
	}
	ret <- 1
}

func printHostMetrics(riemann raidman.Client, c Cluster, h Host, ret chan int) {
	ch := make(chan int)
	log.Printf("Reading %s metrics", h.Name)
	for _, m := range h.Metric {
		go printMetric(riemann, c, h, m, ch)
	}
	// drain the channel
	for _ = range h.Metric {
		<-ch
	}
	ret <- 1
}

func printMetric(riemann raidman.Client, c Cluster, h Host, m Metric, ret chan int) {
	if m.Type != "string" {
		metricName := strings.Map(graphiteStringMap, m.Name)
		val, err := strconv.ParseFloat(m.Val, 64)
		if err != nil {
			log.Printf("%s\n", err)
			// panic(err)
		}
		attributes := make(map[string]string)
		attributes["Cluster"] = c.Name
		attributes["Location"] = h.Location
		attributes["Owner"] = c.Owner

		tags := []string{"ganglia"}
		var event = &raidman.Event{
			State:      "success",
			Host:       h.Name,
			Service:    metricName,
			Metric:     float64(val),
			Ttl:        30,
			Attributes: attributes,
			Tags:       tags,
			Time:       time.Now().Unix(),
		}

		err = riemann.Send(event)
		if err != nil {
			log.Printf("%s\n", err)
			// panic(err)
		}
	}
	ret <- 1
}

func getMetrics(ganglia_conn io.Reader, riemann raidman.Client) {
	// read xml into memory
	gmeta, err := readXmlFromFile(ganglia_conn)
	if err != nil {
		log.Fatal("xml.unmarshal: ", err)
	}

	c := make(chan int)
	cs := 0

	// dispatch goroutines
	for _, cl := range gmeta.Cluster {
		log.Print("Reading clusters")
		/* log.Printf("Cluster %s: %#v\n", cl.Name, cl)*/
		go printClusterMetrics(riemann, &cl, c)
		cs++
	}

	for _, gr := range gmeta.Grid {
		for _, cl := range gr.Cluster {
			go printClusterMetrics(riemann, &cl, c)
			cs++
		}
	}

	// drain channel
	for cs > 0 {
		<-c
		cs--
	}
}

func getClusterMetrics(clusterCfg ClusterConfig, riemann raidman.Client) {

	host := clusterCfg.Host
	if host == "" {
		return
	}
	port := clusterCfg.Port
	if port == 0 {
		port = 8649
	}
	interval := clusterCfg.Interval
	if interval == 0 {
		interval = 10
	}
	hostAddr := host + ":" + strconv.Itoa(port)
	for {

		ganglia_conn, err := net.Dial("tcp", hostAddr)
		if err != nil {
			log.Fatal("Dial ganglia: ", err)
		}
		defer ganglia_conn.Close()
		log.Printf("Connected to ganglia %s ", hostAddr)

		getMetrics(ganglia_conn, riemann)
		ganglia_conn.Close()
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func main() {
	flag.Parse()
	viper.SetDefault("RiemanHost", "localhost")
	viper.SetDefault("RiemanPort", 5555)
	viper.SetConfigName("ggg")            // name of config file (without extension)
	viper.AddConfigPath("$HOME/projects") // call multiple times to add many search paths
	viper.ReadInConfig()                  // Find and read the config file
	viper.Debug()                         // Find and read the config file

	var conf config

	err := viper.Marshal(&conf)
	if err != nil {
		log.Fatal("unable to decode into struct, %v", err)
	}
	riemann, err := raidman.Dial("tcp", conf.RiemanHost+":"+strconv.Itoa(conf.RiemanPort))
	if err != nil {
		panic(err)
	}
	defer riemann.Close()
	var event = &raidman.Event{
		State:   "success",
		Host:    "localhost",
		Service: "GGG Startup",
		Time:    time.Now().Unix(),
		Ttl:     10,
	}

	err = riemann.Send(event)
	if err != nil {
		panic(err)
	}
	ch := make(chan int)
	for _, clusterCfg := range conf.Clusters {
		log.Printf("Reading host %s:%s every %s", clusterCfg.Host, clusterCfg.Port, clusterCfg.Interval)
		go getClusterMetrics(clusterCfg, *riemann)
	}
	for _ = range conf.Clusters {
		<-ch
	}
}
