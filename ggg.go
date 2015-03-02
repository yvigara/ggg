package main

import (
	"code.google.com/p/go-charset/charset"
	_ "code.google.com/p/go-charset/data"
	"encoding/xml"
	"errors"
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
	RiemannHost string
	RiemannPort int
	Timeout     int
	Retries     int
	EventQueue  int
	Clusters    []ClusterConfig
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

func printClusterMetrics(eventsChan chan<- *raidman.Event, cl *Cluster) {
	log.Print("Reading hosts")
	for _, hst := range cl.Host {
		log.Printf("Reading host %s", hst.Name)
		printHostMetrics(eventsChan, cl, &hst)
	}
}

func printHostMetrics(eventsChan chan<- *raidman.Event, c *Cluster, h *Host) {
	log.Printf("Reading %s metrics", h.Name)
	for _, m := range h.Metric {
		printMetric(eventsChan, c, h, &m)
	}
}

func printMetric(eventsChan chan<- *raidman.Event, c *Cluster, h *Host, m *Metric) {
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
		eventsChan <- event
	}
}

func getMetrics(gmeta *GangliaXml, eventsChan chan<- *raidman.Event) {

	// dispatch goroutines
	for _, cl := range gmeta.Cluster {
		log.Print("Reading clusters")
		/* log.Printf("Cluster %s: %#v\n", cl.Name, cl)*/
		printClusterMetrics(eventsChan, &cl)
	}

	for _, gr := range gmeta.Grid {
		for _, cl := range gr.Cluster {
			printClusterMetrics(eventsChan, &cl)
		}
	}

}

func connectGanglia(conf *config, hostAddr string) (net.Conn, error) {
	for i := 0; i <= conf.Retries; i++ {
		ganglia_conn, err := net.Dial("tcp", hostAddr)
		if err == nil {
			log.Println("Connected to: " + hostAddr)
			return ganglia_conn, nil
		}
		log.Println(err)
		time.Sleep(time.Duration(conf.Timeout) * time.Second)
	}
	return nil, errors.New("Can't connect to " + hostAddr)
}

func (clusterCfg ClusterConfig) GetClusterMetrics(conf *config, eventsChan chan<- *raidman.Event) {

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
		interval = 30
	}
	hostAddr := host + ":" + strconv.Itoa(port)
	for {
		ganglia_conn, err := connectGanglia(conf, hostAddr)
		if ganglia_conn == nil {
			log.Println(err)
			return
		}
		// read xml into memory
		gmeta, err := readXmlFromFile(ganglia_conn)
		if err != nil {
			log.Fatal("xml.unmarshal: ", err)
		}

		getMetrics(&gmeta, eventsChan)
		ganglia_conn.Close()
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func connectRiemann(conf *config) (*raidman.Client, error) {
	riemannUrl := conf.RiemannHost + ":" + strconv.Itoa(conf.RiemannPort)
	for i := 0; i <= conf.Retries; i++ {
		riemann, err := raidman.Dial("tcp", riemannUrl)
		if err == nil {
			log.Println("Connected to: " + riemannUrl)
			return riemann, nil
		}
		log.Println(err)
		time.Sleep(time.Duration(conf.Timeout) * time.Second)
	}
	return nil, errors.New("Can't connect to " + riemannUrl)
}

func processEvents(conf *config, eventsChan <-chan *raidman.Event, connErrorChan chan<- error) {
	riemann, err := connectRiemann(conf)
	if err != nil {
		connErrorChan <- err
		return
	}
	for {
		event := <-eventsChan
		err := riemann.Send(event)
		if err != nil {
			riemann.Close()
			riemann, err = connectRiemann(conf)
		}
		if riemann == nil {
			connErrorChan <- err
			return
		}
	}
}

func main() {
	flag.Parse()
	viper.SetDefault("RiemanHost", "localhost")
	viper.SetDefault("RiemanPort", 5555)
	viper.SetDefault("Timeout", 20)
	viper.SetDefault("Retries", 5)
	viper.SetDefault("EventQueue", 100)
	viper.SetConfigName("ggg")            // name of config file (without extension)
	viper.AddConfigPath("$HOME/projects") // call multiple times to add many search paths
	viper.ReadInConfig()                  // Find and read the config file
	viper.Debug()                         // Find and read the config file

	var conf config

	err := viper.Marshal(&conf)
	if err != nil {
		log.Fatal("unable to decode into struct, %v", err)
	}
	var event = &raidman.Event{
		State:   "success",
		Host:    "localhost",
		Service: "GGG Startup",
		Time:    time.Now().Unix(),
		Ttl:     10,
	}

	connErrorChan := make(chan error)
	eventsChan := make(chan *raidman.Event, conf.EventQueue)
	eventsChan <- event
	go processEvents(&conf, eventsChan, connErrorChan)

	for _, clusterCfg := range conf.Clusters {
		log.Printf("Reading host %s:%s every %s", clusterCfg.Host, clusterCfg.Port, clusterCfg.Interval)
		go clusterCfg.GetClusterMetrics(&conf, eventsChan)
	}
	for {
		connError := <-connErrorChan
		log.Println("Fatal Error:")
		log.Println(connError)
		break
	}
}
