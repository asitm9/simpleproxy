package main

import (
	"fmt"
	"os"
	"time"
	"github.com/spf13/viper"
	"net/http/httputil"
	"net/http"
	"net/url"
	"io/ioutil"
	"bytes"
	"strings"
	"encoding/json"
	"github.com/Shopify/sarama"
)

var (
	proxy  *httputil.ReverseProxy
	JobQueue chan Job
	MaxQue int
	MaxWorker int
)

type Job struct {
	Payload Payload
}

type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	Quit chan bool
}

type Payload struct {
	//Req *http.Request
	//Resp *http.Response
	ReqURI string
	ReqH *map[string]interface{}
	Req []byte
	Resp []byte
	StatusCode int
}

type transport struct {
	http.RoundTripper
}

func (t *transport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	resp, err = t.RoundTripper.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	//fmt.Printf("---> reqUri %s",req.RequestURI)
	reqHMap := make(map[string]interface{})
	for key, value := range req.Header{
		reqHMap[key] = value
	}

	var b2 []byte
	var er1 error
	if req.Body!=nil{
		b2, er1 = ioutil.ReadAll(req.Body)
		if er1!=nil{
			fmt.Printf("%s", er1)
		}
	}
	stcode := resp.StatusCode
	p := Payload{req.RequestURI,&reqHMap,b2,b,stcode}
	//p.WriteToKafka()
	work := Job{Payload:p}
	JobQueue <- work
	body := ioutil.NopCloser(bytes.NewReader(b))
	resp.Body = body
	return resp, nil
}

func Logger(h http.Handler) http.Handler{
	return http.HandlerFunc(func (w http.ResponseWriter, req *http.Request){

		h.ServeHTTP(w,req)
	})
}

func handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("AM9-9i5mJ4cPp", "9i5mJ4cPp")
	proxy.ServeHTTP(w, r)
}

func (p *Payload) WriteToKafka(){
	//b, err := ioutil.ReadAll(p.Req.Body)
	//if err!=nil{
	//	fmt.Printf("err %s", err)
	//}
	totalJsonMap := make(map[string]interface{})

	totalJsonMap["uri"] = p.ReqURI
	totalJsonMap["reqHMap"] = p.ReqH
	totalJsonMap["res"] = string(p.Resp)
	totalJsonMap["req"] = string(p.Req)
	totalJsonMap["statuscode"] = p.StatusCode
	data1, _ := json.Marshal(totalJsonMap)

	brokerList := strings.Split(viper.GetString("brokers"), ",")
	topic := viper.GetString("topic")
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 600 * time.Microsecond
	//fmt.Printf("%s %s %s", data1, brokerList, topic)
	producer, errp := sarama.NewAsyncProducer(brokerList, config)

	if errp != nil {
		fmt.Printf("Please check broker's address ")
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		//Key: sarama.StringEncoder(key),
		Value: sarama.StringEncoder(data1),
	}

	producer.Input() <- message
	fmt.Printf("In kafka")
	fmt.Printf("\n in kafka-- %v", string(p.ReqURI))
	data, _ := json.Marshal(p.ReqH)
	fmt.Printf("req header %s", data)
}

/////////////////////////////////////////////////////////////
func main(){
	fmt.Printf("gana\n")
	if os.Getenv("brokers") == ""{
		panic(fmt.Errorf("Please mention the brokers\n"))
		os.Exit(1)
	}
	viper.Set("brokers",os.Getenv("brokers"))
	if os.Getenv("topic") == ""{
		panic(fmt.Errorf("Please mention the topic\n"))
		os.Exit(1)
	}
	viper.Set("topic",os.Getenv("topic"))
	if os.Getenv("host") == ""{
		panic(fmt.Errorf("Please mention the host\n"))
		os.Exit(1)
	}
	viper.Set("host",os.Getenv("host"))


	proxy = httputil.NewSingleHostReverseProxy(&url.URL{
		Scheme: "http",
		Host:   viper.GetString("host"),
	})
	MaxQue = 6
	MaxWorker = 9
	JobQueue = make(chan Job, MaxQue)
	dispatcher := NewDispatcher(MaxWorker)
	dispatcher.Run()

	proxy.Transport = &transport{http.DefaultTransport}
	handler2 := Logger(http.HandlerFunc(handle))
	http.Handle("/", handler2)
	http.ListenAndServe(":8090", nil)


}

/////////////////////////////////////////////////////////////
func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		Quit:       make(chan bool)}

}

func (w Worker) Start() {
	go func(){
		for{
			w.WorkerPool <- w.JobChannel

			select{
			case job := <-w.JobChannel:
				job.Payload.WriteToKafka()
			case <- w.Quit:
				return
			}
		}
	}()
}

func (w Worker) Stop(){
	go func() {
		w.Quit <- true
	}()
}

type Dispatcher struct {
	WorkerPool chan chan Job
	Maxworker int
}

func NewDispatcher(maxWorkers int) *Dispatcher{
	fmt.Printf("in dispatcher\n")
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{WorkerPool:pool,Maxworker:maxWorkers}
}

func (d *Dispatcher) Run() {
	for i :=0; i< d.Maxworker; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	fmt.Printf("in dispatcher dispatch\n")
	for {
		select {

		case job := <- JobQueue:
			// a job request has been received
			fmt.Printf("a job request has been received\n")
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.WorkerPool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}