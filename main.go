package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/fgrosse/graphigo"
	"time"

	"github.com/segmentio/kafka-go"
)

type PriceData struct {
	Date     time.Time
	Station  string
	PostCode string
	PDiesel  float64
	PE5      float64
	PE10     float64
}

type Aggregation struct {
	Region  int
	PDiesel float64
	PE5     float64
	PE10    float64
	Hour    time.Time
}

func main() {

	graphiteClient := &graphigo.Client{
		Address: "10.50.15.52:2003",
		Prefix:  "inf19a.dieschokohasen.tanker",
	}
	if err := graphiteClient.Connect(); err != nil {
		panic(err) // do proper error handling
	}
	defer graphiteClient.Close()

	done := make(chan bool)

	for i := 0; i <= 9; i++ {
		go func(partition int) {
			readKafka(partition, graphiteClient)
			done <- true
		}(i)
	}

	for i := 0; i <= 9; i++ {
		<-done
	}
}

func readKafka(partition int, client *graphigo.Client) {
	r := getReader(partition)

	dataSlice := make([]PriceData, 1)
	var currentHour time.Time

	for {
		m, err := r.ReadMessage(context.Background())

		if err != nil {
			break
		}

		data := parseJSON(m.Value)

		if data.Date.Hour() == currentHour.Hour() {

		} else {
			aggregation := aggregateData(dataSlice, currentHour, partition)
			sendSingle(aggregation, client)
			dataSlice = make([]PriceData, 1)
			currentHour = time.Date(data.Date.Year(), data.Date.Month(), data.Date.Day(), data.Date.Hour(), 0, 0, 0, time.UTC)
		}
	}
}

func aggregateData(data []PriceData, hour time.Time, region int) *Aggregation {
	dieselSum := 0.0
	e5Sum := 0.0
	e10Sum := 0.0
	count := float64(len(data))

	return &Aggregation{
		Hour:    hour,
		Region:  region,
		PDiesel: dieselSum / count,
		PE10:    e10Sum / count,
		PE5:     e5Sum / count,
	}
}

func sendSingle(aggregation *Aggregation, client *graphigo.Client) {
	fmt.Printf("send %v\n", *aggregation)
	defer client.Send(graphigo.Metric{Name: fmt.Sprintf("%d.E5", aggregation.Region), Value: aggregation.PE5, Timestamp: aggregation.Hour})
	defer client.Send(graphigo.Metric{Name: fmt.Sprintf("%d.E10", aggregation.Region), Value: aggregation.PE10, Timestamp: aggregation.Hour})
	defer client.Send(graphigo.Metric{Name: fmt.Sprintf("%d.Diesel", aggregation.Region), Value: aggregation.PDiesel, Timestamp: aggregation.Hour})
}

func getReader(partition int) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"10.50.15.52:9092"},
		Topic:     "tankerkoenig",
		Partition: partition,
		MinBytes:  10e3,
		MaxBytes:  10e3,
	})
}

func parseJSON(b []byte) *PriceData {
	var data PriceData
	json.Unmarshal(b, &data)
	return &data
}
