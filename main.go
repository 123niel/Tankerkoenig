package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/fgrosse/graphigo"
	"log"
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
		go func() {
			priceData := readPriceDataFromPartition(i)
			aggregations := aggregate(priceData)
			sendData(aggregations, graphiteClient)
			done <- true
		}()
	}

	for i := 0; i <= 9; i++ {
		<-done
	}
}

func readPriceDataFromPartition(partition int) []PriceData {
	conn := openConnection(partition)

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(1e3, 1e6)

	b := make([]byte, 10e3) // 10KB max per message

	var dataSlice []PriceData

	for {
		n, err := batch.Read(b)
		if err != nil {
			break
		}
		var data PriceData
		err = json.Unmarshal(b[:n], &data)
		dataSlice = append(dataSlice, data)

		if err != nil {
			fmt.Println(err)
			break
		}
	}

	if err := batch.Close(); err != nil {
		log.Fatal("failed to close batch:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}

	return dataSlice
}

func aggregate(data []PriceData) []Aggregation {
	fmt.Sprintf("Aggregating for region %d", data[0].PostCode)
	lastDate := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	pE5Sum := float64(0)
	PE10Sum := float64(0)
	pDieselSum := float64(0)
	count := float64(0)
	var aggregations []Aggregation

	for _, element := range data {
		if element.Date.Hour() == lastDate.Hour() {
			pE5Sum += element.PE5
			PE10Sum += element.PE10
			pDieselSum += element.PDiesel
			count++
		} else {
			tempDate := time.Date(lastDate.Year(), lastDate.Month(), lastDate.Day(), lastDate.Hour(), 0, 0, 0, time.UTC)
			aggregation := Aggregation{
				PE5:     pE5Sum / count,
				PE10:    PE10Sum / count,
				PDiesel: pDieselSum / count,
				Hour:    tempDate,
			}
			aggregations = append(aggregations, aggregation)
			pE5Sum = element.PE5
			PE10Sum = element.PE10
			pDieselSum = element.PDiesel
			count = 1
			lastDate = element.Date
		}
	}

	return aggregations[1:]
}

func sendData(aggregations []Aggregation, client *graphigo.Client) {
	fmt.Sprintf("Sending for region %d", aggregations[0].Region)
	region := aggregations[0].Region
	counter := 0
	for _, element := range aggregations {
		metric := graphigo.Metric{Name: fmt.Sprintf("%d.E5", element.Region), Value: element.PE5, Timestamp: element.Hour}
		defer client.Send(metric)
		defer client.Send(graphigo.Metric{Name: fmt.Sprintf("%d.E10", element.Region), Value: element.PE10, Timestamp: element.Hour})
		defer client.Send(graphigo.Metric{Name: fmt.Sprintf("%d.Diesel", element.Region), Value: element.PDiesel, Timestamp: element.Hour})
		fmt.Sprintf("Sent %d entries for region %d", counter, region)
	}
}

func openConnection(partition int) *kafka.Conn {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "10.50.15.52:9092", "tankerkoenig", partition)
	if err != nil {
		log.Fatal("faied to dial leader:", err)
	}

	return conn
}
