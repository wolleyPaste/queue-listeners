package main

import (
	"encoding/json"
	"fmt"
	"queue-listeners/src/common"
	"queue-listeners/src/db"
	"strconv"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/ini.v1"
)

const iniFileName string = "settings.ini"

func loadIni() map[string]string {
	inidata, err := ini.Load(iniFileName)
	if err != nil {
		log.Fatalf("failed to load ini file: %v", err)
	}

	settings := make(map[string]string)

	// Load workers/listeners parameters
	section := inidata.Section("listeners")
	settings["workersNumber"] = section.Key("number").String()

	// Load RabbitMQ parameters
	section = inidata.Section("rabbitmq")
	settings["queue"] = section.Key("queue").String()

	settings["rabbitmqUsername"] = section.Key("username").String()
	settings["rabbitmqPassword"] = section.Key("password").String()

	settings["rabbitmqHost"] = section.Key("host").String()
	settings["rabbitmqPort"] = section.Key("port").String()

	// Load PostgreSQL parameters
	section = inidata.Section("postgreSQL")
	settings["psqlDBName"] = section.Key("db").String()

	settings["psqlUsername"] = section.Key("username").String()
	settings["psqlPassword"] = section.Key("password").String()

	settings["psqlHost"] = section.Key("host").String()
	settings["psqlPort"] = section.Key("port").String()

	return settings
}

// Load ini file
var settings = loadIni()

func listener(conn *amqp.Connection, listenerId int, wg *sync.WaitGroup) {
	defer wg.Done()

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}
	defer channel.Close()

	// Consume messages
	msgs, err := channel.Consume(
		settings["queue"], // queue
		"",                // consumer
		true,              // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	if err != nil {
		log.Fatalf("failed to register a consumer: %v", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var data map[string]string
			err := json.Unmarshal(d.Body, &data)
			if err != nil {
				log.Errorf("error decoding JSON: %v", err)
				continue
			}
			log.Infof("received a message, length: %d B", len(d.Body))

			// Extract the message data into a Paste struct to be inserted to db
			paste := common.Paste{
				ID: data["id"],

				Text: data["text"],

				CreationDate: time.Now(),
			}

			if title, ok := data["title"]; ok {
				paste.Title = title
			}

			if pw, ok := data["password"]; ok {
				paste.Password = pw
			}

			if expStr, ok := data["expiration"]; ok {
				if expTime, err := time.Parse(time.RFC3339, expStr); err == nil {
					paste.ExpirationDate = expTime
				} else {
					log.Errorf("error parsing expiration date: %v", err)
					continue
				}
			}

			err = db.InsertPaste(paste)
			if err != nil {
				log.Errorf("error inserting paste into db: %v", err)
			}
		}
	}()

	log.Infof("listener %d is up and listening for messages", listenerId)
	<-forever
}

func main() {
	// Connect to RabbitMQ
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/",
		settings["rabbitmqUsername"], settings["rabbitmqPassword"], settings["rabbitmqHost"], settings["rabbitmqPort"]))
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Connect to PostgreSQL
	postgresql := common.PostgreSQL{
		DBName: settings["psqlDBName"],

		Host: settings["psqlHost"],
		Port: settings["psqlPort"],

		Username: settings["psqlUsername"],
		Password: settings["psqlPassword"],
	}

	err = db.InitPostgres(postgresql)
	if err != nil {
		log.Fatalf("failed to connect to PostgreSQL: %v", err)
	}

	// Setup workers/listeners
	workersNumber, err := strconv.Atoi(settings["workersNumber"])
	if err != nil {
		log.Fatalf("failed to load workers number: %v", err)
	}

	// Setup a wait group
	var wg sync.WaitGroup
	wg.Add(workersNumber)

	for i := range workersNumber {
		go listener(conn, i+1, &wg)
	}

	wg.Wait()
}
