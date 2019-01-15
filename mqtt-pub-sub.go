package main

import (
	"fmt"
	"log"
	"net/url"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	mqttMethod           = "mqtt://"
	RABBITMQ_USERNAME    = "xxxxx"
	RABBITMQ_PASSWORD    = "xxxxx=="
	RABBITMQ_URN         = "xxxxx.io"
	RABBITMQ_PORT        = "1883"
	RABBITMQ_TOPIC       = "hello"
	QoS0                 = 0
	QoS1                 = 1
	QoS2                 = 2
	RetainedMessageFalse = false
	RetainedMessageTrue  = true
	PublisherClientID    = "pub"
	SubscriberClientID   = "sub"
	True                 = true
	False                = false
)

// This is the sample ebnf for mqtt url: mqtt://<user>:<pass>@<server>.cloudmqtt.com:<port>/<topic>
var MQTT_URI = mqttMethod + RABBITMQ_USERNAME + ":" + RABBITMQ_PASSWORD + "@" + RABBITMQ_URN + ":" + RABBITMQ_PORT + "/" + RABBITMQ_TOPIC

func connect(clientId string, uri *url.URL) mqtt.Client {
	opts := createClientOptions(clientId, uri)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(3 * time.Second) {
	}
	if err := token.Error(); err != nil {
		log.Fatal(err)
	}
	return client
}

// client options
func createClientOptions(clientId string, uri *url.URL) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("ssl://%s", uri.Host))
	opts.SetUsername(uri.User.Username())
	opts.SetAutoReconnect(True)
	opts.SetCleanSession(False)
	opts.SetKeepAlive(time.Second * 300)
	password, _ := uri.User.Password()
	opts.SetPassword(password)
	opts.SetClientID(clientId)
	return opts
}

func listen(uri *url.URL, topic string) {
	client := connect(SubscriberClientID, uri)
	client.Subscribe(topic, QoS0, func(client mqtt.Client, msg mqtt.Message) {
		log.Printf("<--Subscribed Message: [%s] %s\n", msg.Topic(), string(msg.Payload()))
	})
}

func main() {
	// mqtt.DEBUG = log.New(os.Stdout, "", 0)
	uri, err := url.Parse(MQTT_URI)
	log.Printf("URI: %v", uri)
	if err != nil {
		log.Fatal(err)
	}
	topic := uri.Path[1:len(uri.Path)]
	log.Printf("Topic: %v", topic)

	go listen(uri, topic)

	client := connect(PublisherClientID, uri)
	timer := time.NewTicker(1 * time.Second)
	for t := range timer.C {
		client.Publish(topic, QoS0, RetainedMessageFalse, t.String())
		log.Printf("-->Published Message: %v", t.String())
		log.Printf("%v\n", client.IsConnectionOpen())
		log.Println("-----------------------------------")
	}
}
