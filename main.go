package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	var help, pub, sub, emulator, copy bool
	var projectId, topic, subscription, outputFolder, inputFolder string
	flag.BoolVar(&help, "h", false, "display usage text")
	flag.BoolVar(&copy, "copy", true, "only valid for while using -sub, it will create another subscription on the same topic instead of using the existing one, enabled by default")
	flag.BoolVar(&pub, "pub", false, "use this parameter if you want to publish messages from a folder")
	flag.BoolVar(&sub, "sub", false, "use this parameter if you want to subscribe to a topic")
	flag.BoolVar(&emulator, "emulator", false, "if the client should use pubsub emulator")
	flag.StringVar(&projectId, "projectId", "test", "project name/id")
	flag.StringVar(&topic, "topic", "test", "topic name/id")
	flag.StringVar(&subscription, "subscription", "test", "subscription name/id")
	flag.StringVar(&inputFolder, "inputFolder", "pubsub_input", "input folder where the files will be read from")
	flag.StringVar(&outputFolder, "outputFolder", "pubsub_output", "output folder where the files will be written to")

	flag.Parse()

	if help {
		flag.PrintDefaults()
		return
	}

	if emulator {
		// set the pubsub emulator host (this is the only way that the library will see it)
		os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
	}

	if pub {
		publishing(mustCreatePubsubClient(projectId), topic, inputFolder)
	} else if sub {
		subscribing(mustCreatePubsubClient(projectId), copy, topic, subscription, outputFolder)
	} else {
		println("nothing to do, need either -pub or -sub")
	}
}

func mustCreatePubsubClient(projectId string) *pubsub.Client {
	pubsubClient, err := pubsub.NewClient(context.Background(), projectId)

	if err != nil {
		log.Fatal(err)
	}

	return pubsubClient
}

func publishing(pubsubClient *pubsub.Client, topicId, folder string) {
	ctx := context.Background()
	topic := mustGetTopic(ctx, pubsubClient, topicId)

	defer pubsubClient.Close()

	var message PubSubMessage

	err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)

		if err != nil {
			return err
		}

		err = json.NewDecoder(file).Decode(&message)

		if err != nil {
			return err
		}

		result := topic.Publish(ctx, &pubsub.Message{
			Data:       []byte(message.Payload),
			Attributes: message.Headers,
		})

		<-result.Ready()

		serverID, err := result.Get(ctx)

		if err != nil {
			return nil
		}

		log.Printf("published with success: %s\n", serverID)

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func mustGetTopic(ctx context.Context, pubsubClient *pubsub.Client, topicId string) *pubsub.Topic {
	topic, err := pubsubClient.CreateTopic(ctx, topicId)

	if err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatal(err)
	} else {
		topic = pubsubClient.Topic(topicId)
	}

	return topic
}

func subscribing(pubsubClient *pubsub.Client, copy bool, topicId, subscriptionId, outputFolder string) {
	ctx := context.Background()

	topic := mustGetTopic(ctx, pubsubClient, topicId)
	outputFolder = fmt.Sprintf("%s/%s/%s", outputFolder, topicId, subscriptionId)

	if copy {
		subscriptionId = fmt.Sprintf("%s-clone", subscriptionId)
		log.Println("copy is enabled, therefore a new subscription will be created")
	} else {
		log.Println("WARN copy is NOT enabled, therefore this will this consumer will might consume messages you wanted to be processed")
	}

	sub, err := pubsubClient.CreateSubscription(ctx, subscriptionId, pubsub.SubscriptionConfig{
		Topic:            topic,
		ExpirationPolicy: time.Duration(24) * time.Hour,
	})

	if err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatal(err)
	} else {
		sub = pubsubClient.Subscription(subscriptionId)
	}

	err = os.MkdirAll(outputFolder, 0777)

	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	err = sub.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
		defer message.Ack()
		log.Printf("message received: %s\n", message.ID)
		var msg PubSubMessage
		msg.Id = message.ID
		msg.Headers = message.Attributes
		msg.Payload = string(message.Data)

		buf := bytes.Buffer{}
		enc := json.NewEncoder(&buf)
		enc.SetIndent("", "    ")
		err := enc.Encode(&msg)

		if err != nil {
			log.Fatal(err)
		}

		err = ioutil.WriteFile(fmt.Sprintf("%s/%s.json", outputFolder, msg.Id), buf.Bytes(), 0777)

		if err != nil {
			log.Fatal(err)
		}
	})

	if err != nil {
		log.Fatal(err)
	}

	// workers := 5
	// wg := sync.WaitGroup{}
	// for i := 0; i < workers; i++ {
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		err = sub.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
	// 			log.Printf("message received: %s\n", message.ID)
	// 			var msg PubSubMessage
	// 			msg.Id = message.ID
	// 			msg.Headers = message.Attributes
	// 			msg.Payload = string(message.Data)

	// 			buf := bytes.Buffer{}
	// 			enc := json.NewEncoder(&buf)
	// 			enc.SetIndent("", "    ")
	// 			err := enc.Encode(&msg)

	// 			if err != nil {
	// 				log.Fatal(err)
	// 			}

	// 			err = ioutil.WriteFile(fmt.Sprintf("%s/%s.json", outputFolder, msg.Id), buf.Bytes(), 0777)

	// 			if err != nil {
	// 				log.Fatal(err)
	// 			}
	// 		})

	// 		if err != nil {
	// 			log.Fatal(err)
	// 		}
	// 	}()
	// }
}

type PubSubMessage struct {
	Id      string            `json:"id"`
	Headers map[string]string `json:"headers"`
	Payload string            `json:"payload"`
}
