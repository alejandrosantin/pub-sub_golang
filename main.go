package main

import (
	"fmt"
	"time"
)

type Topic struct {
	message string
	id      int
}

type Publisher struct {
	name string
}

type Broker struct {
	TopicBuffer chan Topic            //incoming buffer
	Subscribers map[int][]*Subscriber //mapping topics & subscriber
}

type Subscriber struct {
	name   string
	topic  Topic      //topic to which it subscribes
	buffer chan Topic // subscriber buffer
}

func (pub *Publisher) Publish(topic Topic, queue *Broker) bool {
	fmt.Println("Publishing Topic: ", topic.message, "...")
	queue.TopicBuffer <- topic
	fmt.Println("Published topic ", topic.message, "to message queue")
	return true
}

func (pub *Publisher) SignalStop(queue *Broker) bool {
	return queue.SignalStop()
}

func (sub *Subscriber) Subscribe(queue *Broker) bool {
	fmt.Println("Subscriber: ", sub.name, "subscribing to Topic: ", sub.topic.message, "...")
	(*queue).Subscribers[sub.topic.id] = append((*queue).Subscribers[sub.topic.id], sub)
	fmt.Println("Subscriber ", sub.name, "subscribed to Topic ", sub.topic.message)
	return true
}

func (sub *Subscriber) ConsumeBuffer() bool {
	for topic := range (*sub).buffer {
		fmt.Println("Consumed ", topic.message, "from Subscriber: ", sub.name)
	}
	fmt.Println("Subscriber ", sub.name, "closed")
	return true
}

func (sub *Broker) NotifySubscriber() bool {
	for topic := range sub.TopicBuffer {
		subscribers := sub.Subscribers[topic.id]
		for _, s := range subscribers {
			s.buffer <- topic
		}
	}
	return true
}

func (sub *Broker) SignalStop() bool {
	for _, v := range sub.Subscribers {
		for _, i := range v {
			close(i.buffer)
		}
	}
	return true
}

func main() {
	topics := []Topic{
		{
			message: "bannerUpdated",
			id: 1,
		},
		{
			message: "bannerCreated",
			id: 2,
		},
	}
	broker := Broker{
		TopicBuffer: make(chan Topic, 3), //buffer for messages
		Subscribers: make(map[int][]*Subscriber),
	}
	publisher := Publisher{
		name: "banner-ABM",
	}
	subscriber1 := Subscriber{
		name: "banner-service",
		buffer: make(chan Topic, 2),
		topic: topics[0],
	}
	subscriber2 := Subscriber{
		name: "subscriber 2",
		buffer: make(chan Topic, 2),
		topic: topics[1],
	}
	//Start subscribers consuming its buffer async
	go subscriber1.ConsumeBuffer()
	go subscriber2.ConsumeBuffer()

	//Notify the subscriber to start consuming async
	go broker.NotifySubscriber()

	//Subscribe with the broker for respective topics
	subscriber1.Subscribe(&broker)
	subscriber2.Subscribe(&broker)

	//start publishing messages to broker
	for i := range topics {
		publisher.Publish(topics[i], &broker)
	}

	//signal the system to stop
	<-time.After(1 * time.Second)
	publisher.SignalStop(&broker)
	//<-time.After(1 * time.Second)
	for {}
}