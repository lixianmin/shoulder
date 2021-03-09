package kafkax

import (
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

/********************************************************************
created:    2021-03-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

func CreateTopics(brokerAddress string, topicConfigs ...kafka.TopicConfig) error {
	conn, err := kafka.Dial("tcp", brokerAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return err
	}

	return nil
}
