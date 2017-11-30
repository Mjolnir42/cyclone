/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package erebos

import (
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/wvanbergen/kafka/consumergroup"
	kazoo "github.com/wvanbergen/kazoo-go"
)

// Consumer is a Kafka Consumergroup consumer that sends received
// messages to a Dispatcher
func Consumer(conf *Config, dispatch Dispatcher,
	shutdown, exit chan struct{}, death chan error) {

	kfkConf := consumergroup.NewConfig()
	switch conf.Kafka.ConsumerOffsetStrategy {
	case `Oldest`, `oldest`:
		kfkConf.Offsets.Initial = sarama.OffsetOldest
	case `Newest`, `newest`:
		kfkConf.Offsets.Initial = sarama.OffsetNewest
	default:
		kfkConf.Offsets.Initial = sarama.OffsetNewest
	}
	kfkConf.Offsets.ProcessingTimeout = 10 * time.Second
	kfkConf.Offsets.CommitInterval = time.Duration(
		conf.Zookeeper.CommitInterval,
	) * time.Millisecond
	kfkConf.Offsets.ResetOffsets = conf.Zookeeper.ResetOffset

	var zkNodes []string
	zkNodes, kfkConf.Zookeeper.Chroot = kazoo.ParseConnectionString(
		conf.Zookeeper.Connect,
	)

	consumerTopic := strings.Split(conf.Kafka.ConsumerTopics, `,`)
	consumer, err := consumergroup.JoinConsumerGroup(
		conf.Kafka.ConsumerGroup,
		consumerTopic,
		zkNodes,
		kfkConf,
	)
	if err != nil {
		death <- err
		return
	}

	offsets := make(map[string]map[int32]int64)
	commitNotification := make(chan *Commit, 512)
	done := make(chan struct{})
	go DelayedCommit(consumer, commitNotification, shutdown, done)

runloop:
	for {
		select {
		case err := <-consumer.Errors():
			death <- err
			break runloop
		case <-shutdown:
			break runloop
		case msg := <-consumer.Messages():
			if offsets[msg.Topic] == nil {
				offsets[msg.Topic] = make(map[int32]int64)
			}

			if offsets[msg.Topic][msg.Partition] != 0 &&
				offsets[msg.Topic][msg.Partition] != msg.Offset-1 {
				// incorrect offset
				logrus.Warnf("Unexpected offset on %s:%d. "+
					"Expected %d, found %d.\n",
					msg.Topic,
					msg.Partition,
					offsets[msg.Topic][msg.Partition]+1,
					msg.Offset,
				)
			}
			dispatch(Transport{
				Value:     msg.Value,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Commit:    commitNotification,
			})

			offsets[msg.Topic][msg.Partition] = msg.Offset
		}
	}
	// wait for OffsetCommit to stop before closing consumergroup
	<-done
	// not sending the error into the death channel since shutdown
	// is already initiated
	if err := consumer.Close(); err != nil {
		logrus.Errorf("Error closing the consumer: %s", err.Error())
	}
	// in the error case, wait for the shutdown
	<-shutdown
	close(exit)
}

// DelayedCommit handles submitting processed offsets to the consumergroup.
// Offsets are not committed as they are read, but processing stages are
// expected to signal DelayedCommit via notify which offsets have been
// successfully processed.
func DelayedCommit(cg *consumergroup.ConsumerGroup, notify chan *Commit, shutdown, done chan struct{}) {
	offsets := make(map[string]map[int32]int64)
	uncommitted := make(map[string]map[int32][]*Commit)

notifyloop:
	for {
		select {
		case n := <-notify:
			if offsets[n.Topic] == nil {
				offsets[n.Topic] = make(map[int32]int64)
				uncommitted[n.Topic] = make(map[int32][]*Commit)
			}
			// first offset
			if offsets[n.Topic][n.Partition] == 0 {
				cg.CommitUpto(&sarama.ConsumerMessage{
					Topic:     n.Topic,
					Partition: n.Partition,
					Offset:    n.Offset,
				})
				// processing done, check for next notification
				continue notifyloop
			}
			// offset is in order
			if offsets[n.Topic][n.Partition] == n.Offset-1 {
				cg.CommitUpto(&sarama.ConsumerMessage{
					Topic:     n.Topic,
					Partition: n.Partition,
					Offset:    n.Offset,
				})
				if uncommitted[n.Topic][n.Partition] == nil {
					uncommitted[n.Topic][n.Partition] = []*Commit{}
				}
			uncommittedloop:
				// doubled loop so the inner loop can be restarted
				for {
					for i := range uncommitted[n.Topic][n.Partition] {
						if offsets[n.Topic][n.Partition] == uncommitted[n.Topic][n.Partition][i].Offset-1 {
							// found a notification that is now in order and
							// can be committed
							cg.CommitUpto(&sarama.ConsumerMessage{
								Topic:     uncommitted[n.Topic][n.Partition][i].Topic,
								Partition: uncommitted[n.Topic][n.Partition][i].Partition,
								Offset:    uncommitted[n.Topic][n.Partition][i].Offset,
							})
							// remove committed notification from array
							uncommitted[n.Topic][n.Partition] = append(uncommitted[n.Topic][n.Partition][:i],
								uncommitted[n.Topic][n.Partition][i+1:]...)
							// restart scanning for uncommitted
							// notifications that should now be
							// committed
							continue uncommittedloop
						}
					}
					break uncommittedloop
				}
				// processing done, check for next notification
				continue notifyloop
			}
			// offset is _NOT_ in order, store it for later
			if uncommitted[n.Topic][n.Partition] == nil {
				uncommitted[n.Topic][n.Partition] = []*Commit{}
			}
			uncommitted[n.Topic][n.Partition] = append(uncommitted[n.Topic][n.Partition], n)
		case <-shutdown:
			break notifyloop
		}
	}
	// signal that DelayedCommit has shutdown
	close(done)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
