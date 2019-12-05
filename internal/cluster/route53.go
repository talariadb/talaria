// Copyright 2019 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package cluster

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/route53"
	"github.com/grab/talaria/internal/monitor"
)

const (
	logTag = "route53"
)

// Route53 represents a Route53 DNS updater.
type Route53 struct {
	client  *route53.Route53 // The underlying service client
	monitor monitor.Client
}

// NewRoute53 creates a new route53 updater.
func NewRoute53(region string, monitor monitor.Client) *Route53 {
	sess := session.Must(session.NewSession())
	client := route53.New(sess, &aws.Config{
		Region: aws.String(region),
	})

	return &Route53{
		client:  client,
		monitor: monitor,
	}
}

// Upsert updates a CNAME for a domain name/zone with a target and TTL
func (r *Route53) Upsert(domainName, zoneID string, targets []string, ttl int64) error {
	var records []*route53.ResourceRecord
	for _, target := range targets {
		records = append(records, &route53.ResourceRecord{
			Value: aws.String(target),
		})
	}

	params := &route53.ChangeResourceRecordSetsInput{
		ChangeBatch: &route53.ChangeBatch{ // Required
			Changes: []*route53.Change{ // Required
				{ // Required
					Action: aws.String("UPSERT"), // Required
					ResourceRecordSet: &route53.ResourceRecordSet{ // Required
						Name:            aws.String(domainName), // Required
						Type:            aws.String("A"),        // Required
						ResourceRecords: records,                // Required
						TTL:             aws.Int64(ttl),
						Weight:          aws.Int64(1),
						SetIdentifier:   aws.String("cluster memberlist"),
					},
				},
			},
			Comment: aws.String("automated update"),
		},
		HostedZoneId: aws.String(zoneID), // Required
	}

	_, err := r.client.ChangeResourceRecordSets(params)
	if err != nil {
		r.monitor.Errorf("route53: change resource record failed due to %s, with params %s ", err, params)
	}
	return err
}
