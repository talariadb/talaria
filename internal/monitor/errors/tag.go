// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package errors

import (
	"fmt"
)

// Tag describe the minimum behaviour a tag should have - i.e. a key value pair.
type Tag interface {
	Key() string
	Value() interface{}
}

// category represents a category contract
type category interface {
	Stat() string
}

// ------------------------------------------------------------------------------------------

// WithTag this generates a custom tag which is a simple key value pair.
func WithTag(key string, value interface{}) Tag {
	return &tag{key: key, value: value}
}

// tag represents a tag implementation
type tag struct {
	key   string
	value interface{}
}

// Key the key of the tag.
func (c *tag) Key() string {
	return c.key
}

// Value the value associated with this tag.
func (c *tag) Value() interface{} {
	return c.value
}

// ------------------------------------------------------------------------------------------

// WithCategory represents a category to which will be transformed into a datadog stats tag.
func WithCategory(key string, value interface{}) Tag {
	return &cat{key: key, value: value}
}

// cat represents a category tag
type cat tag

// Key the key of the tag.
func (c *cat) Key() string {
	return c.key
}

// Value the value associated with this tag.
func (c *cat) Value() interface{} {
	return c.value
}

// Stat returns the tag for datadog
func (c *cat) Stat() string {
	return fmt.Sprintf("%s:%v", c.key, c.value)
}
