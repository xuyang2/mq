// a trie implementation of searching a handler with matching topic
// much code is inspired from https://github.com/tylertreat/fast-topic-matching

package matcher

import (
	"errors"
	"strings"
	"sync"
)

type trieMatcher struct {
	sync.Mutex
	root   *node
	option Option
}

type node struct {
	word     string
	subs     map[Handler]struct{}
	parent   *node
	children map[string]*node
}

func (n *node) orphan() {
	if n.parent == nil {
		// Root
		return
	}
	delete(n.parent.children, n.word)
	if len(n.parent.subs) == 0 && len(n.parent.children) == 0 {
		n.parent.orphan()
	}
}

// NewTrieMatcher returns a default trie structure for matching
func NewTrieMatcher(opt Option) Matcher {
	return &trieMatcher{
		root: &node{
			subs:     make(map[Handler]struct{}),
			children: make(map[string]*node),
		},
		option: opt,
	}
}

func (t *trieMatcher) Add(topic string, hdl Handler) (*Operation, error) {
	t.Lock()
	curr := t.root
	for _, word := range strings.Split(topic, t.option.Delimiter) {
		child, ok := curr.children[word]
		if !ok {
			child = &node{
				word:     word,
				subs:     make(map[Handler]struct{}),
				parent:   curr,
				children: make(map[string]*node),
			}
			// with wildcast some, the child is children itself
			if word == t.option.WildcardSome {
				child.children[word] = child
			}

			curr.children[word] = child
		}

		curr = child
	}
	curr.subs[hdl] = struct{}{}
	t.Unlock()

	return &Operation{topic: topic, handler: hdl}, nil
}

func (t *trieMatcher) Remove(sub *Operation) error {
	t.Lock()
	curr := t.root
	for _, word := range strings.Split(sub.topic, t.option.Delimiter) {
		child, ok := curr.children[word]
		if !ok {
			// Operation doesn't exist.
			t.Unlock()
			return errors.New("invalid unsubscription")
		}
		curr = child
	}
	delete(curr.subs, sub.handler)
	if len(curr.subs) == 0 && len(curr.children) == 0 {
		curr.orphan()
	}
	t.Unlock()

	return nil
}

// Lookup is a method that finds all subscribers that match a given routing key in the trie.
// It first splits the routing key into parts, then locks the trie to ensure thread safety, and finally calls the lookup function to find the subscribers.
// It returns a slice of Handlers, which are the subscribers that match the routing key.
func (t *trieMatcher) Lookup(topic string) []Handler {
	t.Lock()
	subs := make(map[Handler]struct{})
	t.lookup(strings.Split(topic, t.option.Delimiter), t.root, subs)
	t.Unlock()

	handlers := make([]Handler, len(subs))
	i := 0
	for sub := range subs {
		handlers[i] = sub
		i++
	}
	return handlers
}

// lookup is a recursive function that searches for all subscribers in the trie that match a given routing key.
// It takes three parameters: words are the parts of the routing key after splitting, node is the current node, and subs is the map to store subscribers.
func (t *trieMatcher) lookup(words []string, node *node, subs map[Handler]struct{}) {
	// If words is empty, it means all parts have been processed, so we add the subscribers of the current node to subs.
	if len(words) == 0 {
		for k, v := range node.subs {
			subs[k] = v
		}
		// If there is a child node that matches the wildcard "#", we also add its subscribers to subs.
		if child, ok := node.children[t.option.WildcardSome]; ok && len(child.children) == 1 {
			for k, v := range child.subs {
				subs[k] = v
			}
		}
		return
	}

	// If there is a child node that matches the first part of words, we recursively look up the remaining parts.
	if n, ok := node.children[words[0]]; ok {
		t.lookup(words[1:], n, subs)
	}
	// If the first part is not an empty string and there is a child node that matches the wildcard "*", we also recursively look up the remaining parts.
	if words[0] != empty {
		if n, ok := node.children[t.option.WildcardOne]; ok {
			t.lookup(words[1:], n, subs)
		}
	}

	// If there is a child node that matches the wildcard "#", we recursively look up the remaining parts for all possible matches.
	if n, ok := node.children[t.option.WildcardSome]; ok {
		if nn, ok := n.children[words[0]]; ok {
			t.lookup(words[1:], nn, subs)
		}
		if words[0] != empty {
			if nn, ok := n.children[t.option.WildcardOne]; ok {
				t.lookup(words[1:], nn, subs)
			}
		}
		t.lookup(words[1:], n, subs)
	}
}
