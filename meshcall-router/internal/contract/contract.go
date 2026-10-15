package contract

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// Document is a portable contract: one or more services.
type Document struct {
	Protocol string
	Services []*Service
	// Raw is the decoded document, for tooling that forwards it unchanged.
	Raw *Object
}

type Balance struct {
	Kind string
	Key  *string
}

type Annotations struct {
	Idempotent  bool
	ReadOnly    bool
	Destructive bool
}

// TypeRef names a payload type and carries its JSON Schema.
type TypeRef struct {
	Module   string
	Qualname string
	Schema   *Object
}

type Method struct {
	Name        string
	Stream      string // unary | server_stream | client_stream | duplex
	Request     *TypeRef
	Parameters  []string
	Response    *TypeRef
	InputItem   *TypeRef
	OutputItem  *TypeRef
	Balance     Balance
	Binding     string
	Description *string
	Annotations Annotations
	Deprecated  *string
	// DuplexReason says why a duplex method needs the shape; duplex only.
	DuplexReason *string
	// Subscription marks a server stream as a subscription; nil otherwise.
	Subscription *Subscription
	Raw          *Object
}

// Subscription is how a subscription resumes: the item property holding each
// item's position and the request parameter that continues after it.
type Subscription struct {
	Cursor *string
	Resume *string
}

type Service struct {
	Name           string
	Version        string
	SourceModule   string
	SourceQualname string
	Balance        Balance
	Description    *string
	Targeted       bool
	Methods        []*Method
	Raw            *Object
}

// Load reads a contract document from disk.
func Load(path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// Parse decodes a contract document.
func Parse(data []byte) (*Document, error) {
	value, err := DecodeOrdered(data)
	if err != nil {
		return nil, fmt.Errorf("contract is not valid JSON: %w", err)
	}
	root, ok := value.(*Object)
	if !ok {
		return nil, errors.New("contract must be a JSON object")
	}
	doc := &Document{Protocol: root.Str("protocol"), Raw: root}
	if doc.Protocol == "" {
		doc.Protocol = "meshcall/1"
	}
	services, _ := root.Values["services"].([]any)
	for i, item := range services {
		obj, ok := item.(*Object)
		if !ok {
			return nil, fmt.Errorf("services[%d] must be an object", i)
		}
		service, err := parseService(obj)
		if err != nil {
			return nil, err
		}
		doc.Services = append(doc.Services, service)
	}
	return doc, nil
}

// Select returns the named service, or the only one when name is empty.
func (d *Document) Select(name string) (*Service, error) {
	if name == "" {
		if len(d.Services) != 1 {
			return nil, errors.New("contract contains multiple services; pass --service")
		}
		return d.Services[0], nil
	}
	for _, s := range d.Services {
		if s.Name == name {
			return s, nil
		}
	}
	return nil, fmt.Errorf("contract does not contain service %q", name)
}

func parseService(obj *Object) (*Service, error) {
	s := &Service{
		Name:           obj.Str("name"),
		Version:        obj.Str("version"),
		SourceModule:   obj.Str("source_module"),
		SourceQualname: obj.Str("source_qualname"),
		Balance:        parseBalance(obj.Obj("balance")),
		Description:    optionalString(obj, "description"),
		Raw:            obj,
	}
	if s.Name == "" || s.SourceQualname == "" {
		return nil, errors.New("service needs name and source_qualname")
	}
	if s.Version == "" {
		s.Version = "0"
	}
	if t, _ := obj.Values["targeted"].(bool); t {
		s.Targeted = true
	}
	methods, _ := obj.Values["methods"].([]any)
	for _, item := range methods {
		m, ok := item.(*Object)
		if !ok {
			return nil, fmt.Errorf("service %s: methods must be objects", s.Name)
		}
		method, err := parseMethod(m)
		if err != nil {
			return nil, fmt.Errorf("service %s: %w", s.Name, err)
		}
		s.Methods = append(s.Methods, method)
	}
	return s, nil
}

func parseMethod(obj *Object) (*Method, error) {
	m := &Method{
		Name:         obj.Str("name"),
		Stream:       obj.Str("stream"),
		Parameters:   obj.Strings("parameters"),
		Balance:      parseBalance(obj.Obj("balance")),
		Binding:      obj.Str("binding"),
		Description:  optionalString(obj, "description"),
		Deprecated:   optionalString(obj, "deprecated"),
		DuplexReason: optionalString(obj, "duplex_reason"),
		Raw:          obj,
	}
	if m.Name == "" {
		return nil, errors.New("method needs a name")
	}
	switch m.Stream {
	case "unary", "server_stream", "client_stream", "duplex":
	default:
		return nil, fmt.Errorf("method %s has unknown stream kind %q", m.Name, m.Stream)
	}
	if m.Binding == "" {
		m.Binding = "instance"
	}
	if m.Parameters == nil {
		m.Parameters = []string{}
	}
	var err error
	if m.Request, err = parseTypeRef(obj.Obj("request")); err != nil || m.Request == nil {
		return nil, fmt.Errorf("method %s: request type is required", m.Name)
	}
	if m.Response, err = parseTypeRef(obj.Obj("response")); err != nil {
		return nil, fmt.Errorf("method %s: %w", m.Name, err)
	}
	if m.InputItem, err = parseTypeRef(obj.Obj("input_item")); err != nil {
		return nil, fmt.Errorf("method %s: %w", m.Name, err)
	}
	if m.OutputItem, err = parseTypeRef(obj.Obj("output_item")); err != nil {
		return nil, fmt.Errorf("method %s: %w", m.Name, err)
	}
	if a := obj.Obj("annotations"); a != nil {
		m.Annotations.Idempotent, _ = a.Values["idempotent"].(bool)
		m.Annotations.ReadOnly, _ = a.Values["read_only"].(bool)
		m.Annotations.Destructive, _ = a.Values["destructive"].(bool)
	}
	if err := checkDuplex(m); err != nil {
		return nil, err
	}
	if s := obj.Obj("subscription"); s != nil {
		m.Subscription = &Subscription{Cursor: optionalString(s, "cursor"), Resume: optionalString(s, "resume")}
		if m.Stream != "server_stream" {
			return nil, fmt.Errorf("method %s: only a server stream can be a subscription", m.Name)
		}
		if (m.Subscription.Cursor == nil) != (m.Subscription.Resume == nil) {
			return nil, fmt.Errorf("method %s: a subscription has cursor and resume together, or neither", m.Name)
		}
	}
	return m, nil
}

// checkDuplex applies the duplex restrictions (docs/protocol.md → Duplex
// calls) so no generator emits a client for a method that breaks them.
func checkDuplex(m *Method) error {
	if m.Stream != "duplex" {
		if m.DuplexReason != nil {
			return fmt.Errorf("method %s: duplex_reason on a %s method", m.Name, m.Stream)
		}
		return nil
	}
	switch {
	case m.DuplexReason == nil || strings.TrimSpace(*m.DuplexReason) == "":
		return fmt.Errorf("method %s: a duplex method needs duplex_reason", m.Name)
	case m.Response != nil:
		return fmt.Errorf("method %s: a duplex method has no response", m.Name)
	case m.InputItem == nil || m.OutputItem == nil:
		return fmt.Errorf("method %s: a duplex method needs input_item and output_item", m.Name)
	case m.Annotations.Idempotent || m.Annotations.ReadOnly:
		return fmt.Errorf("method %s: a duplex method cannot be idempotent or read_only", m.Name)
	}
	return nil
}

func parseTypeRef(obj *Object) (*TypeRef, error) {
	if obj == nil {
		return nil, nil
	}
	ref := &TypeRef{Module: obj.Str("module"), Qualname: obj.Str("qualname"), Schema: obj.Obj("schema")}
	if ref.Qualname == "" || ref.Schema == nil {
		return nil, errors.New("type reference needs qualname and schema")
	}
	return ref, nil
}

func parseBalance(obj *Object) Balance {
	if obj == nil {
		return Balance{Kind: "round_robin"}
	}
	b := Balance{Kind: obj.Str("kind")}
	if b.Kind == "" {
		b.Kind = "round_robin"
	}
	if key, ok := obj.Values["key"].(string); ok {
		b.Key = &key
	}
	return b
}

func optionalString(obj *Object, key string) *string {
	if s, ok := obj.Values[key].(string); ok {
		return &s
	}
	return nil
}
