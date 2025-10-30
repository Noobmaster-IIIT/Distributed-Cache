package consul

import (
	"fmt"
	"strconv"
	"strings"

	"github.comcom/hashicorp/consul/api"
)

// Client wraps the Consul API client.
type Client struct {
	client *api.Client
}

// NewClient creates a new Consul client.
func NewClient() (*Client, error) {
	config := api.DefaultConfig()
	c, err := api.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &Client{client: c}, nil
}

// Register registers a service with the local Consul agent using a unique ID.
func (c *Client) Register(id, name, port string) error {
	portVal, err := strconv.Atoi(strings.TrimPrefix(port, ":"))
	if err != nil {
		return err
	}

	reg := &api.AgentServiceRegistration{
		ID:      id,   // Use the unique ID provided.
		Name:    name, // Use the logical service name.
		Port:    portVal,
		Address: "127.0.0.1",
	}
	return c.client.Agent().ServiceRegister(reg)
}

// Deregister removes a service from the local Consul agent using its unique ID.
func (c *Client) Deregister(id string) error {
	return c.client.Agent().ServiceDeregister(id)
}

// Discover finds a service in the Consul catalog.
func (c *Client) Discover(serviceName string) (string, error) {
	services, _, err := c.client.Catalog().Service(serviceName, "", nil)
	if err != nil {
		return "", err
	}
	if len(services) == 0 {
		return "", fmt.Errorf("no instances of service '%s' found", serviceName)
	}
	// Return the address of the first healthy instance found.
	srv := services[0]
	return fmt.Sprintf("%s:%d", srv.ServiceAddress, srv.ServicePort), nil
}
