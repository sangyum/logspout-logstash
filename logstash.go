package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"

	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams TCP JSON to Logstash.
type LogstashAdapter struct {
	conn  net.Conn
	route *router.Route
}

// NewLogstashAdapter creates a LogstashAdapter with TCP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("tcp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route: route,
		conn:  conn,
	}, nil
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		dockerInfo := DockerInfo{
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
		}
		var js []byte

		var jsonMsg map[string]interface{}
		log.Println(1)
		err := json.Unmarshal([]byte(m.Data), &jsonMsg)
		if err != nil {
			log.Println(2)
			// the message is not in JSON make a new JSON message
			msg := LogstashMessage{
				Message: m.Data,
				Docker:  dockerInfo,
			}
			
			log.Println("3", msg)
			js, err = json.Marshal(msg)
			if err != nil {
				log.Println("logstash:", err)
				continue
			}
		} else {
			log.Println("4")
			// the message is already in JSON just add the docker specific fields as a nested structure
			jsonMsg["docker"] = dockerInfo

			js, err = json.Marshal(jsonMsg)
			log.Println("5")
			if err != nil {
				log.Println("logstash:", err)
				continue
			}
		}
		log.Println("6")
		_, err = a.conn.Write(js)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
	}
}

type DockerInfo struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Image    string `json:"image"`
	Hostname string `json:"hostname"`
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message string     `json:"message"`
	Docker  DockerInfo `json:"docker"`
}
