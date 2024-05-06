package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/mdns"
	"github.com/hashicorp/serf/serf"
)

func connect(
	stopCh <-chan struct{},
	serviceName string,
	ctrl *controller,
) {
	serviceName = fmt.Sprintf("_%s._tcp", serviceName)
	eventsCh := make(chan serf.Event, 32)

	cluster, leave := prepareCluster(eventsCh)
	silence := advertiseThis(serviceName, int(cluster.LocalMember().Port))
	peers := discoverOthers(serviceName)

	if len(peers) > 0 {
		slog.Info("joining cluster", "peers", peers)
		if _, err := cluster.Join(peers, true); err != nil {
			slog.Error("failed to join cluster", "err", err)
			os.Exit(1)
		}
	}

	go func() {
		defer leave()
		defer silence()

		for {
			select {
			case event := <-eventsCh:
				switch event.EventType() {
				case serf.EventQuery:
					query := event.(*serf.Query)

					if query.SourceNode() == cluster.LocalMember().Name {
						// Skip queries from self
						continue
					}

					slog.Debug("received query", slog.Group("query", "name", query.Name, "payload", query.Payload))
					if res := ctrl.handleQuery(query.Name, query.Payload); res != nil {
						query.Respond(res)
					}
				case serf.EventUser:
					userEvent := event.(serf.UserEvent)
					parts := strings.SplitN(userEvent.Name, ":", 2)

					if parts[0] == cluster.LocalMember().Name {
						// Skip events from self
						continue
					}

					slog.Debug("received event", slog.Group("event", "name", parts[1], "payload", userEvent.Payload))
					ctrl.handleUserEvent(parts[1], userEvent.Payload)
				}
			case <-stopCh:
				return
			}
		}
	}()

	ctrl.sendQuery = func(name string, payload []byte) []byte {
		slog.Debug("querying", slog.Group("query", "name", name, "payload", payload))

		queryRes, err := cluster.Query(name, payload, &serf.QueryParam{Timeout: 250 * time.Millisecond})
		if err != nil {
			slog.Error("failed to send query", "err", err)
			return nil
		}

		var resPayload []byte
		for res := range queryRes.ResponseCh() {
			resPayload = res.Payload
			slog.Debug("received response", slog.Group("query", "name", name, "response", resPayload))
			queryRes.Close()
		}
		return resPayload
	}

	ctrl.sendUserEvent = func(command string, payload []byte) {
		slog.Debug("sending event", slog.Group("event", "name", command, "payload", payload))

		if err := cluster.UserEvent(cluster.LocalMember().Name+":"+command, payload, false); err != nil {
			slog.Error("failed to send event", "err", err)
		}
	}
}

func prepareCluster(eventsCh chan<- serf.Event) (*serf.Serf, func()) {
	logger := slog.NewLogLogger(slog.Default().Handler(), slog.LevelDebug)

	cfg := serf.DefaultConfig()
	cfg.Logger = logger
	cfg.MemberlistConfig.Logger = logger
	cfg.EventCh = eventsCh

	slog.Debug("creating cluster")
	cluster, err := serf.Create(cfg)
	if err != nil {
		slog.Error("failed to create cluster", "err", err)
		os.Exit(1)
	}

	return cluster, func() {
		slog.Debug("leaving cluster")
		if err := cluster.Leave(); err != nil {
			slog.Error("failed to leave cluster", "err", err)
			os.Exit(1)
		}
		close(eventsCh)
	}
}

func advertiseThis(serviceName string, servicePort int) func() {
	hostname, err := os.Hostname()
	if err != nil {
		slog.Error("failed to get hostname", "err", err)
		os.Exit(1)
	}

	slog.Debug("advertising service", "service", serviceName, "port", servicePort, "hostname", hostname)
	svc, err := mdns.NewMDNSService(hostname, serviceName, "", "", servicePort, nil, nil)
	if err != nil {
		slog.Error("failed to create mDNS service", "err", err)
		os.Exit(1)
	}

	advertise, err := mdns.NewServer(&mdns.Config{Zone: svc})
	if err != nil {
		slog.Error("failed to create mDNS server", "err", err)
		os.Exit(1)
	}

	return func() {
		slog.Debug("stopping mDNS server")
		if err := advertise.Shutdown(); err != nil {
			slog.Error("failed to stop mDNS server", "err", err)
			os.Exit(1)
		}
	}
}

func discoverOthers(serviceName string) []string {
	peers := []string{}

	entriesCh := make(chan *mdns.ServiceEntry, 1)
	discoverDone := make(chan struct{})
	slog.Debug("discovering peers", "service", serviceName)
	go func() {
		defer close(discoverDone)
		for entry := range entriesCh {
			peers = append(peers, fmt.Sprintf("%s:%d", entry.AddrV4, entry.Port))
		}
	}()

	params := mdns.DefaultParams(serviceName)
	params.Entries = entriesCh
	params.DisableIPv6 = true

	if err := mdns.Query(params); err != nil {
		slog.Error("failed to discover peers", "err", err)
		os.Exit(1)
	}

	close(entriesCh)
	<-discoverDone

	return peers
}
