package kafkaconn

// zk.go：ZooKeeper broker 发现（connection_source=zookeeper，Phase 2，
// IMPL_PLAN §0.2/§5）。go-zookeeper/zk 读取 /brokers/ids（支持 chroot：
// zk.Connect 的 server 串 host:port/chroot 会自动为所有路径加 chroot 前缀），
// 逐 broker 节点解析 JSON {host, port}。
//
// broker 不可达语义：ZK 连接/读取失败返回业务错（main 层 bizError →
// -32000），不崩溃；为避免 zk.Connect 内建重试拖长等待，先做一次快速
// TCP 预拨（5s），不可达立即失败。

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	zkDialTimeout    = 5 * time.Second
	zkSessionTimeout = 10 * time.Second
	zkBrokersPath    = "/brokers/ids"
)

// zkBrokerAddress 是 zk_servers 单条地址（host:port[/chroot]）。
type zkBrokerAddress struct {
	Host   string
	Port   int
	Chroot string
	Raw    string
}

// parseZKServers 解析 zk_servers 列表（逗号/空白分隔的单地址再拆分；校验
// host:port[/chroot] 形状，port 缺失报错）。
func parseZKServers(raw []string) ([]string, error) {
	expanded := make([]string, 0, len(raw))
	for _, item := range raw {
		for _, part := range strings.FieldsFunc(item, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' }) {
			if part = strings.TrimSpace(part); part != "" {
				expanded = append(expanded, part)
			}
		}
	}
	if len(expanded) == 0 {
		return nil, errf("zkServers is required for zookeeper connection source")
	}
	servers := make([]string, 0, len(expanded))
	for _, item := range expanded {
		addr, err := parseZKServerAddress(item)
		if err != nil {
			return nil, err
		}
		servers = append(servers, addr.Raw)
	}
	return servers, nil
}

// parseZKServerAddress 校验并拆解单条 zk 地址（含 chroot 还原回原样串，
// go-zookeeper 原生支持）。
func parseZKServerAddress(raw string) (zkBrokerAddress, error) {
	item := strings.TrimSpace(raw)
	if item == "" {
		return zkBrokerAddress{}, errf("zkServers contains an empty address")
	}
	hostPort := item
	chroot := ""
	if idx := strings.IndexByte(item, '/'); idx >= 0 {
		hostPort = item[:idx]
		chroot = item[idx:]
		if strings.TrimSpace(strings.Trim(chroot, "/")) == "" {
			return zkBrokerAddress{}, errf("zkServers entry %q has an empty chroot path", item)
		}
	}
	host, portText, err := net.SplitHostPort(hostPort)
	if err != nil {
		return zkBrokerAddress{}, errf("zkServers entry %q must be host:port[/chroot]", item)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 {
		return zkBrokerAddress{}, errf("zkServers entry %q has an invalid port %q", item, portText)
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return zkBrokerAddress{}, errf("zkServers entry %q is missing the host", item)
	}
	return zkBrokerAddress{Host: host, Port: port, Chroot: chroot, Raw: item}, nil
}

// zkBrokerConfig 是 /brokers/ids/<id> 节点的 JSON 载荷（Kafka 3.x 形状；
// listener_security_protocol_map 等字段忽略）。
type zkBrokerConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// parseZKBrokerConfig 解析 broker 节点 JSON。
func parseZKBrokerConfig(data []byte) (host string, port int, err error) {
	var cfg zkBrokerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return "", 0, fmt.Errorf("parse broker config from ZooKeeper: %w", err)
	}
	host = strings.TrimSpace(cfg.Host)
	if host == "" || cfg.Port <= 0 {
		return "", 0, errf("broker config from ZooKeeper is missing host/port")
	}
	return host, cfg.Port, nil
}

// dialZKServer 快速 TCP 预拨（zk.Connect 内建重试会拖长不可达等待）。
// ctx 贯穿（评审 M-7）：取消立即失败，不等 5s 超时。
func dialZKServer(ctx context.Context, addr zkBrokerAddress) error {
	dialer := &net.Dialer{Timeout: zkDialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(addr.Host, strconv.Itoa(addr.Port)))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("zookeeper %s is unreachable: %w", addr.Raw, err)
	}
	_ = conn.Close()
	return nil
}

// discoverBrokersViaZK 经 ZooKeeper 发现 broker 列表（connection_source=
// zookeeper 时替代 Kafka metadata）。失败返回业务错（-32000）。
func (s *Service) discoverBrokersViaZK(ctx context.Context, connectionID string) ([]BrokerInfo, error) {
	entry := s.lookup(connectionID)
	if entry == nil {
		return nil, errConnectionNotFound(connectionID)
	}
	entry.mu.Lock()
	profile := entry.profile
	entry.mu.Unlock()
	return discoverBrokersViaZKProfile(ctx, profile)
}

// discoverBrokersViaZKProfile 是纯实现（profile 注入，单测可直连）。
// ctx 贯穿预拨（评审 M-7）；zk.Connect 本身不支持 ctx，会话建立仍受
// zkDialTimeout/ zkSessionTimeout 上界约束。
func discoverBrokersViaZKProfile(ctx context.Context, profile Profile) ([]BrokerInfo, error) {
	servers, err := parseZKServers(profile.ZKServers)
	if err != nil {
		return nil, err
	}
	addresses := make([]zkBrokerAddress, 0, len(servers))
	for _, server := range servers {
		addr, err := parseZKServerAddress(server)
		if err != nil {
			return nil, err
		}
		addresses = append(addresses, addr)
	}
	// 快速预拨并发化（评审 M-7）：此前串行 N×zkDialTimeout，3 台全不可达
	// 最坏 15s；并发后最坏 5s，取消立即短路。结果按原始顺序取第一个失败，
	// 报错确定性不变。
	dialErrs := make([]error, len(addresses))
	var dialWG sync.WaitGroup
	for i, addr := range addresses {
		dialWG.Add(1)
		go func(i int, addr zkBrokerAddress) {
			defer dialWG.Done()
			dialErrs[i] = dialZKServer(ctx, addr)
		}(i, addr)
	}
	dialWG.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, dialErr := range dialErrs {
		if dialErr != nil {
			return nil, dialErr
		}
	}

	conn, _, err := zk.Connect(servers, zkSessionTimeout, zk.WithDialer(func(network, addr string, timeout time.Duration) (net.Conn, error) {
		dialTimeout := timeout
		if dialTimeout <= 0 || dialTimeout > zkDialTimeout {
			dialTimeout = zkDialTimeout
		}
		return net.DialTimeout("tcp", addr, dialTimeout)
	}))
	if err != nil {
		return nil, fmt.Errorf("connect to ZooKeeper %s: %w", strings.Join(servers, ","), err)
	}
	defer conn.Close()

	children, _, err := conn.Children(zkBrokersPath)
	if err != nil {
		return nil, fmt.Errorf("list %s from ZooKeeper: %w", zkBrokersPath, err)
	}
	sort.Strings(children)

	brokers := make([]BrokerInfo, 0, len(children))
	for _, id := range children {
		data, _, err := conn.Get(zkBrokersPath + "/" + id)
		if err != nil {
			return nil, fmt.Errorf("read %s/%s from ZooKeeper: %w", zkBrokersPath, id, err)
		}
		host, port, err := parseZKBrokerConfig(data)
		if err != nil {
			return nil, fmt.Errorf("broker %s: %w", id, err)
		}
		nodeID, parseErr := strconv.ParseInt(strings.TrimSpace(id), 10, 32)
		if parseErr != nil {
			// 非数字 broker id（评审 L-5）：此前静默为 0，多节点会撞 id。
			return nil, fmt.Errorf("broker node %s: %w", id, parseErr)
		}
		brokers = append(brokers, BrokerInfo{NodeID: int32(nodeID), Host: host, Port: int32(port)})
	}
	if len(brokers) == 0 {
		return nil, errf("zookeeper %s reports no brokers under %s", strings.Join(servers, ","), zkBrokersPath)
	}
	return brokers, nil
}

// zkSeedsForTest 在 connection_source=zookeeper 时经 ZK 解析 Kafka 拨号种子
// （connection/test 探活用；bootstrap 模式直接走 seedBrokers）。
func zkSeedsForTest(ctx context.Context, profile Profile) ([]string, error) {
	brokers, err := discoverBrokersViaZKProfile(ctx, profile)
	if err != nil {
		return nil, err
	}
	seeds := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		seeds = append(seeds, fmt.Sprintf("%s:%d", broker.Host, broker.Port))
	}
	return seeds, nil
}
