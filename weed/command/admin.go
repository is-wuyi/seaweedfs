package command

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
	"github.com/spf13/viper"

	"github.com/seaweedfs/seaweedfs/weed/admin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/handlers"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc" // Register filer_etc credential store
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

var (
	a AdminOptions
)

type AdminOptions struct {
	port             *int
	grpcPort         *int
	master           *string
	masters          *string // deprecated, for backward compatibility
	filerGroup       *string
	adminUser        *string
	adminPassword    *string
	readOnlyUser     *string
	readOnlyPassword *string
	dataDir          *string
	icebergPort      *int
	urlPrefix        *string
	metricsHttpPort  *int
	metricsHttpIp    *string
	debug            *bool
	debugPort        *int
	cpuProfile       *string
	memProfile       *string
}

func init() {
	cmdAdmin.Run = runAdmin // break init cycle
	a.port = cmdAdmin.Flag.Int("port", 23646, "admin server port")
	a.grpcPort = cmdAdmin.Flag.Int("port.grpc", 0, "gRPC server port for worker connections (default: http port + 10000)")
	a.master = cmdAdmin.Flag.String("master", "localhost:9333", "comma-separated master servers")
	a.masters = cmdAdmin.Flag.String("masters", "", "comma-separated master servers (deprecated, use -master instead)")
	a.filerGroup = cmdAdmin.Flag.String("filerGroup", "", "filerGroup for the filers, brokers, and S3 servers")
	a.dataDir = cmdAdmin.Flag.String("dataDir", ".", "directory to store admin configuration and data files (default current dir; required for maintenance task state to persist)")

	a.adminUser = cmdAdmin.Flag.String("adminUser", "admin", "admin interface username")
	a.adminPassword = cmdAdmin.Flag.String("adminPassword", "", "admin interface password (if empty, auth is disabled)")
	a.readOnlyUser = cmdAdmin.Flag.String("readOnlyUser", "", "read-only user username (optional, for view-only access)")
	a.readOnlyPassword = cmdAdmin.Flag.String("readOnlyPassword", "", "read-only user password (optional, for view-only access; requires adminPassword to be set)")
	a.icebergPort = cmdAdmin.Flag.Int("iceberg.port", 8181, "Iceberg REST Catalog port (0 to hide in UI)")
	a.urlPrefix = cmdAdmin.Flag.String("urlPrefix", "", "URL path prefix when running behind a reverse proxy under a subdirectory (e.g. /seaweedfs)")
	a.metricsHttpPort = cmdAdmin.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	a.metricsHttpIp = cmdAdmin.Flag.String("metricsIp", "", "metrics listen ip. If empty, listens on all interfaces.")
	a.debug = cmdAdmin.Flag.Bool("debug", false, "serves runtime profiling data via pprof on the port specified by -debug.port")
	a.debugPort = cmdAdmin.Flag.Int("debug.port", 6060, "http port for debugging")
	a.cpuProfile = cmdAdmin.Flag.String("cpuprofile", "", "cpu profile output file")
	a.memProfile = cmdAdmin.Flag.String("memprofile", "", "memory profile output file")
}

var cmdAdmin = &Command{
	UsageLine: "admin -port=23646 -master=localhost:9333 [-filerGroup=group] [-port.grpc=33646] [-dataDir=/path/to/data]",
	Short:     "启动 SeaweedFS Web 管理界面",
	Long: `启动一个用于 SeaweedFS 集群管理的 Web 管理界面。

  该管理界面提供了一个现代化的 Web 界面,用于:
  - 集群拓扑可视化和监控
  - 卷管理和操作
  - 文件浏览和管理
  - 系统指标和性能监控
  - 配置管理
  - 维护操作

  管理界面会自动从 master 服务器发现 filer。
  用于 worker 连接的 gRPC 服务器运行在配置的 gRPC 端口上(默认:HTTP 端口 + 10000)。

  示例用法:
    weed admin -port=23646 -master="master1:9333,master2:9333"
    weed admin -port=23646 -master="localhost:9333" -filerGroup="tenant-a"
    weed admin -port=23646 -master="localhost:9333" -dataDir="/var/lib/seaweedfs-admin"
    weed admin -port=23646 -port.grpc=33646 -master="localhost:9333" -dataDir="~/seaweedfs-admin"
    weed admin -port=9900 -port.grpc=19900 -master="localhost:9333"
    weed admin -port=23646 -master="localhost:9333" -urlPrefix="/seaweedfs"

  数据目录:
    - 如果指定了 dataDir,管理配置和维护数据会被持久化
    - 如果目录不存在则会自动创建
    - 配置文件以 JSON 格式存储,便于编辑
    - 不指定 dataDir 时,所有配置仅保存在内存中

  身份认证:
    - 如果未设置 adminPassword,管理界面将在无认证状态下运行
    - 如果设置了 adminPassword,用户必须使用 adminUser/adminPassword 登录(完全访问权限)
    - 可选的只读访问:设置 readOnlyUser 和 readOnlyPassword 以获得仅查看权限
    - 只读用户可以查看集群状态和配置,但无法进行更改
    - 重要:配置只读凭据时,必须同时设置 adminPassword
    - 这确保存在管理员账号来管理和授权只读访问
    - 会话通过自动生成的会话密钥进行保护
    - 凭据也可通过 security.toml 的 [admin] 段或环境变量设置:
      WEED_ADMIN_USER、WEED_ADMIN_PASSWORD、WEED_ADMIN_READONLY_USER、WEED_ADMIN_READONLY_PASSWORD
    - 优先级:CLI 标志 > 环境变量 / security.toml > 默认值

  安全配置:
    - 管理 server 从 security.toml 读取 TLS 配置
    - 在 security.toml 中配置 [https.admin] 段以支持 HTTPS
    - 如果设置了 https.admin.key,服务器将以 TLS 模式启动
    - 如果设置了 https.admin.ca,则启用双向 TLS 认证
    - 生产环境部署请设置强 adminPassword
    - 配置防火墙规则以限制管理界面的访问

  security.toml 示例:
    [https.admin]
    cert = "/etc/ssl/admin.crt"
    key = "/etc/ssl/admin.key"
    ca = "/etc/ssl/ca.crt"     # 可选,用于双向 TLS

  Worker 通信:
    - worker 通过 HTTP 端口 + 10000 上的 gRPC 连接
    - worker 使用 security.toml 中的 [grpc.admin] 配置
    - 如果配置了证书,会自动使用 TLS
    - 如果 TLS 不可用,worker 会回退到非安全连接

  插件:
    - 始终在 worker gRPC 端口上启用
    - 在同一个 worker gRPC 端口上注册 plugin.proto gRPC 服务
    - 外部 worker 通过以下方式连接:weed worker -admin=<admin_host:admin_port>
    - 配置了 dataDir 时,插件元数据会持久化到 dataDir/plugin 下

  URL 前缀(子目录部署):
    - 使用 -urlPrefix 在反向代理的子目录下运行管理 UI
    - 示例:-urlPrefix="/seaweedfs" 使 UI 可在 /seaweedfs/admin 访问
    - 反向代理应将 /seaweedfs/* 请求转发到管理服务器
    - 所有静态资源、API 端点和导航链接都会使用该前缀
    - 会话 cookie 的作用域限定于该前缀路径

  调试和分析:
    - 使用 -debug 启动 pprof HTTP 服务器进行实时性能分析(仅限 localhost)
    - 设置 -debug.port 来选择 pprof 端口(默认 6060)
    - 性能分析可通过 http://127.0.0.1:<debug.port>/debug/pprof/ 访问
    - 使用 -cpuprofile 和 -memprofile 在关闭时将性能分析写入文件
    - 警告:-debug 会暴露运行时内部信息;仅在受信任的环境中使用
    - 示例:
      weed admin -debug -debug.port=6060 -master="localhost:9333"
      weed admin -cpuprofile=cpu.prof -memprofile=mem.prof -master="localhost:9333"

  指标:
    - 使用 -metricsPort 在 http://<host>:<metricsPort>/metrics 暴露 Prometheus 指标
    - 使用 -metricsIp 将指标端点绑定到特定 ip(默认:所有接口)
    - 当 -metricsPort 为 0(默认值)时禁用指标
    - 示例:weed admin -metricsPort=9327 -master="localhost:9333"

  维护配置:
    - 可选的 admin.toml 声明维护任务设置
      ([maintenance.vacuum]、[maintenance.balance]、[maintenance.erasure_coding])
    - admin.toml 中的设置会在每次启动时应用,覆盖从管理 UI
      保存的值,因此可以以声明式方式管理
    - 需要 -dataDir;值也可通过 WEED_* 环境变量设置,
      例如 WEED_MAINTENANCE_VACUUM_GARBAGE_THRESHOLD=0.3
    - 设置会同时应用到旧版任务策略和插件配置存储,
      因此对管理 UI 和插件 worker 都生效
    - 生成示例 admin.toml:weed scaffold -config=admin

  配置文件:
    - security.toml 和 admin.toml 文件会按以下顺序读取:"."、"$HOME/.seaweedfs/"、
      "/usr/local/etc/seaweedfs/" 或 "/etc/seaweedfs/"
    - 生成示例 security.toml:weed scaffold -config=security

`,
}

func runAdmin(cmd *Command, args []string) bool {
	if *a.debug {
		grace.StartDebugServer(*a.debugPort)
	}

	*a.cpuProfile = util.ResolvePath(*a.cpuProfile)
	*a.memProfile = util.ResolvePath(*a.memProfile)
	grace.SetupProfiling(*a.cpuProfile, *a.memProfile)

	// Load security configuration
	util.LoadSecurityConfiguration()

	// Optional admin.toml with maintenance task settings
	util.LoadConfiguration("admin", false)

	// Apply security.toml / env var fallbacks for credential flags.
	// CLI flags take precedence over security.toml / WEED_* env vars.
	applyViperFallback(cmd, a.adminUser, "adminUser", "admin.user")
	applyViperFallback(cmd, a.adminPassword, "adminPassword", "admin.password")
	applyViperFallback(cmd, a.readOnlyUser, "readOnlyUser", "admin.readonly.user")
	applyViperFallback(cmd, a.readOnlyPassword, "readOnlyPassword", "admin.readonly.password")

	// Backward compatibility: if -masters is provided, use it
	if *a.masters != "" {
		*a.master = *a.masters
	}

	// Validate required parameters
	if *a.master == "" {
		fmt.Println("Error: master parameter is required")
		fmt.Println("Usage: weed admin -master=master1:9333,master2:9333")
		return false
	}

	// Validate that master string can be parsed
	masterAddresses := pb.ServerAddresses(*a.master).ToAddresses()
	if len(masterAddresses) == 0 {
		fmt.Println("Error: no valid master addresses found")
		fmt.Println("Usage: weed admin -master=master1:9333,master2:9333")
		return false
	}

	// Security validation: prevent empty username when password is set
	if *a.adminPassword != "" && *a.adminUser == "" {
		fmt.Println("Error: -adminUser cannot be empty when -adminPassword is set")
		return false
	}
	if *a.readOnlyPassword != "" && *a.readOnlyUser == "" {
		fmt.Println("Error: -readOnlyUser is required when -readOnlyPassword is set")
		return false
	}
	// Security validation: prevent username conflicts between admin and read-only users
	if *a.adminUser != "" && *a.readOnlyUser != "" && *a.adminUser == *a.readOnlyUser {
		fmt.Println("Error: -adminUser and -readOnlyUser must be different when both are configured")
		return false
	}
	// Security validation: admin password is required for read-only user
	if *a.readOnlyPassword != "" && *a.adminPassword == "" {
		fmt.Println("Error: -adminPassword must be set when -readOnlyPassword is configured")
		return false
	}

	// Set default gRPC port if not specified
	if *a.grpcPort == 0 {
		*a.grpcPort = *a.port + 10000
	}

	// Security warnings
	if *a.adminPassword == "" {
		fmt.Println("WARNING: Admin interface is running without authentication!")
		fmt.Println("         Set -adminPassword for production use")
	}
	fmt.Printf("Starting SeaweedFS Admin Interface on port %d\n", *a.port)
	fmt.Printf("Worker gRPC server will run on port %d\n", *a.grpcPort)
	fmt.Printf("Masters: %s\n", *a.master)
	fmt.Printf("Filers will be discovered automatically from masters\n")
	if *a.dataDir != "" {
		fmt.Printf("Data Directory: %s\n", *a.dataDir)
	} else {
		fmt.Printf("Data Directory: Not specified (configuration will be in-memory only)\n")
	}
	if *a.adminPassword != "" {
		fmt.Printf("Authentication: Enabled (admin user: %s)\n", *a.adminUser)
		if *a.readOnlyPassword != "" {
			fmt.Printf("Read-only access: Enabled (read-only user: %s)\n", *a.readOnlyUser)
		}
	} else {
		fmt.Printf("Authentication: Disabled\n")
	}
	fmt.Printf("Plugin: Enabled\n")

	// Start Prometheus metrics endpoint if a port is configured
	if *a.metricsHttpPort > 0 {
		fmt.Printf("Metrics: http://%s/metrics\n", stats_collect.JoinHostPort(*a.metricsHttpIp, *a.metricsHttpPort))
	}
	go stats_collect.StartMetricsServer(*a.metricsHttpIp, *a.metricsHttpPort)

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\nReceived signal %v, shutting down gracefully...\n", sig)
		cancel()
	}()

	// Normalize URL prefix
	urlPrefix := strings.TrimRight(*a.urlPrefix, "/")
	if urlPrefix != "" && !strings.HasPrefix(urlPrefix, "/") {
		urlPrefix = "/" + urlPrefix
	}
	if urlPrefix != "" {
		fmt.Printf("URL Prefix: %s\n", urlPrefix)
	}

	// Start the admin server with all masters (UI enabled by default)
	err := startAdminServer(ctx, a, true, *a.icebergPort, urlPrefix)
	if err != nil {
		fmt.Printf("Admin server error: %v\n", err)
		return false
	}

	fmt.Println("Admin server stopped")
	return true
}

// startAdminServer starts the actual admin server
func startAdminServer(ctx context.Context, options AdminOptions, enableUI bool, icebergPort int, urlPrefix string) error {
	// Create router
	r := mux.NewRouter()
	r.Use(loggingMiddleware)
	r.Use(recoveryMiddleware)

	// Inject URL prefix into request context for use by handlers and templates
	if urlPrefix != "" {
		r.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := dash.WithURLPrefix(r.Context(), urlPrefix)
				next.ServeHTTP(w, r.WithContext(ctx))
			})
		})
	}

	// Create data directory first if specified (needed for session key storage)
	var dataDir string
	if *options.dataDir != "" {
		dataDir = util.ResolvePath(*options.dataDir)
		if dataDir != *options.dataDir {
			fmt.Printf("Expanded dataDir: %s -> %s\n", *options.dataDir, dataDir)
		}
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return fmt.Errorf("failed to create data directory %s: %v", dataDir, err)
		}
		glog.Infof("Data directory created/verified: %s", dataDir)
	}

	// Write maintenance task settings from admin.toml into the persisted
	// task configs before the server loads them
	if err := dash.NewConfigPersistence(dataDir).ApplyMaintenanceConfigFromToml(util.GetViper()); err != nil {
		return fmt.Errorf("apply admin.toml: %w", err)
	}

	// Detect TLS configuration to set Secure cookie flag
	cookieSecure := viper.GetString("https.admin.key") != ""

	// Session store - load or generate session keys
	authKey, encKey, err := loadOrGenerateSessionKeys(dataDir)
	if err != nil {
		return fmt.Errorf("failed to get session key: %w", err)
	}
	store := sessions.NewCookieStore(authKey, encKey)

	// Configure session options to ensure cookies are properly saved
	cookiePath := "/"
	if urlPrefix != "" {
		cookiePath = urlPrefix + "/"
	}
	store.Options = &sessions.Options{
		Path:     cookiePath,
		MaxAge:   3600 * 24,    // 24 hours
		HttpOnly: true,         // Prevent JavaScript access
		Secure:   cookieSecure, // Set based on actual TLS configuration
		SameSite: http.SameSiteLaxMode,
	}

	// Static files - pre-gzipped and embedded in the binary
	r.Handle("/static", http.RedirectHandler("/static/", http.StatusMovedPermanently))
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", admin.StaticHandler()))

	// Create admin server (plugin is always enabled)
	adminServer := dash.NewAdminServer(*options.master, *options.filerGroup, nil, dataDir, icebergPort)

	if err := adminServer.ApplyPluginConfigFromToml(util.GetViper()); err != nil {
		return fmt.Errorf("apply admin.toml to plugin config: %w", err)
	}

	// Show discovered filers
	filers := adminServer.GetAllFilers()
	if len(filers) > 0 {
		glog.Infof("Discovered filers: %s", strings.Join(filers, ", "))
	} else {
		glog.Infof("No filers discovered from masters")
	}

	// Start worker gRPC server for worker connections
	err = adminServer.StartWorkerGrpcServer(*options.grpcPort)
	if err != nil {
		return fmt.Errorf("failed to start worker gRPC server: %w", err)
	}

	// Set up cleanup for gRPC server
	defer func() {
		if stopErr := adminServer.StopWorkerGrpcServer(); stopErr != nil {
			log.Printf("Error stopping worker gRPC server: %v", stopErr)
		}
	}()

	// Create handlers and setup routes
	authRequired := *options.adminPassword != ""
	adminHandlers := handlers.NewAdminHandlers(adminServer, store)
	adminHandlers.SetupRoutes(r, authRequired, *options.adminUser, *options.adminPassword, *options.readOnlyUser, *options.readOnlyPassword, enableUI)

	// Server configuration
	addr := fmt.Sprintf(":%d", *options.port)
	var handler http.Handler = r
	if urlPrefix != "" {
		stripped := http.StripPrefix(urlPrefix, r)
		handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			// Redirect /prefix (no trailing slash) to /prefix/
			if req.URL.Path == urlPrefix {
				target := urlPrefix + "/"
				if req.URL.RawQuery != "" {
					target += "?" + req.URL.RawQuery
				}
				http.Redirect(w, req, target, http.StatusFound)
				return
			}
			stripped.ServeHTTP(w, req)
		})
	}
	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	// Decide TLS configuration BEFORE launching the server goroutine, so a
	// bad cert or a missing key surfaces as a startup error instead of a
	// silently returned goroutine that leaves startAdminServer blocked on
	// ctx.Done() with no listener.
	var (
		clientCertFile,
		certFile,
		keyFile string
	)
	useTLS := false
	useMTLS := false

	if viper.GetString("https.admin.key") != "" {
		useTLS = true
		certFile = viper.GetString("https.admin.cert")
		keyFile = viper.GetString("https.admin.key")
	}

	if viper.GetString("https.admin.ca") != "" {
		useMTLS = true
		clientCertFile = viper.GetString("https.admin.ca")
	}

	if useMTLS {
		server.TLSConfig = security.LoadClientTLSHTTP(clientCertFile)
	}

	if useTLS {
		getCert, certProvider, certErr := security.NewReloadingServerCertificate(certFile, keyFile)
		if certErr != nil {
			return fmt.Errorf("load admin HTTPS certificate: %w", certErr)
		}
		defer certProvider.Close()
		if server.TLSConfig == nil {
			server.TLSConfig = &tls.Config{}
		}
		server.TLSConfig.GetCertificate = getCert
	}

	// Start server. Surface immediate failures (e.g. bind error) via
	// serveErrCh so the caller doesn't block on ctx.Done() while no server
	// is actually listening. http.ErrServerClosed after Shutdown is normal
	// and not forwarded.
	serveErrCh := make(chan error, 1)
	go func() {
		glog.Infof("Starting SeaweedFS Admin Server on port %d", *options.port)
		var serveErr error
		if useTLS {
			glog.Infof("Starting SeaweedFS Admin Server with TLS on port %d", *options.port)
			serveErr = server.ListenAndServeTLS("", "")
		} else {
			serveErr = server.ListenAndServe()
		}
		if serveErr != nil && serveErr != http.ErrServerClosed {
			serveErrCh <- fmt.Errorf("admin server: %w", serveErr)
		}
	}()

	// Wait for context cancellation or an early serve failure.
	select {
	case <-ctx.Done():
	case err := <-serveErrCh:
		return err
	}

	// Graceful shutdown
	glog.Infof("Shutting down admin server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("admin server forced to shutdown: %w", err)
	}

	adminServer.Shutdown()

	return nil
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := r.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (r *statusRecorder) Push(target string, opts *http.PushOptions) error {
	if p, ok := r.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w}

		next.ServeHTTP(recorder, r)

		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		// Only log errors; 2xx/3xx (including 304 Not Modified cache hits) are normal.
		if status < 400 {
			return
		}

		log.Printf("[HTTP] %v | %3d | %13v | %15s | %-7s %s",
			time.Now().Format("2006/01/02 - 15:04:05"),
			status,
			time.Since(start),
			r.RemoteAddr,
			r.Method,
			r.URL.Path,
		)
	})
}

func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("panic: %v\n%s", err, debug.Stack())
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()

		next.ServeHTTP(w, r)
	})
}

// loadOrGenerateSessionKeys loads or creates authentication/encryption keys for session cookies.
func loadOrGenerateSessionKeys(dataDir string) ([]byte, []byte, error) {
	const keyLen = 32

	if dataDir == "" {
		// No persistence, generate ephemeral keys
		log.Println("No dataDir specified, generating ephemeral session keys")
		authKey := make([]byte, keyLen)
		encKey := make([]byte, keyLen)
		if _, err := rand.Read(authKey); err != nil {
			return nil, nil, err
		}
		if _, err := rand.Read(encKey); err != nil {
			return nil, nil, err
		}
		return authKey, encKey, nil
	}

	sessionKeyPath := filepath.Join(dataDir, ".session_key")

	if data, err := os.ReadFile(sessionKeyPath); err == nil {
		switch len(data) {
		case keyLen:
			authKey := make([]byte, keyLen)
			copy(authKey, data)

			encKey := make([]byte, keyLen)
			if _, err := rand.Read(encKey); err != nil {
				return nil, nil, err
			}
			log.Printf("Warning: Upgrading session key at %s by adding an encryption key; existing cookies will be invalidated", sessionKeyPath)

			combined := append(authKey, encKey...)
			if err := os.WriteFile(sessionKeyPath, combined, 0600); err != nil {
				log.Printf("Warning: Failed to persist upgraded session key: %v", err)
			} else {
				log.Printf("Upgraded session key file to include encryption key: %s", sessionKeyPath)
			}
			return authKey, encKey, nil
		case 2 * keyLen:
			authKey := make([]byte, keyLen)
			encKey := make([]byte, keyLen)
			copy(authKey, data[:keyLen])
			copy(encKey, data[keyLen:])
			glog.Infof("Loaded persisted session key from %s", sessionKeyPath)
			return authKey, encKey, nil
		default:
			glog.Warningf("Invalid session key file (expected %d or %d bytes, got %d), generating new key", keyLen, 2*keyLen, len(data))
		}
	} else if !os.IsNotExist(err) {
		glog.Warningf("Failed to read session key from %s: %v. A new key will be generated.", sessionKeyPath, err)
	}

	key := make([]byte, 2*keyLen)
	if _, err := rand.Read(key); err != nil {
		return nil, nil, err
	}

	if err := os.WriteFile(sessionKeyPath, key, 0600); err != nil {
		glog.Warningf("Failed to persist session key: %v", err)
	} else {
		glog.Infof("Generated and persisted new session key to %s", sessionKeyPath)
	}

	return key[:keyLen], key[keyLen:], nil
}

// applyViperFallback sets a flag's value from viper (security.toml / env var)
// when the flag was not explicitly set on the command line.
func applyViperFallback(cmd *Command, flagPtr *string, flagName, viperKey string) {
	explicitlySet := false
	cmd.Flag.Visit(func(f *flag.Flag) {
		if f.Name == flagName {
			explicitlySet = true
		}
	})
	if !explicitlySet {
		if v := util.GetViper().GetString(viperKey); v != "" {
			*flagPtr = v
		}
	}
}
