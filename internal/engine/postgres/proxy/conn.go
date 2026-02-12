package proxy

import (
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// handleConn establishes a TCP connection to Prod and relays bytes
// bidirectionally between the client and Prod. When either side
// closes or errors, both connections are torn down.
func (p *Proxy) handleConn(clientConn net.Conn, connID int64) {
	defer p.activeConns.Done()

	clientAddr := clientConn.RemoteAddr().String()
	if p.verbose {
		log.Printf("[conn %d] opened from %s", connID, clientAddr)
	}

	prodConn, err := net.DialTimeout("tcp", p.prodAddr, 5*time.Second)
	if err != nil {
		log.Printf("[conn %d] failed to connect to prod %s: %v", connID, p.prodAddr, err)
		clientConn.Close()
		return
	}

	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			clientConn.Close()
			prodConn.Close()
		})
	}
	defer closeBoth()

	var wg sync.WaitGroup
	wg.Add(2)

	// client → prod
	go func() {
		defer wg.Done()
		_, err := io.Copy(prodConn, clientConn)
		if p.verbose && err != nil {
			log.Printf("[conn %d] client→prod: %v", connID, err)
		}
		closeBoth()
	}()

	// prod → client
	go func() {
		defer wg.Done()
		_, err := io.Copy(clientConn, prodConn)
		if p.verbose && err != nil {
			log.Printf("[conn %d] prod→client: %v", connID, err)
		}
		closeBoth()
	}()

	wg.Wait()

	if p.verbose {
		log.Printf("[conn %d] closed from %s", connID, clientAddr)
	}
}
