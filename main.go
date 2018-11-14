package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/EmbeddedEnterprises/service"
	"github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: nxstest numconns pkgsz\n")
		os.Exit(1)
	}

	numConns, ncErr := strconv.ParseInt(os.Args[1], 10, 64)
	pkgSize, szErr := strconv.ParseInt(os.Args[2], 10, 64)
	if ncErr != nil || szErr != nil {
		fmt.Printf("Usage: nxstest numconns pkgsz: %v, %v\n", ncErr, szErr)
		os.Exit(1)
	}

	wg := &sync.WaitGroup{}
	if idx, ok := os.LookupEnv("SUBP"); ok {
		wg.Add(1)
		id, _ := strconv.ParseInt(idx, 10, 64)
		runServiceInstance(id, pkgSize, wg)
		wg.Wait()
		os.Exit(0)
	}

	wg.Add(int(numConns))
	var i int64
	for i = 0; i < numConns; i++ {
		time.Sleep(100 * time.Millisecond)
		go runSubprocess(i, pkgSize, wg)
	}
	wg.Wait()
}

func runSubprocess(index, pkgSize int64, wg *sync.WaitGroup) {
	defer wg.Done()
	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("SUBP=%d", index))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Printf("[%d] -> Start failed: %v\n", index, err)
		return
	}
	if err := cmd.Wait(); err != nil {
		fmt.Printf("[%d] -> Wait failed: %v\n", index, err)
	}
}

func runServiceInstance(index, pkgSize int64, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Printf("[%d] -> Startup\n", index)
	conf := service.Config{
		Name:          fmt.Sprintf("SERVICE %d", index),
		Serialization: client.MSGPACK,
		Version:       "0.0.0",
	}
	srv := service.New(conf)

	srv.Connect()
	fmt.Printf("[%d] -> Connected\n", index)
	topic := fmt.Sprintf("rocks.test.%d.testtopic", index)
	rpc := fmt.Sprintf("rocks.test.%d.testrpc", index)
	if err := srv.Client.Register(rpc, func(ctx context.Context, args wamp.List, _, _ wamp.Dict) *client.InvokeResult {
		time.Sleep(5 * time.Second)
		if err := srv.Client.Publish(topic, nil, args, nil); err != nil {
			fmt.Printf("[%d] -> Publish failed: %v\n", index, err)
		}
		return service.ReturnValue(args)
	}, nil); err != nil {
		fmt.Printf("[%d] -> Register failed: %v", index, err)
	}

	closeChan := make(chan bool)
	go func(close chan bool, service *service.Service, index int64) {
		t := time.NewTicker(10 * time.Millisecond)
		payload := make([]byte, pkgSize)
		rand.Read(payload)
		args := wamp.List{payload}
	outer:
		for {
			select {
			case <-close:
				break outer
			case <-t.C:
				go func() {
					if _, err := service.Client.Call(context.Background(), rpc, nil, args, nil, ""); err != nil {
						fmt.Printf("[%d] -> Call failed: %v\n", index, err)
					}
				}()
			}
		}
	}(closeChan, srv, index)
	srv.Run()
	close(closeChan)
	fmt.Printf("[%d] -> Service Exit!\n", index)
}
