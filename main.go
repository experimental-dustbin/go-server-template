package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type RequestResponse struct {
	Status   string      `json:"status"`
	Response interface{} `json:"response"`
}
type ServerConfiguration struct {
	Port    uint64      `json:"port"`
	Workers uint64      `json:"workers"`
	Meta    interface{} `json:"meta"`
}

func lookupWithDefault[V any](key string, defaultValue string, mapper func(v string) (V, error)) (V, error) {
	value, present := os.LookupEnv(key)
	ret, err := mapper(defaultValue)
	if err != nil {
		log.Fatal("Could not parse default value for key: %+v", key)
	}
	if present {
		ret, err = mapper(value)
		if err != nil {
			log.Fatal("Could not parse value for key: %+v", key)
		}
	}
	return ret, nil
}
func Configuration() ServerConfiguration {
	config := ServerConfiguration{Port: 0, Workers: 0}
	mapper := func(v string) (uint64, error) {
		return strconv.ParseUint(v, 10, 64)
	}
	port, _ := lookupWithDefault[uint64]("PORT", "8080", mapper)
	workers, _ := lookupWithDefault[uint64]("WORKERS", "10", mapper)
	meta, _ := lookupWithDefault[interface{}]("META", "{}", func(v string) (interface{}, error) {
		decoder := json.NewDecoder(strings.NewReader(v))
		var parsed interface{}
		if err := decoder.Decode(&parsed); err != nil {
			return nil, err
		}
		return parsed, nil
	})
	config.Port, config.Workers, config.Meta = port, workers, meta
	return config
}
func handleSignals(server *http.Server, wg *sync.WaitGroup) {
	defer wg.Done()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	log.Println("Shutting down server: %+v", server)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatal("Could not gracefully shutdown the server: %+v", err)
	}
	log.Println("Server shutdown completed")
}
func startServer(server *http.Server, wg *sync.WaitGroup) {
	defer wg.Done()
	err := server.ListenAndServe()
	if err != nil {
		if errors.Is(err, http.ErrServerClosed) {
			log.Println("Server is shutting down")
		} else {
			log.Fatal("Error starting server: %+v", err)
		}
	}
}

type Pool interface {
	Submit(task func()) error
}
type Handler func(context.Context, *http.Request, Pool) (interface{}, error)
type HttpHandler func(http.ResponseWriter, *http.Request)

func RequestHandler(handleRequest Handler, pool Pool) HttpHandler {
	encodeError := func(encoder *json.Encoder, e error) {
		if err := encoder.Encode(RequestResponse{Status: "error", Response: e}); err != nil {
			log.Println("Response encoding error: %+v", err)
			return
		}
	}
	encodeResponse := func(encoder *json.Encoder, r interface{}) {
		if err := encoder.Encode(RequestResponse{Status: "ok", Response: r}); err != nil {
			log.Println("Response encoding error: %+v", err)
			return
		}
	}
	requestHandler := func(w http.ResponseWriter, r *http.Request) {
		encoder := json.NewEncoder(w)
		ctx := context.TODO()
		response, responseErr := handleRequest(ctx, r, pool)
		if responseErr != nil {
			encodeError(encoder, responseErr)
			return
		}
		encodeResponse(encoder, response)
		return
	}
	return requestHandler
}
func WorkerPool(capacity int) Pool {
	workerPool, poolErr := ants.NewPool(int(capacity))
	if poolErr != nil {
		log.Fatal("Could not start ants pool: %+v", poolErr)
	} else {
		log.Println("Started ants pool: %+v", workerPool)
	}
	return workerPool
}
func Server(port int, requestHandler HttpHandler) *http.Server {
	listeningAddress := fmt.Sprintf(":%d", port)
	log.Printf("Listening on %+v", listeningAddress)
	muxer := http.NewServeMux()
	muxer.HandleFunc("/", requestHandler)
	server := &http.Server{Addr: listeningAddress, Handler: muxer}
	return server
}
func waitOn(server *http.Server) {
	wg := &sync.WaitGroup{}
	for _, f := range []func(*http.Server, *sync.WaitGroup){startServer, handleSignals} {
		wg.Add(1)
		go f(server, wg)
	}
	wg.Wait()
}
func startAndWait(handler Handler) {
	configuration := Configuration()
	workerPool := WorkerPool(int(configuration.Workers))
	requestHandler := RequestHandler(handler, workerPool)
	server := Server(int(configuration.Port), requestHandler)
	waitOn(server)
}

type DecodedRequest struct {
	Type    int         `json:"type"`
	Request interface{} `json:"request"`
}

func main() {
	defer ants.Release()
	requestHandler := func(ctx context.Context, r *http.Request, pool Pool) (interface{}, error) {
		decoder, parsed := json.NewDecoder(r.Body), DecodedRequest{}
		if err := decoder.Decode(&parsed); err != nil {
			return parsed, err
		}
		pool.Submit(func() {
			fmt.Println("Handling request in a worker pool: %+v", parsed)
		})
		return nil, nil
	}
	startAndWait(requestHandler)
	log.Println("Server stopped")
}
