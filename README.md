# pubSubMySQL

Made with :heart: from Jeyakaran to Fiskil

This repo contains source code to demonstrate the pipeline from PubSub to MySQL database. The Assumption made were the data flows into PubSub from various MicroServices at regular intervals. The LMS(Log Monitoring System) will pull all the Log information from PubSub and pushed into Database. This can be done via mocking the entire environment. Here, Dockertest(MySQL) and pstest(PubSub) libraries were used to mock the environment. The implementation for flushing and batch size were not yet implemented. 


# PreRequisites

- Go - `go version go1.17.6 darwin/amd64`
- Ginkgo - `Ginkgo Version 2.1.3`

## To install dockertest
- dockertest - `go get github.com/ory/dockertest/v3`
- pstest - `go get cloud.google.com/go/pubsub/pstest`


## To install Ginkgo
```sh
go install -mod=mod github.com/onsi/ginkgo/v2/ginkgo
go get github.com/onsi/gomega/...