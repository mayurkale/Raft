rm -f *~
rm -f src/raft/*~
rm -rf pkg/*
rm -f bin/kvstore
rm -r -f log/*
go fmt *.go
go fmt src/raft/*.go
go fmt src/kvstore/*.go

go build raft
go build github.com/pebbe/zmq4
go install kvstore
