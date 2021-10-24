all:
	env GOOS=linux go build -o oratworker
	zip oratworker.zip oratworker
    
linux:
	go build -o bin/dphelper
    
clean:
	rm -rf bin

