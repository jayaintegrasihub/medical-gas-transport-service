# Stage 1: Build the Go application
FROM golang:alpine AS builder

# Set environment variables for cross-compilation and static linking
ENV GOOS=linux
ENV CGO_ENABLED=0

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source code into the container
COPY . .

# Build the Go app
RUN go build -o medical-gas-transport-service

# Stage 2: Run the Go application
FROM alpine:latest

# Install any required dependencies
RUN apk --no-cache add ca-certificates

# Set the Current Working Directory inside the container
WORKDIR /root/

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /app/medical-gas-transport-service .

# Copy the .env file
COPY .env .

# Command to run the executable
CMD ["./medical-gas-transport-service"]
