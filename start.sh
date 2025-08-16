#!/bin/bash

echo "🚀 Starting Flink Streaming Demo"
echo "================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed."
    exit 1
fi

echo "📦 Building Maven project..."
mvn clean package -q

if [ $? -ne 0 ]; then
    echo "❌ Maven build failed."
    exit 1
fi

echo "✅ Maven build successful"

# Copy JAR to flink-jobs directory
echo "📋 Copying JAR file to Flink jobs directory..."
cp target/flink-streaming-demo-1.0-SNAPSHOT.jar flink-jobs/

echo "🐳 Starting Docker services..."
docker-compose up -d

echo "⏳ Waiting for services to start..."
sleep 30

echo "🔍 Checking service status..."
docker-compose ps

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📊 Access points:"
echo "  - Flink Web UI: http://localhost:8081"
echo "  - Kafka (external): localhost:9092"
echo ""
echo "📝 To submit jobs:"
echo "  1. Open http://localhost:8081"
echo "  2. Go to 'Submit New Job'"
echo "  3. Upload: flink-streaming-demo-1.0-SNAPSHOT.jar"
echo "  4. Select main class:"
echo "     - com.example.MockDataStreamingJob (for DataGen demo)"
echo "     - com.example.KafkaStreamingJob (for Kafka demo)"
echo ""
echo "📋 Useful commands:"
echo "  - View logs: docker-compose logs -f [service-name]"
echo "  - Stop all: docker-compose down"
echo "  - Restart: docker-compose restart [service-name]"
echo ""
echo "🔍 Monitor with:"
echo "  docker-compose logs -f data-generator  # See generated data"
echo "  docker-compose logs -f taskmanager     # See Flink processing"
