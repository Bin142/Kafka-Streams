# Kafka Management System - Hướng Dẫn Cài Đặt Môi Trường

> **Last Updated**: 2026-05-18

---

## 📋 Tổng Quan Yêu Cầu

### Minimum Requirements
| Component | Version | Ghi chú |
|-----------|---------|---------|
| **Java** | 21+ | LTS version, required |
| **Node.js** | 18+ | LTS version, recommended 20.x |
| **Docker** | 24+ | Để chạy Kafka, PostgreSQL, Redis |
| **Docker Compose** | 2.20+ | Đi kèm Docker Desktop |
| **Maven** | 3.9+ | Hoặc dùng Maven Wrapper (./mvnw) |
| **RAM** | 8GB+ | Minimum cho development |
| **Disk** | 10GB+ | Cho Docker images và data |

---

## 🔧 Chi Tiết Phiên Bản Dependencies

### Backend (Java/Spring Boot)

| Dependency | Version | Mục đích |
|------------|---------|----------|
| **Java** | 21 | Runtime |
| **Spring Boot** | 3.3.0 | Framework |
| **Apache Kafka Client** | 3.7.0 | Kafka operations |
| **Confluent Schema Registry** | 7.6.0 | Schema management |
| **PostgreSQL Driver** | 42.7.x | Database (managed by Spring Boot) |
| **Flyway** | 10.x | Database migration |
| **Redis** | - | Caching (via Spring Data Redis) |
| **Caffeine** | 3.x | Local cache |
| **JWT (jjwt)** | 0.12.5 | Authentication |
| **Lombok** | 1.18.32 | Code generation |
| **MapStruct** | 1.5.5 | DTO mapping |
| **SpringDoc OpenAPI** | 2.5.0 | API documentation |

### Frontend (React/TypeScript)

| Dependency | Version | Mục đích |
|------------|---------|----------|
| **React** | 19.x | UI Framework |
| **TypeScript** | 6.x | Type safety |
| **Vite** | 8.x | Build tool |
| **Ant Design** | 5.29.x | UI Components |
| **@ant-design/icons** | 6.x | Icons |
| **React Router** | 7.x | Routing |
| **TanStack Query** | 5.x | Data fetching |
| **Zustand** | 5.x | State management |
| **Axios** | 1.x | HTTP client |
| **Mermaid** | 11.x | Diagrams |
| **Day.js** | 1.x | Date handling |

### Infrastructure (Docker)

| Service | Image | Version | Port |
|---------|-------|---------|------|
| **PostgreSQL** | postgres:16-alpine | 16 | 5432 |
| **Redis** | redis:7-alpine | 7 | 6379 |
| **Zookeeper** | confluentinc/cp-zookeeper | 7.6.0 | 2181 |
| **Kafka** | confluentinc/cp-kafka | 7.6.0 | 9092 |
| **Schema Registry** | confluentinc/cp-schema-registry | 7.6.0 | 8081 |
| **Kafka Connect** | confluentinc/cp-kafka-connect | 7.6.0 | 8083 |

---

## 🖥️ Hướng Dẫn Cài Đặt Theo OS

### macOS

```bash
# 1. Install Homebrew (nếu chưa có)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 2. Install Java 21
brew install openjdk@21
# Add to PATH
echo 'export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# Verify
java -version
# Expected: openjdk version "21.x.x"

# 3. Install Node.js 20 LTS
brew install node@20
# Or use nvm (recommended)
brew install nvm
nvm install 20
nvm use 20

# Verify
node -v  # Expected: v20.x.x
npm -v   # Expected: 10.x.x

# 4. Install Docker Desktop
brew install --cask docker
# Start Docker Desktop from Applications

# Verify
docker --version  # Expected: Docker version 24.x.x
docker compose version  # Expected: Docker Compose version v2.x.x

# 5. Install Maven (optional, project has Maven Wrapper)
brew install maven

# Verify
mvn -v  # Expected: Apache Maven 3.9.x
```

### Windows

```powershell
# 1. Install Chocolatey (nếu chưa có)
# Run PowerShell as Administrator
Set-ExecutionPolicy Bypass -Scope Process -Force
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# 2. Install Java 21
choco install openjdk21 -y

# Verify
java -version
# Expected: openjdk version "21.x.x"

# 3. Install Node.js 20 LTS
choco install nodejs-lts -y
# Or download from https://nodejs.org/

# Verify
node -v  # Expected: v20.x.x
npm -v   # Expected: 10.x.x

# 4. Install Docker Desktop
choco install docker-desktop -y
# Or download from https://www.docker.com/products/docker-desktop/
# Restart computer after installation

# Verify
docker --version
docker compose version

# 5. Install Maven (optional)
choco install maven -y
```

### Ubuntu/Debian Linux

```bash
# 1. Update system
sudo apt update && sudo apt upgrade -y

# 2. Install Java 21
sudo apt install -y openjdk-21-jdk

# Verify
java -version
# Expected: openjdk version "21.x.x"

# Set JAVA_HOME
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc

# 3. Install Node.js 20 LTS
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs

# Verify
node -v  # Expected: v20.x.x
npm -v   # Expected: 10.x.x

# 4. Install Docker
# Remove old versions
sudo apt remove docker docker-engine docker.io containerd runc

# Install Docker
sudo apt install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Add user to docker group (no sudo needed)
sudo usermod -aG docker $USER
newgrp docker

# Verify
docker --version
docker compose version

# 5. Install Maven (optional)
sudo apt install -y maven
```

---

## 🚀 Khởi Chạy Project

### Step 1: Clone và Setup

```bash
# Clone repository
git clone <repository-url>
cd Kafka-Streams

# Verify Java version
java -version
# Must be 21+
```

### Step 2: Start Infrastructure (Docker)

```bash
# Start all services
docker compose up -d

# Check status
docker compose ps

# Expected output:
# NAME                        STATUS
# kafka-mgmt-postgres         running (healthy)
# kafka-mgmt-redis            running (healthy)
# kafka-mgmt-zookeeper        running
# kafka-mgmt-kafka            running (healthy)
# kafka-mgmt-schema-registry  running (healthy)
# kafka-mgmt-connect          running (healthy)

# Wait for all services to be healthy (~1-2 minutes)
docker compose logs -f
# Press Ctrl+C to exit logs
```

### Step 3: Start Backend

```bash
# Development mode (H2 database, auth disabled)
./mvnw spring-boot:run -Dspring-boot.run.profiles=dev

# Or production mode (PostgreSQL, auth enabled)
./mvnw spring-boot:run

# Backend will start at http://localhost:8080
# Swagger UI: http://localhost:8080/api/swagger-ui.html
```

### Step 4: Start Frontend

```bash
# Open new terminal
cd kafka-management-ui

# Install dependencies (first time only)
npm install

# Start development server
npm run dev

# Frontend will start at http://localhost:5173
```

### Step 5: Verify Installation

| Service | URL | Expected |
|---------|-----|----------|
| Backend API | http://localhost:8080/api/v1/clusters | JSON response |
| Swagger UI | http://localhost:8080/api/swagger-ui.html | API documentation |
| Frontend | http://localhost:5173 | React app |
| Kafka | localhost:9092 | Broker running |
| Schema Registry | http://localhost:8081 | `{}` or subjects list |
| Kafka Connect | http://localhost:8083 | Connect info JSON |
| PostgreSQL | localhost:5432 | Database running |
| Redis | localhost:6379 | Cache running |

---

## 🔍 Troubleshooting

### Java Version Issues

```bash
# Check current Java version
java -version

# If wrong version, check JAVA_HOME
echo $JAVA_HOME

# macOS: Switch Java version
export JAVA_HOME=$(/usr/libexec/java_home -v 21)

# Linux: Use update-alternatives
sudo update-alternatives --config java
```

### Docker Issues

```bash
# Check Docker is running
docker info

# If permission denied (Linux)
sudo usermod -aG docker $USER
# Then logout and login again

# Reset Docker (if corrupted)
docker compose down -v
docker system prune -a
docker compose up -d
```

### Port Conflicts

```bash
# Check what's using a port
# macOS/Linux
lsof -i :8080
lsof -i :5173
lsof -i :9092

# Windows
netstat -ano | findstr :8080

# Kill process using port
kill -9 <PID>
```

### Maven Build Issues

```bash
# Clean and rebuild
./mvnw clean install -DskipTests

# If Maven Wrapper not working
chmod +x mvnw  # Linux/macOS
# Or use installed Maven
mvn clean install -DskipTests
```

### Node.js/npm Issues

```bash
# Clear npm cache
npm cache clean --force

# Delete node_modules and reinstall
rm -rf node_modules package-lock.json
npm install

# If permission issues (Linux)
sudo chown -R $USER:$USER ~/.npm
```

---

## 📊 Resource Usage

### Development Environment

| Service | RAM | CPU | Disk |
|---------|-----|-----|------|
| Backend (Spring Boot) | ~500MB | Low | - |
| Frontend (Vite dev) | ~200MB | Low | - |
| PostgreSQL | ~100MB | Low | ~100MB |
| Redis | ~50MB | Low | ~10MB |
| Zookeeper | ~200MB | Low | ~50MB |
| Kafka | ~1GB | Medium | ~500MB |
| Schema Registry | ~300MB | Low | ~50MB |
| Kafka Connect | ~500MB | Low | ~100MB |
| **Total** | **~3GB** | - | **~1GB** |

### Recommended System

- **Development**: 8GB RAM, 4 CPU cores
- **Production**: 16GB+ RAM, 8+ CPU cores

---

## 🔐 Default Credentials

### Development Mode

| Service | Username | Password |
|---------|----------|----------|
| Application | admin | admin123 |
| PostgreSQL | postgres | postgres |
| Redis | - | (no password) |

### Production Mode

⚠️ **Change all default passwords before deploying to production!**

---

## 📁 Configuration Files

| File | Purpose |
|------|---------|
| `src/main/resources/application.yml` | Main Spring Boot config |
| `src/main/resources/application-dev.yml` | Development profile |
| `src/main/resources/clusters.yml` | Kafka cluster configurations |
| `docker-compose.yml` | Docker services |
| `kafka-management-ui/vite.config.ts` | Frontend build config |

---

## 🔗 Useful Links

- [Java 21 Download](https://adoptium.net/temurin/releases/?version=21)
- [Node.js Download](https://nodejs.org/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Platform](https://docs.confluent.io/platform/current/overview.html)
- [Spring Boot Documentation](https://docs.spring.io/spring-boot/docs/current/reference/html/)
- [React Documentation](https://react.dev/)
- [Ant Design](https://ant.design/)
