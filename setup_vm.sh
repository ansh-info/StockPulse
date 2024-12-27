#!/bin/bash

# Update system
sudo apt-get update
sudo apt-get upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.32.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Clone repository
git clone https://github.com/ansh-info/StockPulse.git

# Add current user to docker group
sudo usermod -aG docker $USER

# Configure firewall
sudo ufw allow 8501/tcp
sudo ufw allow 22/tcp
sudo ufw enable

echo "VM setup complete!"
