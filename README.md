## features

# Install docker-compose on GCP VM
```
sudo curl -sSL https://github.com/docker/compose/releases/download/v2.27.1/docker-compose-linux-x86_64 -o /var/lib/google/docker-compose
sudo chmod o+x /var/lib/google/docker-compose
mkdir -p ~/.docker/cli-plugins
ln -sf /var/lib/google/docker-compose ~/.docker/cli-plugins/docker-compose
docker compose version
```
