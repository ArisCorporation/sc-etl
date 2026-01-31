DIRECTUS_URL=https://studio.ariscorp.de \
    DIRECTUS_TOKEN=L1G8Jhh53eYBTcc7pa6WrTTcGZKjN_lq \
    go run ./cmd/scgoetl \
      --channel=LIVE \
      --version=4.5.0-1 \
      --data-root=./data
