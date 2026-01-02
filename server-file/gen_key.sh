#!/bin/bash
set -e

echo "🔐 Generating self-signed TLS certificate for QUIC..."

# Tạo file config cho OpenSSL
cat > cert.conf <<'EOF'
[req]
distinguished_name = dn
req_extensions = v3_req
prompt = no

[dn]
CN = quic-local

[v3_req]
keyUsage = keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1  = 127.0.0.1
IP.2  = 192.168.1.234
EOF

# Generate private key
openssl genrsa -out private.key 2048

# Generate self-signed certificate
openssl req -new -x509 \
  -key private.key \
  -out certificate.pem \
  -days 365 \
  -config cert.conf \
  -extensions v3_req

echo "✅ Done!"
echo " - private.key"
echo " - certificate.pem"
