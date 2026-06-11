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
keyUsage = digitalSignature, keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1  = 127.0.0.1
IP.2  = 192.168.1.234
EOF

# Generate ECDSA P-256 private key (bắt buộc cho WebTransport)
# Rustls yêu cầu định dạng PKCS#8 nên phải convert từ SEC1 sang PKCS#8
openssl ecparam -name prime256v1 -genkey -noout -out temp.key
openssl pkcs8 -topk8 -nocrypt -in temp.key -out private.key
rm temp.key

# Generate self-signed certificate
openssl req -new -x509 \
  -key private.key \
  -out certificate.pem \
  -days 13 \
  -config cert.conf \
  -extensions v3_req

echo "✅ Done!"
echo " - private.key"
echo " - certificate.pem"

openssl x509 -in certificate.pem -outform der | openssl dgst -sha256 -binary | xxd -i

