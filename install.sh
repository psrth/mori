#!/bin/sh
set -e

REPO="psrth/mori"
BINARY="mori"
INSTALL_DIR="/usr/local/bin"

# Detect OS
OS="$(uname -s)"
case "$OS" in
  Linux*)  OS="linux" ;;
  Darwin*) OS="darwin" ;;
  *)       echo "Unsupported OS: $OS" && exit 1 ;;
esac

# Detect architecture
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64)  ARCH="amd64" ;;
  aarch64) ARCH="arm64" ;;
  arm64)   ARCH="arm64" ;;
  *)       echo "Unsupported architecture: $ARCH" && exit 1 ;;
esac

LATEST=$(curl -sI "https://github.com/${REPO}/releases/latest" | grep -i "^location:" | sed 's/.*tag\///' | tr -d '\r\n')

if [ -z "$LATEST" ]; then
  echo "Error: could not determine latest release"
  exit 1
fi

URL="https://github.com/${REPO}/releases/download/${LATEST}/${BINARY}_${OS}_${ARCH}.tar.gz"

echo "Downloading mori ${LATEST} for ${OS}/${ARCH}..."

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

curl -sL "$URL" -o "$TMP/mori.tar.gz"
tar -xzf "$TMP/mori.tar.gz" -C "$TMP"

if [ -w "$INSTALL_DIR" ]; then
  mv "$TMP/$BINARY" "$INSTALL_DIR/$BINARY"
else
  echo "Installing to $INSTALL_DIR (requires sudo)..."
  sudo mv "$TMP/$BINARY" "$INSTALL_DIR/$BINARY"
fi

chmod +x "$INSTALL_DIR/$BINARY"

echo "mori installed successfully to $INSTALL_DIR/$BINARY"
echo "Run 'mori --help' to get started."
