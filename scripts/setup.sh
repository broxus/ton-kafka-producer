#!/usr/bin/env bash
set -eE

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
REPO_DIR=$(cd "${SCRIPT_DIR}/../" && pwd -P)

SERVICE_NAME="ton-kafka-producer"

function print_help() {
  echo 'Usage: setup.sh'
  echo ''
  echo 'Options:'
  echo '  -h,--help         Print this help message and exit'
  echo '  -s,--sync         Restart "timesyncd" service'
}

restart_timesyncd="false"
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
      -h|--help)
        print_help
        exit 0
      ;;
      -s|--sync)
        restart_timesyncd="true"
        shift # past argument
      ;;
      *) # unknown option
        echo 'ERROR: Unknown option'
        echo ''
        print_help
        exit 1
      ;;
  esac
done

service_path="/etc/systemd/system/${SERVICE_NAME}.service"
config_path="/etc/${SERVICE_NAME}/config.yaml"

echo 'INFO: Running native installation'

echo 'INFO: installing and updating dependencies'
sudo apt update && sudo apt upgrade
sudo apt install build-essential llvm clang pkg-config libssl-dev

echo 'INFO: installing Rust'
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

echo "INFO: building ${SERVICE_NAME}"
cd "$REPO_DIR"
RUSTFLAGS="-C target_cpu=native" cargo build --release
sudo cp "$REPO_DIR/target/release/${SERVICE_NAME}" "/usr/local/bin/${SERVICE_NAME}"

echo 'INFO: creating systemd service'
if [[ -f "$service_path" ]]; then
  echo "WARN: $service_path already exists"
else
  sudo cp "$REPO_DIR/contrib/native.service" "$service_path"
fi

echo "INFO: preparing environment"
sudo mkdir -p "/etc/${SERVICE_NAME}"
sudo mkdir -p "/var/db/${SERVICE_NAME}"
if [[ -f "$config_path" ]]; then
  echo "WARN: $config_path already exists"
else
  sudo cp -n "$REPO_DIR/contrib/config.yaml" "$config_path"
fi

sudo wget -O "/etc/${SERVICE_NAME}/ton-global.config.json" \
  https://raw.githubusercontent.com/tonlabs/main.ton.dev/master/configs/ton-global.config.json

if [[ "$restart_timesyncd" == "true" ]]; then
  echo 'INFO: restarting timesyncd'
  sudo systemctl restart systemd-timesyncd.service
fi

echo "INFO: done"
echo ""
echo "INFO: Systemd service: ${SERVICE_NAME}"
echo "      Keys and configs: /etc/${SERVICE_NAME}"
echo "      Node DB and stuff: /var/db/${SERVICE_NAME}"
