#!/usr/bin/env bash
set -eE

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
REPO_DIR=$(cd "${SCRIPT_DIR}/../" && pwd -P)

SERVICE_NAME="ton-kafka-producer"

function print_help() {
  echo "Usage: update.sh [OPTIONS]"
  echo ""
  echo "Options:"
  echo "  -h,--help         Print this help message and exit"
  echo "  -f,--force        Clear \"/var/db/${SERVICE_NAME}\" on update"
  echo "  -s,--sync         Restart \"timesyncd\" service"
  echo "  -r,--reload-cfg   Remove current network config and download a new one"
}

force="false"
restart_timesyncd="false"
reload_config="false"
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
      -h|--help)
        print_help
        exit 0
      ;;
      -f|--force)
        force="true"
        shift # past argument
      ;;
      -s|--sync)
        restart_timesyncd="true"
        shift # past argument
      ;;
      -r|--reload-cfg)
        reload_config="true"
        shift # past argument
      ;;
      *) # unknown option
        echo "ERROR: Unknown option"
        echo ""
        print_help
        exit 1
      ;;
  esac
done

echo "INFO: stopping ${SERVICE_NAME} service"
sudo systemctl stop ${SERVICE_NAME}

if [[ "$force" == "true" ]]; then
  echo "INFO: removing ${SERVICE_NAME} db"
  sudo rm -rf "/var/db/${SERVICE_NAME}"
else
  echo "INFO: skipping \"/var/db/${SERVICE_NAME}\" deletion"
fi

echo "INFO: running update for native installation"

echo "INFO: building ${SERVICE_NAME}"
cd "$REPO_DIR"
RUSTFLAGS="-C target_cpu=native" cargo build --release
sudo cp "$REPO_DIR/target/release/${SERVICE_NAME}" "/usr/local/bin/${SERVICE_NAME}"


if [[ "$reload_config" == "true" ]]; then
  echo "INFO: reload network configuration"
  sudo wget -O "/etc/${SERVICE_NAME}/ton-global.config.json" \
    https://raw.githubusercontent.com/tonlabs/main.ton.dev/master/configs/ton-global.config.json
fi

echo "INFO: preparing environment"
sudo mkdir -p "/var/db/${SERVICE_NAME}"

if [[ "$restart_timesyncd" == "true" ]]; then
  echo "INFO: restarting timesyncd"
  sudo systemctl restart systemd-timesyncd.service
fi

echo "INFO: restarting ${SERVICE_NAME} service"
sudo systemctl restart ${SERVICE_NAME}

echo "INFO: done"
echo ""
echo "INFO: Systemd service: ${SERVICE_NAME}"
echo "      Keys and configs: /etc/${SERVICE_NAME}"
echo "      Node DB and stuff: /var/db/${SERVICE_NAME}"
