#!/usr/bin/env bash
set -euo pipefail
repo_dir="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$repo_dir"
versions=(8.4.0 9.7.0)
maven_args=(-B)
while (($#)); do
    case "$1" in
        --driver)
            if (($# < 2)) || [[ "$2" != 8.4.0 && "$2" != 9.7.0 ]]; then
                echo "--driver requires 8.4.0 or 9.7.0" >&2
                exit 2
            fi
            versions=("$2")
            shift 2
            ;;
        *) maven_args+=("$1"); shift ;;
    esac
done
cargo build -p opensrv-mysql --example connector_j_fixture
# Each Maven/JVM run has exactly one driver. JUnit owns a fresh fixture on an
# ephemeral port; version-specific build/report directories prevent overwrites.
for version in "${versions[@]}"; do
    case "$version" in
        8.4.0) suite=ConnectorJ840Test ;;
        9.7.0) suite=ConnectorJ970Test ;;
    esac
    echo "Running Connector/J $version ($suite)"
    mvn -f mysql/tests/connector-j/pom.xml "${maven_args[@]}" \
        "-Dopensrv.fixture=$repo_dir/target/debug/examples/connector_j_fixture" \
        "-Dconnector-j.version=$version" "-Dconnector-j.test=$suite" test
done
