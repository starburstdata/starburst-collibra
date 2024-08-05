#!/bin/bash

CLEANUP_ENV=true

get_latest_lts()
{
    STARBURST_VERSION=$(curl -s GET https://registry.hub.docker.com/v2/repositories/starburstdata/starburst-enterprise/tags?page_size=150 \
        | jq -r '.results[].name | select(. | test("^[0-9]{3}-e.[0-9]"))' | grep -v 64 | sort -r | head -1)
}

clean_up()
{
    echo ""
    echo "====== Cleaning up ======"
    docker-compose -f tests/docker/docker-compose.yml down
}

run_tests()
{
    echo ""
    echo "====== Setting up test environment ======"
    docker-compose -f tests/docker/docker-compose.yml up -d

    SEP_STARTING=true
    START_TIMEOUT=300
    while $SEP_STARTING; do

        if [[ $(python tests/probe_coordinator.py) -eq 0 ]]; then
            SEP_STARTING=false
            echo "Coordinator started"
        elif [ $( docker ps | grep starburst | wc -l ) -gt 0 ] && [ $START_TIMEOUT -gt 0 ]; then
            echo "Coordinator still starting, sleeping 10s"
            let "START_TIMEOUT = START_TIMEOUT - 10"
            sleep 10
        else
            echo "Coordinator failed to start. Exiting"
            docker-compose -f tests/docker/docker-compose.yml down
            exit 1
        fi
    done

    echo ""
    echo "====== Creating test data products ======"
    python tests/create_data_products.py

    echo ""
    echo "====== Running test suite ======"
    pytest

    status=$?
    if [ $status -ne 0 ]; then
        if [ "${CLEANUP_ENV}" == "true" ]; then
            echo "Tests failed. Exiting."
            clean_up
        else
            echo "Tests failed. Keeping test environment running"
        fi
    fi
}

usage()
{
    echo "Usage: ./test-sep-dp.sh [OPTION]... "
    echo "Optional parameters:"
    echo "  -k, --keep-env           Keep test environment running. Use 'tests/docker/docker-compose.yml down' to tear down when done"
    echo "  -h, --help               display this help and exit"
}

export INPUT_ARGUMENTS="${@}"
while [[ $# -gt 0 ]]; do
    case $1 in
        -k | --keep-env )       CLEANUP_ENV=false
                                ;;
        -h | --help )           usage
                                exit 1
                                ;;
        * )
    esac
    shift
done

echo "Clean up env: $CLEANUP_ENV"

get_latest_lts
echo "STARBURST_VERSION=$STARBURST_VERSION" > tests/docker/.env
run_tests

if [ "${CLEANUP_ENV}" == "true" ]; then
    clean_up
else
    echo "=============================="
    echo "Test environment still running."
    echo "Use 'docker-compose -f tests/docker/docker-compose.yml down' to tear down when done"
fi
