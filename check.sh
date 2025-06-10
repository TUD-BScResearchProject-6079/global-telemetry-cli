#!/bin/bash

set -e
set -o pipefail

echo "Checking code formatting with black..."
black --check .

echo "Checking import order with isort..."
isort --check-only .

echo "Running flake8 for linting..."
flake8 .

echo "Running mypy for type checking..."
mypy .

echo "All checks passed!"
