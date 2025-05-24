#!/bin/bash

set -e  # Exit on any error
set -o pipefail

echo "Checking code formatting with black..."
black .

echo "Checking import order with isort..."
isort .

echo "Running flake8 for linting..."
flake8 .

echo "Running mypy for type checking..."
mypy .

echo "All checks passed!"
