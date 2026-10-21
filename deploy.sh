#!/usr/bin/env bash
# Deploys a given image tag on the Airflow EC2 instance (called by CI through SSM).
# Usage: ./deploy.sh <git sha>
set -euo pipefail

TAG="${1:?usage: deploy.sh <tag>}"
cd "$(dirname "$0")"

# Account, region and repository come from the instance's .env (see energia-infra).
set -a; source .env; set +a
REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.sa-east-1.amazonaws.com"
export ECR_IMAGE="${REGISTRY}/energia-data:${TAG}"

aws ecr get-login-password --region sa-east-1 | docker login --username AWS --password-stdin "$REGISTRY"
git fetch --quiet origin main && git checkout --quiet "$TAG"
docker pull "$ECR_IMAGE"
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --remove-orphans
docker image prune -f
echo "Deployed ${ECR_IMAGE}"
