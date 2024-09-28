#!/bin/bash
set -e

# Variables to configure
AWS_REGION="us-east-1"
REPOSITORY_NAME="fastflight"
IMAGE_NAME="flight-server"
TAG="latest"

# Function to display usage information
usage() {
    echo "Usage: $0 [--push]"
    echo "  --push    Push the Docker image to AWS ECR after building"
    exit 1
}

# Parse command-line arguments
PUSH_TO_ECR=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --push) PUSH_TO_ECR=true ;;
        *) usage ;;
    esac
    shift
done


# Get the directory where the script is located without changing to it
cd "$(dirname "$0")/.."

# Step 3: Generate the requirements.txt (assuming you are using uv for dependency management)
echo "Generating requirements.txt..."
uv pip compile pyproject.toml -o requirements.txt

# Build the Docker image using the Dockerfile in the scripts folder
echo "Building Docker image..."
docker buildx create --use || echo "Buildx already enabled"
docker build -t ${IMAGE_NAME}:${TAG} -f docker/Dockerfile --platform linux/amd64 .

echo "Docker image built successfully"

rm requirements.txt


# Optionally push the Docker image to AWS ECR
if [ "$PUSH_TO_ECR" = true ]; then
  echo "Pushing Docker image to AWS ECR..."

  # Get ECR repository URI
  REPO_URI=$(aws ecr describe-repositories --repository-names "$REPOSITORY_NAME" --query 'repositories[0].repositoryUri' --output text --region "$AWS_REGION")

  if [ -z "$REPO_URI" ]; then
    echo "ECR repository not found!"
    exit 1
  else
    echo "ECR Repository URI: $REPO_URI"
  fi

  # Log in to AWS ECR
  aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$AWS_ACCOUNT_ID".dkr.ecr."$AWS_REGION".amazonaws.com

  # Tag and push the image to ECR
  docker tag "$IMAGE_NAME":"$TAG" "$REPO_URI":"$TAG"
  docker push "$REPO_URI":"$TAG"
else
  echo "Skipping push to AWS ECR."
fi
