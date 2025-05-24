#!/bin/bash
set -e

# Configuration
AWS_REGION="us-east-1"
REPOSITORY_NAME="fastflight"
IMAGE_NAME="fastflight"
TAG="latest"

usage() {
    echo "Usage: $0 [--push] [--tag TAG]"
    echo "  --push       Push to AWS ECR after building"
    echo "  --tag TAG    Docker tag (default: latest)"
    exit 1
}

# Parse arguments
PUSH_TO_ECR=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --push) PUSH_TO_ECR=true ;;
        --tag) TAG="$2"; shift ;;
        *) usage ;;
    esac
    shift
done

# Change to project root
cd "$(dirname "$0")/.."

echo "Building FastFlight Docker image..."
docker build -t ${IMAGE_NAME}:${TAG} -f docker/Dockerfile --platform linux/amd64 .

echo "✅ Docker image built: ${IMAGE_NAME}:${TAG}"

# Push to ECR if requested
if [ "$PUSH_TO_ECR" = true ]; then
    echo "Pushing to AWS ECR..."
    
    # Get ECR repository URI
    REPO_URI=$(aws ecr describe-repositories \
        --repository-names "$REPOSITORY_NAME" \
        --query 'repositories[0].repositoryUri' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null)

    if [ -z "$REPO_URI" ] || [ "$REPO_URI" = "None" ]; then
        echo "❌ ECR repository '$REPOSITORY_NAME' not found in region $AWS_REGION"
        echo "Create it with: aws ecr create-repository --repository-name $REPOSITORY_NAME --region $AWS_REGION"
        exit 1
    fi

    echo "ECR Repository: $REPO_URI"

    # Login to ECR
    aws ecr get-login-password --region "$AWS_REGION" | \
        docker login --username AWS --password-stdin "${REPO_URI%/*}"

    # Tag and push
    docker tag "$IMAGE_NAME:$TAG" "$REPO_URI:$TAG"
    docker push "$REPO_URI:$TAG"
    
    echo "✅ Pushed to ECR: $REPO_URI:$TAG"
else
    echo "Skipping ECR push. Use --push to enable."
fi

echo "
🚀 Docker build complete!

Usage examples:
  # Run FastFlight server
  docker run -p 8815:8815 ${IMAGE_NAME}:${TAG}
  
  # Run FastAPI server  
  docker run -p 8000:8000 ${IMAGE_NAME}:${TAG} start-fastapi
  
  # Run both servers
  docker run -p 8000:8000 -p 8815:8815 ${IMAGE_NAME}:${TAG} start-all
"
