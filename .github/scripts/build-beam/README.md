Build docker image  
```
docker buildx build \
  --platform=linux/amd64 \
  --build-arg GIT_REPO=https://github.com/Polber/beam.git \
  --build-arg GIT_BRANCH=jkinard/yaml-templatization \
  . -t test-beam
```

Run docker image  
```
docker run --rm -it --platform linux/amd64 test-beam
```
