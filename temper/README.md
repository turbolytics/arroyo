# Arroyo map

[Temper](https://github.com/turbolytics/temper) map of what the FileSystem sink
promises when a source finishes, checked against Arroyo 0.16.0. `MATRIX.md` at
the repository root is the result.

- `lattices/sink.contract.yml`: every row a finished source emits ends up in
  exactly one committed file.
- `subjects/`: one file per way of writing files: the default sink on S3
  (`v2`), `'sink.version' = 'v1'` on S3, and a local path.
- `../testing/temper/`: the tests. Each one states what it proves with
  `@pytest.mark.covers`.

Run it:

```sh
docker network create temper-arroyo
docker run -d --name temper-seaweed --network temper-arroyo --network-alias seaweed \
  -p 28333:8333 chrislusf/seaweedfs:3.97 server -dir=/data -s3 -s3.port=8333
curl -X PUT http://localhost:28333/arroyo
docker run -d --name temper-arroyo --network temper-arroyo -p 25115:5115 \
  -e ARROYO__CHECKPOINT_URL=s3://arroyo/checkpoints -e AWS_ENDPOINT=http://seaweed:8333 \
  -e AWS_ALLOW_HTTP=true -e AWS_ACCESS_KEY_ID=temper -e AWS_SECRET_ACCESS_KEY=temper \
  -e AWS_REGION=us-east-1 ghcr.io/arroyosystems/arroyo:0.16.0 cluster
python3 -m venv .venv-temper
.venv-temper/bin/pip install boto3 requests pytest temper-python
brew install turbolytics/tap/temper   # or: go install github.com/turbolytics/temper/cmd/temper@v0.1.3
temper clean
temper run --level ci -- .venv-temper/bin/pytest -q testing/temper
temper matrix --all-levels
```

`temper matrix` exits 1 while any cell is failing.
