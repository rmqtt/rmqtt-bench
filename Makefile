all: release docker

debug:
	git checkout $$(git describe --tags $$(git rev-list --tags --max-count=1))
	cargo build --target x86_64-unknown-linux-musl

release:
	git checkout $$(git describe --tags $$(git rev-list --tags --max-count=1))
	cargo build --release --target x86_64-unknown-linux-musl

docker:
	docker build --no-cache -t rmqtt/rmqtt-bench:$$(git describe --tags $$(git rev-list --tags --max-count=1)) ./
	docker push rmqtt/rmqtt-bench:$$(git describe --tags $$(git rev-list --tags --max-count=1))
	docker build --no-cache -t rmqtt/rmqtt-bench:latest ./
	docker push rmqtt/rmqtt-bench:latest

clean:
	cargo clean
