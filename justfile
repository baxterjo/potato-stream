
stream:
    RUST_LOG=debug cargo run -- --name test_stream --listen 0.0.0.0:10000 --connect 192.168.1.29:9201 stream --loopback

watch-local:
    RUST_LOG=debug cargo run -- --name test_stream --connect 127.0.0.1:10000 watch

watch-remote:
    RUST_LOG=debug cargo run -- --name test_stream --connect 192.168.1.29:9201 watch

repeat:
    RUST_LOG=debug cargo run --no-default-features -- --name test_stream --listen 0.0.0.0:9201 watch

test:
    RUST_LOG=debug cargo test
