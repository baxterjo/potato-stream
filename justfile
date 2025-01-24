set dotenv-load

stream:
    RUST_LOG=debug cargo run -- --name test_stream stream --loopback

watch:
    RUST_LOG=debug cargo run -- --name test_stream watch

test:
    RUST_LOG=debug cargo test
