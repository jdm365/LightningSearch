build_static_lib:
	rm -rf ./target/debug/build/parquet_bindings-*
	cargo build
	mv ./target/debug/libparquet_bindings.a ../lib/
	mv ./target/debug/build/parquet_bindings-*/out/parquet_bindings.h ../lib/



build_static_lib_release:
	rm -rf ./target/release/build/parquet_bindings-*
	cargo build --release
	mv ./target/release/libparquet_bindings.a ../lib/
	mv ./target/release/build/parquet_bindings-*/out/parquet_bindings.h ../lib/
