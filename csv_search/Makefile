build: SHELL:=/bin/bash
build:
	zig build shared;

	swiftc src/swift/main.swift \
		-sdk $(xcrun --show-sdk-path) \
		-o src/swift/build/search_app \
		-L zig-out/lib \
		-lsearch_app \
		-Xlinker -rpath -Xlinker @executable_path/;

	mv zig-out/lib/libsearch_app.dylib src/swift/build;

exec: SHELL:=/bin/bash
exec:
	./src/swift/build/search_app

run: build exec
