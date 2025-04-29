EXEC_PATH := ./swift/build/search_app

build:
	zig build shared
	mkdir -p src/swift/build
	swiftc -O swift/main.swift \
		-sdk "$(shell xcrun --show-sdk-path)" \
		-o swift/build/search_app \
		-L zig-out/lib \
		-lsearch_app \
		-Xlinker -rpath -Xlinker @executable_path/
	mv zig-out/lib/libsearch_app.dylib swift/build/

exec:
	$(EXEC_PATH)

run: build exec

app:
	zig build -Doptimize=ReleaseSafe
	mv zig-out/bin/lightning_search electron_app/
	cd electron_app && npm start

debug:
	lldb $(EXEC_PATH)
