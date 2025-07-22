BIN_PATH  := lightning_search.bin
DATA_PATH := ../../data/mb.csv

install_python:
	## zig build shared -Doptimize=ReleaseFast
	zig build -Doptimize=ReleaseFast
	mv zig-out/lib/liblightning_search.so ./python/lightning_search/
	mv zig-out/include/ffi.h ./python/lightning_search/ffi.h
	cd python && uv pip install .

bench:
	zig build -Doptimize=ReleaseFast
	perf record ./zig-out/bin/$(BIN_PATH)
	perf report

exec:
	$(EXEC_PATH)

run: build exec

app:
	zig build
	mv zig-out/bin/$(BIN_PATH) web_app/
	cd web_app && ./$(BIN_PATH) $(DATA_PATH)

app_release:
	zig build -Doptimize=ReleaseSafe
	mv zig-out/bin/$(BIN_PATH) web_app/
	cd web_app && ./$(BIN_PATH) $(DATA_PATH)

app_release_fast:
	zig build -Doptimize=ReleaseFast
	mv zig-out/bin/$(BIN_PATH) web_app/
	cd web_app && ./$(BIN_PATH) $(DATA_PATH)

debug:
	lldb $(EXEC_PATH)
