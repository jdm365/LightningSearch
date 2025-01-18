import Cocoa
import Foundation

// Bridge to C functions
private let searchLib = dlopen(nil, RTLD_NOW)

private let init_allocators = dlsym(searchLib, "init_allocators")
    .map { unsafeBitCast($0, to: (@convention(c) () -> Void).self) }

private let get_handler = dlsym(searchLib, "get_query_handler_local")
    .map { unsafeBitCast($0, to: (@convention(c) () -> UnsafeMutableRawPointer?).self) }

private let search_func = dlsym(searchLib, "search")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeMutableRawPointer?,
        UnsafePointer<CChar>?,
        UnsafeMutablePointer<UInt32>?,
        UnsafeMutablePointer<UInt32>?,
        UnsafeMutablePointer<UInt32>?,
        UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
    ) -> Void).self) }

private let get_names = dlsym(searchLib, "get_column_names")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer,
        UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>,
        UnsafeMutablePointer<UInt32>
    ) -> Void).self) }

class SearchBridge {
    private var queryHandler: UnsafeMutableRawPointer?
    private var resultCount: UInt32 = 0
    private var startPositions = [UInt32](repeating: 0, count: 100 * 3)
    private var lengths = [UInt32](repeating: 0, count: 100 * 3)
    private var resultBuffers = [UnsafeMutablePointer<CChar>?](repeating: nil, count: 100)
    private var numColumns: UInt32 = 0
    
    init() {
        if let initFn = init_allocators {
            print("Initializing allocators")
            initFn()
        } else {
            print("Failed to get init_allocators function")
        }

        if let handlerFn = get_handler {
            queryHandler = handlerFn()
            print("QueryHandler initialized: \(String(describing: queryHandler))")
        } else {
            print("Failed to get handler function")
        }
    }

    func getColumnNames() -> [String] {
        guard let handler = queryHandler else {
            print("QueryHandler is nil")
            return []
        }

        let names = UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>.allocate(capacity: 128)
        defer {
            // First free each string buffer
            for i in 0..<128 {
                if let ptr = names[i] {
                    ptr.deallocate()
                }
            }
            // Then free the array of pointers
            names.deallocate()
        }
        
        // Allocate buffers for each potential string (assuming max length of 256 chars)
        for i in 0..<128 {
            names[i] = UnsafeMutablePointer<CChar>.allocate(capacity: 256)
        }
        
        if let fn = get_names {
            print("Swift: about to call with handler address: \(String(describing: handler))")
    fn(handler, names, &numColumns)
            fn(handler, names, &numColumns)
            
            var columnNames: [String] = []
            for i in 0..<Int(numColumns) {
                if let namePtr = names[i] {
                    columnNames.append(String(cString: namePtr))
                }
            }
            return columnNames
        }
        
        return []
    }
    
    func performSearch(query: String) -> [[String]] {
        // var resultCount: UInt32 = 0
        // var startPositions = [UInt32](repeating: 0, count: 100 * 3)
        // var lengths = [UInt32](repeating: 0, count: 100 * 3)
        // var resultBuffers = [UnsafeMutablePointer<CChar>?](repeating: nil, count: 100)
        resultCount = 0
        
        print("Query: \(query)")
        var startTime = CFAbsoluteTimeGetCurrent()
        query.withCString { queryStr in
            search_func?(
                queryHandler,
                queryStr,
                &resultCount,
                &startPositions,
                &lengths,
                &resultBuffers
            )
        }
        var timeElapsed = CFAbsoluteTimeGetCurrent() - startTime
        print("Time elapsed: \(timeElapsed) seconds")
        print("Found \(resultCount) results")
        startTime = CFAbsoluteTimeGetCurrent()
        var results: [[String]] = []
        for i in 0..<Int(resultCount) {
            var row: [String] = []
            for j in 0..<Int(numColumns) {
                let start = Int(startPositions[i * Int(numColumns) + j])
                let length = Int(lengths[i * Int(numColumns) + j])
                if let buffer = resultBuffers[i] {
                    let data = Data(bytes: buffer.advanced(by: start), count: length)
                    if let str = String(data: data, encoding: .utf8) {
                        row.append(str)
                    }
                }
            }
            results.append(row)
        }
        timeElapsed = CFAbsoluteTimeGetCurrent() - startTime
        print("Time elapsed populate: \(timeElapsed) seconds")
        
        return results
    }
}

class AppDelegate: NSObject, NSApplicationDelegate, NSTableViewDataSource, NSTableViewDelegate {
    var window: NSWindow!
    var tableView: NSTableView!
    var searchFields: [NSSearchField] = []
    var searchResults: [[String]] = []
    var searchStrings: [String] = []
    let searchBridge = SearchBridge()
    
    func applicationDidFinishLaunching(_ notification: Notification) {
        let columnNames = searchBridge.getColumnNames()

        // Create window
        window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1000, height: 600),
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )

        if let firstSearchField = searchFields.first {
            window.makeFirstResponder(firstSearchField)
        }
        
        // Create search container
        let searchContainer = NSView(
            frame: NSRect(x: 0, y: 0, width: 1000, height: 50)
        )
        window.contentView?.addSubview(searchContainer)
        searchContainer.translatesAutoresizingMaskIntoConstraints = false
        
        // Add search fields
        let colPadding = 20
        let colWidth   = CGFloat(1000 / columnNames.count) * 2
        for i in 0..<columnNames.count {
            let searchField = NSSearchField(
                frame: NSRect(x: colPadding + i * (colPadding + Int(colWidth)), y: 10, width: Int(colWidth), height: 30)
            )
            searchField.placeholderString = "Search \(columnNames[i])..."
            searchField.sendsSearchStringImmediately = true
            searchContainer.addSubview(searchField)
            searchField.target = self
            searchField.action = #selector(searchFieldChanged(_:))
            searchField.tag = i
            searchFields.append(searchField)
        }
        
        // Create scroll view and table
        let scrollView = NSScrollView(
            frame: NSRect(x: 0, y: 0, width: 1000, height: 550)
        )
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        
        tableView = NSTableView(frame: NSRect(x: 0, y: 0, width: 1000, height: 550))
        tableView.dataSource = self
        tableView.delegate = self
        
        // Add columns
        for i in 0..<columnNames.count {
            let column = NSTableColumn(
                identifier: NSUserInterfaceItemIdentifier("column\(i)")
            )
            column.title = columnNames[i]
            column.width = colWidth;
            tableView.addTableColumn(column)

            searchStrings.append("")
        }
        
        scrollView.documentView = tableView
        window.contentView?.addSubview(scrollView)
        
        // Set up constraints
        if let contentView = window.contentView {
            NSLayoutConstraint.activate([
                searchContainer.topAnchor.constraint(equalTo: contentView.topAnchor),
                searchContainer.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
                searchContainer.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
                searchContainer.heightAnchor.constraint(equalToConstant: 50)
            ])
            
            scrollView.translatesAutoresizingMaskIntoConstraints = false
            NSLayoutConstraint.activate([
                scrollView.topAnchor.constraint(equalTo: searchContainer.bottomAnchor),
                scrollView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
                scrollView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
                scrollView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor)
            ])
        }
        
        window.title = "Music Search"
        window.center()

        // tableView.backgroundColor = .clear
        // tableView.enclosingScrollView?.drawsBackground = false
        window.appearance = NSAppearance(named: .vibrantDark)

        NSApp.setActivationPolicy(.regular)
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)

        if let firstSearchField = searchFields.first {
            window.makeFirstResponder(firstSearchField)
        }

        ProcessInfo.processInfo.enableSuddenTermination()
    }
    
    @objc func searchFieldChanged(_ sender: NSSearchField) {
        let startTime = Date()

        let column = sender.tag
        let query = sender.stringValue

        searchStrings[column] = query
        
        // Build query string
        var queryParts = ["TITLE=", "ARTIST=", "ALBUM="]
        for i in 0..<3 {
            queryParts[i] += searchStrings[i]
        }
        let queryString = queryParts.joined(separator: "&")
        
        DispatchQueue.global(qos: .userInitiated).async {
            let searchStartTime = Date()
            let results = self.searchBridge.performSearch(query: queryString)
            let searchTime = -searchStartTime.timeIntervalSinceNow
            
            DispatchQueue.main.async {
                let reloadStartTime = Date()
                self.searchResults = results
                self.tableView.reloadData()
                let reloadTime = -reloadStartTime.timeIntervalSinceNow
                
                let totalTime = -startTime.timeIntervalSinceNow

                print("Search Performance Breakdown:")
                print("- Backend Search Time: \(searchTime * 1000) ms")
                print("- Table Reload Time: \(reloadTime * 1000) ms")
                print("- Total Time: \(totalTime * 1000) ms")
            }
        }
    }
    
    func numberOfRows(in tableView: NSTableView) -> Int {
        return searchResults.count
    }
    
    func tableView(_ tableView: NSTableView, objectValueFor tableColumn: NSTableColumn?, row: Int) -> Any? {
        guard let columnIdentifier = tableColumn?.identifier else { return nil }
        let rowData = searchResults[row]
        let columnIndex = Int(columnIdentifier.rawValue.dropFirst(6))!
        return rowData[columnIndex]
    }
    
    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        return true
    }
}

let app = NSApplication.shared
let delegate = AppDelegate()
app.delegate = delegate
app.run()
