// import Cocoa
import AppKit 
import Foundation
import UniformTypeIdentifiers

let WINDOW_WIDTH  = 1200
let WINDOW_HEIGHT = 900 


// Bridge to C functions
private let searchLib = dlopen(nil, RTLD_NOW)

private let init_allocators = dlsym(searchLib, "init_allocators")
    .map { unsafeBitCast($0, to: (@convention(c) () -> Void).self) }

private let get_handler = dlsym(searchLib, "getQueryHandlerLocal")
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

private let read_header = dlsym(searchLib, "readHeader")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer,
        UnsafePointer<CChar>?
    ) -> Void).self) }

private let get_cols = dlsym(searchLib, "getColumnNames")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer,
        UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>,
        UnsafeMutablePointer<UInt32>
    ) -> Void).self) }

private let get_search_cols = dlsym(searchLib, "getSearchColumns")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer,
        UnsafeMutablePointer<CChar>
    ) -> Void).self) }

private let add_search_col = dlsym(searchLib, "addSearchCol")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer,
        UnsafePointer<CChar>
    ) -> Void).self) }

private let index_file = dlsym(searchLib, "indexFile")
    .map { unsafeBitCast($0, to: (@convention(c) (
        UnsafeRawPointer
    ) -> Void).self) }

class SearchBridge {
    var queryHandler: UnsafeMutableRawPointer?
    private var resultCount: UInt32 = 0
    private var startPositions = [UInt32](repeating: 0, count: 100 * 3)
    private var lengths = [UInt32](repeating: 0, count: 100 * 3)
    private var resultBuffers = [UnsafeMutablePointer<CChar>?](repeating: nil, count: 100)
    private var numColumns: UInt32 = 0
    var columnNames: [String] = []
    var searchColumnNames: [String] = []
    
    init() {
        if let initFn = init_allocators {
            initFn()
        } else {
            print("Failed to get init_allocators function")
        }

        if let handlerFn = get_handler {
            queryHandler = handlerFn()
        } else {
            print("Failed to get handler function")
        }
    }

    func getColumnNames() -> Void {
        guard let handler = queryHandler else {
            print("QueryHandler is nil")
            return
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
        
        if let fn = get_cols {
            fn(handler, names, &numColumns)
            
            for i in 0..<Int(numColumns) {
                if let namePtr = names[i] {
                    columnNames.append(String(cString: namePtr))
                }
            }
            return
        }
    }

    func getSearchColumns() -> Void {
        guard let handler = queryHandler else {
            print("QueryHandler is nil")
            return
        }

        let colMask = UnsafeMutablePointer<CChar>.allocate(capacity: columnNames.count)
        defer {
            colMask.deallocate()
        }

        for i in 0..<columnNames.count {
            colMask[i] = 0;
        }

        if let fn = get_search_cols {
            fn(handler, colMask)
            
            for i in 0..<Int(columnNames.count) {
                if colMask[i] == 1 {
                    searchColumnNames.append(columnNames[i])
                }
            }
            return
        }
    }
    
    func performSearch(query: String) -> [[String]] {
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
   var searchBridge: SearchBridge!
   var columnSelectionWindow: NSWindow!
   var checkboxes: [NSButton] = []
   var searchContainer: NSView!
   var scrollView: NSScrollView!
   var fileSelectionView: NSView!
   
   func applicationDidFinishLaunching(_ notification: Notification) {
       print("Application Started.")
       window = NSWindow(
           contentRect: NSRect(x: 0, y: 0, width: WINDOW_WIDTH, height: WINDOW_HEIGHT),
           styleMask: [.titled, .closable, .miniaturizable, .resizable],
           backing: .buffered,
           defer: false
       )
       
       window.appearance = NSAppearance(named: .vibrantDark)
       
       fileSelectionView = NSView(frame: window.contentView!.bounds)
       window.contentView?.addSubview(fileSelectionView)
       fileSelectionView.translatesAutoresizingMaskIntoConstraints = false
       
       let label = NSTextField(frame: .zero)
       label.stringValue = "Please select a CSV file to index"
       label.isEditable = false
       label.isBordered = false
       label.drawsBackground = false
       label.alignment = .center
       label.font = .systemFont(ofSize: 16)
       label.translatesAutoresizingMaskIntoConstraints = false
       fileSelectionView.addSubview(label)
       
       let button = NSButton(frame: .zero)
       button.title = "Choose File"
       button.bezelStyle = .rounded
       button.target = self
       button.action = #selector(chooseFile)
       button.translatesAutoresizingMaskIntoConstraints = false
       fileSelectionView.addSubview(button)
       
       NSLayoutConstraint.activate([
           fileSelectionView.topAnchor.constraint(equalTo: window.contentView!.topAnchor),
           fileSelectionView.leadingAnchor.constraint(equalTo: window.contentView!.leadingAnchor),
           fileSelectionView.trailingAnchor.constraint(equalTo: window.contentView!.trailingAnchor),
           fileSelectionView.bottomAnchor.constraint(equalTo: window.contentView!.bottomAnchor),
           
           label.centerXAnchor.constraint(equalTo: fileSelectionView.centerXAnchor),
           label.bottomAnchor.constraint(equalTo: fileSelectionView.centerYAnchor, constant: -20),
           
           button.centerXAnchor.constraint(equalTo: fileSelectionView.centerXAnchor),
           button.topAnchor.constraint(equalTo: label.bottomAnchor, constant: 20)
       ])
       
       window.title = "Search"
       window.center()

       NSApp.setActivationPolicy(.regular)
       NSApp.activate(ignoringOtherApps: true)
       window.makeKeyAndOrderFront(nil)
       window.level = .floating

       searchBridge = SearchBridge()
   }

   @objc func chooseFile() {
       let openPanel = NSOpenPanel()
       openPanel.allowsMultipleSelection = false
       openPanel.canChooseDirectories = false
       openPanel.canChooseFiles = true
       openPanel.allowedContentTypes = [UTType.commaSeparatedText]
       
       openPanel.beginSheetModal(for: window) { [weak self] response in
           guard let self = self else { return }
           if response == .OK, let url = openPanel.url {
               self.readHeader(fileURL: url)
           }
       }
   }

   private func readHeader(fileURL: URL) {
       guard let cPath = fileURL.path.cString(using: .utf8) else {
           print("Failed to convert path to C string")
           return
       }
       guard let handler = searchBridge.queryHandler else {
           print("QueryHandler is nil")
           return
       }
       read_header!(handler, cPath)
       searchBridge.getColumnNames()
       showColumnSelection()
   }

   private func showColumnSelection() {
       columnSelectionWindow = NSWindow(
           contentRect: NSRect(x: 0, y: 0, width: 300, height: CGFloat(50 + searchBridge.columnNames.count * 30)),
           styleMask: [.titled, .closable],
           backing: .buffered,
           defer: false
       )
       
       let contentView = NSView(frame: columnSelectionWindow.contentRect(forFrameRect: columnSelectionWindow.frame))
       columnSelectionWindow.contentView = contentView
       
       let label = NSTextField(frame: NSRect(x: 20, y: CGFloat(10 + searchBridge.columnNames.count * 30), width: 260, height: 30))
       label.stringValue = "Select columns to include in search:"
       label.isEditable = false
       label.isBordered = false
       label.drawsBackground = false
       contentView.addSubview(label)
       
       checkboxes.removeAll()
       
       for (index, column) in searchBridge.columnNames.enumerated() {
           let checkbox = NSButton(frame: NSRect(x: 20, y: CGFloat((searchBridge.columnNames.count - 1 - index) * 30 + 20), width: 200, height: 20))
           checkbox.title = column
           checkbox.setButtonType(.switch)
           checkbox.state = .off
           contentView.addSubview(checkbox)
           checkboxes.append(checkbox)
       }
       
       let startButton = NSButton(frame: NSRect(x: 200, y: 20, width: 80, height: 32))
       startButton.title = "Start"
       startButton.bezelStyle = .rounded
       startButton.target = self
       startButton.action = #selector(startIndexing)
       contentView.addSubview(startButton)
       
       columnSelectionWindow.title = "Select Search Columns"
       columnSelectionWindow.center()
       window.beginSheet(columnSelectionWindow) { response in }
   }

   @objc func startIndexing() {
       guard let handler = searchBridge.queryHandler else {
           print("QueryHandler is nil")
           return
       }

       searchBridge.searchColumnNames = checkboxes.enumerated()
           .filter { $0.element.state == .on }
           .map { searchBridge.columnNames[$0.offset] }
       
       if searchBridge.searchColumnNames.isEmpty {
           let alert = NSAlert()
           alert.messageText = "Please select at least one column"
           alert.alertStyle = .warning
           alert.beginSheetModal(for: columnSelectionWindow) { _ in }
           return
       }

       window.endSheet(columnSelectionWindow)
       columnSelectionWindow = nil

       for subview in fileSelectionView.subviews {
           subview.isHidden = true
       }

       let label = NSTextField()
       label.isEditable = false
       label.isBordered = false
       label.drawsBackground = false
       label.stringValue = "Indexing file..."
       label.font = .systemFont(ofSize: 16)
       label.alignment = .center
       label.translatesAutoresizingMaskIntoConstraints = false
        
       window.contentView?.addSubview(label)
       NSLayoutConstraint.activate([
           label.centerXAnchor.constraint(equalTo: window.contentView!.centerXAnchor),
           label.centerYAnchor.constraint(equalTo: window.contentView!.centerYAnchor)
       ])

       // Do the indexing work asynchronously
       DispatchQueue.global(qos: .userInitiated).async {
           // Add columns and index in background
           for i in 0..<self.searchBridge.searchColumnNames.count {
               guard let cString = self.searchBridge.searchColumnNames[i].cString(using: .utf8) else {
                   print("Failed to convert column name to C string")
                   continue
               }
               add_search_col?(handler, cString)
           }

           index_file?(handler)
           
           // Set up UI on main thread
           DispatchQueue.main.async {
               label.removeFromSuperview()
               self.setupSearchInterface()
           }
       }
   }

   private func setupSearchInterface() {
       fileSelectionView.removeFromSuperview()
       
       searchContainer = NSView()
       window.contentView?.addSubview(searchContainer)
       searchContainer.translatesAutoresizingMaskIntoConstraints = false
       
       let searchColPadding = 20
       let searchColWidth = CGFloat(WINDOW_WIDTH / searchBridge.searchColumnNames.count)
       for i in 0..<searchBridge.searchColumnNames.count {
           let searchField = NSSearchField()
           searchField.placeholderString = "Search \(searchBridge.searchColumnNames[i])..."
           searchField.sendsSearchStringImmediately = true
           searchContainer.addSubview(searchField)
           searchField.target = self
           searchField.action = #selector(searchFieldChanged(_:))
           searchField.tag = i
           searchField.translatesAutoresizingMaskIntoConstraints = false
           
           NSLayoutConstraint.activate([
               searchField.leadingAnchor.constraint(equalTo: searchContainer.leadingAnchor, constant: CGFloat(searchColPadding + i * (searchColPadding + Int(searchColWidth)))),
               searchField.centerYAnchor.constraint(equalTo: searchContainer.centerYAnchor),
               searchField.widthAnchor.constraint(equalToConstant: searchColWidth),
               searchField.heightAnchor.constraint(equalToConstant: 30)
           ])
           
           searchFields.append(searchField)
           searchStrings.append("")
       }
       
       scrollView = NSScrollView()
       scrollView.hasVerticalScroller = true
       scrollView.hasHorizontalScroller = true
       window.contentView?.addSubview(scrollView)
       scrollView.translatesAutoresizingMaskIntoConstraints = false
       
       tableView = NSTableView()
       tableView.dataSource = self
       tableView.delegate = self
       
       let colWidth = CGFloat(WINDOW_WIDTH / searchBridge.columnNames.count)
       for i in 0..<searchBridge.columnNames.count {
           let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("column\(i)"))
           column.title = searchBridge.columnNames[i]
           column.width = colWidth
           tableView.addTableColumn(column)
       }
       
       scrollView.documentView = tableView
       
       if let contentView = window.contentView {
           NSLayoutConstraint.activate([
               searchContainer.topAnchor.constraint(equalTo: contentView.topAnchor),
               searchContainer.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
               searchContainer.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
               searchContainer.heightAnchor.constraint(equalToConstant: 50),
               
               scrollView.topAnchor.constraint(equalTo: searchContainer.bottomAnchor),
               scrollView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
               scrollView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
               scrollView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor)
           ])
       }
       
       if let firstSearchField = searchFields.first {
           window.makeFirstResponder(firstSearchField)
       }

        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
   }
   
   @objc func searchFieldChanged(_ sender: NSSearchField) {
       let startTime = Date()

       let column = sender.tag
       let query = sender.stringValue

       searchStrings[column] = query
       
       var queryParts = searchBridge.searchColumnNames
       for i in 0..<searchBridge.searchColumnNames.count {
           queryParts[i] += "=" + searchStrings[i]
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

   func applicationWillTerminate(_ notification: Notification) {
       NSApp.setActivationPolicy(.prohibited)
       NSApp.hide(nil)
       DispatchQueue.main.async {
           exit(0)
       }
   }

   func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
       NSApp.hide(nil)
       return .terminateNow
    }
   
   func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
       return true;
   }
}


let app = NSApplication.shared
let delegate = AppDelegate()
app.delegate = delegate
app.run()
