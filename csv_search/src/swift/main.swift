class AppDelegate: NSObject, NSApplicationDelegate, NSTableViewDataSource, NSTableViewDelegate {
    var window: NSWindow!
    var table_view: NSTableView!
    var search_fields: [NSSearchField] = []
    var search_results: [[String]] = []  // Store current results
    var query_handler: OpaquePointer?    // Handle to Zig search engine
    
    func applicationDidFinishLaunching(_ notification: Notification) {
        // Initialize search engine
        query_handler = search_engine_create()
        
        // Create window as before...
        
        // Add search fields above each column
        let searchContainer = NSView()
        window.contentView?.addSubview(searchContainer)
        searchContainer.translatesAutoresizingMaskIntoConstraints = false
        
        for i in 0..<2 {  // For each column
            let search_field = NSSearchField()
            search_container.addSubview(searchField)
            search_field.translatesAutoresizingMaskIntoConstraints = false
            search_field.target = self
            search_field.action = #selector(searchFieldChanged(_:))
            search_field.tag = i  // To identify which column
            search_fields.append(search_field)
            
            // Position search field above its column
            NSLayoutConstraint.activate([
                search_field.widthAnchor.constraint(equalToConstant: 200),
                search_field.leadingAnchor.constraint(equalTo: searchContainer.leadingAnchor, constant: CGFloat(20 + i * 220))
            ])
        }
        
        // Adjust table position to account for search fields...
    }
    
    @objc func searchFieldChanged(_ sender: NSSearchField) {
        guard let engine = query_handler else { return }
        let column = sender.tag
        let query = sender.stringValue
        
        // Perform search in background
        DispatchQueue.global(qos: .userInitiated).async {
            var resultCount: Int32 = 0
            let results = search_engine_search(engine, query, Int32(column), &resultCount)
            
            // Update UI on main thread
            DispatchQueue.main.async {
                // Update table with new results
                self.updateSearchResults(results, count: Int(resultCount), column: column)
                self.table_view.reloadData()
            }
        }
    }
    
    // Clean up
    deinit {
        if let engine = query_handler {
            search_engine_destroy(engine)
        }
    }
}
