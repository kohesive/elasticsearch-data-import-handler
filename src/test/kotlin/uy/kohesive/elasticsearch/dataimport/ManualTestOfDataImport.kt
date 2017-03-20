package uy.kohesive.elasticsearch.dataimport

import java.io.ByteArrayInputStream

class ManualTestOfDataImport {
    // TODO: This test requires Elasticsearch to be available, it is difficult to Run ES and Spark together due to conflicting dependencies
    companion object {
        @JvmStatic fun main(args: Array<String>) {
            App().run(ClassLoader.getSystemResourceAsStream("manual-test.conf"))
        }
    }
}