package uy.kohesive.elasticsearch.dataimport

import java.io.ByteArrayInputStream
import java.io.File

class ManualTestOfDataImport {
    // TODO: This test requires Elasticsearch to be available, it is difficult to Run ES and Spark together due to conflicting dependencies
    companion object {
        @JvmStatic fun main(args: Array<String>) {
            val confFile = File("./src/test/resources/manual-test.conf")
            App().run(confFile.inputStream(), confFile.parentFile)
        }
    }
}
