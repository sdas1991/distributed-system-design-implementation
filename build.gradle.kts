plugins {
    kotlin("jvm") version "1.9.21" apply false
    kotlin("plugin.serialization") version "1.9.21" apply false
}

allprojects {
    group = "com.shardstream"
    version = "1.0.0"

    repositories {
        mavenCentral()
    }
}
