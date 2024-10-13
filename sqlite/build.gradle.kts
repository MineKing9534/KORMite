group = "de.mineking"
version = "1.0.0"

dependencies {
    implementation(project(":core"))
    implementation("org.jdbi:jdbi3-core:3.45.4")
    implementation("org.jdbi:jdbi3-kotlin:3.45.4")

    implementation(kotlin("reflect"))
    implementation("com.google.code.gson:gson:2.10.1")

    testImplementation("org.xerial:sqlite-jdbc:3.46.1.0")

    testImplementation("ch.qos.logback:logback-classic:1.5.8")
    implementation("io.github.microutils:kotlin-logging-jvm:2.0.11")
}