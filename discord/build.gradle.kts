group = "de.mineking"
version = "1.0.0"

dependencies {
    implementation(project(":core"))
    implementation("org.jdbi:jdbi3-core:3.45.4")
    compileOnly("net.dv8tion:JDA:5.1.0")

    testImplementation(project(":postgres"))
    testImplementation("org.postgresql:postgresql:42.7.4")

    testImplementation("net.dv8tion:JDA:5.1.0")

    testImplementation("ch.qos.logback:logback-classic:1.5.8")
    testImplementation("io.github.microutils:kotlin-logging-jvm:2.0.11")
}