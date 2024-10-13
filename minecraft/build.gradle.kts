repositories {
    maven("https://repo.papermc.io/repository/maven-public/")
}

dependencies {
    implementation(project(":core"))
    implementation("org.jdbi:jdbi3-core:3.45.4")
    compileOnly("io.papermc.paper:paper-api:1.21.1-R0.1-SNAPSHOT")

    implementation(kotlin("reflect"))

    testImplementation(project(":postgres"))
    testImplementation("org.postgresql:postgresql:42.7.4")

    testImplementation("io.papermc.paper:paper-api:1.21.1-R0.1-SNAPSHOT")

    testImplementation("ch.qos.logback:logback-classic:1.5.8")
    testImplementation("io.github.microutils:kotlin-logging-jvm:2.0.11")
}