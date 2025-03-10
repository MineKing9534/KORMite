dependencies {
    implementation(project(":core"))
    compileOnly("net.dv8tion:JDA:5.1.0")

    compileOnly(project(":postgres"))

    testImplementation(project(":postgres"))
    testImplementation("org.postgresql:postgresql:42.7.4")

    testImplementation("net.dv8tion:JDA:5.1.0")
}