group = "de.mineking"
version = "1.0.0"

dependencies {
    implementation(project(":core"))
    implementation("org.jdbi:jdbi3-core:3.45.4")
    implementation("org.jdbi:jdbi3-kotlin:3.45.4")
    implementation("org.postgresql:postgresql:42.7.4")

    implementation(kotlin("reflect"))
    implementation("com.google.code.gson:gson:2.10.1")
}