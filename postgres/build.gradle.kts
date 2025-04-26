dependencies {
    implementation(project(":core"))
    implementation("org.postgresql:postgresql:42.7.4")

    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.0")
}

kotlin {
    compilerOptions {
        //Required for AnnotationTable
        javaParameters = true
    }
}