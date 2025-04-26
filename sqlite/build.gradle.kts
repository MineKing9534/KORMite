dependencies {
    implementation(project(":core"))
    implementation("org.xerial:sqlite-jdbc:3.46.1.0")
}

kotlin {
    compilerOptions {
        //Required for AnnotationTable
        javaParameters = true
    }
}