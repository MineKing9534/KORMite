dependencies {
    implementation(project(":core"))
    implementation("org.postgresql:postgresql:42.7.4")
}

kotlin {
    compilerOptions {
        //Required for AnnotationTable
        javaParameters = true
    }
}