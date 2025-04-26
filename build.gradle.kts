import com.adarshr.gradle.testlogger.theme.ThemeType

plugins {
	kotlin("jvm") version "2.0.10"

	id("com.adarshr.test-logger") version "4.0.0"
	id("test-report-aggregation")

	id("maven-publish")
}

group = "de.mineking"
version = "1.5.0"

val jvmVersion = 17
val release = System.getenv("RELEASE") == "true"

dependencies {
	testReportAggregation(project(":postgres"))
	testReportAggregation(project(":sqlite"))
	testReportAggregation(project(":minecraft"))
	testReportAggregation(project(":discord"))
}

allprojects {
	version = rootProject.version

	apply(plugin = "org.jetbrains.kotlin.jvm")
	apply(plugin = "com.adarshr.test-logger")
	apply(plugin = "test-report-aggregation")
	apply(plugin = "maven-publish")

	repositories {
		mavenCentral()
	}

	dependencies {
		implementation(kotlin("reflect"))
		implementation("org.jdbi:jdbi3-core:3.45.4")
		implementation("org.jdbi:jdbi3-kotlin:3.45.4")

		compileOnly("com.google.code.gson:gson:2.10.1")

		implementation("io.github.microutils:kotlin-logging-jvm:2.0.11")

		testImplementation("ch.qos.logback:logback-classic:1.5.8")
		testImplementation(kotlin("test"))

		testImplementation("com.google.code.gson:gson:2.10.1")
	}

	tasks.test {
		useJUnitPlatform()
	}

	testlogger {
		theme = ThemeType.MOCHA
		showStackTraces = false
	}

	publishing {
		repositories {
			maven {
				url = uri("https://maven.mineking.dev/" + (if (release) "releases" else "snapshots"))
				credentials {
					username = System.getenv("MAVEN_USERNAME")
					password = System.getenv("MAVEN_SECRET")
				}
			}
		}

		publications {
			register<MavenPublication>("maven") {
				from(components["java"])

				groupId = "de.mineking.KORMite"
				artifactId = "KORMite-${ project.name }"
				version = if (release) "${ project.version }" else System.getenv("BRANCH")
			}
		}
	}

	kotlin {
		jvmToolchain(jvmVersion)
	}

	java {
		sourceCompatibility = JavaVersion.toVersion(jvmVersion)
		targetCompatibility = JavaVersion.toVersion(jvmVersion)

		withJavadocJar()
		withSourcesJar()
	}
}

tasks.register("publishAll") {
	dependsOn(":core:publish")

	dependsOn(":postgres:publish")
	dependsOn(":sqlite:publish")

	dependsOn(":minecraft:publish")
	dependsOn(":discord:publish")
}
