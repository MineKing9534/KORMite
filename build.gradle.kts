import com.adarshr.gradle.testlogger.theme.ThemeType

plugins {
	kotlin("jvm") version "2.0.10"

	id("com.adarshr.test-logger") version "4.0.0"
	id("test-report-aggregation")
}

allprojects {
	apply(plugin = "org.jetbrains.kotlin.jvm")
	apply(plugin = "com.adarshr.test-logger")
	apply(plugin = "test-report-aggregation")

	repositories {
		mavenCentral()
	}

	dependencies {
		testReportAggregation(project(":postgres"))
		testReportAggregation(project(":sqlite"))
		testReportAggregation(project(":minecraft"))
		testReportAggregation(project(":discord"))

		testImplementation("ch.qos.logback:logback-classic:1.5.8")
		implementation("io.github.microutils:kotlin-logging-jvm:2.0.11")

		testImplementation(kotlin("test"))
	}

	tasks.test {
		useJUnitPlatform()
	}

	testlogger {
		theme = ThemeType.MOCHA
		showStackTraces = false
	}
}