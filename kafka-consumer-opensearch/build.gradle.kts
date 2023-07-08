plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    implementation("org.apache.kafka:kafka-clients:3.4.0")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:2.0.7")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation("org.slf4j:slf4j-simple:2.0.7")

//    OpenSearch Dependendcy
    implementation("org.opensearch.client:opensearch-rest-high-level-client:2.8.0")

//    GSON Google
//    implementation("org.netbeans.external:com-google-gson:RELEASE113")
//    Changed to different GSON dependency because of code problem with my parse
    implementation("com.google.code.gson:gson:2.9.1")

}

tasks.test {
    useJUnitPlatform()
}