# Aşama 1: Build (Derleme Aşaması)
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /app

# Maven wrapper ve pom dosyalarını kopyala
COPY mvnw pom.xml ./
COPY .mvn .mvn

# Bağımlılıkları indir (Önbellekleme için ayrı adım)
RUN ./mvnw dependency:go-offline -B

# Kaynak kodları kopyala ve projeyi paketle
COPY src src
RUN ./mvnw package -DskipTests

# Aşama 2: Runtime (Çalıştırma Aşaması - Hafif JRE imajı)
FROM eclipse-temurin:21-jre
WORKDIR /app

# Derlenen jar dosyasını builder aşamasından kopyala
COPY --from=builder /app/target/*.jar app.jar

# Uygulama portunu dışarı aç
EXPOSE 8080

# Uygulamayı başlat
ENTRYPOINT ["java", "-jar", "app.jar"]