import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class TestDataGen {
    public static void main(String[] args) {
        int numRows = 10000000;
        String outputFilePath = "src/main/resources/testMetricsLarge.csv";

        Random random = new Random(System.currentTimeMillis());

        Instant startTimestamp = Instant.parse("2022-06-01T00:00:00.000Z");
        Instant endTimestamp = Instant.parse("2022-06-10T00:00:00.000Z");

        try (FileWriter writer = new FileWriter(outputFilePath)) {
            writer.write("Metric,Value,Timestamp\n");

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

            for (int i = 0; i < numRows; i++) {
                int valueTemp = random.nextInt(100) + 1;
                double valuePrecip = random.nextInt(10) / 10.;
                Instant timestamp = startTimestamp.plusSeconds(random.nextInt((int) (endTimestamp.getEpochSecond() - startTimestamp.getEpochSecond())));

                String formattedTimestamp = formatter.format(timestamp);

                writer.write("temperature" + "," + valueTemp + "," + formattedTimestamp + "\n");
                writer.write("precipitation" + "," + valuePrecip + "," + formattedTimestamp + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
