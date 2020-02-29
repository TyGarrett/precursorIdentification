//import util.properties packages
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

//import simple producer packages
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;

//import KafkaProducer packages

//import ProducerRecord packages
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

//Create java class named “SimpleProducer”
public class precursorConsumer {

    private final static String TOPIC = "prec";
    private final static String BOOTSTRAP_SERVER = "172.29.80.71:9092";
    private final static int SIZE_OF_DATABASE = 6500000;
    private final static int PPM_DIVISOR = 1000000;
    private final static double PROTON_MASS = 1.00727826;

    String fafstaFilePath = "";
    int maxDatabaseValue = 0;
    int minDatabaseValue = SIZE_OF_DATABASE;



    private Connection specDbConnection;
    private long sizeOfDb = 0;
    private int ppm = 40;
    public boolean[] database;
    int numberOfMatches = 0;
    int numberOfMismatches = 0;

    int numberOfPrecursors = 0;

    private long lastStatTime = 0;
    File fileLocation;

    public precursorConsumer() throws SQLException {
        database = new boolean[15000000];
        fillBooleanDatabaseFromFafstaFile();
    }

    private void fillBooleanDatabaseFromFafstaFile() {
        File filePath = new File(fafstaFilePath);
        File[] listingAllFiles = filePath.listFiles();

        for (File file : listingAllFiles) {
            fileLocation = findFileswithIndExtension(file);
            int count = 0;
            if (fileLocation != null) {
                System.out.println("Loading " + file.getName() + " into database");

                //Get Database connection to idx file
                try{
                    specDbConnection = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                } catch (SQLException ex){
                    System.out.println(ex);
                    continue;
                }

                try (Statement stmt  = specDbConnection.createStatement();
                     ResultSet rs    = stmt.executeQuery("SELECT * FROM blazmass_sequences")
                ){

                    while (rs.next()) {

                        //System.out.println(count);
                        int massKey = rs.getInt("precursor_mass_key");
                        byte[] arr = rs.getBytes("data");
                        ByteBuffer wrapped = ByteBuffer.wrap(arr).order(ByteOrder.LITTLE_ENDIAN); // big-endian by default

                        for(int i = 0; i<arr.length; i += 16){

                            float precursorMass = wrapped.getFloat();       int seqOffset = wrapped.getInt();
                            int seqLength = wrapped.getInt();               int proteinId = wrapped.getInt();

                            insertPeptideIntoDataBase((int)(precursorMass * 1000));
                            insertPeptideIntoDataBase((int)((precursorMass + PROTON_MASS*1) * 1000));
                            insertPeptideIntoDataBase((int)((precursorMass + PROTON_MASS*2) * 1000));
                            insertPeptideIntoDataBase((int)((precursorMass + PROTON_MASS*3) * 1000));

                            //Skip peptide sequence
                            while(true){
                                if(wrapped.getInt() == Integer.MAX_VALUE){
                                    i+=4;
                                    break;
                                }
                                i+=4;
                            }
                        }
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }
        System.out.println("    Number of precursors loaded: " + numberOfPrecursors);
    }

    public static File findFileswithIndExtension(File file) {

        if(file.getName().toLowerCase().endsWith("idx")) {
            return file;
        }
        return null;
    }

    public synchronized void insertPeptideIntoDataBase(double massKey){

        if(massKey > maxDatabaseValue){
            maxDatabaseValue = (int) massKey;
        }
        if(massKey < minDatabaseValue){
            minDatabaseValue = (int) massKey;
        }

        double tolerance = ppm*massKey/(PPM_DIVISOR);
        //System.out.println("Mass Key: " + massKey + ", Tolerance: " + tolerance);
        //int tolerance = (int) (massKey*0.00004);

        int start = (int) (massKey - tolerance);
        int end = (int) (massKey + tolerance);
        //System.out.println("Mass: " + massKey + "   |   Range: " + (end - start));
        if (start < 0) {
            start = 0;
        }
        if (end > database.length - 1) {
            end = database.length - 1;
        }
        for(int i = start; i<end; i++){
            if(database[i] != true) {
                database[i] = true;
                //numberOfIndexesTrueInDatabase++;
            }
        }
    }

    private void loadFromDatabase(){
        try (Statement stmt  = specDbConnection.createStatement();
             ResultSet rs    = stmt.executeQuery("SELECT massKey FROM PeptideTable")
        ){

            while (rs.next()) {

                int massKey = rs.getInt("massKey");

                //massKey = Daltons * 1000
                int tolerance = ppm*massKey/PPM_DIVISOR;
                //System.out.println("tolerence: " + tolerance + ", massKey: " + massKey);

                int start = massKey - tolerance;
                int end = massKey + tolerance;
                if (start < 0) {
                    start = 0;
                }
                if (end > database.length - 1) {
                    end = database.length - 1;
                }
                    for(int i = start; i<end; i++){
                        database[i] = true;
                        sizeOfDb++;
                    }
                }

            } catch (SQLException ex) {
            ex.printStackTrace();
        }

        System.out.println(sizeOfDb);

    }

    private static Consumer<String, byte[]> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "PrecursorConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<String, byte[]> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    void runConsumer() throws InterruptedException, IOException, Exception {
        final Consumer<String, byte[]> consumer = createConsumer();
        final Producer<String, byte[]> producer = createProducer();

        final int giveUp = 50;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<String, byte[]> consumerRecords =
                    consumer.poll(Duration.ofMillis(200));

            // Timeout incrementer
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            //loop over available ms2 records
            consumerRecords.forEach(record -> {
                //System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                //      record.key(), record.value(), record.partition(), record.offset());

                byte[] data = record.value();
                DoubleBuffer fBuf =
                        ByteBuffer.wrap(data)
                                .order(ByteOrder.LITTLE_ENDIAN)
                                .asDoubleBuffer();
                double[] array = new double[fBuf.remaining()];
                fBuf.get(array);
                //System.out.println(array.length);
                for (int i = 0; i<array.length; i += 2){
                    double monoisotopicMZ = array[i];
                    double chargeState = array[i+1];
                    if(chargeState == 0){
                        //search db with 2 and 3
                        double daltonConversion2 = (double)((monoisotopicMZ - PROTON_MASS) * 2 + PROTON_MASS)*1000;
                        double daltonConversion3 = (double)((monoisotopicMZ - PROTON_MASS) * 3 + PROTON_MASS)*1000;

                        boolean databaseMatchWithCharge2 = database[(int)daltonConversion2];
                        boolean databaseMatchWithCharge3 = database[(int)daltonConversion3];

                        if(databaseMatchWithCharge2 || databaseMatchWithCharge3){
                            numberOfMatches++;
                        } else {
                            numberOfMismatches++;
                        }

                        System.out.println("Charge: " + chargeState + ", monoisotopicMZ: " + monoisotopicMZ + ", conv: " + daltonConversion2);
                        System.out.println("    " + databaseMatchWithCharge2 + "(daltonConversion2)");
                        System.out.println("    " + databaseMatchWithCharge3 + "(daltonConversion3)");
                    } else {
                        //search db with daltonConversion
                        double daltonConversion = (double)((monoisotopicMZ - PROTON_MASS) * chargeState + PROTON_MASS)*1000;
                        boolean databaseMatch = database[(int)daltonConversion];

                        if(databaseMatch){
                            numberOfMatches++;
                        } else {
                            numberOfMismatches++;
                        }

                        System.out.println("Charge: " + chargeState + ", monoisotopicMZ: " + monoisotopicMZ + ", conv: " + daltonConversion);
                        System.out.println("    " + databaseMatch + "(daltonConversion)");
                    }
                }
                runProducer(producer);

            });


            noRecordsCount = 0;
            consumer.commitAsync();
        }

        printStats();

        consumer.close();
        System.out.println("DONE");
    }

    public static Producer<String, byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "PrecursorProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return new KafkaProducer<>(props);
    }

    private static void runProducer(Producer<String, byte[]> producer){
        try {
            final ProducerRecord<String, byte[]> record =
                    new ProducerRecord<>("test", "raw",
                            "Hello Mom ".getBytes());

            RecordMetadata metadata = producer.send((ProducerRecord<String, byte[]>) record).get();

            System.out.printf("sent record(key=%s value=%s) ",
                    record.key(), record.value());


        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            //producer.close();
        }

    }

    private void printStats(){
        System.out.println("------------STATISTICS---------------");
        System.out.println("Ratio match : mismatch: " + numberOfMatches + " : " + numberOfMismatches);
        System.out.println("Percent Match: " + (double)numberOfMatches / (numberOfMismatches + numberOfMatches));

    }


    public void readMs1File() {
        BufferedReader reader;
        lastStatTime = System.nanoTime();
        try {
            reader = new BufferedReader(
                    new FileReader("/home/ty/20180727_TIMS2_12-2_AnBr_SA_200ng_HeLa_50cm_120min_25ms_03CT_1_A1_01_2566_nopd.ms1"));
            ArrayList<ms1Line> singleSpectra = new ArrayList<>();
            String line = reader.readLine();
            double max = 0;
            while (line != null) {
                ms1Line l;

                //Start Of new Scan
                if(line.charAt(0) == 'S'){

                    //Print top 20 elems
                    singleSpectra.sort((o1, o2) -> o1.compareTo(o2));
                    for(int i = 0; i < singleSpectra.size() && i < 5; i++){
                        //System.out.println(singleSpectra.get(i).toString());
                        isPrecursorFoundInDataBase(singleSpectra.get(i));
                    }

                    //Skip next two I lines
                    line = reader.readLine();
                    line = reader.readLine();
                    line = reader.readLine();

                    singleSpectra.clear();
                    //System.out.println("Cleared ArrayList: " + singleSpectra.size());
                    if( System.nanoTime() - lastStatTime > (1000000000)){
                        //printStats();
                        lastStatTime = System.nanoTime();
                    }
                }

                try {
                    l = new ms1Line(line);
                    singleSpectra.add(l);
                } catch (Exception e){
                    System.out.println(e);
                    continue;
                } finally {
                    line = reader.readLine();
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void isPrecursorFoundInDataBase(ms1Line line){
        if(line.charge == 0){
            //search db with 2 and 3
            double daltonConversion2 = (double)((line.mass - PROTON_MASS) * 2 + PROTON_MASS)*1000;
            double daltonConversion3 = (double)((line.mass - PROTON_MASS) * 3 + PROTON_MASS)*1000;

            boolean databaseMatchWithCharge2 = false;
            boolean databaseMatchWithCharge3 = false;
            try{
                databaseMatchWithCharge2 = database[(int)daltonConversion2];
                databaseMatchWithCharge3 = database[(int)daltonConversion3];
            } catch(ArrayIndexOutOfBoundsException e){
                System.out.println(e);
                System.out.println("(int)daltonConversion2"+(int)daltonConversion2);
                System.out.println("(int)daltonConversion3"+(int)daltonConversion3);
                System.out.println("sizeOfDatabse: "+(database));
            }

            if(databaseMatchWithCharge2 || databaseMatchWithCharge3){
                numberOfMatches++;
            } else {
                numberOfMismatches++;
            }

            System.out.println("Charge: " + line.charge + ", mass: " + line.mass + ", conv: " + daltonConversion2);
            System.out.println("    " + databaseMatchWithCharge2 + "(daltonConversion2)");
            System.out.println("    " + databaseMatchWithCharge3 + "(daltonConversion3)");
        } else {
            //search db with daltonConversion
            double daltonConversion = (double)((line.mass - PROTON_MASS) * line.charge + PROTON_MASS)*1000;
            boolean databaseMatch = false;
            try{
                databaseMatch= database[(int)daltonConversion];
            } catch(ArrayIndexOutOfBoundsException e){
                System.out.println(e);
                System.out.println("(int)daltonConversion"+(int)daltonConversion);
                System.out.println("sizeOfDatabse: "+(database));
            }

            if(databaseMatch){
                numberOfMatches++;
            } else {
                numberOfMismatches++;
            }

            //System.out.println("Charge: " + line.charge + ", mass: " + line.mass + ", conv: " + daltonConversion);
            //System.out.println("    " + databaseMatch + "(daltonConversion)");
        }
    }


    public static void main(String args[]) throws Exception {
        Long time = System.nanoTime();
        precursorConsumer pc = new precursorConsumer();
        //pc.loadFromdataBase2();
        System.out.println( (System.nanoTime() - time)/ 1000000000);
        //pc.runConsumer();
        pc.readMs1File();

        //System.out.println(pc.sizeOfDb);
    }
}