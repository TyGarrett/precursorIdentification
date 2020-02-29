import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.*;
import java.util.HashMap;
import java.util.LinkedList;

public class PrecursorIdHandler {

    public static final double PROTON_MASS = 1.00727826;
    private final static int SIZE_OF_DATABASE = 6500000;
    private final static int PPM_DIVISOR = 1000000;

    HashMap<Integer, LinkedList<Float>> map
            = new HashMap<>();

    public boolean[] database;
    int maxDatabaseValue = 0;
    int minDatabaseValue = SIZE_OF_DATABASE;
    File fileLocation;
    private Connection specDbConnection;
    int numberOfPrecursors = 0;
    public long numberOfIndexesTrueInDatabase = 0;
    String fafstaFilePath;

    int numberOfMatches = 0;
    int numberOfMismatches = 0;
    int numberOfThreadsFinished = 0;
    int numberOfThreads = 0;
    int numIdxThreadsDone = 0;

    private int ppm = 40;

    LinkedList<Integer> missedScanNumbers = new LinkedList<>();

    // Initialize the boolean Database
    public PrecursorIdHandler(String filename) throws InterruptedException {
        fafstaFilePath = filename;
        database = new boolean[SIZE_OF_DATABASE];
        fillBooleanDatabaseFromFafstaFile();
        //fillBooleanDatabaseFromFafstaFileThread();
        System.out.println("Min database: " + minDatabaseValue);
        System.out.println("Max database: " + maxDatabaseValue);
        System.out.println("%database full: " + (double)(numberOfIndexesTrueInDatabase)/(maxDatabaseValue - minDatabaseValue));

    }

    public synchronized void addToStatistics(int numMatch, int numMiss){
        numberOfMatches += numMatch;
        numberOfMismatches += numMiss;
        numberOfThreadsFinished++;
    }

    public synchronized void addToMissedScanNumbers(LinkedList<Integer> l){
        for( int i : l){
            missedScanNumbers.add(i);
        }
    }

    private void fillBooleanDatabaseFromFafstaFileThread() throws InterruptedException {
        File filePath = new File(fafstaFilePath);
        File[] listingAllFiles = filePath.listFiles();

        int numberOfIdxThreads = 0;

        for (File file : listingAllFiles) {
            fileLocation = findFileswithIndExtension(file);
            int count = 0;
            if (fileLocation != null) {
                Thread object = new Thread(new IdxDatabaseRunnable(this, file));
                object.start();
                numberOfIdxThreads++;
            }
        }

        while(numberOfIdxThreads != numIdxThreadsDone){
            Thread.sleep(1000);
            System.out.println("sleeping... numberOfReturns: " + numIdxThreadsDone);
        }
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
                numberOfIndexesTrueInDatabase++;
            }
        }
    }

    public static File findFileswithIndExtension(File file) {
        if(file.getName().toLowerCase().endsWith("idx")) {
            return file;
        }
        return null;
    }

    public static File findFilesWithExtension(File file, String ext) {
        if(file.getName().toLowerCase().contains(ext)) {
            return file;
        }
        return null;
    }

    public boolean checkIfDataBaseContainsMass(int daltonConversion) {
        return database[daltonConversion];
    }

    private void printStats(){
        System.out.println("------------STATISTICS---------------");
        System.out.println("Ratio match : mismatch: " + numberOfMatches + " : " + numberOfMismatches);
        System.out.println("Percent Match: " + (double)numberOfMatches / (numberOfMismatches + numberOfMatches));
    }

    private static void runMs1() throws InterruptedException {
        PrecursorIdHandler ph = new PrecursorIdHandler("/home/ty/repos/precursorIdentification/UniProt_Human_reviewed_isoform_contaminant_12-11-2019.fasta_1_KR_NA_cleav_3_1_static_C57-02146.idx");

        File MS1FileFolder = new File("/home/ty/ms1Files/");
        File[] listingAllFiles = MS1FileFolder.listFiles();

        //ExecutorService es = Executors.newCachedThreadPool();
        int i = 0;
        for (File file : listingAllFiles) {
            if (findFilesWithExtension(file, "ms1") != null) {
                ph.numberOfThreads++;
                Thread object = new Thread(new Ms1ReaderRunnable(ph, file));
                object.start();
            }
            i++;
        }

        while(ph.numberOfThreadsFinished != ph.numberOfThreads){
            Thread.sleep(10000);
            System.out.println("sleeping... numberOfReturns: " + ph.numberOfThreadsFinished);
        }

        ph.printStats();

    }

    private static void runMs2() throws InterruptedException, IOException {
        PrecursorIdHandler ph = new PrecursorIdHandler("/home/ty/repos/precursorIdentification/UniProt_Human_reviewed_isoform_contaminant_12-11-2019.fasta_1_KR_NA_cleav_3_1_static_C57-02146.idx");

        File MS2FileFolder = new File("/home/ty/repos/precursorIdentification/ms2Data/");
        File[] listingAllFiles = MS2FileFolder.listFiles();

        //ExecutorService es = Executors.newCachedThreadPool();
        int i = 0;
        for (File file : listingAllFiles) {
            if (findFilesWithExtension(file, "ms2") != null) {
                ph.numberOfThreads++;
                Thread object = new Thread(new Ms2ReaderRunnable(ph, file));
                object.start();
            }
            i++;
        }

        while(ph.numberOfThreadsFinished != ph.numberOfThreads){
            Thread.sleep(10000);
            System.out.println("sleeping... numberOfReturns: " + ph.numberOfThreadsFinished + " / " + ph.numberOfThreads);
        }

        ph.printStats();

        SqtFileLoader sl = new SqtFileLoader("/home/ty/repos/precursorIdentification/");

        int numScansCorrectlyIdentified = 0;
        int tot = 0;
        for(int scanNum : ph.missedScanNumbers){
            if(sl.scanNumberValid(scanNum)){
                numScansCorrectlyIdentified++;
            }
            tot++;
        }
        System.out.println("numScansCorrectlyIdentified: " + numScansCorrectlyIdentified);
        System.out.println("tot: " + tot);

    }

    public static void main(String args[]) throws Exception {
        runMs2();
    }
}
