import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.*;

public class IdxDatabaseRunnable implements Runnable{

    File idxFile;
    PrecursorIdHandler precHandler;
    private Connection specDbConnection;
    int numberOfPrecursors = 0;

    public IdxDatabaseRunnable(PrecursorIdHandler ph, File file){
        precHandler = ph;
        idxFile = file;

    }
    @Override
    public void run() {
        System.out.println("Loading " + idxFile.getName() + " into database");

        //Get Database connection to idx file
        try{
            specDbConnection = DriverManager.getConnection("jdbc:sqlite:" + idxFile.getAbsolutePath());
        } catch (SQLException ex){
            System.out.println(ex);
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

                    precHandler.insertPeptideIntoDataBase(precursorMass * 1000);
                    precHandler.insertPeptideIntoDataBase((precursorMass + precHandler.PROTON_MASS*1) * 1000);
                    precHandler.insertPeptideIntoDataBase((precursorMass + precHandler.PROTON_MASS*2) * 1000);
                    precHandler.insertPeptideIntoDataBase((precursorMass + precHandler.PROTON_MASS*3) * 1000);

                    numberOfPrecursors++;

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
        System.out.println("    Number of precursors loaded: " + numberOfPrecursors);
        precHandler.numIdxThreadsDone++;
    }
}
