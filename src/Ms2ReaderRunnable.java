import scala.Int;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.LinkedList;

public class Ms2ReaderRunnable implements Runnable{

    File ms2File;
    PrecursorIdHandler precHandler;

    private final static double PROTON_MASS = 1.00727826;
    LinkedList<Integer> missedScanNumbers = new LinkedList<>();


    int numberOfMatches = 0;
    int numberOfMismatches = 0;

    public Ms2ReaderRunnable(PrecursorIdHandler ph, File file){
        ms2File = file;
        precHandler = ph;
    }

    @Override
    public void run() {
        System.out.println(ms2File.getName());
        readMs2File();
        precHandler.addToStatistics(numberOfMatches, numberOfMismatches);
        precHandler.addToMissedScanNumbers(missedScanNumbers);
    }

    public void readMs2File() {
        BufferedReader reader;
        try {
            reader = new BufferedReader(
                    new FileReader(ms2File.getAbsolutePath()));
            String line = reader.readLine();
            double max = 0;
            int count = 0;
            long timSincelastPrint = System.currentTimeMillis();

            while (line != null) {
                ms1Line l;

                if(line.charAt(0) == 'S') {
                    double scanNumber = Integer.parseInt(line.split("\t")[1]);

                    //Skip to zline
                    while(line != null){
                        //Start Of new Scan
                        if (line.charAt(0) == 'Z') {
                            String[] zlineStringArr = line.split("\\t");
                            double mz = Double.parseDouble(zlineStringArr[2]);
                            int chargeState = Integer.parseInt(zlineStringArr[1]);

                            if (!isPrecursorFoundInDataBase(mz, chargeState)) {
                                /*
                                System.out.println("------------- " + ms2File.getName() + " Scan " + (int)(scanNumber) + " -------------");
                                System.out.println("Mass (Da): " + NumberFormat.getIntegerInstance().format(mz*1000));
                                System.out.println("Charge: " + chargeState);

                                 */
                                missedScanNumbers.add((int)(scanNumber));
                            }
                            break;
                        }
                        line = reader.readLine();
                    }
                }
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isPrecursorFoundInDataBase(double mz, int charge){
        /*
        if(charge == 0){
            //search db with 2 and 3
            double daltonConversion2 = (double)((mz - PROTON_MASS) * 2 + PROTON_MASS)*1000;
            double daltonConversion3 = (double)((mz - PROTON_MASS) * 3 + PROTON_MASS)*1000;


            boolean databaseMatchWithCharge2 = false;
            boolean databaseMatchWithCharge3 = false;
            try{
                databaseMatchWithCharge2 = precHandler.checkIfDataBaseContainsMass((int)daltonConversion2);
                databaseMatchWithCharge3 = precHandler.checkIfDataBaseContainsMass((int)daltonConversion3);
            } catch(ArrayIndexOutOfBoundsException e){
                System.out.println(e);
                System.out.println("(int)daltonConversion2"+(int)daltonConversion2);
                System.out.println("(int)daltonConversion3"+(int)daltonConversion3);
                System.out.println("sizeOfDatabse: "+(precHandler.database.length));
            } finally{
                if(databaseMatchWithCharge2 || databaseMatchWithCharge3){
                    numberOfMatches++;
                    return true;
                } else {
                    numberOfMismatches++;
                    return false;
                }
            }
            */

        if(mz < 600 || mz > 6000) {
            return true;
        }

        //search db with daltonConversion
        double daltonConversion = (double)((mz - PROTON_MASS) * charge + PROTON_MASS)*1000;
        daltonConversion = (double)(mz)*1000;

        boolean databaseMatch = false;
        try{
            databaseMatch = precHandler.checkIfDataBaseContainsMass((int)daltonConversion);
        } catch(ArrayIndexOutOfBoundsException e){
            System.out.println(e);
            System.out.println("(int)daltonConversion"+(int)daltonConversion);
            System.out.println("sizeOfDatabse: "+(precHandler.database.length));
        } finally {
            if (databaseMatch) {
                numberOfMatches++;
                return true;
            } else {
                numberOfMismatches++;
                return false;
            }
        }
    }
}
