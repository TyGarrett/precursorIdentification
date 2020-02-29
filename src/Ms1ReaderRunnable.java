import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Ms1ReaderRunnable implements Runnable {

    File ms1File;
    PrecursorIdHandler precHandler;

    private final static double PROTON_MASS = 1.00727826;

    int numberOfMatches = 0;
    int numberOfMismatches = 0;

    public Ms1ReaderRunnable(PrecursorIdHandler ph, File file){
        ms1File = file;
        precHandler = ph;
    }

    @Override
    public void run() {
        System.out.println(ms1File.getName());
        readMs1File();
        precHandler.addToStatistics(numberOfMatches, numberOfMismatches);
    }

    public void readMs1File() {
        BufferedReader reader;
        try {
            reader = new BufferedReader(
                    new FileReader(ms1File.getAbsolutePath()));
            ArrayList<ms1Line> singleSpectra = new ArrayList<>();
            String line = reader.readLine();
            double max = 0;
            int count = 0;
            long timSincelastPrint = System.currentTimeMillis();

            //Skip untill first S line
            while(line != null) {
                ms1Line l;

                //Start Of new Scan
                if (line.charAt(0) == 'S') {
                    break;
                }
                line = reader.readLine();
            }

            while (line != null) {
                ms1Line l;

                //Start Of new Scan
                if(line.charAt(0) == 'S'){

                    count += 10000;
                    //Print top 20 elems
                    singleSpectra.sort((o1, o2) -> o1.compareTo(o2)); //  <--- FIX THIS BUG IMPORTANT!!!!!
                    for(int i = 0; i < singleSpectra.size() && i < 20; i++){
                        //System.out.println(singleSpectra.get(i).toString());
                        isPrecursorFoundInDataBase(singleSpectra.get(i));
                    }

                    //Skip next two I lines
                    line = reader.readLine();
                    line = reader.readLine();
                    line = reader.readLine();

                    singleSpectra.clear();

                    if(System.currentTimeMillis() - timSincelastPrint > 10000){
                        System.out.println("File: " + ms1File.getName() + " |   ~Lines Read: " + count);
                        timSincelastPrint = System.currentTimeMillis();
                    }
                }

                try {
                    l = new ms1Line(line);
                    singleSpectra.add(l);
                } catch (Exception e){
                    System.out.println(e);
                    System.out.println(line);
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
                databaseMatchWithCharge2 = precHandler.checkIfDataBaseContainsMass((int)daltonConversion2);
                databaseMatchWithCharge3 = precHandler.checkIfDataBaseContainsMass((int)daltonConversion3);
            } catch(ArrayIndexOutOfBoundsException e){
                System.out.println(e);
                System.out.println("(int)daltonConversion2"+(int)daltonConversion2);
                System.out.println("(int)daltonConversion3"+(int)daltonConversion3);
                System.out.println("sizeOfDatabse: "+(precHandler.database.length));
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
                databaseMatch= precHandler.checkIfDataBaseContainsMass((int)daltonConversion);
            } catch(ArrayIndexOutOfBoundsException e){
                System.out.println(e);
                System.out.println("(int)daltonConversion"+(int)daltonConversion);
                System.out.println("sizeOfDatabse: "+(precHandler.database.length));
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
}
