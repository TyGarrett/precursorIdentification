import edu.scripps.pms.census.util.io.SQTParser;
import edu.scripps.pms.util.sqt.MLine;
import edu.scripps.pms.util.sqt.SQTPeptide;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class SqtFileLoader {
    String sqtFile;
    static File fileLocation = null;

    int nonValidCounter = 0;
    int totalSLines = 0;



    HashMap<Integer, Boolean> scanFoundInSqtMap
            = new HashMap<>();

    public SqtFileLoader(String filename) throws IOException {
        sqtFile = filename;
        loadSqtFile();
    }

    public static File findFileswithSqtExtension(File file) {

        if(file.getName().toLowerCase().endsWith("sqt")) {
            return file;
        }
        return null;
    }

    private void loadSqtFile() throws IOException {

        File filePath = new File(sqtFile);
        File[] listingAllFiles = filePath.listFiles();

        for (File file : listingAllFiles) {
            fileLocation = findFileswithSqtExtension(file);
            if(fileLocation != null) {
                System.out.println("reading file: " + fileLocation.getAbsolutePath());
                SQTParser reader = new SQTParser(fileLocation.getAbsolutePath());

                SQTPeptide peptide;
                MLine mLine;
                for (Iterator<SQTPeptide> itr = reader.getSQTPeptide(); itr.hasNext(); ) {
                    peptide = itr.next();

                    int numMLines = peptide.getNumMlines();
                    int scanNumber = Integer.parseInt(peptide.getSLine().split("\\t")[1]);
                    boolean isValid = false;
                    boolean isReverse = false;



                    int mLineCount = 0;
                    for (Iterator<MLine> mItr = peptide.getMLine(); mItr.hasNext(); ) {
                        if(mLineCount > 1){
                            break;
                        }
                        mLine = mItr.next();

                        // check if mLine has a target value
                        boolean isTarget = false;

                        StringBuffer sb = new StringBuffer();
                        for (Iterator<String> lItr = mLine.getLLine(); lItr.hasNext(); ) {
                            String lLine = lItr.next();

                            if (!lLine.startsWith("Rev"))
                                isTarget = true;
                            sb.append(lLine).append("\t");
                        }
                        isReverse = !isTarget;
                        isValid = (numMLines > 0); //&& isTarget;
                        mLineCount++;
                    }

                    if(!isValid){
                        //System.out.println("numMLines: " + numMLines + ", scanNumber: " + scanNumber +", isValid: " + isValid + " isReverse: " + isReverse);
                        scanFoundInSqtMap.put(scanNumber, false);
                        nonValidCounter++;
                    }
                    totalSLines++;
                }
            }
        }
        System.out.println(nonValidCounter);
        System.out.println(totalSLines);

        System.out.println(((double)nonValidCounter)/(totalSLines));
    }

    public boolean scanNumberValid(int scanNum){
        return scanFoundInSqtMap.containsKey(scanNum);
    }

    public static void main(String args[]) throws Exception {
        SqtFileLoader sl = new SqtFileLoader("/home/ty/repos/precursorIdentification/");
    }
}
