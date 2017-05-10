import java.util.*;
import java.io.*;
import java.text.*;

class StringTest {
    public static void main(String[] args) {
        int columns = 357;
        int rows = 3;

        String[][] MoralDic = new String[columns][rows];

        try {
            File file_dic = new File("MoralDic");
            FileReader fileDicReader = new FileReader(file_dic);
            BufferedReader bufferedDicReader = new BufferedReader(fileDicReader);
            String line;
            int i = 0;

            while ((line = bufferedDicReader.readLine()) != null) {
                String[] Array = line.split("/");
                MoralDic[i][0] = Array[0];
                MoralDic[i][1] = Array[1];
                MoralDic[i][2] = Array[2];
                //System.out.println(Array[0]+"  "+Array[1]+"  "+Array[2]);/
                System.out.println(MoralDic[i][0]+"  "+MoralDic[i][1]+"  "+MoralDic[i][2]);
                i++;
            }
            fileDicReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            File file = new File("Sample.txt");
            //File file = new File("RC_2008-01");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                String[] aArray = line.split("body\":\"");
                String[] bArray = aArray[1].split("\",\"");

                String Cline = bArray[0].replaceAll("[-+.*^!':(),?]", "");
                Cline = Cline.replace("\\n", " ");
                Cline = Cline.replace("\\r", " ");
                Cline = Cline.replace("\\", " ");
                Cline = Cline.replace("/", " ");
                Cline = Cline.replace("\"", " ");

                String[] subLine = Cline.trim().toLowerCase().split(" ");
                System.out.println(Cline+"["+subLine.length+"]");

                List<String> Keys = new ArrayList<>();

                for (int j = 0; j < subLine.length; j++) {
                   //System.out.println(subLine[j]);
                   for (int k = 0; k < columns; k++) {
                       if(MoralDic[k][1].equals("1")) {
                           if(subLine[j].indexOf(MoralDic[k][0]) == 0 ) {
                               Keys.add(MoralDic[k][2]);
                               /*
                               System.out.println("---> " +
                                                  subLine[j] + " has " +
                                                  MoralDic[k][0] + 
                                                  ": " + 
                                                  MoralDic[k][2] + 
                                                  " at " +
                                                  subLine[j].indexOf(MoralDic[k][0]) );
                             */
                           }
                       }
                       else {
                           if(subLine[j].equals(MoralDic[k][0])) {
                               Keys.add(MoralDic[k][2]);
                               //System.out.println("---> "+subLine[j]+": "+MoralDic[k][2]);
                           }
                       }
                   } 
                }
                for (String z : Keys) {
                        System.err.println(z);
                }
            }
            fileReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

