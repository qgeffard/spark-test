import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.time.Month;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws IOException {


        /**
         * EXO 1 : Conf Spark
         *
         * Initialiser la Conf Spark
         *
         */

        SparkConf conf = new SparkConf();
        conf.setAppName("Spark Test");
        conf.setMaster("local");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.setLogLevel("ERROR");


        /**
         * EXO 2 : Chargement du fichier de validations
         *
         * 1) Demander a Spark de lire le fichier de validations
         * 2) Afficher le nombre de lignes du fichier
         */

        System.out.println();
        System.out.println("***** EXO 2 *****");

        JavaRDD<String> lines = sparkContext.textFile("/home/omahjoub/workspace/spark-test/src/main/resources/validations.csv");

        long count = lines.count();

        System.out.println("Nombre de lignes dans le ficher de validations : " + count);

        System.out.println("*****  END  *****");
        System.out.println();


        /**
         * EXO 3 : Mapping ligne / objet
         *
         * 1) Demander a Spark de mapper les lignes lues vers des objets Validation
         * 2) Afficher le nombre d'objets
         * 3) Demander a Spark de mettre le résultat produit dans son cache
         */

        System.out.println();
        System.out.println("***** EXO 3 *****");

        System.out.println("*****  END  *****");
        System.out.println();


    }
}

