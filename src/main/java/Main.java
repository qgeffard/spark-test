import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

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

        JavaRDD<String> lines = sparkContext.textFile("validations.csv");

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

        JavaRDD<Validation> validations = lines
                // On filtre le Header du CSV
                .filter(line -> !line.equals("JOUR;CODE_STIF_TRNS;CODE_STIF_RES;CODE_STIF_ARRET;LIBELLE_ARRET;ID_REFA_LDA;CATEGORIE_TITRE;NB_VALD"))
                        // On transforme chaque ligne en Objet Validation
                .map(Validation::new);

        // On met le RDD en cache mémoire pour une prochaine utilisation
        validations.cache();

        count = validations.count();
        System.out.println("Nombre total d'objets Validation : " + count);

        System.out.println("*****  END  *****");
        System.out.println();


        /**
         * EXO 4 : Total de validations des passagers
         *
         * Demander a Spark de calculer le nombre total de validations des passagers sur le 1 er semestre 2015
         */

        System.out.println();
        System.out.println("***** EXO 4 *****");

        System.out.println("*****  END  *****");
        System.out.println();


    }

}

