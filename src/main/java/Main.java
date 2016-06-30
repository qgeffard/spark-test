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

        JavaRDD<String> lines = sparkContext.textFile("/Users/oussamamahjoub/Documents/workspace/spark-test/src/main/resources/validations.csv");

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

        // Le total des voyageurs qui ont validé leur titre de transport sur le 1 er semestre 2015
        long totalValidations = validations.map(Validation::getValidations).reduce((v1, v2) -> v1 + v2);
        System.out.println("Nombre total de validations des passagers pour tout le semestre : " + totalValidations);

        System.out.println("*****  END  *****");
        System.out.println();



        /**
         * EXO 5 : Total de validations des passagers pour le mois de janvier
         *
         * Demander a Spark de calculer le nombre total de validations des passagers pour le mois de janvier
         */

        System.out.println();
        System.out.println("***** EXO 5 *****");

        // Le total des voyageurs qui ont validé leur titre de transport sur le 1 er semestre 2015
        long totalValidationsJanvier = validations.filter(v1 -> v1.getJour().getMonth().equals(Month.JANUARY)).map(Validation::getValidations).reduce((v1, v2) -> v1 + v2);
        System.out.println("Nombre total de validations des passagers pour le mois de janvier : " + totalValidationsJanvier);

        System.out.println("*****  END  *****");
        System.out.println();



        /**
         * EXO 6 : Pourecentage des validations par mois
         *
         * Demander a Spark de calculer le pourecentage des validations par mois
         */

        System.out.println();
        System.out.println("***** EXO 6 *****");

        // Pourecentage des validations par mois :
        // On regroupe par mois
        validations.groupBy(v -> v.getJour().getMonth())
                // On calcule le total des validations par mois
                .map(tuple -> {
                    Map<Month, Long> result = new HashMap<>();
                    result.put(tuple._1(), 0L);
                    tuple._2().forEach(validation -> {
                        result.put(tuple._1(), result.get(tuple._1()) + validation.getValidations());
                    });
                    return result;
                })
                        // On recupère les resultat dans une map "Mois/Total validations du mois"
                .reduce((v1, v2) -> {
                    Map<Month, Long> result = new HashMap<Month, Long>();
                    result.putAll(v1);
                    result.putAll(v2);
                    return result;
                })
                        // Pour chaque ligne on divise par le total des validation du semestre et on affiche le résultat
                .forEach((month, aLong) -> System.out.println(month + " : " + ((double) aLong / totalValidations) * 100 + " %"));

        System.out.println("*****  END  *****");
        System.out.println();



        /**
         * EXO 7 : Top 5 des validation par gare
         *
         * Demander a Spark de calculer le top 5 des validation par gare
         */

        System.out.println();
        System.out.println("***** EXO 7 *****");

        // Top 5 des validations par gare :
        // On groupe par gare
        validations.groupBy(v -> v.getLibelleArret())
                // Pour chaque gare on calcule le total des validations
                .mapToPair(tuple -> {
                    Map<String, Long> result = new HashMap<>();
                    result.put(tuple._1(), 0L);
                    tuple._2().forEach(validation -> {
                        result.put(tuple._1(), result.get(tuple._1()) + validation.getValidations());
                    });
                    return new Tuple2<String, Long>(tuple._1(), result.get(tuple._1()));
                })
                        // On trie par ordre ascendant sur les validations et on garde le top 5
                .top(5, MyTupleComparator.INSTANCE)
                .stream().forEach(stringLongTuple2 -> System.out.println(stringLongTuple2._1() + " : " + stringLongTuple2._2() + " =====> " + ((double)  stringLongTuple2._2() / totalValidations) * 100 + " %"));

        System.out.println("*****  END  *****");
        System.out.println();
    }

    // Comparator sur les validations par gare
    static class MyTupleComparator implements Comparator<Tuple2<String, Long>>, Serializable {

        final static MyTupleComparator INSTANCE = new MyTupleComparator();

        public int compare(Tuple2<String, Long> t1, Tuple2<String, Long> t2) {
            return t1._2.compareTo(t2._2);    // sort descending
        }
    }

}

