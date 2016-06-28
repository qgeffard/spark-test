import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.time.Month;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf();
        conf.setAppName("Spark Test");
        conf.setMaster("local");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> lines = sparkContext.textFile("/home/omahjoub/workspace/spark-test/src/main/resources/validations.csv");

        long count = lines.count();

        System.out.println(count);

        JavaRDD<Validation> validations = lines
                // On filtre le Header du CSV
                .filter(line -> !line.equals("JOUR;CODE_STIF_TRNS;CODE_STIF_RES;CODE_STIF_ARRET;LIBELLE_ARRET;ID_REFA_LDA;CATEGORIE_TITRE;NB_VALD"))
                // On transforme chaque ligne en Objet Validation
                .map(Validation::new);

        // On met le RDD en cache mémoire pour une prochaine utilisation
        validations.cache();

        count = validations.count();
        System.out.println(count);

        // Le total des voyageurs qui ont validé leur titre de transport sur le 1 er semestre 2015
        long totalValidations = validations.map(Validation::getValidations).reduce((v1, v2) -> v1 + v2);
        System.out.println(totalValidations);

        // Le total des voyageurs qui ont validé leur titre de transport sur le 1 er semestre 2015
        long totalValidationsJanvier = validations.filter(v1 -> v1.getJour().getMonth().equals(Month.JANUARY)).map(Validation::getValidations).reduce((v1, v2) -> v1 + v2);
        System.out.println(totalValidationsJanvier);

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


    }

    // Comparator sur les validations par gare
    static class MyTupleComparator implements Comparator<Tuple2<String, Long>>, Serializable {

        final static MyTupleComparator INSTANCE = new MyTupleComparator();

        public int compare(Tuple2<String, Long> t1, Tuple2<String, Long> t2) {
            return t1._2.compareTo(t2._2);    // sort descending
        }
    }

}

