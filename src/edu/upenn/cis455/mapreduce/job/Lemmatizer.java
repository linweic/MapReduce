package edu.upenn.cis455.mapreduce.job;

import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.util.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 *
 */
public class Lemmatizer {

    private static Lemmatizer lemmatizer = null;
    private StanfordCoreNLP pipeline;

    private Lemmatizer() {
        Properties props;
        props = new Properties();
        props.put("annotators", "tokenize,ssplit,pos,lemma");

        this.pipeline = new StanfordCoreNLP(props);
    }

    private static void init() {
        if (lemmatizer == null) {
            lemmatizer = new Lemmatizer();
        }
    }

    public static List<String> lemmatize(String docText) {
        init();

        List<String> lemmas = new LinkedList<>();

        Annotation document = new Annotation(docText);
        lemmatizer.pipeline.annotate(document);

        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                lemmas.add(token.get(CoreAnnotations.LemmaAnnotation.class));
            }
        }
        return lemmas;
    }
    public static void main(String[] args){
    	List<String> words = lemmatize("shoe shoes");
    	for(String word:words) System.out.println(word);
    }
}
