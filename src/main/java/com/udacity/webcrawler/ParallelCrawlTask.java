package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ParallelCrawlTask extends RecursiveAction {
    private final Clock clock;
    private final String url;
    private final Instant deadline;
    private final int maxDepth;
    private final Map<String, Integer> counts;
    private final ConcurrentSkipListSet<String> visitedUrls;
    private final PageParserFactory parserFactory;
    private final List<Pattern> ignoredUrls;

    public ParallelCrawlTask(Clock clock,
                             String url,
                             Instant deadline,
                             int maxDepth,
                             Map<String, Integer> counts,
                             ConcurrentSkipListSet<String> visitedUrls,
                             PageParserFactory parserFactory,
                             List<Pattern> ignoredUrls) {
        this.clock = clock;
        this.url = url;
        this.deadline = deadline;
        this.maxDepth = maxDepth;
        this.counts = counts;
        this.visitedUrls = visitedUrls;
        this.parserFactory = parserFactory;
        this.ignoredUrls = ignoredUrls;
    }

    @Override
    protected void compute() {
        if (maxDepth == 0
                || clock.instant().isAfter(deadline)
                || ignoredUrls.stream().anyMatch(pattern -> pattern.matcher(url).matches())
                || !visitedUrls.add(url)) {
            return;
        }
        PageParser.Result result = parserFactory.get(url).parse();
        result.getWordCounts().forEach((key, value) -> {
            if (counts.containsKey(key)) {
                counts.put(key, value + counts.get(key));
                return;
            }
            counts.put(key, value);
        });
        List<ParallelCrawlTask> tasks = result
                .getLinks()
                .stream()
                .map(link -> new ParallelCrawlTask(clock, link, deadline, maxDepth - 1, counts, visitedUrls, parserFactory, ignoredUrls))
                .collect(Collectors.toList());
        invokeAll(tasks);
    }
}
