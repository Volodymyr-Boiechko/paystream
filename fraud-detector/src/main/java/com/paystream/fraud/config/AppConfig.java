package com.paystream.fraud.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.paystream.fraud.service.FraudRule;
import com.paystream.fraud.service.FraudScorer;
import com.paystream.fraud.service.HighAmountRule;
import com.paystream.fraud.service.NegativeAmountRule;
import com.paystream.fraud.service.RoundAmountRule;
import com.paystream.fraud.service.SuspiciousCountryRule;
import java.util.List;

public class AppConfig {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    private AppConfig() {
    }

    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }

    public static FraudScorer fraudScorer() {
        List<FraudRule> rules = List.of(
            new NegativeAmountRule(),
            new HighAmountRule(),
            new SuspiciousCountryRule(),
            new RoundAmountRule()
        );
        return new FraudScorer(rules);
    }
}
