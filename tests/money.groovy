def rich_list(threshold) {
    Integer t = threshold;
    BigDecimal cutOff = t.intValue();
    return g.V('@class', 'wallet').has('amount', T.gt, cutOff).inE().outV();
}

