#!/usr/bin/env Rscript

library(ggplot2)

args = commandArgs(trailingOnly=TRUE)
if (length(args) >= 3) {
    xmax <- as.numeric(args[3])
} else {
    xmax <- 10e3
}

df <- read.csv(args[1], sep=" ")
summary(df)
ggplot(df, aes(x=Latency_us, colour=factor(Ops))) + stat_ecdf() + coord_cartesian(xlim=c(0, xmax)) + facet_grid(ShardType ~ NumShards)

ggsave(args[2], width=8, height=5)
