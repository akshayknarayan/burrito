#!/usr/bin/env Rscript

library(ggplot2)

args = commandArgs(trailingOnly=TRUE)

df <- read.csv(args[1], sep=" ")
summary(df)
ggplot(df, aes(x=NumShards, y=Ops, colour=ShardType)) + geom_line()

ggsave(args[2], width=8, height=5)
