#!/usr/bin/env Rscript

library(ggplot2)

args <- commandArgs(trailingOnly=TRUE)
df <- read.csv(args[1])
df$Network_us <- df$Total_us - df$Server_us
print(summary(df))

ggplot(df, aes(x=Network_us, colour=interaction(proto, container))) + stat_ecdf() + ylab("CDF") + xlab("RPC Ping RTT (us)")
ggsave(args[2], width=12, height=6)
