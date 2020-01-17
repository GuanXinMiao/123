library(ggplot2)
library(dplyr)
setwd('C:\\Users\\user\\Desktop\\FunnowProject\\customer')
df <- read.csv('PersonalBehavior.csv', encoding = 'UTF-8', header = TRUE)
df
df.segm <- df %>%
  mutate(AmountZone = ifelse(between(OrderAmount, 1, 1), '1', 
                             ifelse(between(OrderAmount, 2, 2), '2',
                                    ifelse(between(OrderAmount, 3, 5),'3~5', '>5'))))%>%
  mutate(segm.rec = ifelse(between(recency, 0, 5), '0-5day(s)',
                           ifelse(between(recency, 6, 10), '6-10days',
                                  ifelse(between(recency, 11, 30),'11-30days', '>31days'))))

df.segm$AmountZone <- factor(df.segm$AmountZone, levels = c('>5', '3~5', '2', '1'))
df.segm$segm.rec <- factor(df.segm$segm.rec, levels = c('>31days', '11-30days', '6-10days', '0-5day(s)'))
df.segm

lcg <- df.segm%>%
  group_by(segm.rec, AmountZone) %>%
  summarise(quantity = n()) %>%
  mutate(client='customerAmount')%>%
  ungroup()

lcg.matrix = as.data.frame(table(df.segm$AmountZone, df.segm$segm.rec))
lcg.matrix$AmountZone = row.names(lcg.matrix)
lcg.matrix

lcg.adv <- lcg%>%
  mutate(rec.type = ifelse(segm.rec %in% c('>31days', '11-30days'), 'not recent', 'recent'),
         Amount.type = ifelse(AmountZone %in% c('>5', '3~5'), 'frequent', 'infrequent'),
         customer.type = interaction(rec.type, Amount.type))
lcg.adv

ggplot(lcg.adv, aes(x=client, y=quantity, fill=customer.type))+
  theme_bw()+
  theme(panel.grid = element_blank()) +
  geom_rect(aes(fill = customer.type), xmin = -Inf, xmax = Inf, ymin = -Inf, ymax = Inf, alpha = 0.1) +
  facet_grid(AmountZone ~ segm.rec) +
  geom_bar(stat='identity', alpha=0.7) +
  geom_text(aes(y=max(quantity)/2, label=quantity), size=4) +
  ggtitle('FunNow RFM graph') +
  xlab('recency') + ylab('OrderAmount') +
  theme(plot.title = element_text(color="red", size=30 ),
        axis.title.x = element_text(color="blue", size=20, face="bold"),
        axis.title.y = element_text(color="black", size=20, face="bold"))+
  guides(fill=guide_legend(title="customer type"))+
scale_fill_discrete(breaks = c('not recent.frequent','recent.frequent','not recent.infrequent','recent.infrequent'), 
                    labels = c('previously customer','important customer','one order customer','new customer'))

lcg.sub <- df.segm %>%
  group_by(Category, segm.rec, AmountZone) %>%
  summarise(quantity=n())%>%
  mutate(client='CustomerAmount')%>%
  ungroup()
head(lcg.sub, 70)

ggplot(lcg.sub, aes(x=client, y=quantity, fill=Category))+
  theme_bw()+
  scale_fill_brewer(palette = 'Set1') +
  theme(panel.grid = element_blank())+
  geom_bar(stat='identity', position='fill', alpha = 0.6)+
  facet_grid(AmountZone ~ segm.rec) +
  ggtitle('RF Graph(Category)') +
  xlab('recency') + ylab('OrderAmount') +
  theme(plot.title = element_text(color="red", size=30),
        axis.title.x = element_text(color="blue", size=20, face="bold"),
        axis.title.y = element_text(color="#993333", size=20, face="bold"))
  +guides(fill=guide_legend(title="Category"))