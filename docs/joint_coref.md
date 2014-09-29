Let $syn$ be a syntax tree (syntax trees for the whole document), and we want to decide coreference links conditioned on syntax.

$$P(link|syn,w, \theta)$$

This coreference model could be deterministic, in which case the link
probability is deterministically 1 or 0 based on the syntax (and wordforms $w$, for gender or other noun-analysis information)
input.  Or it could be probabilistic, like a logistic regression model for antecedent selection, in which case $\theta$ parameterizes the model.  There are some details here for whether it's a history-based logreg, or a global MRF, etc.

**Inference:** If we have parses we trust we would infer coreference via

$$\max_{link} P(link|syn,w,\theta)$$

Since syntax is uncertain -- we have little idea which parse is the correct one, and even just knowing the spans we're wrong 10% of the time -- we'd like to integrate out the syntax variable, using both correct and incorrect parses to inform whether there is a link.

$$\max_{link} P(link|w,\theta) = \sum_{syn} P(link|syn,w,\theta) P(syn|w)$$

where $P(syn|w)$ is the distribution over syntactic trees according to a probabilistic parsing model (like a PCFG, or tree CRF).  So if the coref model was deterministic, you sum over every possible tree, each of which votes for either there's a link or not, and you weight the votes by the probability of each tree.  If the model is probabilistic, you weighted-ly average the probabilistic predictions, weighted by tree probabilities.

In practice of course you can't enumerate all possible trees.  Fortunately, for PCFG and analogous tree-CRF's, there exists the "inside-outside sampler" which draws full joint samples from $P(syn|w)$ in cubic time.  Thus it is easy to get a bunch of samples $\{syn^{(s)}\}_{s=1..S}$, having identical and independently distributed $syn^{(s)} \sim P(syn|w)$.  In general, a way to approximate $E_{x \sim D}[f(x)]$ is as $\frac{1}{S} \sum_{x^{(s)}} f(x^{(s)})$ (if you have a bunch of samples $x^{(s)} \sim D$); this is called Monte Carlo integration.  We can apply this same technique to the inference problem here.  Take the average of coreference predictions, across the syntax samples.

$$P(link|w) \approx \frac{1}{S} \sum_{syn^{(s)}} P(link|syn^{(s)},w) $$

The accuracy of this approximation gets arbitrarily good with more samples.  It's governed by the central limit theorem.  Consider a bunch of sample predictions $P(link|syn^{(s)})$ for each syntax sample $s=1..S$.  If $\sigma$ is the standard deviation among these samples, then the standard error of the estimate of $P(link|w)$ is $\sigma/\sqrt{S}$.  Since that the highest $\sigma$ can go is $0.5$ (the std dev of a bernoulli is $\sqrt{p(1-p)}$, which is maxed out when $p=0.5$), so the 95\% confidence interval of $P(link|w)$ is plus or minus $1/\sqrt{S}$  (since $x \pm 1.96 \sqrt{p(1-p)}/\sqrt{S} \approx x \pm 2(0.5)/\sqrt{S}$).  So 100 samples gives you the prediction probability $P(link|w)$ within $0.1$ absolute accuracy, and 1000 samples is within $0.03$. And that's the worst-case scenario when the true probability is near $0.5$; when it's close to 1 or 0 (which might be common?), Monte Carlo averaging is even more accurate.

One nice thing about this is that if we're using standard syntax models, which assume sentence independence, it's easy to sample parses for an entire document by just sampling each sentence independently.  The entire-document parse is then fed into calculating Hobbs distances, or whatever.  This is much simpler compared to a non-sampling approach to representing uncertainty, in which you try to come up with deterministic approximations of important marginal distributions you want.  This works for things that have local interactions -- like how the inside-outside algorithm can compute marginal probabilities for span non-terminals -- but anything that requires long-distance features, such as calculating a Hobbs path distance, definitely does not cleanly factorize across PCFG independence assumptions.  Sampling means you don't have to worry about this: just repeat the simple calculation on every sample.

**Learning:** The parser and coreference system both could be trained.  For parsing, the simplest thing to do is use a parser that was trained in the usual way, perhaps on a totally different dataset.  In practice, often we don't have labeled datasets that are labeled for all the different levels of analysis we would like.

For the coref system, the easy thing to do is train it with gold standard parses.

$$\max_{\theta} \log P(link|syn^{(gold)},w,\theta)$$

However, it might be good to train it on predicted parses, not gold standard parses, so it learns not to overly trust the parses.  People usually just take the one-best output of a parser to do this (and I think the CoNLL coref competitions might have done something like this in their distribution of the data? not sure.).  However, since we're being smart about parse uncertainty, we might want to average over multiple parses somehow.  Specifically, assume
the goal is to maximize the incomplete data likelihood, averaging over all possible parses,

$$\log \sum_{syn} P(link,syn|w,\theta) = \sum_{syn} P(link|syn,w,\theta) P(syn|w)$$

which can be learned with EM,

 * E-step: update $q(syn) = P(syn|w,link,\theta) \propto P(syn|w) P(link|syn,w,\theta)$
 * M-step: calculate new $\arg\max_{\theta} \sum_{syn} q(syn) \log P(link|syn,w,\theta)$ ... note that $P(syn|w)$ doesn't appear in there because we're not learning parser parameters right now (though we could, if we had the right data for it).

The first step of EM is very simple.  Consider an initialization to $\theta$ where all features from syntax trees have parameters $0$, so all $P(link|syn,w,\theta)$ are the same no matter what $syn$ is.  Then the M-step simplifies to maximizing $J(\theta)=\sum_{syn} P(syn|w) \log P(link|syn,w,\theta)$.  We can again apply Monte Carlo integration with the parse sampler to represent the M-step objective as

$$J(\theta) \approx J^{MC}(\theta) = \frac{1}{S} \sum_{syn^{(s)}} \log P(link|syn^{(s)},w,\theta)$$

So it's like training on $S$ copies of the training data, each with different a different parse sampling.  This is pretty intuitive: instead of training on just the one-best parse output (like many other people do), we just train on multiple parses at once.

How accurate is the MC approximation to the true objective ($J^{MC}$ versus $J$)?  I bet it's super close.  Above I discussed the standard error of a single linking probability.  For calculating the gradient of $J$, you care about expected \emph{counts} over linking decisions across the entire training set.  This makes the variability due to the Monte Carlo approximation even smaller.  You could always check $S=10$ versus $S=1000$ and see if the gradients are very different.  I wonder of $S=1$ might even be enough.  (Hinton's contrastive divergence technique is all about taking gradient steps using a one-sample Monte Carlo approximation, though the setup there is slightly different than what we have here.)

**EM iterations via importance sampling:**
OK, this simple trick (train on all the parse samples) might work fine by itself, but  actually it's only the first EM iteration.  Maybe if we do full EM with multiple iterations, we might do even better.  To do the second EM iteraation, we'd need to calculate expected log-likelihood under new E-step posterior inferences of the parse samples, $q(syn) \propto P(syn|w) P(link|syn,w,\theta^{(old)})$.  Unfortunately the inside-outside sampler won't work here, because the nasty non-local likelihood function $P(link|syn,w)$ is hanging off of the parses and doesn't factorize within the chart nicely.

How to calculate this?  One option is importance sampling.  In importance sampling, the goal is to calculate an expectation under distribution $p$ when all you have is access to samples from a different proposal distribution $q$ (not the same $q$ as above sorry), and you can calculate the unnormalized true probabilities $\tilde{p}(x)$.  IS approximates $E_{x \sim p}[f(x)]$ as 

$$E_{x \sim p}[f(x)] = \sum_x p(x) f(x) = \sum_x \frac{p(x)}{q(x)} q(x) f(x) \approx
\sum_{s} \frac{p(x^{(s)})}{q(x^{(s)})} f(x^{(s)})
\approx \frac{1}{\sum_{s} w^{(s)}}  \sum_{s} w^{(s)} f(x^{(s)})$$

The first approximation is the usual MC approximation for samples $x^{(s)} \sim q(x)$, and the second assumes weights defined as $w^{(s)} \approx \frac{\tilde{p}(x^{(s)})}{q(x^{(s)})}$.  (A normalization term, based on the sum of the importance weights, has to be applied because we're using the unnormalized probabilities $\tilde{p}(x)$. This is all a summary of the Murphy chapter on this; many other references online too.)

We can apply this here by using the prior as the proposal; specifically sample from the inside-outside algorithm again, samples from $p(syn|w)$, to target the distribution proportional to $p(syn|w)p(link|syn,w,\theta^{(old)})$.  The importance weights then are

$$w^{(s)} = \frac{p(syn^{(s)}|w) p(link|syn^{(s)},w,\theta^{(old)})}{p(syn^{(s)}|w)}
= P(link|syn^{(s)},w,\theta^{(old)})$$

So you just reweight by the likelihoods.  I guess this happens whenever you use an importance sampler to target an unnormalized posterior using the prior as the proposal.

So the IS weighting terms basically fold the E-step into the M-step, whose objective under importance sampling can be written as

$$J^{IS}(\theta) = \frac{1}{W} \sum_s P(link|syn^{(s)},w,\theta^{(old)}) \log P(link|syn^{(s)},w,\theta)$$

where $W$ is the sum of the importance weights (but perhaps can be ignored since it does not affect gradient directions, only magnitudes?).  When you retrain your coreference model's parameters $\theta$, you no longer pay equal attention to all syntax samples like you did in the first EM iteration.  Instead, you pay more attention to syntax trees which gave good predictions of the gold-standard coreference links (according to last iteration's coref model $\theta^{(old)}$).

I think there might be some further details to work out with parses for multiple sentences, when gold-standard linkage decisions reach across multiple sentences.  (This is important because it's one of the reasons we're using the sampling approach in the first place.)  I guess one way to do it is to treat the parses for all sentences in the entire document as one random variable $syn$, and all the links as one random variable $link$; then the $P(link|syn,w)$ weighting term would be the product of the probabilities of all linkage decisions in the document, and $\log P(link|syn,w)$ would be the sum of their individual log-probabilities.

**Cascaded sampling for other pipeline stuff:**
The above inference and learning approaches don't apply just to parsing-then-coref, but rather, any NLP pipeline with multiple stages of analysis.  Finkel et al 2006 talk about this, though only for inference, and they don't emphasize the advantages of the fact that sampling allows long-distance features in downstream models.  I've been thinking about getting posterior uncertainty over event extraction or other information extraction tasks that require coreference as input.  If you do too much modeling at every stage, I think the importance sampler might start getting crappy because the prior-based proposals start to strongly disagree with the next-step likelihoods, and you need a very large number of samples in order to get lucky and have an adequate number of samples from the reasonably high portion of the final posterior space.

**Other work:**
Finkel et al 2006 is the most important previous work.

Boschee 2013 describes an event extraction system (SERIF) for which coreference and parsing have a joint model, and they claim beam search to jointly maximize both helps.  There are very few details though.  I read the older paper they cite at one point but it didn't have a ton of details if I remember right (I should reread).  
 
 * Paper: http://brenocon.com/Boschee2013Events.pdf
 * My notes plus a copy of what appears to be the older SERIF paper: https://www.evernote.com/shard/s46/sh/b2db650c-0e1a-4790-9c8a-b1decd728d75/5e348184632fb8b6471a1556600cdc47

