## Bikewise problem statement repo

### Technical challenge.
The objective of this challenge is to help us to have an overview
of your technical knowledge, design and architectural thinking.
Please do not forget about the good practices :) (testing, project structure,
code sample running)
The challenge has 3 main objectives.
* assess experience and knowledge regarding data ingestion
* assess experience and knowledge regarding data flow
* assess experience and knowledge regarding big data
1) Write an application that is able to ingest data from the following REST
service:
 https://www.bikewise.org/documentation/api_v2
 * Requirements:
 * new data has to ingested on a daily basis
 * output must be big data friendly. What do we mean by big data friendly?
 The incidents should be easy and cost effective to query on a big data
 environment. How to save the list of incidents from each request so that,
 so that most big data tools would be able to reach incidents as a "row"?
* Please choose one from the questions below
2) Suppose that another team wants to use this data to do some text analyses on
data.
How would you set up an architecture so that other teams do not
need to worry about the data ingestion?
3) The data that we are reading is updated from time to time.
Write a code or write the design (architecture) how to build
a history of the incidents. In another words, how do we keep
all versions of the incidents in our data storage? Such as any field of the
incident changed and we need to keep track of this incident again
