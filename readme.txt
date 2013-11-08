Material usado?

Play 2.2.1 (Java)
Hadoop 1.2.1
Hbase 0.94.12

Como correr? 
(Linux)
colocar pasta CN em Desktop
ant getResources
ant run

Que consiste em:

ant getResources 
1. Download de todo o material necessario, cerca 287 MB, para o seu computador (hadoop, hbase e play).

ant run
1. Limpeza dos .classes anteriormente gerados.
2. Iniciar hbase e criação de tabelas.
Atenção: O Linux poderá queixar-se por causa das permissões na pasta CN. O ideal é executar chmod 777 -R CN.(Estando no Desktop). 
3. Geração de .classes
4. Run do MapReduce

Mais info em (build.xml)

Como fazer queries ao sistema?

1. Executar ./ResoucesApp/play-2.2.0/play na pasta web.
2. Executar start na consola do play.

query 1 - localhost:9000/1/phoneID/date
Exemplo:  localhost:9000/1/968281362/12-10-2013

query 2 - localhost:9000/2/cellID/date/time
Exemplo:  localhost:9000/2/TMN-OEIRAS-1/12-10-2013/15

query 3 - localhost:9000/3/phoneID/date
Exemplo:  localhost:9000/3/TMN-OEIRAS-1/12-10-2013


Mais info (web/conf/routes)

Breve explicação:

Map:
Recebe logs, envia para o reduce os eventos 2, 3, 4, 5 e 8 os restantes são descartados.
Para responder á query 1 são usados os eventos: 2, 4
Para responder á query 2 são usados os eventos: 2,3,4,5,8
Para responder á query 3 são usados os eventos: 4,5,8

Assumimos que quando existe um evento 2 ou 4 significa que o telemovel se ligou a uma cell (2) ou network + cell (4).

Reduce:

O reduce sumariza as keys colocando-as na Base de dados Hbase.
A base de dados contem key-value pairs.

Query 1
Key : phoneID + "_" + date
Value : lista sequencial de CellIDs

Query 2
Key : cellID + "_" + date + "_" + time
Value : lista de phoneID's presentes num dado dia, numa dada célula a uma dada hora(em ponto).

Query 3
Key : phoneID + "_" + date
Value : Tempo em minutos que um phoneID esteve fora da rede numa data.

Proximos passos:
Correr a nossa aplicação na Amazon. Nesta primeira entrega usamos HBase localmente. ElasticMapReduce usa HBase como store para os resultados, o que torna o nosso projecto próprio para o mesmo. Depois, faremos deploy do nosso projecto a uma escala distribuida em vez de local. Nesse caso precisaremos de gerir acessos ao nosso site com um load balancer.

