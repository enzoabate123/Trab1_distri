# Relatório — Monitoramento de sensores com Kafka e Docker

**Autores:**
- Enzo Abate - 2312936
- Lucca Sznaider - 2220504

## Objetivo

O trabalho implementa um sistema de monitoramento de sensores de uma fábrica inteligente para demonstrar balanceamento de carga, elasticidade e tolerância a falhas. Sensores simulados enviam leituras continuamente ao Kafka, e aplicações consumidoras processam esses dados para identificar anomalias. Os testes verificam a distribuição das partições, o comportamento durante a parada de componentes e a recuperação do sistema.

## Arquitetura e implementação

A aplicação foi executada localmente com Docker Compose, por meio do terminal Linux do WSL no Windows. Os serviços se comunicam pela rede interna do Compose. Não foi necessário executar os programas Python diretamente no computador hospedeiro.

| Componente | Implementação | Responsabilidade |
|---|---|---|
| Sensores | Três containers Python: `sensor-1`, `sensor-2` e `sensor-3` | Gerar e publicar leituras simuladas |
| Cluster Kafka | Três containers `apache/kafka:3.7.2` | Armazenar, replicar e disponibilizar mensagens |
| Inicialização | Serviço `kafka-init` | Aguardar acesso ao Kafka e criar o tópico se não existir |
| Consumidores | Instâncias Python do serviço `consumer` | Processar mensagens no grupo `processadores` |
| Persistência | SQLite no volume `consumer-data` | Armazenar alertas para consulta posterior |
| Operação e testes | `Makefile` | Iniciar serviços, simular falhas e coletar evidências |

O fluxo de dados é: sensores → tópico `dados-sensores` → consumidores → alertas no SQLite. O cluster usa KRaft, com cada nó exercendo os papéis de broker e controller, sem ZooKeeper. O tópico possui seis partições e duas réplicas por partição. As partições permitem dividir o trabalho, enquanto a replicação permite manter cópias dos dados em brokers diferentes.

O arquivo `producer/producer.py` gera um JSON com `timestamp`, `sensor_id`, `temperatura` e `vibracao`. O produtor tenta se conectar novamente se a conexão inicial falhar, serializa o JSON em UTF-8, publica a mensagem e aguarda o intervalo configurado. Na versão atual, o envio não usa chave; a mudança e sua consequência para a ordenação são detalhadas na seção de dificuldades.

O arquivo `consumer/consumer.py` lê as mensagens do tópico, verifica os valores de temperatura e vibração, identifica leituras acima dos limites configurados e grava os alertas em um banco SQLite compartilhado entre as instâncias. A verificação usa comparação estrita: valores iguais ao limite não geram alerta por aquele critério. Uma leitura que ultrapasse ambos os limites gera um registro com os dois motivos.

A tabela `alertas` contém identificador, sensor, temperatura, vibração, motivo e data de criação do registro. O consumidor também imprime a partição, o offset e os dados recebidos. O acesso ao SQLite usa a biblioteca padrão do Python; a conexão de gravação tem timeout de 30 segundos.

Configuramos o Docker Compose para executar duas instâncias do consumidor no grupo `processadores`. A configuração `restart: on-failure` permite reiniciar o processo se ele falhar, inclusive durante uma tentativa de conexão antes de o Kafka estar disponível. Essa retomada é feita pelo Docker; o consumidor não possui um laço próprio de tentativas de conexão.

O tópico foi configurado com seis partições e fator de replicação 2. Nas consultas ao grupo, verificamos que cada consumidor recebeu três partições. Além dos testes iniciais, usamos as evidências da execução `logs/relatorio-20260925-215434/` para verificar o processamento e a persistência dos alertas.

## Configuração, instalação e execução

São necessários Docker com suporte a containers Linux, Docker Compose v2 e GNU Make. No Windows, os comandos do Makefile devem ser executados em um ambiente como o WSL, pois utilizam ferramentas de shell Linux. O Docker precisa estar iniciado e acessível nesse terminal. O download das imagens e das dependências Python requer conexão com a internet na primeira construção.

Após obter o repositório, acesse sua pasta. Na primeira instalação, crie o arquivo de configuração a partir do exemplo; se já existir um `.env` configurado, preserve-o:

```bash
cp .env.example .env
make up
```

Os Dockerfiles constroem as aplicações com Python 3.12 e instalam `kafka-python-ng`. O produtor usa a versão 2.2.2 fixada em `requirements.txt`; o Dockerfile do consumidor não fixa a versão. As evidências desta execução identificam o cliente consumidor como 2.2.3.

Os valores fornecidos em `.env.example` são:

| Variável | Valor | Finalidade |
|---|---|---|
| `KAFKA_TOPIC` | `dados-sensores` | Nome do tópico |
| `KAFKA_NUM_PARTITIONS` | `6` | Partições na criação do tópico |
| `KAFKA_REPLICATION_FACTOR` | `2` | Réplicas por partição |
| `INTERVALO_ENVIO` | `2` | Intervalo entre envios, em segundos |
| `TEMP_MIN` / `TEMP_MAX` | `20.0` / `95.0` | Faixa de temperatura simulada |
| `VIBR_MIN` / `VIBR_MAX` | `0.1` / `10.0` | Faixa de vibração simulada |
| `GROUP_ID` | `processadores` | Grupo compartilhado pelos consumidores |
| `TEMP_LIMITE` / `VIBR_LIMITE` | `80.0` / `8.0` | Limites de detecção |
| `DB_PATH` | `/data/alertas.db` | Caminho do SQLite dentro do container |

Os endereços dos brokers e os identificadores dos sensores são definidos no Compose. Alterar as variáveis de partições ou replicação não modifica automaticamente um tópico já existente, pois a inicialização usa `--create --if-not-exists`.

## Como operar a aplicação

| Comando | Uso |
|---|---|
| `make up` | Construir e iniciar com dois consumidores |
| `make logs` | Acompanhar os logs continuamente |
| `make status` | Consultar containers ativos e parados |
| `make topic` | Consultar partições, líderes e réplicas |
| `make group` | Consultar membros e partições atribuídas |
| `make offsets` | Consultar offsets e atraso do grupo |
| `make alerts` | Consultar total e últimos alertas no SQLite |
| `make fail-consumer` | Parar o primeiro consumidor listado |
| `make fail-broker` | Parar o broker `kafka-2` |
| `make recover` | Iniciar novamente os containers parados |
| `make scale N=4` | Alterar o número de consumidores para quatro |
| `make scale N=2` | Voltar a dois consumidores |
| `make load` | Recriar os sensores com intervalo de 0,1 segundo |
| `make normal` | Restaurar o intervalo definido no `.env` |
| `make evidence ETAPA=manual` | Salvar consultas e logs do estado atual |
| `make down` | Remover containers preservando o volume SQLite |

O comando `make alerts` abre um container temporário para uma consulta de leitura ao banco; ele não cria outro consumidor no grupo. Para sair do acompanhamento de logs, use Ctrl+C; isso encerra o acompanhamento, sem parar os serviços. O serviço `kafka-init` é uma tarefa de inicialização: encerrar com `Exited (0)` é o comportamento esperado.

## Metodologia dos testes

Executamos cenários manuais e, depois, o roteiro do Makefile:

```bash
make report
```

O roteiro inicia com dois consumidores e coleta nove etapas: operação normal; falha do consumidor; recuperação do consumidor; falha do broker; recuperação do broker; maior carga com dois consumidores; maior carga com quatro consumidores; redução para dois; restauração do intervalo normal.

Cada etapa espera 45 segundos antes da coleta. Dentro da coleta, duas consultas são separadas por uma pausa de 10 segundos. Essas pausas são intencionais, para permitir a estabilização e a observação do avanço do processamento. O tempo total inclui ainda construções, recriações de containers e execução das consultas. Em máquinas mais lentas, o tempo de espera pode ser aumentado:

```bash
make report ESPERA=90
```

As saídas ficam em `logs/relatorio-DATA-HORA/`, com uma subpasta por etapa. `antes.txt` reúne estado dos containers, tópico, grupo, offsets e alertas; `depois.txt` registra uma segunda consulta de offsets e alertas; `containers.log` contém os últimos três minutos de logs. As janelas de logs podem se sobrepor entre etapas. Os nomes antes/depois representam as duas amostras dentro da etapa, e não necessariamente os instantes anteriores e posteriores à falha.

Para interpretar as consultas, usamos `Leader` para identificar o broker líder, `Replicas` para as cópias configuradas e `Isr` para as réplicas sincronizadas. No grupo, `ASSIGNMENT` mostra a atribuição de partições. `CURRENT-OFFSET` representa a posição confirmada pelo grupo, `LOG-END-OFFSET` o fim do log, e `LAG` a diferença entre ambos. Como a confirmação dos offsets é periódica, o atraso consultado não mede diretamente a quantidade de mensagens ainda não processadas naquele instante.

Se o roteiro for interrompido, a restauração pode ser feita com:

```bash
make recover
make scale N=2
make normal
```

## Dificuldades encontradas e soluções

### 1. Dois consumidores ativos, mas apenas um recebendo mensagens novas

Ao acompanhar `docker compose logs -f`, inicialmente apareciam os três sensores, mas apenas um consumidor mostrava processamento de mensagens novas. Nossa primeira dificuldade foi distinguir um consumidor parado de um consumidor ativo sem mensagens nas suas partições.

O comando `docker compose ps` mostrava os dois consumidores ativos. Em seguida, consultamos as atribuições do grupo. A saída abaixo é um recorte da consulta enviada no terminal durante o diagnóstico; mantivemos apenas as colunas relevantes:

```text
HOST          #PARTITIONS  ASSIGNMENT
/172.18.0.9   3            dados-sensores(3,4,5)
/172.18.0.8   3            dados-sensores(0,1,2)
```

Isso mostrou que a atribuição de partições funcionava. Porém, receber três partições não significa receber metade das mensagens. Para identificar onde os dados chegavam, consultamos os offsets duas vezes. Os recortes abaixo preservam os valores observados nas duas consultas compartilhadas durante o diagnóstico:

```text
Primeira consulta
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
3          58              58              0
4          55              55              0
5          49              49              0
0          49              49              0
1          11528           11528           0
2          5779            5780            1
```

```text
Segunda consulta
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
3          58              58              0
4          55              55              0
5          49              49              0
0          49              49              0
1          11542           11544           2
2          5787            5787            0
```

As partições 1 e 2 receberam, respectivamente, 16 e 7 mensagens novas. As demais permaneceram sem alteração. Como as duas partições estavam atribuídas ao mesmo consumidor, apenas ele apresentava atividade nova nos logs.

O produtor usava o identificador do sensor como chave. Essa escolha mantinha as mensagens de cada sensor na mesma partição, concentrando o tráfego em poucas partições. Para a demonstração de balanceamento, alteramos o envio:

```python
# Antes
producer.send(topic, key=sensor_id, value=payload)

# Depois: versão atual do código
producer.send(topic, value=payload)
```

O identificador continua no JSON, mas deixou de ser usado como chave de particionamento. Após reconstruir os produtores, observamos atividade nas duas instâncias. Essa mudança favoreceu a distribuição das mensagens, mas deixou de garantir a ordem das leituras de um mesmo sensor entre partições.

Na execução posterior, o consumidor que atende as partições 3, 4 e 5 já apresentava mensagens. Trecho literal de `01-normal/containers.log`:

```text
consumer-2  | 2026-09-26T00:52:54.136218913Z partição=3 offset=335 dados={'timestamp': '2026-09-26T00:52:54.130816', 'sensor_id': 'sensor-2', 'temperatura': 21.0, 'vibracao': 6.92}
consumer-2  | 2026-09-26T00:52:54.611414499Z partição=4 offset=313 dados={'timestamp': '2026-09-26T00:52:54.608087', 'sensor_id': 'sensor-1', 'temperatura': 24.95, 'vibracao': 3.97}
```

### 2. Interpretar a presença de apenas um consumidor após a simulação de falha

Depois de executar `make fail-consumer`, os logs passaram a mostrar somente o consumidor restante. Inicialmente houve dúvida se isso representava um problema. Nesse cenário, porém, parar uma instância era a ação planejada; o resultado esperado era a outra assumir todas as partições.

A coleta `02-falha-consumidor/antes.txt` registrou a parada de uma instância e a atribuição das seis partições à outra. Abaixo, recortes com apenas as colunas relevantes:

```text
NAME                      STATUS
trab1_distri-consumer-1    Exited (137) 46 seconds ago
trab1_distri-consumer-2    Up 28 hours
```

```text
GROUP          HOST          #PARTITIONS  ASSIGNMENT
processadores  /172.18.0.9   6            dados-sensores(0,1,2,3,4,5)
```

A consulta ao grupo permitiu comprovar o rebalanço, em vez de concluir apenas pela quantidade de nomes visíveis nos logs. Após `make recover`, voltamos a observar duas instâncias com três partições cada.

### 3. Distinguir a detecção de anomalias da gravação no banco

Outra dificuldade de validação foi comprovar que o alerta não ficava apenas no terminal. Para isso, acrescentamos uma consulta de leitura ao SQLite, usando o caminho configurado em `DB_PATH`. A consulta mostrou tanto a quantidade acumulada quanto os últimos registros gravados.

Trecho literal de `09-final/depois.txt`:

```text
Total de alertas: 10010
Ultimos 10: id, sensor, temperatura, vibracao, motivo, criado_em
(10010, 'sensor-3', 39.06, 8.73, 'vibracao', '2026-09-26 01:06:39')
(10009, 'sensor-2', 81.43, 4.34, 'temperatura', '2026-09-26 01:06:35')
(10008, 'sensor-2', 61.0, 9.13, 'vibracao', '2026-09-26 01:06:31')
```

Esses registros comprovam que havia alertas persistidos por temperatura e vibração. O total é acumulado no banco, incluindo execuções anteriores; não representa a quantidade de alertas produzidos exclusivamente neste teste. Não identificamos, nas evidências citadas, uma falha de gravação no SQLite: a dificuldade foi obter a confirmação da persistência.

## Observação dos testes integrados: logs normais durante a queda de um broker

Ao executar `make fail-broker`, os consumidores continuaram mostrando mensagens, o que inicialmente gerou dúvida sobre se a falha havia ocorrido. A continuidade era o comportamento desejado. Para confirmar a falha, consultamos o estado do container e as réplicas sincronizadas.

Na coleta `04-falha-broker/antes.txt`, o estado do serviço foi:

```text
NAME       STATUS
kafka-2    Exited (137) 48 seconds ago
```

No mesmo arquivo, as partições que tinham uma réplica no broker 2 passaram a mostrar apenas a outra réplica em `Isr`. Recorte da consulta ao tópico, com espaços normalizados:

```text
Topic: dados-sensores  Partition: 1  Leader: 1  Replicas: 1,2  Isr: 1
Topic: dados-sensores  Partition: 2  Leader: 3  Replicas: 2,3  Isr: 3
Topic: dados-sensores  Partition: 3  Leader: 1  Replicas: 2,1  Isr: 1
Topic: dados-sensores  Partition: 5  Leader: 3  Replicas: 3,2  Isr: 3
```

O consumidor continuou operando com os brokers disponíveis. Depois da recuperação, a consulta compartilhada no terminal mostrou novamente as duas réplicas sincronizadas. Por exemplo:

```text
Topic: dados-sensores  Partition: 2  Leader: 3  Replicas: 2,3  Isr: 3,2
Topic: dados-sensores  Partition: 3  Leader: 1  Replicas: 2,1  Isr: 1,2
```

Aprendemos que logs aparentemente normais não descartam uma falha de infraestrutura. Foi necessário combinar o estado dos containers, as informações do tópico e o avanço do processamento para interpretar o teste.

## Elasticidade e processamento sob maior carga

Para aumentar a frequência de geração, `make load` aplica uma configuração temporária com intervalo de 0,1 segundo entre os envios, sem editar o `.env`. Esse intervalo é vinte vezes menor que o valor padrão de 2 segundos, mas não representa uma medição de vazão: o tempo de envio e de espera pelo Kafka também participa do ciclo.

Com essa carga, ampliamos o grupo de dois para quatro consumidores. A consulta em `07-carga-quatro-consumidores/antes.txt` mostrou a distribuição abaixo. Foram mantidas apenas as colunas relevantes:

```text
GROUP          HOST           #PARTITIONS  ASSIGNMENT
processadores  /172.18.0.9    1            dados-sensores(5)
processadores  /172.18.0.7    2            dados-sensores(0,1)
processadores  /172.18.0.8    1            dados-sensores(4)
processadores  /172.18.0.6    2            dados-sensores(2,3)
```

As seis partições ficaram distribuídas entre quatro instâncias, sem sobreposição nas atribuições. Depois, executamos `make scale N=2`. Em `08-reducao-consumidores/antes.txt`, observamos:

```text
GROUP          HOST           #PARTITIONS  ASSIGNMENT
processadores  /172.18.0.9    3            dados-sensores(3,4,5)
processadores  /172.18.0.8    3            dados-sensores(0,1,2)
```

Isso demonstra expansão e redução do grupo com redistribuição automática das partições. Como o comando de escala reaplica o Compose base, o roteiro executa `make load` novamente após cada alteração de escala para manter a condição de maior carga.

Também observamos atraso nas consultas realizadas durante a carga. Recorte de `07-carga-quatro-consumidores/antes.txt`:

```text
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
0          1065            1083            18
1          12647           12659           12
2          6769            6773            4
3          1130            1134            4
5          1080            1098            18
4          1043            1061            18
```

Após restaurar o intervalo normal, a amostra `09-final/depois.txt` apresentou:

```text
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
3          1678            1678            0
4          1576            1576            0
5          1635            1636            1
0          1614            1615            1
1          13189           13192           3
2          7332            7333            1
```

Os dados mostram processamento em todas as partições e menor atraso confirmado nessa amostra final. Não permitem concluir que quatro consumidores aumentaram a vazão em determinada proporção: não realizamos um benchmark controlado de desempenho.

## Resultados, limitações e melhorias

| Objetivo | Resultado observado |
|---|---|
| Cluster com múltiplos brokers | Três brokers, seis partições e replicação 2 |
| Sensores em containers distintos | Três produtores gerando temperatura e vibração |
| Balanceamento | Partições atribuídas aos membros do mesmo grupo; tráfego distribuído após retirar a chave |
| Falha de consumidor | Instância restante assumiu as seis partições |
| Falha de broker | Continuidade observada com `kafka-2` parado e outros líderes disponíveis |
| Recuperação | Retorno dos consumidores e das réplicas sincronizadas |
| Elasticidade | Expansão de dois para quatro consumidores e redução para dois |
| Persistência | Consulta final encontrou 10010 alertas acumulados no SQLite |
| Evidências | Consultas e logs separados por etapa do roteiro |

As evidências mostraram consumidores no mesmo grupo, processamento distribuído após a mudança do envio, reatribuição das partições durante a falha de um consumidor e alertas persistidos no SQLite. Os testes integrados também mostraram continuidade durante a indisponibilidade de um broker e posterior recuperação das réplicas.

Essas verificações não comprovam ausência absoluta de perdas ou duplicações, nem medem o tempo exato de recuperação. Para isso, seria necessário comparar individualmente as mensagens produzidas e processadas e medir os instantes de falha e retomada. Pequenos valores de `LAG` em uma consulta isolada também não demonstram, por si só, um problema de processamento.

A principal dificuldade funcional observada foi a concentração inicial das mensagens em um consumidor. A retirada da chave resolveu a demonstração de distribuição, com a contrapartida da perda de ordenação por sensor entre partições. Não identificamos falha de persistência nas consultas apresentadas.

Há limites na implementação atual. Os serviços de aplicação dependem da criação dos containers Kafka, mas não aguardam a conclusão do `kafka-init`; portanto, a configuração do tópico deve ser conferida após uma instalação limpa. O volume declarado protege o SQLite, mas o Compose não configura volumes explícitos para persistir os dados dos brokers após a remoção dos containers. A recuperação testada foi de processos parados, não a perda de toda a máquina ou recriação integral do cluster.

O banco SQLite compartilhado pode limitar a concorrência de gravação em cargas maiores; este trabalho não mediu seu limite. A confirmação de offsets não é coordenada transacionalmente com a gravação dos alertas, e o produtor não valida individualmente o resultado de cada envio por meio do futuro retornado por `send`. Esses pontos devem ser tratados antes de afirmar garantias de entrega sem perdas ou duplicações.

Como melhorias, propomos coordenar a inicialização com a disponibilidade do tópico, persistir os dados dos brokers em volumes, fixar as versões das dependências e incluir identificadores únicos nas mensagens para comparar produção, processamento e gravação. As mudanças não foram necessárias para demonstrar os cenários observados, mas tornariam os testes de confiabilidade mais completos.

Os blocos desta seção usam saídas reais compartilhadas durante o diagnóstico ou arquivos da coleta `logs/relatorio-20260925-215434/`. Recortes de colunas e normalizações de espaços foram indicados; não foram inventadas mensagens de erro para representar as dificuldades.
