# Banco de Dados II - Trabalho 1

## Criar transações em diferentes linguagens de programação

<p>O desafio aqui é implementar uma transação em diferentes linguagens de programação. Ao implementar uma transação deve-se ficar atento a questão de como inicia-lá e finaliza-lá. Ou seja, algumas linguagens apenas executam a operação de commit quando for explicitamente invocado o comando de finalização.
</p>
<p><b>Detalhes que devem ser atendidas:</b><ul>
  <li>0- Usar o esquema presente no link (usado para criação de triggers). </li>
<li> 1-  Inserir as 10.001 tuplas na tabela product usando o Postgres, os dados estão no arquivo data.csv (moodle); 
<li> 2- A linguagem a ser utilizada por cada grupo será definida via sorteio; </li>
<li> 3- Cada grupo irá elaborar um vídeo (máximo de 5min) com a explicação e o grupo deve estar presente em aula para responder as dúvidas. O grupo deve participar do vídeo como um todo. </li></ul></p>
<p><b>Funções a serem implementadas:</b>
<ul><li>1- Fazer a inserção dos novos produtos na tabela Product utilizando uma função com transações implícitas vs. transações explícitas (comando insert a cada nova inserção). Compare o tempo de inserção. Explique o motivo da variação do tempo.</li>
<li>2- Implementar uma função que cause um rollback na transação (ex: um campo mal formado). Alguma tupla foi salva no caso da implícita e da explícita? <li></ul>
  </p>
