# Convencoes de trabalho neste projeto

## Caracteres

Nenhum texto ou codigo deste projeto (documentacao, notebooks, comentarios,
nomes de arquivo) usa caracteres que nao possam ser digitados num teclado
comum: sem travessao longo (em dash), sem travessao medio (en dash), sem
setas unicode, sem aspas curvas (smart quotes), sem caracteres de desenho
de caixa, sem emoji. Acentos do portugues (a, c, e etc.) sao permitidos.
No lugar de travessao, usa-se virgula, ponto e virgula, ou hifen comum
cercado de espaco.

Por que: o material vai virar base de estudo (Kurukshetra), e o aluno
precisa conseguir reproduzir/transcrever tudo digitando manualmente, sem
depender de copiar e colar caracteres especiais.

## Explicacao de decisoes

Toda decisao arquitetural (por que Delta Lake e nao Parquet puro, por que
Volume e nao S3, por que window function pra reconciliar granularidade
etc.) e explicada no momento em que e tomada, nao so implementada.

## Trechos do material original destacados como importantes

Quando o roteiro do curso (video/NotebookLM) marcar um trecho como aviso,
disclaimer, ou ponto de atencao ("cuidado com X", "isso e opcional", "erro
comum aqui e Y"), esse trecho e reproduzido e explicado no momento em que o
codigo correspondente e escrito, nao só mencionado de passagem.

## Trechos de codigo importantes

Codigo que carrega uma decisao nao obvia (uma regex, uma window function,
um coalesce encadeado, uma chave substituta via hash) e explicado linha a
linha ou por partes, nao so colado inteiro.

## Motivo geral

Toda essa documentacao extra tem um destino: depois que o projeto estiver
funcionando de ponta a ponta, ele vira materia-prima para os capitulos de
Engenharia de Dados do Kurukshetra (exercicios, checkpoints de memoria,
gabaritos). Por isso o padrao de explicacao aqui e mais detalhado do que
seria necessario so para "fazer funcionar".
