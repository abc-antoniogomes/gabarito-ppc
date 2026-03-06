# 📊 Projeto Google — Validação de Gabarito

Dashboard interno para validar cruzamentos entre produtos do catálogo (MDM) e anúncios encontrados pelo scraping Google.

---

## 📁 Estrutura da Pasta

```
Projeto Google/
├── gabarito.csv          ← Base principal de dados (gerado pelo script abaixo)
├── gabarito_validado.xlsx ← Salvo pelo Streamlit ao clicar "Salvar Validação"
├── imagens_mdm.csv       ← Catálogo de imagens do MDM (EAN, CodigoProdutoFabricante, imagem)
├── dashboard.py          ← Dashboard de validação
├── gerar_gabarito.py     ← Script para (re)gerar o gabarito.csv
└── README.md             ← Este arquivo
```

> **Os JSONs e arquivos xlsx pesados não são necessários para rodar o Streamlit.**
> Eles só são usados ao rodar `gerar_gabarito.py`.

---

## 🚀 Como usar

### 1. Gerar o gabarito (rodar uma vez)

```bash
py gerar_gabarito.py
```

Lê `relatorio_identificado.xlsx` + JSONs em `JSON_GOOGLE/` e `JSON_SHOPPING/`,
extrai thumbnails e links reais por vendedor, e salva `gabarito.csv`.

> ⚠️ Após gerar o gabarito, os JSONs e o xlsx podem ser removidos da pasta — o Streamlit não vai precisar deles.

### 2. Rodar o dashboard

```powershell
# Dentro da pasta do projeto
py -m streamlit run dashboard.py
```

Abre no navegador em `http://localhost:8501`

---

## 📋 Colunas do gabarito.csv

| Coluna | Descrição |
|--------|-----------|
| `EAN_Pesquisado` | EAN do produto pesquisado |
| `Vendedor` | Nome do vendedor/anunciante |
| `Origem` | `Google Shopping` ou `Google Search` |
| `Nome_do_Anuncio` | Título do anúncio retornado |
| `Preco` | Preço encontrado |
| `Link_Real` | URL real do anúncio (não SerpAPI) |
| `Thumbnail_Vendedor` | URL da imagem do anúncio deste vendedor |
| `CodigoTop` | Código interno do melhor match no catálogo |
| `NomeTop` | Nome do produto interno |
| `MDM_Fabricante` | Fabricante conforme o MDM |
| `Revisado` | `Sim` / `Não` — preenchido manualmente no Streamlit |
| `Valido` | `Sim` / `Não` — preenchido manualmente no Streamlit |

---

## 🔄 Fluxo de atualização

1. Rodar `gerar_gabarito.py` para regenerar com novos dados
2. Abrir o Streamlit (`py -m streamlit run dashboard.py`) e validar linha a linha
3. Clicar "Salvar Validação" → gera `gabarito_validado.xlsx`
4. Enviar `gabarito_validado.xlsx` para o Google Drive / planilha compartilhada
