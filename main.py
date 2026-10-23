import os
import sys
import uuid
import requests
from datetime import datetime, timezone
from google.cloud import firestore

# =====================================================================
# 1. CONFIGURAÇÕES GERAIS E BANCO DE DADOS
# =====================================================================
DATABASE_ID = os.getenv("FIRESTORE_DATABASE_ID", "ai-studio-59483fc8-da5a-456d-a48b-6ca587e00652")

# Mapeamento oficial das 5 contas x Lojas no CRM
MAPA_CONTAS_LOJAS = {
    "614985114619505": {
        "loja_nome": "Robmar Automoveis",
        "loja_slug": "robmar-automoveis",
        "segmento": "automoveis"
    },
    "2236596563762777": {
        "loja_nome": "Shineray São João",
        "loja_slug": "shineray-sao-joao",
        "segmento": "motos"
    },
    "1357823405728639": {
        "loja_nome": "Shineray Nova Iguaçu",
        "loja_slug": "shineray-nova-iguacu",
        "segmento": "motos"
    },
    "1751330312970905": {
        "loja_nome": "Shineray Belford Roxo",
        "loja_slug": "shineray-belford-roxo",
        "segmento": "motos"
    },
    "3408513186029752": {
        "loja_nome": "Shineray Valqueire",
        "loja_slug": "shineray-valqueire",
        "segmento": "motos"
    }
}

# =====================================================================
# 2. FUNÇÃO AUXILIAR DE PAGINAÇÃO DA GRAPH API DO META
# =====================================================================
def consultar_meta_api(endpoint, params, token, versao_api):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.facebook.com/{versao_api}/{endpoint}"
    resultados = []
    
    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        dados = resp.json()
        
        if "error" in dados:
            print(f"❌ Erro Meta API ({endpoint}): {dados['error'].get('message')}")
            break
            
        resultados.extend(dados.get("data", []))
        url = dados.get("paging", {}).get("next")
        params = {}  # Esvazia os params porque a chave 'next' já traz a query string completa
        
    return resultados

# =====================================================================
# 3. ROTINA PRINCIPAL DE SINCRONIZAÇÃO
# =====================================================================
def executar_sincronizacao():
    print("🚀 [JOB] A iniciar sincronização de tráfego pago Meta...")
    
    # Inicialização do Firestore no ambiente de nuvem via Application Default Credentials
    db = firestore.Client(database=DATABASE_ID)
    
    # Leitura do documento central de credenciais
    doc_config = db.collection("trafego_pago_meta_ads").document("credenciais_meta_ads").get()
    if not doc_config.exists:
        print("❌ Erro crítico: Documento 'credenciais_meta_ads' não encontrado.")
        sys.exit(1)
        
    config = doc_config.to_dict()
    token = config.get("token")
    versao_api = config.get("versaoApi", "v21.0")
    ativo = config.get("ativo", True)
    
    if not ativo:
        print("⏸️ Sincronização pausada pelas configurações do banco. A encerrar.")
        return

    if not token:
        print("❌ Erro crítico: Token do Meta não configurado no Firestore.")
        sys.exit(1)

    sync_id = str(uuid.uuid4())
    agora_iso = datetime.now(timezone.utc).isoformat()
    
    subcolecao_anuncios = (
        db.collection("trafego_pago_meta_ads")
        .document("anuncios_ativos")
        .collection("itens")
    )
    
    total_ativos = 0
    total_inativados = 0

    for conta_id_num, info_loja in MAPA_CONTAS_LOJAS.items():
        conta_id_api = f"act_{conta_id_num}"
        loja_crm = info_loja["loja_nome"]
        loja_slug = info_loja["loja_slug"]
        segmento = info_loja["segmento"]

        print(f"\n📂 A processar Loja: {loja_crm} ({conta_id_api})")

        # 1. Consulta Campanhas
        campanhas = consultar_meta_api(
            f"{conta_id_api}/campaigns", 
            {"fields": "id,name", "limit": 100}, 
            token, 
            versao_api
        )
        mapa_campanhas = {c["id"]: c.get("name", "Campanha sem nome") for c in campanhas}

        # 2. Consulta Conjuntos de Anúncios
        adsets = consultar_meta_api(
            f"{conta_id_api}/adsets", 
            {"fields": "id,name", "limit": 100}, 
            token, 
            versao_api
        )
        mapa_adsets = {s["id"]: s.get("name", "Conjunto sem nome") for s in adsets}

        # 3. Consulta Anúncios Ativos
        ads_ativos = consultar_meta_api(
            f"{conta_id_api}/ads", 
            {
                "fields": "id,name,campaign_id,adset_id,status", 
                "effective_status": '["ACTIVE"]', 
                "limit": 100
            }, 
            token, 
            versao_api
        )

        # 4. Upsert em Lote (Batch) dos Anúncios Ativos
        batch = db.batch()
        batch_count = 0

        for ad in ads_ativos:
            ad_id = str(ad.get("id"))
            camp_id = str(ad.get("campaign_id"))
            adset_id = str(ad.get("adset_id"))

            doc_ref_ad = subcolecao_anuncios.document(ad_id)
            dados = {
                "ad_id": ad_id,
                "ad_name": ad.get("name"),
                "adset_id": adset_id,
                "adset_name": mapa_adsets.get(adset_id, "Não identificado"),
                "campaign_id": camp_id,
                "campaign_name": mapa_campanhas.get(camp_id, "Não identificado"),
                "account_id": conta_id_api,
                "loja_nome": loja_crm,
                "loja_slug": loja_slug,
                "segmento": segmento,
                "status": "ACTIVE",
                "sync_id": sync_id,
                "atualizado_em": agora_iso
            }
            batch.set(doc_ref_ad, dados, merge=True)
            batch_count += 1
            total_ativos += 1

            if batch_count >= 400:
                batch.commit()
                batch = db.batch()
                batch_count = 0

        if batch_count > 0:
            batch.commit()

        print(f"   ✅ {len(ads_ativos)} anúncios ativos atualizados.")

        # 5. Inativação de anúncios pausados ou removidos da Meta
        docs_no_banco = (
            subcolecao_anuncios
            .where("account_id", "==", conta_id_api)
            .where("status", "==", "ACTIVE")
            .stream()
        )

        batch_inativar = db.batch()
        inativar_count = 0

        for doc_banco in docs_no_banco:
            dados_banco = doc_banco.to_dict()
            if dados_banco.get("sync_id") != sync_id:
                batch_inativar.update(doc_banco.reference, {
                    "status": "INACTIVE",
                    "atualizado_em": agora_iso
                })
                inativar_count += 1
                total_inativados += 1

                if inativar_count >= 400:
                    batch_inativar.commit()
                    batch_inativar = db.batch()
                    inativar_count = 0

        if inativar_count > 0:
            batch_inativar.commit()
            print(f"   ⏸️ {inativar_count} anúncios alterados para INACTIVE.")

    # 6. Atualização de Metadados da Execução
    db.collection("trafego_pago_meta_ads").document("anuncios_ativos").set({
        "total_anuncios_ativos": total_ativos,
        "total_anuncios_inativados": total_inativados,
        "ultima_execucao": agora_iso,
        "ultimo_sync_id": sync_id
    }, merge=True)

    print("\n🏁 [JOB FINALIZADO COM SUCESSO]")
    print(f"   • Anúncios ativos mantidos/inseridos: {total_ativos}")
    print(f"   • Anúncios inativados nesta rodada: {total_inativados}")

if __name__ == "__main__":
    executar_sincronizacao()
