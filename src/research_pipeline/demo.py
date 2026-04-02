"""デモモード用パイプライン — DEMO_MODE=true の時に使用されます。

実際のスクレイピングをスキップして、UIの確認用サンプルデータを返します。
"""

from __future__ import annotations
import time
import random
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ResearchResult:
    product_name: str
    shopee_price: float
    japan_supplier_price: float
    estimated_profit_jpy: float
    roi_percent: float
    supplier_url: str
    competition_price: Optional[float] = None
    match_confidence: str = ""
    match_method: str = ""
    japan_source: str = ""
    shopee_url: str = ""


@dataclass
class PipelineReport:
    keyword: str
    results: List[ResearchResult] = field(default_factory=list)
    products_scraped: int = 0
    japan_sources_found: int = 0
    matches_found: int = 0
    profitable_count: int = 0
    elapsed_seconds: float = 0.0


# キーワード別サンプルデータ
_DEMO_DATA = {
    "たまごっち": [
        ("たまごっち スマート コーデオン ホワイト Tamagotchi Smart", 1850, 3480, 4820, 58.3, "rakuten", "https://item.rakuten.co.jp/bandai/tamagotchi-smart-white/", 1920),
        ("たまごっち ユニ ホワイト Tamagotchi Uni 新品", 2480, 5980, 3620, 38.1, "yahoo_shopping", "https://store.shopping.yahoo.co.jp/tamagotchi-uni-white", 2550),
        ("たまごっち オリジナル 復刻版 Original 20th Anniversary", 980, 1580, 3240, 67.2, "mercari", "https://www.mercari.com/jp/items/tamagotchi-original-20th/", 1050),
        ("たまごっち スマートデラックス ピンク Tamagotchi Smart DX Pink", 2190, 4580, 3980, 51.2, "rakuten", "https://item.rakuten.co.jp/bandai/tamagotchi-smart-dx-pink/", 2280),
        ("たまごっち meets ワンダーガーデンver Meets Wonder Garden", 1640, 2980, 4120, 62.8, "amazon_jp", "https://www.amazon.co.jp/dp/B07TAMAGOTCHI/", None),
    ],
    "ポケモン": [
        ("ポケモンカード スカーレット&バイオレット 拡張パック 1BOX 36パック入り", 3200, 5500, 6800, 71.4, "rakuten", "https://item.rakuten.co.jp/pokemon-card/sv-box-36p/", 3350),
        ("ピカチュウ ぬいぐるみ XL サイズ 公式 Pokemon Center", 1450, 2800, 3920, 54.8, "amazon_jp", "https://www.amazon.co.jp/dp/B0PIKACHU01/", 1520),
        ("ポケモンカード 151 強化拡張パック 1BOX", 4800, 9800, 4200, 29.7, "yahoo_shopping", "https://store.shopping.yahoo.co.jp/pokemon-151-box", 4900),
        ("ポケモン モンスターボール プレミアム Premium Ball", 2100, 3980, 4560, 59.2, "rakuten", "https://item.rakuten.co.jp/premium-ball-pokemon/", 2200),
    ],
    "default": [
        ("バンダイ ガンプラ RG ウイングガンダムEW 1/144スケール", 1280, 2200, 4380, 63.2, "rakuten", "https://item.rakuten.co.jp/bandai/rg-wing-gundam-ew/", 1350),
        ("ワンピースカード ブースターパック 双璧の覇者 1BOX", 2650, 4800, 5200, 67.8, "yahoo_shopping", "https://store.shopping.yahoo.co.jp/one-piece-card-box", 2750),
        ("ドラゴンボール超 アドバーズ フィギュア 孫悟空 Ultra Instinct", 1890, 3500, 4100, 56.9, "amazon_jp", "https://www.amazon.co.jp/dp/B0GOKU001/", 1960),
        ("鬼滅の刃 ねんどろいど 竈門炭治郎 グッドスマイルカンパニー", 2340, 4800, 3680, 42.3, "rakuten", "https://item.rakuten.co.jp/goodsmile/tanjiro-nendoroid/", 2420),
        ("呪術廻戦 フィギュア 五条悟 特別版 約25cm PVC製", 2780, 5200, 4920, 55.4, "mercari", "https://www.mercari.com/jp/items/jujutsu-gojo-figure/", 2850),
        ("遊戯王 マスターデュエル スターターパック 2024 正規品", 980, 1480, 3750, 78.4, "yahoo_shopping", "https://store.shopping.yahoo.co.jp/yugioh-starter-2024", 1050),
        ("鉄腕アトム レトロ フィギュア 復刻版 手塚プロダクション", 1560, 2800, 3840, 60.2, "amazon_jp", "https://www.amazon.co.jp/dp/B0ASTRO001/", None),
    ],
}


def run_demo_pipeline(keyword: str) -> PipelineReport:
    """キーワードに応じたサンプルデータを返すデモパイプライン。"""
    t0 = time.time()

    # キーワードに合うサンプルデータを選択
    products_data = None
    for key, data in _DEMO_DATA.items():
        if key != "default" and key in keyword:
            products_data = data
            break
    if products_data is None:
        products_data = _DEMO_DATA["default"]

    # 処理中っぽく少し待機
    time.sleep(random.uniform(2.5, 4.0))

    results = []
    for (name, shopee_price, jp_price, profit, roi, source, url, comp_price) in products_data:
        results.append(ResearchResult(
            product_name=name,
            shopee_price=float(shopee_price),
            japan_supplier_price=float(jp_price),
            estimated_profit_jpy=float(profit),
            roi_percent=float(roi),
            supplier_url=url,
            competition_price=float(comp_price) if comp_price else None,
            match_confidence="high",
            match_method="title_fuzzy",
            japan_source=source,
            shopee_url=f"https://shopee.ph/search?keyword={keyword}",
        ))

    n = len(results)
    report = PipelineReport(
        keyword=keyword,
        results=results,
        products_scraped=random.randint(35, 50),
        japan_sources_found=random.randint(n * 3, n * 5),
        matches_found=random.randint(n + 2, n + 8),
        profitable_count=n,
        elapsed_seconds=time.time() - t0,
    )
    return report
