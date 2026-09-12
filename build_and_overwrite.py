import os
import re
from google import genai
from pyspark.sql import SparkSession

# 1. 由 Python 安全解析各種觸發來源的指示，完全避免 Bash 語法錯誤
event_name = os.environ.get("EVENT_NAME", "")
input_prompt = os.environ.get("USER_PROMPT_INPUT", "").strip()
comment_body = os.environ.get("COMMENT_BODY", "").strip()

if event_name == "schedule":
    user_requirement = (
        "每日定期更新：校對交通與各雪場最新狀態，保持推薦最佳化。"
    )
elif event_name == "issue_comment":
    user_requirement = re.sub(r"^/build\s*", "", comment_body).strip()
elif input_prompt:
    user_requirement = input_prompt
else:
    user_requirement = "標準頂級滑雪美饌行程總覽"

print(f"本次執行需求: {user_requirement}")

# 2. 啟動 PySpark 建立 12/11 ~ 12/20 結構化行程資料表
spark = (
    SparkSession.builder.appName("SnowboardTripPipeline")
    .master("local[*]")
    .getOrCreate()
)

trip_records = [
    (
        "12/11",
        "高崎",
        "裝備補給與都市整備",
        "東京/機場 -> 北陸新幹線/JR高崎線 (約50分)",
        "高崎市區溫泉商旅",
        "無滑雪 (大型雪具整頓/雪蠟補給)",
        "登利平 (群馬烤雞便當)、Shango (炸豬排義大利麵)",
        "Gateau Festa Harada 法國脆餅 (高崎限定生吐司款)",
        "群馬地酒 水芭蕉 純米大吟釀、榛名牧場鮮乳",
        "高崎達摩不倒翁煎餅、上州名物烤饅頭",
    ),
    (
        "12/12",
        "丸沼高原 -> 佐久平",
        "波浪地形與日光白根山粉雪巡航",
        "高崎出發專車/租車 -> 丸沼 (約75分) -> 下午轉往佐久平 (約80分)",
        "佐久平站前飯店 (東橫INN/Aqua Hotel)",
        "丸沼高原 (標高2,000m)：天然波浪地形公園 (Frozen Wave)、樹林野雪",
        "佐久乃屋 安養寺熟成味噌拉麵、信州十四草蕎麥麵",
        "和泉屋菓子店 信州蘋果千層派、甘酒霜淇淋",
        "佐久名釀 澤之花 純米吟釀、信州白桃果汁",
        "安養寺味噌漬起司、輕井澤果醬",
    ),
    (
        "12/13",
        "Asama 2000 (高峰高原)",
        "乾燥粉雪與高海拔刻滑 (Carving) 訓練",
        "佐久平站蓼科口 -> 高峰高原接駁巴士 (JR Bus, 約60分直達)",
        "佐久平站前飯店",
        "Asama 2000 (標高2,000m)：日本超乾粉雪、平整壓雪陡坡刻滑練習",
        "高峰高原餐廳 信州牛黑咖哩、佐久平站前「竹幸亭」地雞炭火燒",
        "雲上 Cafe 手工生乳捲、八岳牛奶泡芙",
        "長野精釀 Yona Yona Ale、高山焙茶拿鐵",
        "淺間山熔岩巧克力、白樺高原牛奶夾心餅",
    ),
    (
        "12/14",
        "Asama 2000 -> 長野站",
        "晨間饅頭坡 (Mogul) 訓練後轉進長野門戶",
        "接駁巴士返回佐久平站 -> 北陸新幹線 (21分) 抵達長野站",
        "長野站直結飯店 (Hotel Metropolitan)",
        "Asama 2000：上午進行饅頭坡與短半徑回轉練習，中午後撤收轉移",
        "明治亭 醬汁炸豬排蓋飯、蕎麥處 草笛 (核桃蕎麥麵)",
        "いろは堂 (炭烤信州御燒餅)、旬彩蘋果肉桂派",
        "志賀高原啤酒 (玉村本店 IPA)、千曲杏桃沙士",
        "八幡屋礒五郎 七味唐辛子 (限定罐)、信州水鈴飴",
    ),
    (
        "12/15",
        "長野 -> 熊之湯 -> 橫手山",
        "直攻粉雪天堂熊之湯，午後進駐橫手山",
        "長野站東口 23 號站牌 -> 長電急行巴士 (75分) 直達熊之湯",
        "Hotel & Onsen 2307 Shiga Kogen (橫手山麓)",
        "熊之湯滑雪場：全北向單一山谷極致細粉雪、巨型天然饅頭坡、單板技術挑戰",
        "熊之湯山頂餐廳 野澤菜牛肉麵、Hotel 2307 炭火信州牛溫泉會席",
        "橫手山頂「Crumpet Cafe」英式正統鬆餅、高山溫泉饅頭",
        "志賀高原深層水低溫滴濾咖啡、玉村本店「其之十」美式 IPA",
        "志賀高原高山硫磺溫泉粉、善光寺味噌煎餅",
    ),
    (
        "12/16-17",
        "橫手山・澀峠 (志賀高原)",
        "全日本最高海拔滑雪與絕美樹冰巡航",
        "飯店出門即 Ski-in/Ski-out 橫手山纜車，全山漫遊",
        "Hotel & Onsen 2307 Shiga Kogen (連泊濃湯溫泉)",
        "橫手山・澀峠 (標高2,307m)：日本最高纜車站、壯觀樹冰群、超長景觀粉雪道",
        "橫手山滿天望餐廳 (羅宋牛肉湯配軟法麵包)、高山小屋燒咖哩",
        "橫手山頂「雲之上麵包店」手工富士蘋果麵包、焦糖布丁",
        "雪山特調濃醇熱可可、長野地酒「大雪溪」濁酒",
        "日本國道最高地點 (2,172m) 到達紀念餅乾、白葡萄乾黑巧",
    ),
    (
        "12/18",
        "志賀高原 -> 東京",
        "雪山撤收，新幹線返回東京繁華市區",
        "飯店接駁 -> 長電急行巴士 -> 長野站轉乘北陸新幹線 (82分) 抵達東京",
        "東京市區飯店 (銀座/丸之內商圈)",
        "上午橫手山最後兩趟巡航，中午退房啟程下山",
        "銀座 篝 (米其林雞白湯松露拉麵)、築地直送特選握壽司",
        "銀座篝火焦糖布丁、資生堂 Parlour 花椿千層蛋糕",
        "東京製茶所手工煎茶特調、銀座經典威士忌蘇打 (Highball)",
        "東京香蕉 2026 生黑巧限定版、NewYork Perfect Cheese",
    ),
    (
        "12/19",
        "橫濱",
        "港灣微風漫步與異國紅磚夜景",
        "東京站 -> JR 東海道線/橫須賀線 (約30分) 直達橫濱",
        "橫濱港未來景觀飯店 / 皇家花園飯店",
        "無滑雪 (港灣散策、紅磚倉庫探訪與慶祝晚宴)",
        "橫濱中華街百年「同發」脆皮燒臘、馬車道十番館 復古法式牛排",
        "馬車道經典焦糖烤布丁、元町喜久家 (Rum Ball 萊姆酒球)",
        "橫濱啤酒 (Yokohama Brewery 經典拉格)、港未來特調雞尾酒",
        "橫濱 Ariake Harbour 栗子蛋糕、紅磚瓦甘醇巧克力薄餅",
    ),
    (
        "12/20",
        "橫濱 -> 機場 -> 回高雄",
        "滿載戰利品直飛返家",
        "橫濱站 YCAT -> 京急巴士/特快車 (直達羽田約30分/成田約85分) -> 直飛高雄小港 (KHH)",
        "溫暖的家 (高雄)",
        "圓滿回程",
        "機場貴賓室 / 一風堂 特選黑帶豚骨拉麵",
        "羽田/成田限定 白桃鮮果大福",
        "伊藤園特濃日本綠茶、ASAHI 極上生啤酒",
        "免稅店 Royce 生巧克力、獺祭二割三分",
    ),
]

columns = [
    "date",
    "location",
    "theme",
    "transport",
    "stay",
    "ski_tech",
    "food",
    "dessert",
    "drinks",
    "omiyage",
]

df = spark.createDataFrame(trip_records, columns)
metrics_json = df.toPandas().to_json(orient="records", force_ascii=False)
spark.stop()

# 3. 呼叫 Gemini 生成極致深色雪山風 Dashboard
api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    raise ValueError("找不到 GEMINI_API_KEY，請確認 Secrets 是否設定！")

client = genai.Client(api_key=api_key)

prompt = f"""
你是由頂級日本滑雪教練、專業自由行策劃師與資深前端 UI/UX 設計師組成的菁英團隊。
以下是經由 PySpark 處理完畢的行程與情報資料集：
{metrics_json}

使用者的當前客製需求：
"{user_requirement}"

請直接輸出一個完整、自包含且視覺震撼的單頁 index.html：

【UI/UX 規範】：
1. 視覺設計：極致深色雪山奢華風（Dark Snow Theme），背景使用夜空深藍黑 (#0b1120 / #020617)，文字與邊框搭配冰雪藍 (#38bdf8 / #7dd3fc) 與金色榮譽徽章。
2. 引入資源：直接引用 Tailwind CSS (CDN)、Lucide Icons (CDN) 與 Google Fonts (Inter, Noto Sans TC)。
3. 頂部狀態列（Hero Header）：
   - 標題：「2026 日本長野・志賀・淺間頂級滑雪美饌遠征」。
   - 包含：10天9夜、橫跨4大雪場 (丸沼/Asama/熊之湯/橫手山2307m)、高雄往返 (KHH)。
   - 附帶「當前套用需求標籤」與「更新時間戳記」。
4. 每日卡片排版（Daily Itinerary Cards）：
   - 每日需清楚呈現：日期、地點、當日重點主題。
   - ⛷️ 滑雪地形指標（標高、雪質、波浪/刻滑/天然饅頭/樹冰等技術重點）。
   - 🚌 交通指南（交通工具、所需時間與路線）。
   - 4 大美饌伴手禮獨立彩色膠囊（Pills）：
     * 🍽️ 精選正餐
     * 🍰 必吃甜點與糕點
     * 🍶 特選地酒與飲品
     * 🎁 必買名產伴手禮
5. 響應式佈局：手機版流暢直列卡片，電腦版優雅寬屏時間軸。
6. 程式碼限制：僅輸出純 HTML 代碼，嚴禁包含 ```html 或 ``` 等 Markdown 圍欄字串。
"""

response = client.models.generate_content(
    model="gemini-3.6-flash",
    contents=prompt,
)

clean_html = response.text.replace("```html", "").replace("```", "").strip()

# 4. 覆寫根目錄 index.html
with open("index.html", "w", encoding="utf-8") as f:
    f.write(clean_html)

print("專屬滑雪行程 index.html 已成功生成並覆寫！")
