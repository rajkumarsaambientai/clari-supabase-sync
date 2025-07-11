import requests
import json
import csv
import os
import time
from datetime import datetime, timedelta

API_KEY = "1aAo6cu02x3eVFeHqK51O8GbW2CkXA8y4sMOUx1d"
API_PASSWORD = "732f1e7a-5f7f-435e-a701-558467bd70cc"
BASE_URL = "https://rest-api.copilot.clari.com"
HEADERS = {
    "X-Api-Key": API_KEY,
    "X-Api-Password": API_PASSWORD,
    "Accept": "application/json",
    "Content-Type": "application/json"
}

MINIMAL_FIELDS = [
    'call_id', 'opportunity_ids', 'account_ids', 'contact_ids',
    'full_summary', 'key_takeaways', 'topics_discussed', 'key_action_items', 'deal_stage_live'
]

LOG_FILE = "clari_copilot_batch.log"

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(msg)

def extract_minimal_fields(call_id, call):
    crm_info = call.get('crm_info', {})
    opportunity_ids = crm_info.get('deal_id', '')
    account_ids = crm_info.get('account_id', '')
    contact_ids = ','.join(crm_info.get('contact_ids', [])) if isinstance(crm_info.get('contact_ids', []), list) else crm_info.get('contact_ids', '')
    deal_stage_live = call.get('deal_stage_live', '')
    summary = call.get('summary', {})
    full_summary = summary.get('full_summary', '')
    key_takeaways = summary.get('key_takeaways', '')
    if isinstance(key_takeaways, list):
        key_takeaways_str = '\n'.join(f'- {item}' for item in key_takeaways)
    else:
        key_takeaways_str = key_takeaways
    topics_discussed = summary.get('topics_discussed', [])
    topics_json = json.dumps([
        {'name': t.get('name', ''), 'summary': t.get('summary', '')}
        for t in topics_discussed
    ])
    key_action_items = summary.get('key_action_items', [])
    action_items_json = json.dumps([
        {'action_item': a.get('action_item', ''), 'owner': a.get('owner_name', '')}
        for a in key_action_items
    ])
    return {
        'call_id': call_id,
        'opportunity_ids': opportunity_ids,
        'account_ids': account_ids,
        'contact_ids': contact_ids,
        'full_summary': full_summary,
        'key_takeaways': key_takeaways_str,
        'topics_discussed': topics_json,
        'key_action_items': action_items_json,
        'deal_stage_live': deal_stage_live
    }

def fetch_call_details(call_id, max_retries=3):
    url = f"{BASE_URL}/call-details?id={call_id}"
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                data = response.json()
                call = data.get('call', {})
                return call
            elif response.status_code in (429, 500, 502, 503, 504):
                log(f"[RETRY] Call {call_id}: API status {response.status_code}, attempt {attempt}/{max_retries}. Waiting 30s before retry.")
                time.sleep(30)
            else:
                log(f"[WARN] Call {call_id}: API status {response.status_code}")
                return None
        except Exception as e:
            log(f"[ERROR] Call {call_id}: {e}")
            time.sleep(30)
    log(f"[FAIL] Call {call_id}: Failed after {max_retries} attempts.")
    return None

def process_hardcoded_batch(call_ids, output_csv):
    log(f"Processing {len(call_ids)} hardcoded call IDs.")
    rows = []
    start_time = datetime.now()
    for idx, call_id in enumerate(call_ids):
        call_start = datetime.now()
        log(f"Processing call {idx+1}/{len(call_ids)}: {call_id}")
        call = fetch_call_details(call_id)
        if call:
            row = extract_minimal_fields(call_id, call)
            rows.append(row)
        else:
            log(f"[WARN] Skipping call {call_id} (no data)")
        # Progress reporting every 50 calls
        if (idx + 1) % 50 == 0 or (idx + 1) == len(call_ids):
            elapsed = datetime.now() - start_time
            avg_time = elapsed / (idx + 1)
            remaining = avg_time * (len(call_ids) - (idx + 1))
            log(f"Progress: {idx+1}/{len(call_ids)} calls. Elapsed: {str(elapsed).split('.')[0]}, Est. remaining: {str(remaining).split('.')[0]}")
        # Safe pacing: wait 1 second between requests
        time.sleep(1)
    # Write output CSV
    log(f"Writing output to: {os.path.abspath(output_csv)}")
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=MINIMAL_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    log(f"Batch CSV saved to {output_csv}")

def main():
    call_ids = [
        # Paste your call IDs here, one per line, as you did above
        "9b0f3ee3-8b91-4ab6-8177-7f07af53ddbf",
"a5b23f40-d2b5-4ea3-9e17-76ce0b08dc8f",
"542c15d1-2427-4017-ab72-5aa9d23617ce",
"3e33e870-4cae-4212-bdb2-1ce87f95d604",
"a212cd82-d043-4380-b58c-e5c77703a2db",
"c38bf4ea-3e77-47f7-8161-2a4fdfdaea2b",
"283c262d-3144-4e38-8629-ce996f44369b",
"47580b8f-05f2-4bd0-916b-dcdebebb7480",
"3bbf4111-a549-4f72-b00c-5a87a11db096",
"33bd5f55-b1d2-432e-b192-ceca87c44b00",
"e9404f48-ddf9-446d-952c-be8fa44903e6",
"9897d703-e814-46fc-915e-89639e4dd385",
"1dd86b63-34c4-4bee-866c-d9d1a6227c56",
"da82f7ef-b67d-4c7f-a608-d8158006ee82",
"77b287ad-37fb-41ec-a8e6-5f79bba80919",
"9419086a-a9c9-4f6f-bfd9-a513dce7ef93",
"4a9c2f04-0756-4771-a307-495e6696df3b",
"e34e4d3e-7632-445b-984d-cd562e1ce0d8",
"09fd8658-9291-4c90-a6b5-13abdd9e6021",
"e77ff423-3982-4915-921f-d6f42855ac3a",
"7310524e-6013-49e0-b5fe-3b70b0654343",
"25fb1749-85e5-4374-9955-3808e50fd8af",
"0716e4e5-5d9d-4cd5-a0c4-c2a950c96171",
"afe2c8b8-ee4e-4ead-bf0d-0fd85d879b11",
"16a01a05-c639-4459-aaa0-90f730136de4",
"40ea9b13-c46f-4a63-8113-244ce5730e9a",
"ec9ea225-dd16-400b-afbc-6ae869a981db",
"8d172b1f-f1ed-4207-b5f8-958691db94cb",
"f7a49030-6fe0-456a-a692-80dd529ca255",
"20ba7e9e-48fa-4722-9442-0134e4ce56e9",
"12d91c39-b22e-46f1-877f-ad675a6b0835",
"3fe7bcc5-3e88-4218-937b-546ab38a9a21",
"202de993-31ba-4158-a385-04c312792e8a",
"c3afc1f6-ad0e-4345-8508-2b614c4fe2dc",
"b8e56afd-b02d-4ee3-a9c3-b1a9fc8ebc7e",
"37a180e5-2239-4f65-ac5b-4dd9fad6f59f",
"42c1510a-46d4-4691-9b46-c786c2ddec94",
"1b43bf3d-5277-468b-818d-d7a6b2ac7345",
"92b7002f-854c-4e70-ba79-f3185b397de3",
"f7775d4c-7050-4a82-baca-b4d19383c451",
"081cb030-2174-4b9f-968f-44be8f2509b5",
"29ef4e8c-7c7b-45d4-8c39-8ad9741b856f",
"6ec393b2-8eaa-4e60-bbb6-dcd2bd181eb0",
"9bdf56f6-d74c-43ee-9df2-80c15b2e552f",
"2ffd64ed-6e45-4f74-be26-509e51e3d39a",
"0f004ccc-2252-4017-8609-2420359507bb",
"4f63f8b3-f6f6-413e-8e7f-fb4ccddd2f8f",
"b617ce13-dddc-404a-a8e2-25092dd35d71",
"7cfa73c3-3283-4885-814f-40e05efb6fda",
"e6ef69c6-9a99-4b63-9abb-b763c61646b7",
"1bb1691a-aa84-44a7-9bcb-0d21fc791cfb",
"6d98ded1-b999-4cb6-b0cb-a4900888c8c9",
"1450e21c-a9e5-498c-8d8f-bb86393c1204",
"73fbe808-3417-46ed-8710-893740825dce",
"f73639c4-30f4-4b87-80ab-6b2e9bcbe645",
"7b58ad8e-1d87-4b3e-bdfe-6b8a3957cc27",
"be05cde9-1411-42a8-ad01-72334a5dc726",
"b1afd6a0-f2a2-412a-b3fa-943fadb6e8ce",
"de19dbe5-24ba-4035-a990-f9c23c4c4947",
"e3177ce3-8e51-4afa-b75c-e28343afa0e1",
"b778b18b-0736-4860-8aed-d14ab9b9e766",
"c222e54e-d793-4078-93bc-20e71d0795cb",
"1e3e57c1-105e-4c6a-8fba-cce3eeecedd4",
"d8c85ff9-97fe-439d-b284-021456cded3d",
"17219460-f974-4598-ad95-f165a58f7012",
"02a3090b-282c-4e1c-bd43-6434a48505a2",
"349611c9-98e5-4c3e-a785-b69e14fa3dfd",
"551f5859-265b-454b-98aa-0e0b6541c83d",
"0a318295-6104-4ff1-8f44-de592ecc214d",
"29dd09bc-b794-48f5-836a-78225048b040",
"19d4842f-e0a6-48eb-944c-b4eba307614a",
"e41784e9-1d08-4a86-b6b1-940e24e56467",
"ba7a07a1-22a5-41ed-9812-6036837b49f1",
"7745c743-8594-4c8f-a8ce-1e7c4d2ce2f2",
"511b3d85-6e05-4e46-82e5-276c64233913",
"f8450d99-fd16-4bed-8fda-3c889c0bdf03",
"a3bdeaa6-99d8-4590-9dc6-ab8a76640b78",
"e6b2bfc8-b998-4d53-b527-b400475717bf",
"fb4012ec-4320-488a-8941-8be6d5f52ca9",
"fa9c0ad4-babb-4766-a865-09915834b246",
"1a33e9df-ca66-4bdc-953e-e3248c8363e4",
"d57683dc-3c55-48bc-be70-3e1df32682b8",
"68426355-5305-429a-b19e-1d992e695b6e",
"c444a5b6-32f2-481b-b282-38a76b104cff",
"68b878ab-efef-4899-83d1-8367c05d6ea8",
"3cb3cf87-4171-4ca1-8443-7acce46be04c",
"8cb0e0dc-ff84-4a51-9d74-fe003cda3fe3",
"af03858e-ca8b-4677-90d7-ade6baef13dc",
"5327de76-51f4-4b08-803d-453a16bb691e",
"b42a0441-16b3-4a18-81ea-86aae7a2a842",
"1aed2c63-d0d0-4fd7-b9e6-9f641a461228",
"57706d5b-1769-4f65-b124-a490242211f1",
"36800b23-4d96-46a2-b7b6-4293a873a372",
"bf1f13fc-a429-4691-ad12-291ca817a910",
"1cb09882-cd18-42cb-9db3-7883abd0e8e9",
"9f0ced3a-028d-4460-af5e-28f2231fdd2e",
"57e9457a-00b3-4ba3-8e7a-1feec78681c5",
"f05c040a-fea7-478a-854f-d5a8c78fafb7",
"52f7f500-59fe-4aea-bbe3-4bac1062681b",
"01feefdb-508d-4ff5-af00-4d0f505809ea",
"bdfa8f8f-710f-4092-9bb5-496e4bd05dfd",
"dbebe8d7-59c2-43aa-addf-b1de2cf41f38",
"63c8a46b-432f-4c5f-b65b-c39da2b1b204",
"b03b7b2f-5692-4370-a3ff-eedeacb16882",
"b11962f6-46c6-41bd-b3c9-7f338fe432be",
"f50d8c20-e159-4f80-95b1-f78931d99040",
"a9dad189-02ca-4fdb-80d0-8ed8a48a09bd",
"971c047c-6b74-45fc-8ee7-6e59653233b4",
"9abce9c4-fe52-4209-840f-5788745a1311",
"6d1c97c2-c319-40db-a82f-39b6aaebff80",
"14b6a242-adf3-4f9a-ba22-935275178345",
"c6e02b3a-e98f-4507-aea5-aea5f2c9d9b3",
"6e89b98b-0e58-47b5-8d03-66617108a07d",
"61692410-6ef3-473b-9d94-1b744f87f050",
"aab9b016-9f44-4853-8ff7-d85ba487fea3",
"50e5903f-32db-414f-bbc2-37fcd67028dc",
"e1d0f15e-e721-4d28-aa99-bda12fc8739a",
"68a93abc-52b2-43b4-af83-2a7e04df5dc0",
"4697c8cf-b738-4b15-a5bc-24c4e7dc6de2",
"a12f1119-6f3d-404b-aeaa-2e260d2871f0",
"19beda77-49ea-48a5-bbfb-4a27dbb059d6",
"768cf804-3079-47d9-9d24-f208d7f065cb",
"ff7fb462-220b-457e-90f3-30053efcfc57",
"c50416f6-2072-48c6-8c53-06120c98d331",
"4cb3c701-16ba-48ee-88ff-569f724e13cb",
"56b5e882-f9e3-4568-bea0-ad36d6604fd5",
"3e48084b-3a3a-4527-ac0b-d051059f72f3",
"970c6f90-ce21-4634-881a-1bf5aee5ad57",
"80629455-f304-4680-98bf-0c6c7e64037f",
"c30b5b62-3ab6-4b1a-b8c6-ff296daea47a",
"0b6bb449-14d9-457e-82bd-897e910fc9cb",
"205e29c3-bf47-479e-b7bb-dfc1f5fa2387",
"3f06712c-c096-4c88-955d-333babb7b7ce",
"ee5e9967-44cc-4d9c-b2a2-108716b24fdc",
"d37e7393-239a-4c0a-9e6d-9e9b0e321e03",
"77e5e4d5-52d8-45b3-95b1-070350b77edc",
"a0327f11-22a4-438f-b1e7-9da3baa9661b",
"b546ec4a-6684-4a65-b1e1-dc1e193caf45",
"045fcb38-b92e-45ed-9cf3-970b9a164c46",
"d3fe695f-5b82-4dde-a9f5-a99e61692d75",
"2a4bec66-adde-41d7-a2b6-2916546763f3",
"b343b140-a55f-4584-b366-04fd96faf858",
"24555049-2b8a-4a7a-9c2c-73f7fbd09c8f",
"cd58ed41-e37b-44ed-b020-cd5bd87c4051",
"6d97426b-86c4-4796-892a-76b777c84c89",
"9f5d5ae6-3a26-4cf7-b9e2-8c6595c88e34",
"7173891e-93f9-4714-9b23-ab7a1f56d39a",
"021a6733-7744-441c-b01c-078a9a47039b",
"ecc1f3d7-582e-4da7-b6e3-31f8bd9dad47",
"d1e7082c-8b4d-4267-ba30-a5537e846c52",
"f81d1fcd-e291-44df-ad57-5a0ceb99cfe2",
"86d0338b-cb23-47a8-8109-81cdfaeaa61b",
"b191ca1b-6d79-4b46-a440-bba86d413746",
"dfe36fee-2c64-42a7-b46f-7a679c25e1d3",
"98da61fd-85c0-407f-87cd-f40c298b65a7",
"ca410e83-70e0-46db-a314-9c92b95f38d3",
"c9d120b3-5032-43f3-9c58-03600866f650",
"97bdab88-ccc2-4040-b861-5c4130d2de91",
"eee48fdd-c88b-4ca7-8bd8-1a072359db06",
"b614545d-aed9-4cbb-9b99-99c62a06e640",
"d22ced03-9866-4fc5-9bbb-aa9550aa391b",
"1091090d-b85d-4991-ab3d-52e1c63a0ad6",
"3a208d6e-1292-4ce4-aaa7-536c6b094fd1",
"c58236f5-b20b-4cdb-bd1b-4650bc115eae",
"8b0962a0-bada-4dce-8ce2-d37444517c39",
"a9d0492b-7175-4e3a-a2d9-3fc4d3f457a6",
"56b34a26-ddc4-41e0-9227-fabcee3a2288",
"bb93ca88-ae5b-46b0-8510-bada0327186b",
"102f5a0c-b360-40c2-84cd-e569c04c0db9",
"ce7d3145-4110-4b9f-ab07-d08583ef7357",
"530f4b77-6599-4dd2-a2cb-5f515fd3da69",
"9f0f7e76-dcd4-4c33-9970-f9e29c9ce39f",
"181241fd-8102-444d-9695-6f08fd49f8e6",
"676949bb-cae5-4250-b676-2fba6e18cd73",
"3c21430f-1584-4a57-8c57-518ee202bc29",
"f4e1dbb9-bd87-4ae4-a900-aa6c3a34ec24",
"a728c2f6-f142-46ab-8912-0f1e4583bea5",
"cf383fc4-1fc1-4549-a5fd-58170ad4da9e",
"c4bf37f2-b0ea-40ed-8e69-bd832e0baeec",
"67dadf7b-9323-4bb6-a0f6-9dcaccc051fb",
"88e63951-31d8-4605-add0-e5ea81c2d477",
"e97f6c89-ff7f-491f-ad00-05c2c82d30e4",
"01d28a04-205c-4a6a-98ca-7aceebc34cb7",
"c0175acb-3dc0-4930-85c7-b148ee6f6dd0",
"7774701c-b2fd-423a-935d-09f42fcf4e21",
"a104edb2-59f9-45d5-aec1-d15df05765ae",
"c54ac016-01db-4bd4-94f3-7d8068f5a214",
"f48e20ef-e6db-4268-83a2-8f80906e12e6",
"2b88f1cb-6875-4506-9300-a42595935b4c",
"619b0436-388b-454d-9eb4-fb40eca9407b",
"c18e4a12-9ffd-4861-8089-68083ce6deda",
"c6ea83ca-4648-4c1f-b67f-c0667a761ac2",
"f53c545f-68bf-4518-a323-9d17fa873b3d",
"bc13cb2d-4850-4a37-9455-e6ed2dcb53c2",
"94376c4b-b2bd-4d8c-a657-a04e3b7b81a5",
"59ef0b29-b839-46ce-8562-ab14eb924983",
"e2bb7d5e-5649-41c1-a995-dfe973fb9a1f",
"ea7f3f86-f11f-4481-96ce-82c59b95b1b1",
"9083cc1d-d208-4e38-9b12-923351025f1d",
"3adc8eee-8479-4447-bdc3-f01de7a6eb14",
"1e5575dc-8ba9-49e0-9adc-65901ebf7542",
"793a65cc-b92e-499e-9b1b-ddb629742e36",
"d8fc9ca0-cb8d-4a89-a909-05a69c341313",
"87a69a9f-3dca-4770-9778-e1a37fbfe275",
"e21a3c68-5bcb-463b-be5c-b448d3ba40b7",
"3fd3f5df-a23b-4b0c-be48-a8bbd0511b15",
"4ad6d310-55fd-4273-a90f-a118a8846a67",
"823e1605-5745-4e9d-af0b-214d5b712ce1",
"3177f50b-afd5-4486-9a78-f649d164ce67",
"db0ce52e-3eca-428c-b2a6-b38ad88890ce",
"c20edc92-89dd-48d4-a802-dae27ecc5647",
"9c3df1d8-bd84-475b-aec6-a9a53826bede",
"d732ac98-49d0-4b83-8814-b7b3446c715d",
"663b7cb9-ec0a-468c-adbb-5b6982730b24",
"06137d4e-c7ac-446b-8dc9-32c33b8cd595",
"e2a4e240-1c1b-4655-bc14-774596e15dd2",
"374d388f-e8ef-4fe9-84f1-a1505d292ac0",
"9ee8db0e-19ed-43a7-a9c8-b20d49eb61c0",
"9a608408-391e-4140-9a41-d83ed21ee0a8",
"bc7cba62-5288-47e4-9be1-931a0f0ab080",
"cb888a1f-37c8-4e9e-9551-123f1d087631",
"9d9bac84-b6a9-45d8-b6e8-54e6392f34ea",
"e7e9494a-c99c-415c-9e6f-62ec21e162fc",
"d5051e91-d8df-486a-b0ff-5d5ac40fb51a",
"caa0b81e-4016-4191-b19c-5af56b8ba883",
"2e77b324-f3bb-4d53-a5ed-314c1bdd45cd",
"9eeca60b-73d4-4d59-ab9d-4e63c15171d2",
"653ae9ed-0cdc-4cdf-84f1-abf1fd169be2",
"b1086080-d6f0-4620-99d4-0a32062d8ad5",
"0ecfaa89-d7a7-4e31-a290-dcce50e0698d",
"d7ecc09a-25f1-4dc3-b4d3-9b7fc921c829",
"aaea8156-48ad-422a-b00b-6b0a02c29733",
"1bf062d6-db85-46d0-b9c5-e43b1587080a",
"b949539c-ba3c-4f3c-87b3-2812fefafbe1",
"a3df569f-b22f-47e9-95c0-fdcbaa0fb4d6",
"3ab58959-f3e5-4307-ae88-7b22c519f975",
"2ee4ffbf-cff6-4eff-a9f6-75ae7c368f17",
"a0cc9e54-7ac1-42c6-a23c-52effa5351ea",
"131a7734-d573-48c6-ac83-d34042631457",
"4a7eda8b-cace-46b9-b886-bfefb8ea3d3c",
"6be708a1-59de-4a54-9862-c155c2a49413",
"d6f8c637-6624-4cf5-8934-1c677bdf5c7f",
"b74fe95f-e95f-4b33-844f-b4f3b2aed576",
"52e703e6-f236-4b46-b154-794de386e82b",
"21b2d268-3330-4a9d-9ec2-29ba3dd3a7f2",
"f1ce6323-396f-454f-9221-e6637db97fd9",
"e950751c-9dbc-453d-a9f6-7df3e92fbff8",
"5732c9c6-b916-4181-9221-aa20bcd7ca85",
"04760677-b31a-4304-9ca3-0e6aae0916f0",
"88e10625-af6d-4232-8a4c-98dfaa6ca829",
"84095556-9b1e-4ec0-afab-bb56d1fe1007",
"452b2be1-6d29-4e1d-88db-d4ddde753365",
"dc76f4cd-e3ac-4cad-8028-957db1450720",
"3f83b742-23c2-4446-b8e9-168f4d22bb25",
"fd4c5665-29b8-47ff-a78d-ceae7752c7b6",
"bcd4959b-f286-4ed8-9af9-bfa91d31bebb",
"101061eb-4fa8-4ce4-b5eb-118ff900abee",
"93d704bb-9e26-4d34-b2fb-9abf08699d1b",
"77378623-7f19-47a5-a20d-14a951cd52d1",
"83748939-c969-4c46-a202-e9bb634e3fb1",
"ae672e11-0f4a-4996-a17a-58a71d26e209",
"62073bed-57ab-41ff-86aa-1379cd0beeef",
"1061616b-da2f-4b0e-9596-d179a5717771",
"2869f538-4129-424e-8984-4c11312bad21",
"233c1ca3-f03e-4091-9270-73bb71cdbfde",
"0701e539-b9a9-4297-89bd-cded790d70a2",
"c29aa7bd-49e9-4961-a1ff-81666e432706",
"f1017e80-14de-4433-88c6-17cae2a216b4",
"add2216f-e913-4710-838c-6f981c1adbdb",
"4de3e8bf-30b9-42ea-85d5-40eecd7278ca",
"5bcf9099-76ce-4cbc-beaf-57d3e85f53d7",
"0f074d15-8b6c-4fad-9d41-33843453eccc",
"32398761-75df-4d18-b2f5-dd21d0092169",
"6a41b09b-1ebe-4c1d-b59d-1107c232dbc8",
"3eb8f10e-0a81-494a-91d5-53f08ac7521b",
"2eeac4d1-8959-4a85-94e5-2c5184c399c4",
"fa7ac044-a9d5-453b-bce9-9ac2bf324661",
"f528d963-62bb-4e4b-9bdb-eacc5eaf60b8",
"18e1534d-e8f3-47f7-91cd-092a1e5c5f87",
"dd535df0-f6fc-4765-a315-b3c17e0e47c5",
"01008d98-111a-40e8-a5bf-a816f799afeb",
"6f8572a0-5338-45b9-94bc-e841f36f3a22",
"1ce70484-ae55-47ff-9fda-620e4b02363a",
"d1bbd826-1eff-41dc-8dec-1b8ec4ce6f95",
"bdf91492-7229-4920-be6e-ffdc7685d72e",
"cb19d9d4-f0e0-47e4-a7fd-23eb342cc71f",
"0357f1a3-754d-4d7b-b863-2ee294765545",
"8a95df8a-9129-47c7-8318-e4ec4d68437d",
"62382170-5a8c-4ef3-9c62-cca2f1070b63",
"475a1ca4-b974-4ea5-8869-b07c5f0a2ae7",
"d2198de9-19aa-4d68-b264-3700d372cf6b",
"a6949cd1-c186-4cc3-8b1d-c5f222d3f6e0",
"fa70f23d-b458-4c40-8207-335f13ecb8e2",
"a6393782-ae3e-45c6-a04d-c98c9f3f0221",
"a0df3bfb-d8ec-497c-9b6b-72bfc6d2e418",
"0d02f727-0c0c-4892-bcf2-e60a5942e809",
"cba46f84-8da3-48f2-9c90-763c54420e6d",
"20cc54ae-c705-4857-b5f8-dbcb526927b8",
"ba459b98-7284-4e8b-b1f1-624ea7435f4c",
"53757acc-d95f-4284-b920-08fe96b2d246",
"08568442-ae1c-4fcb-a36d-f84eea15c2f8",
"4ded1d1d-bc5b-4157-9bbe-ee78ac546805",
"d8ff1e3b-e68a-45de-b9e4-c3f7e1f65ac4",
"f04c86c7-6f57-4bff-8a45-042b0c8ce7c8",
"f762fb58-04d5-46d6-aa4c-b9ceb9b6ed0b",
"b9e793a4-e360-49dc-a0e9-a3ba2fe49298",
"05c15f4e-d6a5-4e14-812c-e8577cfa4cb8",
"3646831b-08f5-444f-8d32-73955b52accf",
"cfff085b-c0e0-4cfd-b5b1-c7f43324c66f",
"eac5c1d0-feb2-4fa9-a93e-6ad9fadce170",
"e3e0494d-6c4e-4fee-8181-933e2977f1d9",
"69f51ad8-ea0c-406b-9529-b98c11ddde00",
"e2aa03fe-9142-43dc-b1b0-ac6673a077cf",
"40d45731-d584-4f44-b390-f6f20abc3e88",
"7c132883-5d49-4089-b21a-96dd26523045",
"9b75f73f-7a4d-4dfe-80cb-b7c6431d9e6d",
"d8566a8a-3741-4963-9ca7-81bfd9828b27",
"3ae2d44b-ef22-448e-aadb-580ada8a6d50",
"1905251a-e2dc-44f2-8c7a-707e42b0656e",
"9dba7409-02db-48c7-a516-2ef0045b4ebb",
"b56aca5f-80c1-4121-ad81-ea1f82609681",
"45005954-f38b-4cc7-8630-4af6d3ba72e6",
"e0c422b0-aac7-42c2-9733-4051c69d8ee4",
"ac46c854-7d40-4f6d-b2be-f1f01c3bd926",
"d8f36271-2b1b-49e0-9187-e89d85ac8c46",
"4b1d76b8-598f-40b8-abac-47daf34dea5d",
"5fa158ed-9f18-44cd-8e87-24c3397b38f2",
"873680af-a3f5-457a-a6bc-216a78c7f884",
"2f94fd5d-ee72-43a8-97bc-3ed8d5e997f3",
"c598762e-6180-4376-a1ea-b39113b2ca47",
"3690a25f-90da-4dda-a6c5-f4610e4fa94e",
"21692f64-0b5e-4ec7-99e9-44c838c2576c",
"d0cc165f-a49f-487d-ae99-612fbf8afa28",
"9d52dc90-1514-494f-aa68-a79fed506d75",
"24f2e128-1313-4047-8a72-a91093b12d00",
"ecc00ec1-b37e-4c2e-9dae-a4cdad01dbdc",
"1b08abe9-c846-4c06-8467-41ad83d94324",
"4e242342-fbd0-4418-8b73-9bdd4a1afa89",
"aa754fc8-e150-4b19-b9b9-c3247605154f",
"7c9401e6-523b-4a18-800f-df5f84788f77",
"2cce8be6-5dc4-4473-a04c-17f413b54938",
"f35e76da-d16b-4de0-91fb-130a2232afaa",
"4a40b40c-c0b5-4899-8a41-7a25b5896d27",
"479e8225-974d-4a00-afc2-60967e55f1e1",
"3d553a0d-8036-478e-a475-4271190b4671",
"20912ceb-a8cc-4971-8329-7247946251e1",
"89b2140f-165b-4e1a-98f0-2877679025a6",
"963d9d70-56c3-455c-9100-78bba32e1706",
"50152620-53a8-43ea-8191-278819bf8478",
"9668349b-c973-4b66-8e89-aa61e2576b2a",
"19c076dd-d5c4-4578-a624-3c961bd606eb",
"a6c2dc09-fc12-4061-832c-1d0917b43684",
"8b9b0f43-cea2-4a64-886e-d1e4a3cddad7",
"8f77d2f5-09f9-433e-be1f-c3d13b724b6f",
"a2f7f005-f6de-4e56-826e-7ba03920e7ce",
"a0460b91-4fce-465d-9f9a-ccb06d27df0a",
"30adfc32-3e0c-4ece-b5ab-46bf8c70bdd1",
"06805113-9345-4e5b-ae9c-243c513aa57c",
"25461273-3706-4bb5-8bda-03f4bf8c86b0",
"efb62e8b-ad8e-44ca-80d7-a2118b5b018e",
"e99d2f8e-0f2f-4bc7-a0ea-9606cb684c8f",
"80f9e536-19f2-4a42-87e4-6951e6921ceb",
"78e5aa01-df36-4ed3-b7f6-7632eb80cec8",
"d57e1c1a-d477-4c7b-ba9a-070fea0073de",
"accd7b6a-c931-47ce-8453-b31ae02869ca",
"fb29ae36-6db7-44a1-9375-7e10dbcbb828",
"9effebe9-2cb4-447f-9a2d-f9a156d3a548",
"0531e2a5-e052-41ee-a778-f70a2de91527",
"237eeaa1-6bd1-46e8-a298-9e2635881044",
"a4da7b5b-1871-4199-b089-a995c727cecb",
"f8366b5f-db76-4c31-a821-bee895c92329",
"f62ed270-0237-4300-bbab-006e12d9a427",
"edc481c5-a76f-4262-9e5c-5c2e75a98b12",
"2faaef4c-245d-4482-b7dc-6873dc7dacd0",
"db4e6678-3b51-4334-8f63-067f3d32bb0e",
"ade44520-83b4-4a26-8615-78b771598cc5",
"8c41e3e2-b27b-46bf-b9a3-59a543a7be39",
"0622c34c-ac68-406c-ac91-fccc6d5ddb22",
"6016f3b2-f469-4b64-b4fc-e977495e1fa6",
"659de21e-d7da-4fe2-8d09-1ca1b88a8009",
"94c2a84e-0e74-40ad-9b2d-df10e55cc5ad",
"dcf05e31-c7bd-41fa-b447-08b974797a0e",
"f671e7d1-502d-49dd-b34e-ee42c0ecb343",
"efddc4d2-16eb-4a85-9e8e-a0a15171e14a",
"ac2316a6-67ee-40da-812a-7813cc535a1c",
"ab37a0ce-490f-4d9c-a18f-45db065a9e75",
"7c371977-450a-4e8d-aa86-b1c4d3f5fef7",
"d849f14f-5948-4748-be25-aad83f729ef1",
"6100570c-23d2-4113-b018-fbb4ffc445dc",
"7f64b3cd-283e-4c50-a5b8-346f67bfb2f3",
"af471f43-8230-4043-be99-4d327baf0233",
"a04694be-8834-4d94-b3c2-9fad9ea40603",
"4f48412f-9ec4-4ede-8559-14b21b348e9f",
"0ac68a3b-0ac3-402e-8172-867abed39123",
"63443a95-5552-44a3-8fdb-8ebc37abaf04",
"6e27e6ab-a044-46a8-ab61-8a1bf0b575f1",
"a0d714b7-529a-49b0-9505-82e583d279f8",
"30f3cd79-ea98-4693-9e9d-a02667cb4c46",
"7bcbd5f3-e76e-478e-81f7-e692dc2bb6d5",
"761b0674-32c8-436c-98af-a53708e038a2",
"5686251d-a9e3-4109-9e8a-fe6afe20e7b1",
"9234afd8-3d4c-48df-9379-dadd47860176",
"fd370e8e-be51-428e-a254-6beb74aa2f8e",
"bc762fa2-9470-4843-8c7f-dc2dbeba67b2",
"1ab832d1-ce88-471f-ae0a-70cbd7f948b3",
"710617a3-5a53-4941-a9d9-20dc83c0ceb7",
"55977970-4785-4d42-901d-8cd5aeca22b6",
"505f94b4-fba4-45ac-8c7a-dd10ca4b1196",
"da459bd6-b441-45fb-ab4a-8650fa741bd7",
"6f20cfef-2f43-417a-b38e-e71ba5113f7a",
"c72792c9-c375-4eb7-9b24-2847de9af5da",
"f2702443-2611-43b7-8d28-8c8449208990",
"29e15a4d-a504-4112-9ee7-471f0b44d46a",
"48bc360f-46ed-4778-a109-49915991a635",
"c22df9ce-2b2a-4e1b-967d-109360720c74",
"8d05468e-f000-458a-845a-273904c2aec0",
"7ca8c181-82bd-40ac-8ca0-0e401b7bd7c8",
"f20c9ffd-e1fb-427f-be42-5af78d60381b",
"2bc65fd0-8ad3-4e2e-9ac4-ddbf9d4f96b2",
"35331201-8d9e-4fd4-8b62-37c58afb5965",
"68b9a0e7-f423-4a4c-96a4-6889a06070ef",
"098aa0a4-57cc-4b73-b6cd-247d9030f202",
"70541fb9-d04d-4cb6-a387-f2d1d8e28569",
"b7be550e-2b8e-48d8-98bc-27c05d59b3a0",
"75e76b41-7184-4e78-92e4-b73d50241dc3",
"ed3ad6c7-8c48-4c3b-9d3e-5d6c35f18219",
"2bbcd9cd-9b29-4217-b514-c1d4a46f6f0d",
"179cb632-063c-4977-901e-7acbd3c9fa3f",
"d3a09996-5759-466b-8689-1433b33e3329",
"e7f62911-2414-4176-a495-ffd5e99a131d",
"02fc2a35-3671-4527-929f-bee942590a69",
"74e0db10-8319-4e23-b733-b2468642cd04",
"19f2babf-b18c-419d-bdd3-abad9c4d336a",
"b57a34c8-79cb-4af1-a32e-789cfed85f2c",
"45bdfaa1-4977-48c9-999c-d3a686780880",
"1b27371c-b8f1-432e-91c2-e757ea58bac2",
"5e9d0405-665e-45d9-945a-d8c8428ed964",
"9d21f35f-abe8-4994-9992-0315ab7f3ac8",
"ddb965dc-b9c7-4f04-afee-f42baae776a1",
"47801939-870c-47ce-ae0f-a91a1df7c512",
"728ff430-390a-49b7-a4ea-32972c3e3e15",
"2a57ef15-1bdf-4009-8cf0-3783999d24ec",
"7573ca36-1528-4216-9abb-43d40cb8918c",
"2d795f35-3fe3-4ebc-9dfe-0f0ffc620502",
"73c89c7f-a251-4284-a308-6a3f56c26892",
"840c8f8b-bf78-4202-8061-abde150cc173",
"828cd7fb-4082-42ce-9435-442d8b262b7e",
"0452e6f2-c945-4c6f-90cf-c5f28aa1c1ca",
"b8c99c0f-0370-4ddd-a12a-dfd5ab30c98c",
"0033fd77-188d-43e9-99fa-14478d0486ef",
"e02dd3ce-350d-4809-9dfd-2eb86d1be16e",
"2c45db7e-e0d0-40cb-a82c-d1709f5fc160",
"8c9d5dd2-0076-4b5b-886a-dd895645397a",
"3bedb186-9034-43d2-8719-e94c037d21b2",
"79d7e73f-e114-4388-87e4-db3d6a5b73be",
"c280c252-a4c9-4e9c-a16b-7fa34fac66f8",
"992204c0-94b0-464a-a83d-fd7fb7aaae38",
"c97bfee5-590d-4767-9205-75983470797f",
"d6791868-8ce2-446b-9ee9-79a9c14dcbff",
"504bf427-61bd-41a5-9ec2-5f22354d3b97"
        
    ]
    output_csv = 'clari_copilot_api_enriched.csv'
    # Clear previous log
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    process_hardcoded_batch(call_ids, output_csv)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[FATAL] Exception occurred: {e}")
