# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'pinnacleestate_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    
    start_urls = ['https://wix.shareiiit.com/realEstate/info.php?pageId=nlaz7&compId=TPASection_kbtfv2ry&viewerCompId=TPASection_kbtfv2ry&siteRevision=243&viewMode=site&deviceType=desktop&locale=en&tz=America%2FNew_York&regionalLanguage=en&width=980&height=3098&instance=nZHb9gcHOYJDKCN5-0ljRUiw5_OYpPuDbrosHYTcsrI.eyJpbnN0YW5jZUlkIjoiNmFiYTViY2YtNDU4NC00MDI3LTgxNGMtNDQ3MDA5MTJjZjFhIiwiYXBwRGVmSWQiOiIxM2FhMmZkMi0xZDc0LTE0NjUtNDYzNC01YTM5NGQwNzBhZWUiLCJzaWduRGF0ZSI6IjIwMjEtMDMtMjRUMDc6MjE6MTguNjcxWiIsInZlbmRvclByb2R1Y3RJZCI6InByZW1pdW0iLCJkZW1vTW9kZSI6ZmFsc2UsImFpZCI6IjY1NDBlZjVjLWU0MGQtNGNkNS1hOTYzLTIxNmFhNGE4M2RlNiIsInNpdGVPd25lcklkIjoiNmNmODE2NzktY2I4Ni00NzkyLWExZWQtYjlhMmRhZmM0MjMzIn0&commonConfig=%7B%22brand%22%3A%22wix%22%2C%22bsi%22%3A%22e36037b5-86b1-4cdb-8e8c-500e2ed0d34c%7C4%22%2C%22BSI%22%3A%22e36037b5-86b1-4cdb-8e8c-500e2ed0d34c%7C4%22%7D&target=_top&section-url=https%3A%2F%2Fwww.pinnacleestate.co.uk%2Fproperties%2F&vsi=d507fc73-58b7-4125-973f-80f7c341cd37']  # LEVEL 1
    
    formdata = {
        "how": "byPage",
        "perPage": "10",
        "offset": "0",
    }
    
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "cookie": "AWSALBCORS=6vCss0sEXWMiF6HbSf+gmFf7UKgih/MPcbIT/zSqH5q/6qi2llnQLgqm675A+8g88TysecDnvQcEzwsUmHilIX80JIRYrx8QB8ff9vHTuRhIwlG2iGgK2Yvkb6Zp",
        "origin": "https://wix.shareiiit.com",
        "referer":" https://wix.shareiiit.com/realEstate/ppp?pageId=nlaz7&compId=TPASection_kbtfv2ry&viewerCompId=TPASection_kbtfv2ry&siteRevision=243&viewMode=site&deviceType=desktop&locale=en&tz=America%2FNew_York&regionalLanguage=en&width=980&height=3098&instance=nZHb9gcHOYJDKCN5-0ljRUiw5_OYpPuDbrosHYTcsrI.eyJpbnN0YW5jZUlkIjoiNmFiYTViY2YtNDU4NC00MDI3LTgxNGMtNDQ3MDA5MTJjZjFhIiwiYXBwRGVmSWQiOiIxM2FhMmZkMi0xZDc0LTE0NjUtNDYzNC01YTM5NGQwNzBhZWUiLCJzaWduRGF0ZSI6IjIwMjEtMDMtMjRUMDc6MjE6MTguNjcxWiIsInZlbmRvclByb2R1Y3RJZCI6InByZW1pdW0iLCJkZW1vTW9kZSI6ZmFsc2UsImFpZCI6IjY1NDBlZjVjLWU0MGQtNGNkNS1hOTYzLTIxNmFhNGE4M2RlNiIsInNpdGVPd25lcklkIjoiNmNmODE2NzktY2I4Ni00NzkyLWExZWQtYjlhMmRhZmM0MjMzIn0&commonConfig=%7B%22brand%22%3A%22wix%22%2C%22bsi%22%3A%22e36037b5-86b1-4cdb-8e8c-500e2ed0d34c%7C4%22%2C%22BSI%22%3A%22e36037b5-86b1-4cdb-8e8c-500e2ed0d34c%7C4%22%7D&target=_top&section-url=https%3A%2F%2Fwww.pinnacleestate.co.uk%2Fproperties%2F&vsi=d507fc73-58b7-4125-973f-80f7c341cd37",
        "sec-ch-ua": '"Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }
    
    def start_requests(self):
        
        yield FormRequest(
            url=self.start_urls[0],
            formdata=self.formdata,
            headers=self.headers,
            callback=self.parse
        )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        data = json.loads(response.body)
        for item in data["assets"]:
            follow_url = f"https://www.pinnacleestate.co.uk/properties/propE{item['id']}"
            prop_type = item["data"]["id1422289550652"]["val"]["selectedItem"]
            if get_p_type_string(prop_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(prop_type),"item":item})
            seen = True
        
        if page == 1 or seen:
            self.formdata["offset"] = str(page)
            yield FormRequest(
                url=self.start_urls[0],
                formdata=self.formdata,
                headers=self.headers,
                callback=self.parse,
                dont_filter=True,
                meta={"page": page+1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item =  response.meta.get('item')
        
        item_loader.add_value("external_source", "Pinnacleestate_Co_PySpider_united_kingdom")
        item_loader.add_value("title", item["title"])
        item_loader.add_value("external_id", item["id"])
        status = item["data"]["id1593025040814"]["val"]["selectedItem"]
        if status and "let agreed" in status.lower():
            return
        latitude = item["data"]["id1422290161888"]["val"]
        longitude = item["data"]["id1422290161888"]["val"]
        if "markerLat" in latitude:
            item_loader.add_value("latitude", str(latitude["markerLat"]))
            item_loader.add_value("longitude", str(longitude["markerLng"]))
    
        rent = item["price"]
        if rent:                              
            item_loader.add_value("rent_string", rent) 
        item_loader.add_value("currency", "GBP")

        description = item["data"]["id1422289759907"]["val"]["val"]        
        if description:
            item_loader.add_value("description", description.replace("&#44;",",").strip())
        address = item["data"]["id1422273614114"]["val"]["val"]
        if address:  
            address = address.replace("&#44;",",")
            item_loader.add_value("address", address)
            address = address.split(",")[-2].strip()
            if len(address.split(" ")) >1:
                city = " ".join(address.split(" ")[:-2])
                zipcode = " ".join(address.split(" ")[-2:])
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
            else:
                item_loader.add_value("city", address)

        room_count = item["data"]["id1422289669592"]["val"]["val"]
        if room_count:
            item_loader.add_value("room_count", room_count)
        bathroom_count = item["data"]["id1422289688432"]["val"]["val"]
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
  
        balcony = item["data"]["id1422290117358"]["val"]["checked"]
        if balcony:
            item_loader.add_value('balcony', True)
        else:
            item_loader.add_value('balcony', False)

        parking = item["data"]["id1422290151855"]["val"]["checked"]
        if parking:
            item_loader.add_value('parking', True)
        else:
            item_loader.add_value('parking', False)
           
        images = [x["uri"] for x in item["images"]]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Pinnacle Estate")
        item_loader.add_value('landlord_phone', '02088630098')
        item_loader.add_value('landlord_email', 'office@pinnacleestate.co.uk')
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower() or "residential" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None