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
import dateparser
from python_spiders.helper import ItemClear
class MySpider(Spider):
    name = 'anthonybyrne_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
    def start_requests(self):
        formdata = {
            "how": "byPage",
            "perPage": "1000",
            "filter[id1422289483499]": "---",
            "filter[id1422289669592][min]": "",
            "filter[id1422289669592][max]": "",
            "priceFilter[min]": "",
            "priceFilter[max]": "",
        }
        yield FormRequest(
            url="https://wix.shareiiit.com/realEstate/info.php?pageId=w0o7b&compId=TPASection_jjiiibjx&viewerCompId=TPASection_jjiiibjx&siteRevision=1591&viewMode=site&deviceType=desktop&locale=en&tz=Europe%2FDublin&width=983&height=3197&instance=DSyW25pENaGsTmZ_7BFbEvvO4higXC8Np8rR3Z4fdjA.eyJpbnN0YW5jZUlkIjoiYmNmMzAzYmItZmZjZC00NmVkLThmM2YtMTEyYTYwYTYzNDhhIiwiYXBwRGVmSWQiOiIxM2FhMmZkMi0xZDc0LTE0NjUtNDYzNC01YTM5NGQwNzBhZWUiLCJzaWduRGF0ZSI6IjIwMjEtMDItMTlUMDc6NTI6NTcuNTYxWiIsInZlbmRvclByb2R1Y3RJZCI6InByZW1pdW0iLCJkZW1vTW9kZSI6ZmFsc2UsImFpZCI6ImUwNTg1MzJjLWYxZDQtNGUyZi1iMGQ3LTgxNWE3YTZiMDc4ZSIsInNpdGVPd25lcklkIjoiOTJmZjQ0MGYtMGUwMC00Mjk3LWIyZjgtNDRjNTk1N2IwMWNjIn0&currency=EUR&currentCurrency=EUR&vsi=7e716af7-707e-4381-9d1e-f5804085cf44&commonConfig=%7B%22brand%22%3A%22wix%22%2C%22bsi%22%3A%222ece9511-bff8-4d69-aab7-8271aa6be03d%7C2%22%2C%22BSI%22%3A%222ece9511-bff8-4d69-aab7-8271aa6be03d%7C2%22%7D&target=_top&section-url=https%3A%2F%2Fwww.anthonybyrne.ie%2Flettings%2F",
            callback=self.parse,
            formdata=formdata,
        )


    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)

        for item in data["assets"]:

            let_agreed = item["data"]["id1422289622959"]["isFilled"]
            if "LET AGREED" in let_agreed:
                return

            prop_type = item["data"]["id1422289483499"]["isFilled"]
            if prop_type and ("apartment" in prop_type.lower() or "duplex" in prop_type.lower()):
                p_type = "apartment"
            elif prop_type and ("house" in prop_type.lower() or "bungalow" in prop_type.lower()):
                p_type = "house"
            elif prop_type and "studio" in prop_type.lower():
                p_type = "studio"
            else:
                continue
            if "id1422289622959" in item["data"]:
                status = item["data"]["id1422289622959"]["isFilled"]
                if status and "agreed" in status.lower():
                    continue
            follow_url = f"https://www.anthonybyrne.ie/lettings/propE{item['id']}"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":p_type, "item":item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Anthonybyrne_PySpider_ireland")
        item = response.meta.get("item")
        item_loader.add_value("external_id", item["id"])
        images = item["images"]
        for image in images:
            image = str(image).split("uri': '")[1].split("'")[0]
            item_loader.add_value("images", image)

        rent =item["price"]
        title =item["title"]
        latitude = item["data"]["id1422290161888"]["val"]["centerLat"]
        longitude = item["data"]["id1422290161888"]["val"]["centerLng"]
        description = item["data"]["id1422289759907"]["val"]["val"]
        meters = item["data"]["id1422289725449"]["val"]["val"].split(" ")[0]
        if description:
            item_loader.add_value("description", description)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", meters.strip())
        item_loader.add_value("rent", rent.replace("PCM","").strip())
        
        item_loader.add_value("address",title)
        item_loader.add_value("city",title.split(",")[1].split(",")[0].strip())
        item_loader.add_value("room_count", item["data"]["id1422289669592"]["val"]["val"])
        item_loader.add_value("bathroom_count", item["data"]["id1422289688432"]["val"]["val"])

        item_loader.add_value("energy_label", item["data"]["id1427623419374"]["val"]["val"])

        if item["data"]["id1422289771411"]["val"]["checked"]:
            item_loader.add_value("parking", True)
        item_loader.add_value("landlord_name", "Anthony Byrne Property Services")
        item_loader.add_value("landlord_phone", "041 984 9930")
        item_loader.add_value("landlord_email", "info@anthonybyrne.ie")

        yield item_loader.load_item()