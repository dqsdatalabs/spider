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
    name = 'russellandbutler_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    headers = {
        'Content-Type': 'application/json'
    }

    def start_requests(self):
        
        payload="{\r\n    \"searchCriteria\": {\r\n        \"CriteriaID\": null,\r\n        \"AccountID\": 1475,\r\n        \"CompletedOnly\": false,\r\n        \"SoldSince\": \"/Date(-62135596800000)/\",\r\n        \"MinDate\": \"/Date(-62135596800000)/\",\r\n        \"MaxDate\": \"/Date(-62135596800000)/\",\r\n        \"ArticleIDs\": [],\r\n        \"OfficeID\": 0,\r\n        \"SaleProducts\": false,\r\n        \"MinNumberOfRooms\": 0,\r\n        \"MaxNumberOfRooms\": 0,\r\n        \"Features\": [],\r\n        \"MinPrice\": 0,\r\n        \"MaxPrice\": 0,\r\n        \"Postcodes\": null,\r\n        \"SortBy\": \"5\",\r\n        \"SortDirection\": 1,\r\n        \"Location\": \"\",\r\n        \"FreeText\": null,\r\n        \"IncludeSSTC\": false,\r\n        \"IncludeUnderOffer\": true,\r\n        \"UseInPreVal\": false,\r\n        \"ShowAll\": false,\r\n        \"ShowOnlineSold\": false,\r\n        \"DisplayCriteriaShort\": \"No price range set, No beds set, All areas\",\r\n        \"HasPhotos\": false,\r\n        \"MapInformation\": null,\r\n        \"PropertyCount\": 0,\r\n        \"CriteriaName\": null,\r\n        \"Articles\": null,\r\n        \"FoundNoResults\": false,\r\n        \"PageIndex\": 0,\r\n        \"PageSize\": 10,\r\n        \"AdvertSlotsCount\": 0,\r\n        \"ApplicationID\": 1354,\r\n        \"LoginName\": null,\r\n        \"Password\": null\r\n    }\r\n}"
        p_url = "https://russellandbutler.com/Search/GetProperties"
        yield Request(
            p_url,
            callback=self.parse,
            body=payload,
            headers=self.headers,
            method="POST",
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data = json.loads(response.body)
        for item in data["result"]["ArticlesMapInformation"]:
            address= item["Address"]
            lat, lng = item["Latitude"], item["Longitude"]
            title = item["Title"]
            room_count = None
            bathroom_count = None
            for room_data in data["result"]["ArticlesBrief"]: 
                if room_data["ArticleID"] == item["ArticleID"]:
                    room_count = room_data["NumberOfBedrooms"]
                    bathroom_count = room_data["NumberOfBathrooms"]
            follow_url = f"https://russellandbutler.com/Property/Details?Id={item['ArticleID']}"
            yield Request(follow_url, callback=self.populate_item, meta={"lat":lat, "lng":lng, "address":address, "title":title, "room_count":room_count, "bathroom_count":bathroom_count})
        
        if page == 2 or seen:
            p_url = "https://russellandbutler.com/Search/GetProperties"
            payload="{\r\n    \"searchCriteria\": {\r\n        \"CriteriaID\": null,\r\n        \"AccountID\": 1475,\r\n        \"CompletedOnly\": false,\r\n        \"SoldSince\": \"/Date(-62135596800000)/\",\r\n        \"MinDate\": \"/Date(-62135596800000)/\",\r\n        \"MaxDate\": \"/Date(-62135596800000)/\",\r\n        \"ArticleIDs\": [],\r\n        \"OfficeID\": 0,\r\n        \"SaleProducts\": false,\r\n        \"MinNumberOfRooms\": 0,\r\n        \"MaxNumberOfRooms\": 0,\r\n        \"Features\": [],\r\n        \"MinPrice\": 0,\r\n        \"MaxPrice\": 0,\r\n        \"Postcodes\": null,\r\n        \"SortBy\": \"5\",\r\n        \"SortDirection\": 1,\r\n        \"Location\": \"\",\r\n        \"FreeText\": null,\r\n        \"IncludeSSTC\": false,\r\n        \"IncludeUnderOffer\": true,\r\n        \"UseInPreVal\": false,\r\n        \"ShowAll\": false,\r\n        \"ShowOnlineSold\": false,\r\n        \"DisplayCriteriaShort\": \"No price range set, No beds set, All areas\",\r\n        \"HasPhotos\": false,\r\n        \"MapInformation\": null,\r\n        \"PropertyCount\": 0,\r\n        \"CriteriaName\": null,\r\n        \"Articles\": null,\r\n        \"FoundNoResults\": false,\r\n        \"PageIndex\": " + str(page) + ",\r\n        \"PageSize\": 10,\r\n        \"AdvertSlotsCount\": 0,\r\n        \"ApplicationID\": 1354,\r\n        \"LoginName\": null,\r\n        \"Password\": null\r\n    }\r\n}"
            yield Request(
                p_url,
                callback=self.parse,
                body=payload,
                headers=self.headers,
                method="POST",
                meta={"page":page+1}
            )
        



     # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("address", response.meta.get('address'))
        item_loader.add_value("latitude", str(response.meta.get('lat')))
        item_loader.add_value("longitude", str(response.meta.get('lng')))
        item_loader.add_value("title", response.meta.get('title'))

        f_text = " ".join(response.xpath("//div[@id='property-description']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Russellandbutler_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"Id=":1})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='property-description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value=response.meta["room_count"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value=response.meta["bathroom_count"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='container']//h2/strong/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'AVAILABLE')]/text()", input_type="F_XPATH", lower_or_upper=0, replace_list={"available":"", "from":"", "mid":"", "beginning":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='single-property-image']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//img[@id='floorplan-image']/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@id='property-description']//text()[contains(.,'EPC RATING')]", input_type="F_XPATH", split_list={"EPC RATING":1, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'PARKING') or contains(.,'GARAGE')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'BALCONY')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'FURNISHED')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'TERRACE')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//li[contains(.,'WASHING MACHINE')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'DISHWASHER')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Russell & Butler Lettings", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01280 815999", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="lettings@russellandbutler.co.uk", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="//div[@id='property-description']//text()[contains(.,'NO PETS')]", input_type="F_XPATH", tf_item=True, tf_value=False)

        address = response.xpath("//div[@class='container']//h2/text()").get()
        if address: 
            if len(address.split(',')) > 2:
                item_loader.add_value("zipcode", address.split(',')[-1].strip())
                item_loader.add_value("city", address.split(',')[-2].strip())

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None