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
    name = 'staffordshire_castle_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="Staffordshire_Castle_Estates_Co_PySpider_united_kingdom"
    start_urls = ["https://www.staffordshire.castle-estates.co.uk/api/1/properties/list?propsearch=a81359&page=1&limit=9999&no_sess_update=0"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data = json.loads(response.body)
        for item in data["data"]["properties"]:
            status = item["properties_banner_text"]
            if status and ("agreed" in status.lower() or status.strip().lower() == "let"):
                continue
            follow_url = response.urljoin(item["properties_url"])
            yield Request(follow_url, callback=self.populate_item, meta={"items":item})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.staffordshire.castle-estates.co.uk/api/1/properties/list?propsearch=a81359&page={page}&limit=9999&no_sess_update=0"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[@class='address']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[@class='description']//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return

        title = "".join(response.xpath("//div[@class='address']//text()").getall())
        item_loader.add_value("title", title.strip())
        
        items = response.meta.get('items')
        
        external_id = items["properties_property_ref"]
        item_loader.add_value("external_id", external_id)
        
        address = items["properties_address_formatted"]
        item_loader.add_value("address", address)
        
        city = items["properties_town"]
        item_loader.add_value("city", city)
        
        zipcode = items["properties_postcode"]
        item_loader.add_value("zipcode", zipcode)
        
        latitude = items["properties_latitude"]
        item_loader.add_value("latitude", latitude)
        
        longitude = items["properties_longitude"]
        item_loader.add_value("longitude", longitude)
        
        description = "".join(response.xpath("//div[@class='description']//text()").getall())
        if description:
            desc = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", desc)
        
        room_count = items["properties_bedrooms"]
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = items["properties_bathrooms"]
        item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = items["properties_price"]
        item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "GBP")
        
        if "Deposit" in desc:
            deposit = desc.split("Deposit")[1].split("weeks")[0].strip().split(" ")[-1]
            price = int(float(rent))/4
            item_loader.add_value("deposit", int(deposit)*int(float(price)))
        
        available_date = items["properties_lettings_date_available"]
        item_loader.add_value("available_date", available_date)
        
        furnished = items["properties_furnished_types_name"]
        if "Unfurnished" not in furnished:
            item_loader.add_value("furnished", True)
        
        features = str(items["properties_features_list"])
        if "No Pets Allowed" in features:
            item_loader.add_value("pets_allowed", False)
        if "Parking" in features:
            item_loader.add_value("parking", True)
        if "Floor" in features:
            floor = features.split("Floor")[0].split("'")[-1].strip()
            item_loader.add_value("floor", floor)
        
        energy_label = response.xpath("//div[contains(@class,'eerc')]/span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        images = str(items["properties_photos"]).split("'properties_images_url': '")
        for i in range(1, len(images)):
            item_loader.add_value("images",images[i].split("',")[0])
        
        item_loader.add_value("landlord_name", "CASTLE ESTATES")
        
        landlord_phone = items["branches_main_tel"]
        item_loader.add_value("landlord_phone", landlord_phone)
        
        landlord_email = items["branches_email"]
        item_loader.add_value("landlord_email", landlord_email)
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None