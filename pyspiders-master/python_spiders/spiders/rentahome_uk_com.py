# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'rentahome_uk_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    start_urls = ["https://www.rentahome-uk.com/Search?listingType=6&areainformation=&radius=&minprice=&maxprice=&bedrooms=&cipea=1&statusids=1&obc=Price&obd=Descending"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//h2/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.rentahome-uk.com/Search?listingType=6&areainformation=&radius=&minprice=&maxprice=&bedrooms=&cipea=1&statusids=1&obc=Price&obd=Descending&perpage=5&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[contains(@class,'md-nineWide sm-twelveWide')]//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        item_loader.add_value("external_id", response.url.split("/")[-1])
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Rentahome_Uk_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//title/text()", input_type="F_XPATH", split_list={" in ":1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//title/text()", input_type="F_XPATH", split_list={" in ":1, ",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//title/text()", input_type="F_XPATH", split_list={" in ":1, ",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='row fullDescription']/div[1]//text()", input_type="M_XPATH", replace_list={"Description":""})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='fullDetails']/div[@class='row']//h2[contains(.,'£')]/div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='row fullDescription']/div[1]//text()[contains(.,'Available') or contains(.,'available')]", input_type="F_XPATH", split_list={" the ":-1," from ":-1," FROM ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='property-photos-device1']//a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')] | //div[@class='row fullDescription']/div[1]//text()[contains(.,'fully furnished') ]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Rent A Home Property Management", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="02079232372", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="m.yilmaz@rentahome-uk.com", input_type="VALUE")

        if response.xpath("//div[@class='row fullDescription']/div[1]//text()[contains(.,'BATHROOM') or contains(.,'Bathroom')]").get(): item_loader.add_value("bathroom_count", 1)

        map_url = f"https://www.rentahome-uk.com/Map-Property-Search-Results?references={response.url.split('/')[-1]}"
        headers = {
            'Connection': 'keep-alive',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': response.url,
            'Accept-Language': 'tr,en;q=0.9'
        }
        room_count = response.xpath("//div[@class='row fullDescription']/div[1]//text()").re_first(r"(\d)\s\w*\s*bedroom")
        if room_count:
            item_loader.add_value("room_count", room_count)
        yield Request(map_url, method="POST", headers=headers, callback=self.get_latlng, meta={"item_loader":item_loader})

    def get_latlng(self, response):

        item_loader = response.meta["item_loader"]

        data = json.loads(response.body)
        latitude = str(data["items"][0]["lat"]) if len(data["items"]) > 0 and "lat" in data["items"][0].keys() else None
        longitude = str(data["items"][0]["lng"]) if len(data["items"]) > 0 and "lng" in data["items"][0].keys() else None

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value=latitude, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value=longitude, input_type="VALUE")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None