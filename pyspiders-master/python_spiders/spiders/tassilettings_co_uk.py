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
    name = 'tassilettings_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.tassilettings.co.uk/Map-Property-Search-Results?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&minprice=&maxprice=&bedrooms="]

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["items"]:
            status = item["icontext"]
            if status and "available" not in status.lower():
                continue
            lat, lng = item["lat"], item["lng"]
            follow_url = f"https://www.tassilettings.co.uk/property/residential/for-rent/{item['id']}"
            yield Request(follow_url, callback=self.populate_item, meta={"lat":lat, "lng":lng})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[@class='descriptionsColumn']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return

        item_loader.add_value("latitude", str(response.meta["lat"]))
        item_loader.add_value("longitude", str(response.meta["lng"]))

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Tassilettings_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='descriptionsColumn']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//text()[contains(.,'Bathroom')]", input_type="F_XPATH", get_num=True, split_list={"Bathroom":0, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h2[@class='fdPropPrice']/div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//text()[contains(.,'Available')]", input_type="F_XPATH", split_list={"Available":1, "-":0}, replace_list={"from":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//text()[contains(.,'Deposit')]", input_type="F_XPATH", get_num=True, split_list={"Deposit":0, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='property-photos-device1']//a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Tassi Lettings", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0115 947 4330", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@tassilettings.co.uk", input_type="VALUE")
        
        room_count = response.xpath("//text()[contains(.,'Bed ') or contains(.,'bedroom') or contains(.,'-bed')]").get()
        if room_count:
            if "Bed" in room_count:
                room_count = room_count.split("Bed")[0].lower().replace("double","").strip().split(" ")[-1]
            elif "-bed" in room_count:
                room_count = room_count.split("-bed")[0].lower().replace("double","").strip().split(" ")[-1]
            elif "bedroom" in room_count:
                room_count = room_count.split("bedroom")[0].lower().replace("double","").strip().split(" ")[-1]
        rooms = response.xpath("//li[contains(.,'BEDROOM') or contains(.,'Bedroom')]//text()[not(contains(.,'Double'))]").get()
        from word2number import w2n
        if room_count:
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            else:
                try:
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
                except:
                    if rooms and rooms.split(" ")[0].isdigit():
                        item_loader.add_value("room_count", rooms.split(" ")[0])
                    elif rooms:
                        try:
                            item_loader.add_value("room_count", w2n.word_to_num(rooms))
                        except:
                            pass
                    elif response.xpath("//text()[contains(.,'STUDIO') or contains(.,'studio') or contains(.,'studio')]").get():
                        item_loader.add_value("room_count", "1")
        elif response.xpath("//text()[contains(.,'STUDIO') or contains(.,'studio') or contains(.,'studio')]").get():
            item_loader.add_value("room_count", "1")
        
        bathroom_count = response.xpath("//li[contains(.,'BATHROOM')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0]
            try: item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
            except: pass
        
        parking = response.xpath("//li[contains(.,'PARKING')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//li[contains(.,'FURNISHED') or contains(.,'Furnished') or contains(.,' furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
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