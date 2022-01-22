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
    name = 'thepropertyoutlet_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.thepropertyoutlet.com/Search?listingType=6&statusids=1&obc=Price&obd=Descending&category=1&areadata=&areaname=&radius=&minprice=&maxprice=&bedrooms=&perpage=36"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//h4/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.thepropertyoutlet.com/Search?listingType=6&statusids=1&obc=Price&obd=Descending&category=1&areadata=&areaname=&radius=&minprice=&maxprice=&bedrooms=&perpage=36&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[@class='fullDetailTop']//ul//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[@class='descriptionText']//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: 
            return
        try:
            address = response.xpath("//script[@id='movetoFDTitle']/text()").get()
            if address:
                address = address.replace('<h3>', '').replace('</h3>', '').strip()
                city = address.split(',')[-2].strip()
                zipcode = address.split(',')[-1].strip()
                item_loader.add_value("address", address)
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
        except:
            pass

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//text()[contains(.,'DEPOSIT') or contains(.,'Deposit')]", input_type="F_XPATH", get_num=True, split_list={".":0, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Thepropertyoutlet_PySpider_united_kingdom", input_type="VALUE")
        if response.xpath("//text()[contains(.,'AVAILABLE NOW')]").get():
            ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="now", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//li[contains(.,'Washing Machine')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//text()[contains(.,'BEDROOM')]", input_type="F_XPATH", get_num=True, split_list={"BEDROOM":0, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking')]", input_type="F_XPATH", tf_item=True)

        if response.xpath("//text()[contains(.,'BATH') or contains(.,'SHOWER')]"): item_loader.add_value("bathroom_count", 1)
        if not item_loader.get_collected_values("room_count"): 
            if response.xpath("//text()[contains(.,'BEDROOM')]"): item_loader.add_value("room_count", 1)

        description = " ".join(response.xpath("//div[@class='descriptionText']//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

            if 'unfurnished' in description.lower():
                item_loader.add_value("furnished", False)
            elif 'furnished' in description.lower():
                item_loader.add_value("furnished", True)

        rent = response.xpath("//div[contains(@class,'FDPrice')]//h4/div[1]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split('£')[-1].strip().split(' ')[0].strip().replace(',', ''))
            item_loader.add_value("currency", 'GBP')
        
        images = [x for x in response.xpath("//div[@class='FDSlider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        item_loader.add_value("external_id", response.url.split("/")[-1])
        
        item_loader.add_value("landlord_name", 'The Property Outlet')
        item_loader.add_value("landlord_phone", '0117 935 45 65')
        item_loader.add_value("landlord_email", 'lettings@thepropertyoutlet.com')

        map_url = "https://www.thepropertyoutlet.com/Map-Property-Search-Results?references=" + response.url.split('/')[-1]
        yield Request(map_url, callback=self.get_latlng, meta={"item_loader":item_loader})
        
    def get_latlng(self, response):
        item_loader = response.meta["item_loader"]

        data = json.loads(response.body)
        latitude = str(data["items"][0]["lat"]) if data["items"][0]["lat"] else None
        longitude = str(data["items"][0]["lng"]) if data["items"][0]["lng"] else None
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
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("suite" in p_type_string.lower() or "room" in p_type_string.lower()):
        return "room"
    else:
        return None