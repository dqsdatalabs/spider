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
    name = 'rhwestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.rhwestates.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&page=1"
                
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//a[contains(@class,'hexButton')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen=True
        
        if page ==2 or seen:        
            f_url = response.url.replace(f"page={page-1}", f"page={page}")
            yield Request(f_url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = " ".join(response.xpath("//h2[contains(.,'Summary')]//parent::article/text()").getall())
        if get_p_type_string(property_type):
            property_type = get_p_type_string(property_type)
            item_loader.add_value("property_type", property_type)
        else: return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Rhwestates_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = " ".join(response.xpath("//h3//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h3//text()").get()
        if address:
            city = address.split(",")[-2].strip()
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//div[contains(@class,'fdPrice')]//div/text()").get()
        if rent:
            rent = rent.replace("£","").replace(",","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//h2[contains(.,'Summary')]//parent::article/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if property_type == "studio":
            item_loader.add_value("room_count", "1")
        else:
            room_count = response.xpath("//div[contains(@class,'fdRooms')]//span[contains(.,'bed')]//text()").get()
            if room_count:
                room_count = room_count.strip().split(" ")[0]
                item_loader.add_value("room_count", room_count)
                

        bathroom_count = response.xpath("//div[contains(@class,'fdRooms')]//span[contains(.,'bath')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'gallery')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//span[contains(.,'Floor Plan')]//parent::a/@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        parking = response.xpath("//ul[contains(@class,'keyFeat')]//li[contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//ul[contains(@class,'keyFeat')]//li[contains(.,'balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//ul[contains(@class,'keyFeat')]//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//ul[contains(@class,'keyFeat')]//li[contains(.,'Floor ')]//text()").get()
        if floor:
            floor = floor.split(" ")[0]
            item_loader.add_value("floor", floor)

        item_loader.add_value("landlord_name", "RHW ESTATES")
        item_loader.add_value("landlord_phone", "0207 431 7121")
        item_loader.add_value("landlord_email", "lettings@rhwestates.co.uk")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and ("studio" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    else:
        return None