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
    name = 'courtenay_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://www.courtenay.co.uk/search?category=1&listingtype=6&statusids=1,2']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//figure"):
            follow_url = response.urljoin(item.xpath("./a[contains(.,'Details')]/@href").get())
            status = item.xpath("./div[@class='status']/text()").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 1 or seen:
            url = f"https://www.courtenay.co.uk/search?category=1&listingtype=6&statusids=1,2&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Courtenay_Co_PySpider_united_kingdom")

        property_type = "".join(response.xpath("//div[contains(@class,'fdFeatures')]//li//text()").getall())
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: 
            property_type = "".join(response.xpath("//div[contains(@class,'fdDescription')]//text()").getall())
            if get_p_type_string(property_type): 
                # print(response.url)
                item_loader.add_value("property_type", get_p_type_string(property_type))
            else:
                return

        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = response.xpath("//div[contains(@class,'fdTitle')]//following-sibling::h2//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'fdTitle')]//following-sibling::h2//text()").get()
        if address:
            city = address.split(",")[-2]
            zipcode = address.split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = "".join(response.xpath("//div[contains(@class,'fdTitle')]//following-sibling::h3//div/text()").getall())
        if rent:
            if "PCM" in rent:
                rent = rent.split("£")[1].split(" ")[0].replace(",","")
                item_loader.add_value("rent", rent)
            else:
                rent = rent.split("£")[1].split(" ")[0].replace(",","")
                if rent.isdigit():
                    item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'fdDescription')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'fdFeatures')]//li[contains(.,'BEDROM') or contains(.,'BEDROOM')]//text()").get()
        if room_count:
            room_count = room_count.lower().split("bed")[0].strip()
            if room_count == "double":
                item_loader.add_value("room_count", "2")
            else:
                room_count = room_count.replace("double","").strip()
                if room_count.isdigit():
                    item_loader.add_value("room_count", room_count)
        bathroom_count = response.xpath("//div[contains(@class,'fdFeatures')]//li[contains(.,'BATHROOM')]//text()").get()
        if bathroom_count:
            if "BATHROOM WITH SHOWER" in bathroom_count:
                item_loader.add_value("bathroom_count", "1")
            else:
                bathroom_count = bathroom_count.lower().split("bath")[0].strip()
                if bathroom_count.isdigit():
                    item_loader.add_value("bathroom_count", bathroom_count)
        images = [x for x in response.xpath("//div[contains(@class,'gallery')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//i[contains(@class,'floorplan')]//parent::a//@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'fdFeatures')]//li[contains(.,'AVAILABLE')]//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                available_date = available_date.split("AVAILABLE")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        furnished = response.xpath("//div[contains(@class,'fdFeatures')]//li[contains(.,'FURNISHED')]//text()[not(contains(.,'UNFURNISHED'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//div[contains(@class,'fdFeatures')]//li[contains(.,' FLOOR')]//text()[not(contains(.,'FLOORING'))]").get()
        if floor:
            floor = floor.split("FLOOR")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor.strip())

        item_loader.add_value("landlord_name", "COURTENAY")
        item_loader.add_value("landlord_phone", "020 7228 9911")
        item_loader.add_value("landlord_email", "lettings@courtenay.co.uk")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and ("studio" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None