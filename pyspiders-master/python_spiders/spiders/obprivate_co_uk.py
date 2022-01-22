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
    name = 'obprivate_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.obprivate.co.uk/search.ljson?channel=lettings&fragment="]

    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        
        for item in data["properties"]:
            status = item["status"]
            if status and "to let" not in status.lower().strip():
                continue
            follow_url = response.urljoin(item["property_url"])
            latitude = item["lat"]
            longitude = item["lng"]
            yield Request(follow_url,callback=self.populate_item, meta={"latitude": latitude, "longitude": longitude})
        
        if data["pagination"]["has_next_page"]:
            page = data["pagination"]["current_page"] + 1
            p_url = f"https://www.obprivate.co.uk/search.ljson?channel=lettings&fragment=page-{page}"
            yield Request(p_url, callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        full_text = " ".join(response.xpath("//div[@id='full-description']//text()").getall())
        if get_p_type_string(full_text):
            item_loader.add_value("property_type", get_p_type_string(full_text))
        else:
            return

        item_loader.add_value("external_source", "Obprivate_Co_PySpider_united_kingdom")

        external_id = response.url.split('properties/')[-1].split('/')[0]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//div[@class='property_information__heading']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@id='full-description']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//div[@class='feature']/div[contains(text(),'Bedrooms') or contains(text(),'bedrooms')]/../div[@class='number']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div[@class='feature']/div[contains(text(),'Bathroom') or contains(text(),'bathroom')]/../div[@class='number']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//div[@class='price']/text()").get()
        if rent:
            rent = rent.split('£')[-1].lower().split('p')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent)) * 4))
            item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x.split('url(')[-1].split(')')[0].strip().strip("'")) for x in response.xpath("//div[@id='property_show__gallery']/div/@style").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//a[contains(@title,'Floorplan')]/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.meta.get("latitude")
        if latitude:
            item_loader.add_value("latitude", str(latitude))

        longitude = response.meta.get("longitude")
        if longitude:
            item_loader.add_value("longitude", str(longitude))
        
        energy_label = response.xpath("//text()[contains(.,'EPC Rating')]").get()
        if energy_label:
            energy_label = energy_label.split('EPC Rating')[1].split('=')[-1].strip().split(' ')[0].strip().strip('.')
            if energy_label in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label)
        
        floor = response.xpath("//div[@class='amenity' and contains(.,'Floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.lower().split('floor')[0].strip())

        balcony = response.xpath("//div[@class='amenity' and contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//div[@class='amenity' and contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[@class='amenity' and contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//div[@class='amenity' and contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        dishwasher = response.xpath("//div[@class='amenity' and contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        item_loader.add_value("landlord_name", "OLIVER BERNARD PRIVATE")
        item_loader.add_value("landlord_phone", "+447791328344")
        item_loader.add_value("landlord_email", "info@obprivate.co.uk")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
