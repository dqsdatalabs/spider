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
    name = 'portlandresidential_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.portlandresidential.co.uk/properties/?page=1&propind=L&orderBy=PriceSearchAmount&orderDirection=DESC&searchbymap=false&hideProps=1&searchType=list&sortBy=highestPrice"]

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='displayaddress']"):
            follow_url = response.urljoin(item.xpath("./h3/a/@href").get())
            room_count = item.xpath("./../div[@class='icons']/span[@class='beds']/text()").get()
            bathroom_count = item.xpath("./../div[@class='icons']/span[@class='bathrooms']/text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={"room_count": room_count, "bathroom_count": bathroom_count})
        
        next_page = response.xpath("//a[@id='next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        desc = "".join(response.xpath("//div[@class='description']//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Portlandresidential_Co_PySpider_united_kingdom")

        external_id = response.xpath("//div[@class='reference']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())

        address = response.xpath("//div[@class='address']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip().split(",")[1].strip())
            if "Terrace" in address:
                item_loader.add_value("terrace",True)
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='description']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.meta.get("room_count")
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.meta.get("bathroom_count")
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//div[@class='price']/span[@class='displayprice']/text()").get()
        term = response.xpath("//div[@class='price']/span[@class='displaypricequalifier']/text()").get()
        if rent and term:
            if 'pppw' in term.lower():
                rent = rent.split('£')[-1].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            elif 'pcm' in term.lower():
                rent = rent.split('£')[-1].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))
                item_loader.add_value("currency", 'GBP')
        
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='photocontainer']//div[@class='propertyimagelist']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='hiddenfloorplan']//div[@class='propertyimagelist']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//div[@id='maplinkwrap']/a/@href").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('lat=')[1].split('&')[0].strip())
            item_loader.add_value("longitude", latitude.split('lng=')[1].split('&')[0].strip())
        
        energy_label = response.xpath("//text()[contains(.,'EPC rating')]").get()
        if energy_label:
            energy_label = energy_label.split('EPC rating')[1].strip().split(' ')[0].strip().strip('.')
            if energy_label in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", "Portland Residential")
        item_loader.add_value("landlord_email", "contact@portlandresidential.co.uk")
        item_loader.add_value("landlord_phone", "0191 281 25 25")  

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
