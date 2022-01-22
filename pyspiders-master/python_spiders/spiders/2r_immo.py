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
    name = '2r_immo'
    execution_type = 'testing'
    country = 'denmark'
    locale ='fr'
    start_urls = ['https://2r-immo.com/resultats?transac=location']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        data = "".join(response.xpath("//script//text()[contains(.,'var properties')]").extract())
        if data:

            detail_url = []
            url = data.split("var properties = [")[1].split('"lien": "')
            for u in url:    
                jseb = re.sub("\s{2,}", " ", u.split('"prix":')[0].replace('",',""))
                detail_url.append(jseb)  
            
            for i in detail_url[1:]:
                follow_url = f"https://2r-immo.com/{i}"
                yield Request(follow_url, callback=self.populate_item)
                

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")
        property_type = ""
        desc = "".join(response.xpath("//title/text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return

        desc = "".join(response.xpath("//div[@class='col-sm-8']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Rlm_PySpider_denmark")
        item_loader.add_value("external_id", response.url.split("-")[-1].strip())
        
        rent = response.xpath("//ul/li//span[@class='label price']/text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace("\xa0","").replace(" ","").strip())
        else:
            rent = response.xpath("//div[@class='col-sm-8']//span[@class='text-primary-color']/text()").extract_first()
            if rent:
                item_loader.add_value("rent_string", rent.replace("\xa0","").replace(" ","").strip())


        square_meters = "".join(response.xpath("//ul[@class='list-unstyled amenities amenities-detail']/li[strong[contains(.,'Surface')]]/text()").extract())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())

        room_count = "".join(response.xpath("//ul[@class='list-unstyled amenities amenities-detail']/li/i[@class='icons icon-bedroom']/following-sibling::text()").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        else:
            room_count = "".join(response.xpath("//li[contains(.,'pi')]//following-sibling::strong/text()").extract())
            if room_count:
                item_loader.add_value("room_count", room_count.strip().split(" ")[0])


        bathroom_count = "".join(response.xpath("//ul[@class='list-unstyled amenities amenities-detail']/li/i[@class='icons icon-bathroom']/following-sibling::text()[contains(.,'bain')]").extract())
        if bathroom_count:
            item_loader.add_value("room_count", bathroom_count.strip().split(" ")[0])
        else:
            
            bathroom_count = "".join(response.xpath("//li[contains(.,'eau')]//following-sibling::strong/text()").extract())
            if bathroom_count:
                item_loader.add_value("room_count", bathroom_count.strip().split(" ")[0])

        address = "".join(response.xpath("//ul[@class='list-unstyled amenities amenities-detail']/li/address/text()").extract())
        if address:
            item_loader.add_value("address", address.strip().split(" ")[0])

        images = [response.urljoin(x) for x in response.xpath("//li[@class='img-container']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath("utilities", "//ul/li[contains(.,'Charges')]/strong/text()")
        item_loader.add_xpath("deposit", "//ul/li[contains(.,'garantie')]/strong/text()")
        item_loader.add_xpath("floor", "//ul/li[contains(.,'Etage')]/strong/text()")
        item_loader.add_xpath("energy_label", "substring-before(substring-after(//div[@class='col-sm-6']/img[@alt='DPE']/@src,'DPE_'),'.')")

        elevator = response.xpath("//ul/li[contains(.,'Ascenseur ')]/strong/text()").extract_first()
        if elevator:
            if "non" in elevator:
                item_loader.add_value("elevator",False)
            else:
                item_loader.add_value("elevator",True)

        balcony = response.xpath("//ul/li[contains(.,'Balcon')]/strong/text()").extract_first()
        if balcony:
            if "0" in balcony:
                item_loader.add_value("balcony",False)
            else:
                item_loader.add_value("balcony",True)
                
        parking = response.xpath("//ul/li[contains(.,'Parking')]/strong/text()").extract_first()
        if parking:
            if "0" in parking:
                item_loader.add_value("parking",False)
            else:
                item_loader.add_value("parking",True)

        item_loader.add_value("landlord_phone", "02 40 48 08 72")
        item_loader.add_value("landlord_name", "2R immo")

        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and (" hus " in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None