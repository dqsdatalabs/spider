# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear

class MySpider(Spider):
    name = 'coldwellbanker_fr' 
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Coldwellbanker_PySpider_france"
    def start_requests(self):
        url= "https://www.coldwellbanker.fr/proprietes?order_by=price_asc&#properties"
        yield Request(url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='properties_list col3']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)

        next_button = response.xpath("//ul[@class='pagination']/li/a[contains(.,'»')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        prop= response.xpath("//article//p[contains(.,'Type de mandat')]//following-sibling::strong/text()[contains(.,'Vente') or contains(.,'Programme')]").extract_first()
        if prop:return

        description = " ".join(response.xpath("//article//p[contains(.,'Type de bien')]//following-sibling::strong/text()").getall())
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description)) 

        meters = response.xpath("//div[@id='surface']/text()").extract_first()
        if meters:
            item_loader.add_value("square_meters",meters.split(" ")[0])
    
        rent = response.xpath("substring-before(//p[contains(.,'Prix')]/strong/text(),'/')").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace("CC","").replace(" ","").replace('\u202f', '').replace("\xa0","").strip())

        deposit = response.xpath("//p[contains(.,'Dépôt de garantie')]/strong/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("€","").replace(" ","").replace('\u202f', '').replace("\xa0","").strip())


        utilities = response.xpath("//p[contains(.,'Dépôt de garantie')]/strong/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.replace("€","").replace(" ","").replace('\u202f', '').replace("\xa0","").strip())


        images = [x for x in response.xpath("//div[@class='gallery_list']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        address = response.xpath("//title/text()").extract_first()
        if address:
            adr = address.split("-")[0].replace("Appartement","")
            item_loader.add_value("address",adr)

                


        
        item_loader.add_xpath("room_count","//div[@id='nb_rooms']/text()")
        item_loader.add_xpath("floor","//p[contains(.,'Étage')]/strong/text()")
        item_loader.add_xpath("title","//h1/text()")

        furnished = response.xpath("//div[@class='features_list']/p/text()[contains(.,'Meublé')]").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[@class='features_list']/p/text()[contains(.,'Ascenseur')]").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)


        item_loader.add_xpath("landlord_name", "normalize-space(//div[@class='wrap']//h3/text())")

        yield item_loader.load_item()





def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "maison" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None