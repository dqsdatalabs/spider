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
    name = 'mediocasa_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Mediocasa_PySpider_italy"
    start_urls = ['https://www.mediocasa.it/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='homepreview']//div[@class='immobile']/a/text()[1]").extract():
            follow_url = f"https://www.mediocasa.it/agenzie/regione/{item}"
            yield Request(follow_url, callback=self.agence_item, meta={"property_type": response.meta.get('property_type')})

    def agence_item(self, response):
        for link in response.xpath("//div[@class='subaglinksbar']/a[@class='linktwo']/@href").extract():
            yield Request(response.urljoin(link), callback=self.jump)

    def jump(self,response):
        rent_url = response.url+"/affitto"
        yield Request(rent_url, callback=self.parse_listing)

    def parse_listing(self,response):
        for item in response.xpath("//div[@class='listitem']"):
            follow_url = item.xpath("./div[@class='imagebox']/a/@href[not(contains(.,'#'))]").extract_first()
            yield Request(response.urljoin(follow_url), callback=self.populate_item)

    def populate_item(self,response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//h2[@class='schedatitolo']/text()")

        prop_type = response.url
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        external_id = "".join(response.xpath("//h5[@class='riferimento']/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())
        adres=response.xpath("//h2[@class='schedatitolo']/text()").get() 
        if adres:
            item_loader.add_value("address",adres.split("affitto a")[-1])

        rent = response.xpath("//div[@class='prezzo_immobile_scheda']/text()").get()
        if rent:
            rent = rent.strip().replace(".","")
            if rent.isdigit():
                if int(rent) > 0:
                    item_loader.add_value("rent", rent)

        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath("//tr/td[contains(.,'Mq')]/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())

        room_count = response.xpath("//tr/td[contains(.,'Locali')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//tr/td[contains(.,'Bagni')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        floor = response.xpath("//tr/td[contains(.,'Piano')]/following-sibling::td/text()").get()
        if floor:
            floor = re.findall(r'\d+', floor)
            item_loader.add_value("floor", floor)

        uti = response.xpath("//tr/td[contains(.,'Spese')]/following-sibling::td/text()").get()
        if uti:
            utilities = re.findall(r'\d+', uti)
            if utilities:
                item_loader.add_value("utilities", utilities)    
        
        description = "".join(response.xpath("//div[@class='immobile_descrizione']/text()").getall())
        if description:
            item_loader.add_value("description",description)

        city = response.url
        if city:
            city = city.split('/')[6:7]
            item_loader.add_value("city", city)

        address = response.xpath("//span[@class='zona']/text()").get()
        if address:
            item_loader.add_value("address", address)

        energy_label = response.xpath("//li[contains(.,'energetica')]/b/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        parking = response.xpath("//ul/li/text()[contains(.,'box')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//ul/li/text()[contains(.,'auto')]").get()
            if parking:
                item_loader.add_value("parking", True)

        elevator = response.xpath("//ul/li/text()[contains(.,'ascensore')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        images = [x for x in response.xpath("//div[@class='gallery']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        landlord_name = response.xpath("//div[@class='right_block']/div[contains(.,'Affiliato')]/i[1]/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Medio Casa")
        landlord_phone = response.xpath("//div[@class='right_block']/div[contains(.,'Affiliato')]/text()[contains(.,'Tel.')]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.split('Tel.')[-1].strip())
        else:
            item_loader.add_value("landlord_phone", "06 45652997") 
        item_loader.add_value("landlord_email", "franchising@mediocasa.it")

        yield item_loader.load_item()
    

def get_p_type_string(p_type_string):
    if p_type_string and "studenti" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartamento" in p_type_string.lower() or "Attico" in p_type_string.lower() or "appartamento" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("affitto" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villa" in p_type_string.lower() or "indipendente" in p_type_string.lower()):
        return "house"
    elif p_type_string and "en-suite" in p_type_string.lower():
        return "room"
    else:
        return None

