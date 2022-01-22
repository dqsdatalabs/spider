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
    name = 'agenceoptimus_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://agence-optimus.fr/ventes/location/appartements/yonne-seine-et-marne/0/0/0/0/1", "property_type": "apartment"},
	        {"url": "https://agence-optimus.fr/ventes/location/maisons/yonne-seine-et-marne/0/0/0/0/1", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//div[@id='resultat']/div[@class='list']//p/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Agence_Optimus_PySpider_france")
        item_loader.add_xpath("title", "//h1/text()")


        external_id = "".join(response.xpath("//div[@class='content']//li[span[.='Référence']]/span[2]/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        utilities = "".join(response.xpath("//div[@class='content']//li[span[.='Charges']]/span[2]/text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.strip())

        rent = "".join(response.xpath("//span[@class='interesse']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.strip())

        energy_label = " ".join(response.xpath("//div[@id='DPE']/img[1]/@src[not(contains(.,'dpe_vierge'))]").getall()).strip()   
        if energy_label:
            label = response.xpath("substring-before(substring-after(//div[@id='DPE']/img[1]/@src[not(contains(.,'dpe_vierge'))],'='),'&')").extract_first()
            item_loader.add_value("energy_label", energy_label_calculate(label))

        images = [ response.urljoin(x) for x in response.xpath("//div[@class='allPhoto']/div/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        address = " ".join(response.xpath("//h1/text()").getall()).strip()   
        if address:
            address = address.split(" ")[-1].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())


        item_loader.add_xpath("square_meters","substring-before(//div[@class='content']//li[span[.='Surf habitable']]/span[2]/text(),'m²')")
        item_loader.add_xpath("room_count","//div[@class='content']//li[span[.='Nb Chambre(s)']]/span[2]/text()")


        description = " ".join(response.xpath("//div[@class='content']/h4//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        item_loader.add_value("landlord_phone", "03 86 66 44 95")
        item_loader.add_value("landlord_name", "Agence Optimus")
        item_loader.add_value("landlord_email", "agence.optimus89@gmail.com")
        

        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label